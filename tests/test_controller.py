import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from slurm_controller.core import prepare,connect,get,put,tick,status,control,validate,adapt,lock
from slurm_controller.scheduler import FakeSlurm,Slurm
from slurm_controller.reasoning import diagnose,FakeBackend,CodexBackend


class CampaignTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.inputs = self.base/'input'; self.inputs.mkdir()
        for i in range(6):
            (self.inputs/str(i)).write_text('data '+str(i))
        self.root = self.base/'campaign'
        self.policy = dict(batch_size=2,wave_tasks=2,poll_seconds=1,retry_seconds=1,accounting_grace=1,cooldown=2)
        self.command = [sys.executable,'-c','import shutil,sys; shutil.copyfile(sys.argv[1],sys.argv[2])','{input}','{output}']
        self.make()

    def tearDown(self):
        self.tmp.cleanup()

    def make(self,policy=None,command=None):
        prepare(self.root,self.inputs,self.base/'output',command or self.command,dict(self.policy,**(policy or {})))
        control(self.root,'start')
        self.s = FakeSlurm(self.root)
        self.now = time.time()

    def step(self):
        self.now += 100
        tick(self.root,self.s,self.now)

    def finish(self,n=30):
        for _ in range(n):
            self.step()
            if status(self.root)['state'] in ('complete','held'):
                break

    def test_end_to_end(self):
        self.finish()
        self.assertEqual(status(self.root)['units'],{'done':6})
        self.assertTrue(validate(self.root))
        self.assertEqual(status(self.root)['submissions'],2)

    def test_transient_retry_and_shared_limits(self):
        self.s.failures={0:'NODE_FAIL'}
        self.finish()
        self.assertEqual(status(self.root)['units'],{'done':6})
        with connect(self.root) as db:
            self.assertEqual(db.execute('SELECT max(attempts) FROM units').fetchone()[0],2)
            self.assertLessEqual(db.execute("SELECT count(*) FROM waves WHERE state='active'").fetchone()[0],1)

    def test_lost_submission_restart(self):
        self.s.lose_response=True
        self.step()
        reserved=status(self.root)['reserved_cpu_seconds']
        self.s=FakeSlurm(self.root)
        self.step()
        self.assertEqual(len(self.s.load()),1)
        self.assertEqual(status(self.root)['reserved_cpu_seconds'],reserved)
        self.finish()
        self.assertEqual(status(self.root)['units'],{'done':6})

    def test_no_blind_resubmit(self):
        self.s.outage=True
        self.step()
        self.s.outage=False
        self.finish(4)
        self.assertEqual(len(self.s.load()),0)
        self.assertEqual(status(self.root)['submissions'],1)

    def test_incomplete_output_retry(self):
        self.step(); self.step()
        with connect(self.root) as db:
            row=db.execute("SELECT * FROM units WHERE state='done' LIMIT 1").fetchone()
            self.assertIsNotNone(row)
        # Test missing result on terminal success in another campaign wave.
        original=self.s.observe
        def incomplete(job):
            result=original(job)
            with connect(self.root) as db:
                row=db.execute("SELECT * FROM units WHERE state='active' LIMIT 1").fetchone()
                if row:
                    (self.base/'output'/(row['identity']+'.out')).unlink(missing_ok=True)
            self.s.observe=original
            return result
        self.s.observe=incomplete
        self.finish()
        self.assertEqual(status(self.root)['units'],{'done':6})
        self.assertGreater(status(self.root)['submissions'],2)

    def test_cancel_and_stop(self):
        self.step(); control(self.root,'stop'); self.step()
        self.assertEqual(status(self.root)['units'].get('cancelled'),4)
        self.assertEqual(status(self.root)['submissions'],1)
        with self.assertRaises(ValueError): control(self.root,'resume')

    def test_accounting_absence_is_not_failure(self):
        self.step()
        self.s.observe=lambda job:{}
        self.finish(4)
        self.assertEqual(status(self.root)['units']['active'],4)
        self.assertEqual(status(self.root)['submissions'],1)

    def test_service_outage_backoff(self):
        self.step(); self.s.outage=True; self.step()
        with connect(self.root) as db:
            deadline=get(db,'next_poll'); outages=get(db,'outages')
        tick(self.root,self.s,deadline-0.1)
        with connect(self.root) as db: self.assertEqual(get(db,'outages'),outages)

    def test_budget_exhaustion(self):
        with connect(self.root) as db:
            p=get(db,'policy'); p['max_submissions']=1; put(db,'policy',p)
        self.finish()
        self.assertEqual(status(self.root)['state'],'held')
        self.assertEqual(status(self.root)['submissions'],1)

    def test_circuit_breaker(self):
        with connect(self.root) as db:
            p=get(db,'policy'); p['failure_threshold']=2; put(db,'policy',p)
        self.s.failures={0:'FAILED',1:'FAILED'}
        self.step(); self.step()
        self.assertEqual(status(self.root)['state'],'paused')

    def test_pause(self):
        control(self.root,'pause'); self.finish(2)
        self.assertEqual(status(self.root)['submissions'],0)
        control(self.root,'resume'); self.finish()
        self.assertEqual(status(self.root)['state'],'complete')

    def test_lock_and_manifest_immutability(self):
        with lock(self.root):
            with self.assertRaises(RuntimeError):
                with lock(self.root): pass
        with connect(self.root) as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("UPDATE units SET path='changed'")

    def test_adaptive_increase_decrease(self):
        with connect(self.root) as db:
            now=time.time(); put(db,'last_adjust',now-10)
            db.execute("UPDATE units SET state='done' WHERE id=0")
            adapt(db,now); self.assertEqual(get(db,'throttle'),3)
            adapt(db,now+10,True); self.assertEqual(get(db,'throttle'),2)

    def test_model_unavailable_and_policy_validation(self):
        class Down:
            def analyze(self,*a): raise RuntimeError('offline')
        self.assertEqual(diagnose(self.root,'outage',{},Down())['action'],'hold')
        diagnose(self.root,'outage',{},Down())
        self.assertEqual(status(self.root)['model_calls'],1)
        class Unsafe:
            def analyze(self,*a): return dict(action='submit',category='unknown',explanation='bad')
        self.assertEqual(diagnose(self.root,'unsafe',{},Unsafe())['action'],'hold')
        self.finish(); self.assertEqual(status(self.root)['state'],'complete')

    def test_attempt_ceiling(self):
        with connect(self.root) as db:
            p=get(db,'policy'); p['max_attempts']=1; put(db,'policy',p)
        self.s.failures={0:'NODE_FAIL'}; self.finish()
        self.assertEqual(status(self.root)['state'],'held')
        self.assertEqual(status(self.root)['units']['held'],2)


if __name__=='__main__': unittest.main()

class AdapterTests(unittest.TestCase):
    setUp = CampaignTests.setUp
    tearDown = CampaignTests.tearDown
    make = CampaignTests.make
    def test_slurm_parser_accounting_and_missing_queue(self):
        adapter=Slurm(self.root)
        def command(*argv):
            if argv[0]=='squeue': raise RuntimeError('Invalid job id specified')
            return '42_0|CANCELLED by 123|0:15|2020-01-01T00:00:00|\n42_1|COMPLETED|1:0|2020-01-01T00:00:00|\n42.batch|COMPLETED|0:0|2020-01-01T00:00:00|'
        adapter.command=command
        states=adapter.observe('42')
        self.assertEqual(states['0']['state'],'CANCELLED')
        self.assertEqual(states['1']['state'],'FAILED')
        self.assertEqual(len(states),2)

    def test_reconciliation_deduplicates_parent(self):
        adapter=Slurm(self.root)
        adapter.command=lambda *a:'42|sc-token|\n42_0|sc-token|\n99|other|'
        self.assertEqual(adapter.find('sc-token',time.time()),['42'])

    def test_zero_throttle_rejected(self):
        with self.assertRaises(ValueError): Slurm(self.root).throttle('42',0)

    def test_content_validation_rejects_same_size_corruption(self):
        from slurm_controller.runner import valid
        a=self.base/'a'; b=self.base/'b'; a.write_text('abc'); b.write_text('abd')
        self.assertFalse(valid(a,3,b))

    def test_reasoning_schema_and_budget(self):
        self.assertEqual(diagnose(self.root,'good',{},FakeBackend())['action'],'hold')
        with connect(self.root) as db:
            p=get(db,'policy'); p['model_calls']=1; put(db,'policy',p)
        self.assertIn('budget',diagnose(self.root,'second',{},FakeBackend())['explanation'])

    def test_live_preflight_denies_unconfigured_cluster(self):
        with self.assertRaises(ValueError): Slurm(self.root).preflight()
