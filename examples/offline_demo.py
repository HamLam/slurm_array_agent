"""Run from repository root: python -m examples.offline_demo."""
import json
from pathlib import Path
import sys
import tempfile
import time
from slurm_controller.core import prepare,control,tick,status,validate,connect
from slurm_controller.scheduler import FakeSlurm
from slurm_controller.reasoning import diagnose


def main():
    with tempfile.TemporaryDirectory(prefix='slurm-offline-') as directory:
        root=Path(directory); inputs=root/'inputs'; inputs.mkdir()
        for i in range(24): (inputs/str(i)).write_text('sample '+str(i)+'\n')
        campaign=root/'campaign'
        command=[sys.executable,'-c','import shutil,sys;shutil.copyfile(sys.argv[1],sys.argv[2])','{input}','{output}']
        prepare(campaign,inputs,root/'outputs',command,
                dict(batch_size=3,wave_tasks=4,poll_seconds=1,retry_seconds=1,accounting_grace=1,cooldown=2))
        control(campaign,'start')
        scheduler=FakeSlurm(campaign); scheduler.lose_response=True
        now=time.time()
        tick(campaign,scheduler,now+10)
        # Restart the adapter after a lost response. Durable identity is reconciled.
        scheduler=FakeSlurm(campaign); scheduler.failures={0:'NODE_FAIL'}
        class Down:
            def analyze(self,*args): raise RuntimeError('Model unavailable')
        diagnose(campaign,'demo-model-down',{},Down())
        for i in range(1,51):
            tick(campaign,scheduler,now+10+i*100)
            if status(campaign)['state'] in ('complete','held'): break
        validate(campaign)
        report=dict(simulated=True,steps=i,status=status(campaign),
                    caveat='Temporary local outputs removed after demonstration; no Slurm commands invoked')
        with connect(campaign) as db:
            report['audit_counts']=dict(db.execute('SELECT kind,count(*) FROM audit GROUP BY kind'))
            report['max_attempts']=db.execute('SELECT max(attempts) FROM units').fetchone()[0]
        assert report['status']['units']=={'done':24},report
        Path('artifacts/offline-demo.json').write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps(report,indent=2))

if __name__=='__main__': main()
