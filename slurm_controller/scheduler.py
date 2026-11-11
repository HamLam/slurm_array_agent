import getpass
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import time
from .core import connect, get
from .runner import run


class Slurm:
    TERMINAL = {'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','BOOT_FAIL','PREEMPTED','DEADLINE'}

    def __init__(self,root):
        self.root = Path(root).resolve()

    def command(self,*argv):
        try:
            return subprocess.run(argv,check=True,text=True,stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE,timeout=30).stdout
        except (subprocess.SubprocessError,OSError) as exc:
            raise RuntimeError(str(exc)+' '+str(getattr(exc,'stderr',''))[:2000]) from exc

    def preflight(self):
        with connect(self.root) as db:
            c = get(db,'cluster')
        for key in ('account','partition','qos','verified_limits'):
            if not c.get(key):
                raise ValueError('Cluster profile requires '+key)
        if not c.get('execution_authorized',False):
            raise ValueError('Explicit cluster execution authorization is required')
        for key in ('account','partition','qos'):
            if not re.fullmatch(r'[A-Za-z0-9_.-]+',c[key]):
                raise ValueError('Invalid profile identifier')
        config = self.command('scontrol','show','config')
        with connect(self.root) as db:
            p = get(db,'policy')
        limit = re.search(r'MaxArraySize\s*=\s*(\d+)',config)
        if not limit or p['wave_tasks'] > int(limit.group(1)):
            raise ValueError('Array limit unobservable or exceeded')
        partitions = self.command('sinfo','-h','-o','%P')
        if c['partition'] not in [x.rstrip('*') for x in partitions.split()]:
            raise ValueError('Partition not observable')
        associations = self.command('sacctmgr','-nP','show','assoc','user='+getpass.getuser(),
                                    'format=Account,Partition,QOS')
        if not any(r.split('|')[0] == c['account'] and c['qos'] in r.split('|')[2].split(',')
                   and r.split('|')[1] in ('',c['partition'])
                   for r in associations.splitlines() if len(r.split('|'))>=3):
            raise ValueError('Account/partition/QoS association not verified')
        return dict(user=getpass.getuser(),config=config,associations=associations)

    def submit(self,token,count,throttle):
        self.preflight()
        with connect(self.root) as db:
            c,p = get(db,'cluster'),get(db,'policy')
        runner = Path(__file__).with_name('runner.py')
        script = self.root/(token+'.sh')
        script.write_text('#!/bin/bash\nset -euo pipefail\nexec '+
                          ' '.join(shlex.quote(x) for x in (sys.executable,str(runner),str(self.root),token))+
                          ' "${SLURM_ARRAY_TASK_ID}"\n')
        self.command('bash','-n',str(script))
        text = self.command('sbatch','--parsable','--export=NONE','--no-requeue',
                            '--job-name='+token,'--array=0-'+str(count-1)+'%'+str(throttle),
                            '--account='+c['account'],'--partition='+c['partition'],'--qos='+c['qos'],
                            '--cpus-per-task='+str(p['cpus']),'--mem='+str(p['memory_mb'])+'M',
                            '--time='+str(max(1,int((p['task_seconds']+59)//60))),
                            '--output='+str(self.root/'logs'/'%A_%a.slurm'),str(script)).strip()
        job = text.split(';')[0]
        if not job.isdigit():
            raise RuntimeError('Ambiguous sbatch response')
        return job

    def find(self,token,created):
        # Search both sources. An empty result is ambiguous, never safe to resubmit.
        queue = self.command('squeue','-h','-u',getpass.getuser(),'-o','%F|%j')
        accounting = self.command('sacct','-nP','-X','-u',getpass.getuser(),
                                  '-S',time.strftime('%Y-%m-%dT%H:%M:%S',time.localtime(created-60)),
                                  '-o','JobIDRaw,JobName%100')
        found = set()
        for line in (queue+'\n'+accounting).splitlines():
            cols = line.strip().split('|')
            if len(cols)>1 and cols[1] == token:
                job = cols[0].split('_')[0]
                if job.isdigit():
                    found.add(job)
        return sorted(found)

    def observe(self,job):
        # Queue queries support diagnosis; only sacct terminal records authorize recovery.
        try:
            self.command('squeue','-h','-r','-j',job,'-o','%i|%T')
        except RuntimeError as exc:
            if 'Invalid job id' not in str(exc):
                raise
        rows = self.command('sacct','-nP','-X','-j',job,
                            '-o','JobIDRaw,State,ExitCode,End')
        result = {}
        for line in rows.splitlines():
            c = line.strip().split('|')
            if len(c)<4 or not re.fullmatch(re.escape(job)+r'_\d+',c[0]):
                continue
            state = c[1].split()[0].rstrip('+')
            try:
                end = time.mktime(time.strptime(c[3],'%Y-%m-%dT%H:%M:%S'))
            except ValueError:
                end = time.time()
            if state == 'COMPLETED' and c[2] != '0:0':
                state = 'FAILED'
            result[c[0].split('_')[1]] = dict(state=state,age=max(0,time.time()-end))
        return result

    def throttle(self,job,value):
        if value < 1:
            raise ValueError('Throttle zero means unlimited')
        self.command('scontrol','update','JobId='+job,'ArrayTaskThrottle='+str(value))

    def cancel(self,job):
        self.command('scancel',job)


class FakeSlurm(Slurm):
    """Persistent bounded simulation; executes local argv, never invokes Slurm."""
    def __init__(self,root):
        super().__init__(root)
        self.path = self.root/'fake-slurm.json'
        self.failures = {}  # {element_index: transient state}, once per scheduler instance
        self.lose_response = False
        self.outage = False

    def load(self):
        return json.loads(self.path.read_text()) if self.path.exists() else {}

    def save(self,jobs):
        tmp = self.path.with_suffix('.partial')
        tmp.write_text(json.dumps(jobs))
        os.replace(tmp,self.path)

    def preflight(self):
        return {'simulated':True}

    def submit(self,token,count,throttle):
        if self.outage:
            raise RuntimeError('Simulated scheduler outage')
        jobs = self.load()
        job = str(len(jobs)+1000)
        jobs[job] = dict(token=token,count=count,throttle=throttle,results={})
        self.save(jobs)
        if self.lose_response:
            self.lose_response = False
            raise RuntimeError('Simulated lost submission response')
        return job

    def find(self,token,created):
        if self.outage:
            raise RuntimeError('Simulated scheduler outage')
        return [j for j,v in self.load().items() if v['token']==token]

    def observe(self,job):
        if self.outage:
            raise RuntimeError('Simulated scheduler outage')
        jobs = self.load()
        v = jobs[job]
        count = 0
        for idx in range(v['count']):
            if str(idx) in v['results']:
                continue
            if count >= v['throttle']:
                break
            count += 1
            state = self.failures.pop(idx,None)
            if not state:
                code = run(self.root,v['token'],idx)
                # A valid scheduler completion can still have invalid/missing app output.
                state = 'COMPLETED'  # Output validator catches simulated incomplete results.
            v['results'][str(idx)] = dict(state=state,age=1000000)
        self.save(jobs)
        return v['results']

    def throttle(self,job,value):
        if value < 1:
            raise ValueError('Invalid throttle')
        jobs = self.load(); jobs[job]['throttle']=value; self.save(jobs)

    def cancel(self,job):
        jobs = self.load()
        for idx in range(jobs[job]['count']):
            jobs[job]['results'].setdefault(str(idx),dict(state='CANCELLED',age=1000000))
        self.save(jobs)
