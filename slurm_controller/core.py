import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import time
import uuid

DEFAULTS = dict(batch_size=100, wave_tasks=100, concurrency=2, max_concurrency=8,
                max_attempts=3, max_submissions=100, max_cpu_seconds=360000,
                cpus=1, memory_mb=1024, task_seconds=600, duration_seconds=86400,
                poll_seconds=30, retry_seconds=60, accounting_grace=300,
                failure_threshold=5, min_free_bytes=1048576, cooldown=120,
                model_calls=10, model_seconds=300, model_timeout=30)


def connect(root):
    db = sqlite3.connect(Path(root) / 'campaign.sqlite', timeout=30)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA foreign_keys=ON')
    db.execute('PRAGMA synchronous=FULL')
    db.execute('PRAGMA cache_size=-8192')
    return db


def audit(db, kind, detail):
    db.execute('INSERT INTO audit(at,kind,detail) VALUES(?,?,?)',
               (time.time(), kind, json.dumps(detail)))


def get(db, key):
    return json.loads(db.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()[0])


def put(db, key, value):
    db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)', (key, json.dumps(value)))


@contextlib.contextmanager
def lock(root):
    with open(Path(root) / 'controller.lock', 'a') as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('Another controller owns this campaign')
        yield


def prepare(root, inputs, output, command, policy=None, mode='fake', cluster=None):
    root, inputs, output = map(lambda p: Path(p).resolve(), (root, inputs, output))
    root.mkdir(parents=True, exist_ok=True)
    if (root / 'campaign.sqlite').exists():
        raise ValueError('Campaign already exists; manifest cannot be replaced')
    if not inputs.is_dir() or not command or not all(isinstance(x, str) for x in command):
        raise ValueError('Require input directory and argv command')
    if inputs == output or inputs in output.parents or output in inputs.parents:
        raise ValueError('Input and output trees must be disjoint')
    if root == inputs or inputs in root.parents:
        raise ValueError('Campaign directory must be outside input tree')
    p = dict(DEFAULTS, **(policy or {}))
    if set(p) != set(DEFAULTS) or any(type(v) not in (int, float) or v <= 0 for v in p.values()):
        raise ValueError('Unknown policy key or nonpositive limit')
    if p['concurrency'] > p['max_concurrency'] or mode not in ('fake', 'slurm'):
        raise ValueError('Invalid concurrency or scheduler')
    if p['batch_size'] > 10000 or p['wave_tasks'] > 10000:
        raise ValueError('Batch and wave sizes must not exceed 10000')
    for k in ('batch_size','wave_tasks','concurrency','max_concurrency','max_attempts','max_submissions','cpus','memory_mb','failure_threshold','model_calls'):
        if type(p[k]) is not int:
            raise ValueError(k + ' must be integer')
    output.mkdir(parents=True, exist_ok=True)
    (root / 'receipts').mkdir()
    (root / 'logs').mkdir()
    db = connect(root)
    db.executescript('''
    CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
    CREATE TABLE units(id INTEGER PRIMARY KEY,identity TEXT UNIQUE NOT NULL,path TEXT NOT NULL,
      size INTEGER NOT NULL,mtime INTEGER NOT NULL,task INTEGER NOT NULL,
      state TEXT NOT NULL DEFAULT 'ready',attempts INTEGER NOT NULL DEFAULT 0);
    CREATE INDEX unit_task ON units(task,state);
    CREATE INDEX unit_state ON units(state,task);
    CREATE TABLE waves(token TEXT PRIMARY KEY,job TEXT,state TEXT NOT NULL,created REAL NOT NULL,throttle INTEGER NOT NULL);
    CREATE TABLE assignments(token TEXT,idx INTEGER,task INTEGER,PRIMARY KEY(token,idx));
    CREATE TABLE audit(id INTEGER PRIMARY KEY,at REAL,kind TEXT,detail TEXT);
    CREATE TABLE incidents(id TEXT PRIMARY KEY,state TEXT,response TEXT);
    ''')
    with db:
        for key, value in dict(policy=p, command=command, output=str(output), inputs=str(inputs),
                               mode=mode, cluster=cluster or {}, state='prepared', created=time.time(),
                               reserved_cpu=0, next_poll=0, outages=0, failures=0,
                               next_retry=0, throttle=p['concurrency'], last_adjust=0,
                               last_rate=0, last_done=0, model_calls=0, model_seconds=0).items():
            put(db,key,value)
        n = 0
        # scandir recursion is depth bounded; no directory-wide file list is built.
        def walk(directory):
            with os.scandir(directory) as entries:
                for ent in entries:
                    if ent.is_dir(follow_symlinks=False):
                        yield from walk(ent.path)
                    elif ent.is_file(follow_symlinks=False):
                        yield ent
        digest = hashlib.sha256()
        for ent in walk(inputs):
            st = ent.stat(follow_symlinks=False)
            identity = hashlib.sha256(str(Path(ent.path).relative_to(inputs)).encode()).hexdigest()
            db.execute('INSERT INTO units(id,identity,path,size,mtime,task) VALUES(?,?,?,?,?,?)',
                       (n,identity,ent.path,st.st_size,st.st_mtime_ns,n//p['batch_size']))
            digest.update((identity+'\n').encode())
            n += 1
        put(db,'manifest_digest',digest.hexdigest())
        put(db,'total',n)
        db.executescript("""CREATE TRIGGER immutable_units BEFORE UPDATE OF id,identity,path,size,mtime,task ON units BEGIN SELECT RAISE(ABORT,'immutable manifest'); END;
        CREATE TRIGGER no_delete BEFORE DELETE ON units BEGIN SELECT RAISE(ABORT,'immutable manifest'); END;""")
        audit(db,'prepared',dict(units=n,python=os.sys.version,command=command))
    db.close()
    return n


def status(root):
    with connect(root) as db:
        return dict(state=get(db,'state'), total=get(db,'total'),
                    units=dict(db.execute('SELECT state,count(*) FROM units GROUP BY state')),
                    submissions=db.execute('SELECT count(*) FROM waves').fetchone()[0],
                    reserved_cpu_seconds=get(db,'reserved_cpu'), throttle=get(db,'throttle'),
                    output=get(db,'output'), model_calls=get(db,'model_calls'))


def validate(root):
    from .runner import valid
    with connect(root) as db:
        p = get(db,'policy')
        if shutil.disk_usage(get(db,'output')).free < p['min_free_bytes']:
            raise ValueError('Storage reserve violated')
        if db.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            raise ValueError('Database integrity failure')
        if get(db,'mode') == 'slurm':
            from .scheduler import Slurm
            Slurm(root).preflight()
        # Bounded iteration; finished results are checked again before reporting validation.
        for row in db.execute("SELECT * FROM units WHERE state='done'"):
            if not valid(Path(get(db,'output')) / (row['identity']+'.out'), row['size'], row['path']):
                raise ValueError('Published result no longer validates')
        audit(db,'validated',{})
    return True


def control(root, action):
    with lock(root), connect(root) as db:
        state = get(db,'state')
        if action in ('start','resume'):
            if state in ('stopped','complete'):
                raise ValueError('Stopped/completed campaigns cannot resume')
            if action == 'start' and state != 'prepared':
                raise ValueError('Use resume for an existing campaign')
            put(db,'state','running')
        elif action == 'pause':
            put(db,'state','paused')
        elif action == 'stop':
            put(db,'state','stopped')
        audit(db,'control',action)


def adapt(db, now, pressure=False):
    p = get(db,'policy')
    elapsed = now-get(db,'last_adjust')
    if elapsed < p['cooldown']:
        return
    done = db.execute("SELECT count(*) FROM units WHERE state='done'").fetchone()[0]
    rate = (done-get(db,'last_done'))/max(elapsed,1)
    old = get(db,'throttle')
    bad = pressure or get(db,'failures') > 0 or (get(db,'last_rate') > 0 and rate < get(db,'last_rate')*.8)
    new = max(1,old-1) if bad else min(p['max_concurrency'],old+int(rate>0))
    put(db,'throttle',new)
    put(db,'last_adjust',now)
    put(db,'last_rate',rate)
    put(db,'last_done',done)
    audit(db,'throughput',dict(validated_per_hour=rate*3600,old=old,new=new,pressure=pressure))


def tick(root, scheduler, now=None):
    from .runner import valid
    now = time.time() if now is None else now
    with lock(root), connect(root) as db:
        p = get(db,'policy')
        if now < get(db,'next_poll'):
            return
        put(db,'next_poll',now+p['poll_seconds'])
        db.commit()
        # Only one outstanding wave: original and retry arrays share the same budget.
        active = db.execute("SELECT * FROM waves WHERE state IN ('intent','active') LIMIT 1").fetchone()
        try:
            if active:
                token = active['token']
                job = active['job']
                if job is None:
                    matches = scheduler.find(token,active['created'])
                    if len(matches) != 1:
                        audit(db,'ambiguous_submission',dict(token=token,matches=matches))
                        return  # Even zero matches is NOT permission to resubmit.
                    job = matches[0]
                    db.execute("UPDATE waves SET job=?,state='active' WHERE token=?",(job,token))
                if get(db,'state') == 'stopped':
                    scheduler.cancel(job)
                observations = scheduler.observe(job)
                audit(db,'observation',dict(job=job,states=observations))
                unfinished = False
                failures = 0
                for assignment in db.execute('SELECT * FROM assignments WHERE token=?',(token,)).fetchall():
                    idx, task = assignment['idx'], assignment['task']
                    if not db.execute("SELECT 1 FROM units WHERE task=? AND state='active'",(task,)).fetchone():
                        continue
                    evidence = observations.get(str(idx))
                    if not evidence or evidence['state'] not in scheduler.TERMINAL:
                        unfinished = True
                        continue
                    state = evidence['state']
                    if evidence.get('age',0) < p['accounting_grace']:
                        unfinished = True
                        continue
                    rows = db.execute("SELECT * FROM units WHERE task=? AND state='active'",(task,)).fetchall()
                    receipt = Path(root)/'receipts'/token/(str(idx)+'.jsonl')
                    published_ids = set()
                    if receipt.exists():
                        if receipt.stat().st_size > p['batch_size']*4096:
                            raise ValueError('Oversized compute receipt')
                        with receipt.open() as records:
                            for record in records:
                                r = json.loads(record)
                                if r.get('ok') is True:
                                    published_ids.add(r.get('id'))
                    for row in rows:
                        output = Path(get(db,'output'))/(row['identity']+'.out')
                        published = row['id'] in published_ids
                        if published and valid(output,row['size'],row['path']):
                            result = 'done'
                        elif state == 'CANCELLED' or get(db,'state') == 'stopped':
                            result = 'cancelled'
                        elif state in ('NODE_FAIL','BOOT_FAIL','PREEMPTED','COMPLETED') and row['attempts'] < p['max_attempts']:
                            result = 'ready'
                            failures += 1
                        else:
                            result = 'held'
                            failures += 1
                        db.execute('UPDATE units SET state=? WHERE id=?',(result,row['id']))
                    audit(db,'terminal',dict(task=task,state=state))
                if not unfinished:
                    db.execute("UPDATE waves SET state='terminal' WHERE token=?",(token,))
                    put(db,'next_retry',now+p['retry_seconds'])
                put(db,'failures',failures)
                if failures >= p['failure_threshold']:
                    put(db,'state','paused')
                    audit(db,'circuit_breaker',dict(failures=failures))
                pressure = shutil.disk_usage(get(db,'output')).free < p['min_free_bytes']
                adapt(db,now,pressure)
                if unfinished and get(db,'throttle') != active['throttle']:
                    scheduler.throttle(job,get(db,'throttle'))
                    db.execute('UPDATE waves SET throttle=? WHERE token=?',(get(db,'throttle'),token))
                put(db,'outages',0)
                return
            if get(db,'state') != 'running':
                return
            if now-get(db,'created') > p['duration_seconds']:
                put(db,'state','held'); audit(db,'duration_exhausted',{}); return
            if now < get(db,'next_retry'):
                return
            tasks = [r[0] for r in db.execute("SELECT task FROM units WHERE state='ready' GROUP BY task LIMIT ?",(p['wave_tasks'],))]
            if not tasks:
                unresolved = db.execute("SELECT 1 FROM units WHERE state!='done' LIMIT 1").fetchone()
                put(db,'state','held' if unresolved else 'complete')
                audit(db,'finished',dict(unresolved=bool(unresolved)))
                return
            cost = len(tasks)*p['cpus']*(int((p['task_seconds']+59)//60)*60)
            if (db.execute('SELECT count(*) FROM waves').fetchone()[0] >= p['max_submissions'] or
                    get(db,'reserved_cpu')+cost > p['max_cpu_seconds']):
                put(db,'state','held'); audit(db,'budget_exhausted',{}); return
            if shutil.disk_usage(get(db,'output')).free < p['min_free_bytes']:
                put(db,'state','paused'); audit(db,'storage_pressure',{}); return
            token = 'sc-'+uuid.uuid4().hex
            throttle = get(db,'throttle')
            db.execute("INSERT INTO waves VALUES(?,NULL,'intent',?,?)",(token,now,throttle))
            for idx,task in enumerate(tasks):
                db.execute('INSERT INTO assignments VALUES(?,?,?)',(token,idx,task))
                db.execute("UPDATE units SET state='active',attempts=attempts+1 WHERE task=? AND state='ready'",(task,))
            put(db,'reserved_cpu',get(db,'reserved_cpu')+cost)
            audit(db,'submission_intent',dict(token=token,tasks=len(tasks),reserved_cpu=cost))
            db.commit() # Crash-safe boundary BEFORE scheduler side effect.
            job = scheduler.submit(token,len(tasks),throttle)
            db.execute("UPDATE waves SET job=?,state='active' WHERE token=?",(job,token))
            audit(db,'submitted',dict(token=token,job=job))
        except (OSError, RuntimeError, ValueError) as exc:
            outages = get(db,'outages')+1
            put(db,'outages',outages)
            put(db,'next_poll',now+min(3600,p['poll_seconds']*2**min(outages,7)))
            audit(db,'service_error',dict(error=str(exc)[:2000],backoff_until=get(db,'next_poll')))
