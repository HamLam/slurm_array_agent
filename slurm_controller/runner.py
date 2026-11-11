"""Compute-side runner: read-only manifest, per-element atomic receipts; no DB writes."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time


def valid(path, expected_size, source=None):
    # The bundled application profile is a byte-preserving transformation.
    # Other applications must supply their own validator before deployment.
    if not path.is_file() or path.stat().st_size != expected_size:
        return False
    if source is None:
        return True
    def digest(file):
        h = hashlib.sha256()
        with open(file,'rb') as stream:
            for chunk in iter(lambda:stream.read(1024*1024),b''):
                h.update(chunk)
        return h.digest()
    return digest(path) == digest(source)


def run(root, token, idx):
    root = Path(root).resolve()
    db = sqlite3.connect('file:'+str(root/'campaign.sqlite')+'?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    meta = {r['key']: json.loads(r['value']) for r in db.execute('SELECT * FROM meta')}
    task = db.execute('SELECT task FROM assignments WHERE token=? AND idx=?',(token,idx)).fetchone()[0]
    receipt_dir = root/'receipts'/token
    receipt_dir.mkdir(parents=True,exist_ok=True)
    receipt = receipt_dir/(str(idx)+'.jsonl')
    partial = receipt.with_suffix('.partial')
    failed = False
    # Explicit allowlist prevents forwarding controller model credentials.
    env = {k:os.environ[k] for k in ('PATH','LANG','LC_ALL') if k in os.environ}
    with partial.open('w') as records:
        for row in db.execute("SELECT * FROM units WHERE task=? AND state='active'",(task,)):
            final = Path(meta['output'])/(row['identity']+'.out')
            tmp = final.with_name(final.name+'.'+token+'.partial')
            ok, error = False, ''
            try:
                source = Path(row['path'])
                st = source.stat()
                if st.st_size != row['size'] or st.st_mtime_ns != row['mtime']:
                    raise ValueError('Input changed after preparation')
                argv = [a.replace('{input}',str(source)).replace('{output}',str(tmp)) for a in meta['command']]
                with (root/'logs'/(token+'-'+str(idx)+'.log')).open('ab') as log:
                    result = subprocess.run(argv,env=env,stdout=log,stderr=log,
                                            timeout=meta['policy']['task_seconds'],check=False)
                if result.returncode != 0 or not valid(tmp,row['size'],source):
                    raise ValueError('Application failed or incomplete output')
                with tmp.open('rb') as published:
                    os.fsync(published.fileno())
                os.replace(tmp,final)
                ok = True
            except (OSError,ValueError,subprocess.TimeoutExpired) as exc:
                failed = True
                error = str(exc)[:1000]
            records.write(json.dumps(dict(id=row['id'],ok=ok,error=error,at=time.time()))+'\n')
        records.flush()
        os.fsync(records.fileno())
    os.replace(partial,receipt)
    db.close()
    return int(failed)


if __name__ == '__main__':
    sys.exit(run(sys.argv[1],sys.argv[2],int(sys.argv[3])))
