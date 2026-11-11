"""Synthetic indexed million-entry manifest benchmark, not filesystem throughput."""
import hashlib
import json
from pathlib import Path
import resource
import sqlite3
import tempfile
import time


def main():
    start=time.monotonic()
    with tempfile.TemporaryDirectory(prefix='slurm-million-') as directory:
        db=sqlite3.connect(str(Path(directory)/'manifest.sqlite'))
        db.execute('PRAGMA cache_size=-8192')
        db.execute('CREATE TABLE units(id INTEGER PRIMARY KEY,identity TEXT UNIQUE,path TEXT,size INTEGER,mtime INTEGER,task INTEGER,state TEXT,attempts INTEGER)')
        db.execute('CREATE INDEX unit_task ON units(task,state)')
        db.execute('CREATE INDEX unit_state ON units(state,task)')
        for offset in range(0,1000000,1000):
            db.executemany('INSERT INTO units VALUES(?,?,?,?,?,?,?,?)',
                ((i,hashlib.sha256(str(i).encode()).hexdigest(),'/synthetic/'+str(i),1,0,i//100,'ready',0)
                 for i in range(offset,offset+1000)))
            db.commit()
        insert_seconds=time.monotonic()-start
        queries=0
        qstart=time.monotonic()
        for task in range(0,10000,10):
            rows=db.execute('SELECT id FROM units WHERE task=? AND state=?',(task,'ready')).fetchall()
            assert len(rows)==100
            queries+=1
        plan=[list(r) for r in db.execute("EXPLAIN QUERY PLAN SELECT id FROM units WHERE task=9999 AND state='ready'")]
        report=dict(synthetic=True,entries=db.execute('SELECT count(*) FROM units').fetchone()[0],
                    insertion_seconds=insert_seconds,query_seconds=time.monotonic()-qstart,
                    indexed_queries=queries,query_plan=plan,
                    peak_rss_kib=int(next(line.split()[1] for line in Path('/proc/self/status').read_text().splitlines() if line.startswith('VmHWM:'))),
                    inherited_ru_maxrss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                    memory_method='Linux /proc/self/status VmHWM; getrusage includes launcher high-water mark on this host',
                    database_bytes=(Path(directory)/'manifest.sqlite').stat().st_size,
                    batch_entries=1000,python=__import__('sys').version,
                    limitation='Synthetic database only; no filesystem discovery, Slurm, or scientific throughput measured')
        Path('artifacts/million-manifest.json').write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps(report,indent=2))

if __name__=='__main__': main()
