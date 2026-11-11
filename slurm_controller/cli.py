import argparse
import json
import signal
import time
from pathlib import Path
from .core import prepare,validate,status,control,tick,connect,get
from .scheduler import FakeSlurm,Slurm


def main():
    parser = argparse.ArgumentParser(description='Persistent single-agent Slurm campaign controller')
    parser.add_argument('operation',choices=['prepare','validate','start','status','pause','resume','stop','run','diagnose'])
    parser.add_argument('campaign')
    parser.add_argument('--profile',help='JSON preparation profile')
    parser.add_argument('--once',action='store_true')
    parser.add_argument('--authorize-live',action='store_true')
    args = parser.parse_args()
    if args.operation == 'prepare':
        if not args.profile:
            parser.error('prepare requires --profile')
        config = json.loads(Path(args.profile).read_text())
        print(prepare(args.campaign,**config))
        return
    with connect(args.campaign) as db:
        mode = get(db,'mode')
        interval = get(db,'policy')['poll_seconds']
    scheduler = FakeSlurm(args.campaign) if mode == 'fake' else Slurm(args.campaign)
    if args.operation == 'validate':
        print(validate(args.campaign)); return
    if args.operation == 'status':
        print(json.dumps(status(args.campaign),indent=2)); return
    if args.operation == 'diagnose':
        from .reasoning import diagnose,CodexBackend
        print(diagnose(args.campaign,'manual',{'status':status(args.campaign)},CodexBackend())); return
    if args.operation in ('start','resume'):
        if mode == 'slurm' and not args.authorize_live:
            parser.error('Live start/resume requires --authorize-live and authorized cluster profile')
        validate(args.campaign)
        control(args.campaign,args.operation)
        print(json.dumps(status(args.campaign))); return
    if args.operation in ('pause','stop'):
        control(args.campaign,args.operation); return
    done = False
    def shutdown(*_):
        nonlocal done
        done = True
    signal.signal(signal.SIGTERM,shutdown)
    signal.signal(signal.SIGINT,shutdown)
    # Lifetime lock prevents interleaved daemon loops. Short campaign lock permits controls.
    from .core import lock
    import fcntl
    with open(Path(args.campaign)/'daemon.lock','a') as daemon:
        fcntl.flock(daemon,fcntl.LOCK_EX|fcntl.LOCK_NB)
        while not done:
            tick(args.campaign,scheduler)
            if args.once or status(args.campaign)['state'] in ('complete','held'):
                break
            for _ in range(max(1,int(interval))):
                if done:
                    break
                time.sleep(1)
