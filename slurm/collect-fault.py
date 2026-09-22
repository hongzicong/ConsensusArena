"""On Jed: preserve raw evidence in home and package portable result artifacts."""
import json
import os
from pathlib import Path
import sys
import tarfile

def main(job):
    source=Path('/scratch')/os.environ['USER']/('consensusarena-fault-'+job)
    output=Path.home()/'ConsensusArena/fault-results'/job
    output.mkdir(parents=True,exist_ok=True)
    # Source and binary are frozen separately; keep all experiment data/logs.
    archive=output/'raw.tar.gz'
    if not archive.exists():
        temp=output/'raw.tar.gz.tmp'
        with tarfile.open(temp,'w:gz',compresslevel=1) as tar:
            for p in source.rglob('*'):
                if p.is_file() and 'binary' not in p.relative_to(source).parts:
                    tar.add(p,arcname=p.relative_to(source).as_posix())
        temp.replace(archive)
    portable=output/'results.tar.gz'
    with tarfile.open(portable,'w:gz') as tar:
        for p in source.rglob('*'):
            if not p.is_file(): continue
            rel=p.relative_to(source)
            if rel.parts[0] in ('binary','ranks','logical'): continue
            if 'stdout' in rel.parts or 'logs' in rel.parts: continue
            if 'status' in rel.parts and p.name not in ('crash-target.json','start-unix-ns'): continue
            if p.suffix in ('.csv','.json','.jsonl','.conf','.txt') or p.name=='start-unix-ns': tar.add(p,arcname=rel.as_posix())
    print(json.dumps(dict(job=job,portable=str(portable),raw=str(archive))))

if __name__=='__main__': main(sys.argv[1])
