"""Derive four ablation input roots from the unchanged KCensus n7 suite."""
from pathlib import Path
import hashlib
import json
import shutil
import sys

HERE=Path(__file__).resolve().parent
SOURCE=HERE.parent/'kcensus-suite'
ARMS=('full','no-cold','no-hot','neither')
sys.path.insert(0,str(HERE.parent))
from topology import generated_layout

def generate():
    manifest=json.loads((SOURCE/'manifest.json').read_text())
    for arm in ARMS:
        root=HERE/arm
        if root.exists():
            raise FileExistsError('refusing to overwrite '+str(root))
        root.mkdir()
        cloned=dict(manifest,name=manifest['name']+'-agreement-'+arm,agreement_ablation=arm)
        (root/'manifest.json').write_text(json.dumps(cloned,indent=2)+'\n')
        shutil.copytree(SOURCE/'sources',root/'sources')
        for name in manifest['topologies']:
            source=SOURCE/name;target=root/name;target.mkdir()
            for filename in ('latency.conf','workload.conf','case.json'):
                shutil.copy2(source/filename,target/filename)
            config=(target/'workload.conf').read_text(encoding='utf-8')
            (target/'workload.conf').write_text(config.rstrip()+'\nagreementAblation: '+arm+'\n',encoding='utf-8',newline='\n')
            metadata=json.loads((target/'case.json').read_text())
            metadata['agreement_ablation']=arm
            metadata['ablation_base_case_sha256']=generated_layout(source)['case_sha256']
            (target/'case.json').write_text(json.dumps(metadata,indent=2)+'\n')
            result=generated_layout(target)
            assert result['replica_count']==7 and len(result['clients'])==7
            assert (target/'latency.conf').read_bytes()==(source/'latency.conf').read_bytes()
    print('Generated four input roots. launch.ps1 selects 26 active A/B scenes (8+2+8+8).')

if __name__=='__main__':generate()
