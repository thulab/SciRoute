"""Check owner payload coalescing configuration without connections or mounts."""
import argparse
import os
from pathlib import Path
import subprocess
import tempfile


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bin',required=True,type=Path)
    binary=parser.parse_args().bin.resolve(strict=True)
    cases=[('payload_read_coalescing',v,'owner',False) for v in ('','0','1','TRUE','yes','falsex')]
    cases += [('payload_read_coalescing',v,'owner',True) for v in (None,'true','false')]
    cases += [('payload_read_coalescing',v,'strict',False) for v in ('true','false')]
    cases += [('payload_cache_bytes',v,'owner',False) for v in ('','-1','8388609','1.5','true','18446744073709551616')]
    cases += [('payload_cache_bytes',v,'owner',True) for v in ('0','1','8388608')]
    cases += [('payload_cache_bytes',v,'strict',False) for v in ('0','8388608')]
    with tempfile.TemporaryDirectory(prefix='iotdbfs-payload-coalesce-config-') as directory:
        root=Path(directory)
        env={k:v for k,v in os.environ.items() if not k.startswith('IOTDB_')}
        env['IOTDB_CONFIG_FILE']=str(root/'config.properties')
        for key,value,mode,valid in cases:
            lines=['iotdb.host=127.0.0.1','iotdb.port=1','iotdb.db=config_validation',
                f'log.dir={root / "logs"}',f'storage.version_directory.mode={mode}']
            if mode=='owner':lines+=['storage.version_directory.table_prefix=vd_config_validation',
                f'storage.version_directory.owner_directory={root / "owner"}']
            if value is not None:lines.append(f'storage.version_directory.{key}={value}')
            (root/'config.properties').write_text('\n'.join(lines)+'\n')
            result=subprocess.run([str(binary),'grep'],env=env,cwd=root,capture_output=True,text=True,timeout=10)
            expected='requires the owner socket' if valid else 'Invalid configuration'
            if result.returncode!=2 or expected not in result.stdout+result.stderr:
                raise AssertionError(f'payload startup parsing failed: {key}/{mode}/{value!r}')
    print(f'{len(cases)} startup configuration cases passed; no connections or mounts')


if __name__=='__main__':main()
