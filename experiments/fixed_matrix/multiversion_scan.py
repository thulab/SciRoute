"""Native-only FS-scan supplement for the sealed V1/V8/V32 matrix."""
import importlib.util
from pathlib import Path
p=Path(__file__).resolve().parents[1]/'cross_version/work/multiversion-scan-20261027/collect.py'
s=importlib.util.spec_from_file_location('multiversion_scan_stage',p)
module=importlib.util.module_from_spec(s);s.loader.exec_module(module)
if __name__=='__main__':module.main()
