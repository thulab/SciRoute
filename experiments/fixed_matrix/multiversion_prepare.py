"""Frozen 4.4 prepare entry; stage owns implementation and receipts."""
import importlib.util
from pathlib import Path
p=Path(__file__).resolve().parents[1]/'cross_version/work/multiversion-reclaim-20261027/prepare.py'
s=importlib.util.spec_from_file_location('multiversion_prepare_stage',p)
module=importlib.util.module_from_spec(s);s.loader.exec_module(module)
if __name__=='__main__':module.main()
