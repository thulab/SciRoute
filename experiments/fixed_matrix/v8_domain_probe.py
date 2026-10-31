"""Entry for sealed V8 candidate-domain sharing diagnostics."""
import importlib.util
from pathlib import Path

path = Path(__file__).resolve().parents[1] / 'cross_version/work/v8-domain-20261027/run_probe.py'
spec = importlib.util.spec_from_file_location('v8_domain_stage', path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

if __name__ == '__main__':
    module.main()
