"""Entry for sealed-input Native HDF5 process-pool scale diagnostics."""
import importlib.util
from pathlib import Path

path = Path(__file__).resolve().parents[1]/'cross_version/work/native-process-scale-20261026/run_probe.py'
spec = importlib.util.spec_from_file_location('native_process_scale_stage', path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

if __name__ == '__main__':
    module.main()
