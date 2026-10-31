"""Fresh diagnostic entry; existing fixed-matrix protocols remain untouched."""
import importlib.util
from pathlib import Path

path = Path(__file__).resolve().parents[1]/'cross_version/work/four-task-diagnosis-20261025/run_diagnosis.py'
spec = importlib.util.spec_from_file_location('iotdbfs_four_task_diagnosis',path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

if __name__ == '__main__':
    module.main()
