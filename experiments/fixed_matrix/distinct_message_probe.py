"""Run the isolated single-client, four-distinct-message experiment."""
import importlib.util
from pathlib import Path

path=Path(__file__).resolve().parents[1]/'cross_version/work/distinct-message-probe-20261025/run_probe.py'
spec=importlib.util.spec_from_file_location('distinct_message_probe',path)
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

if __name__=='__main__':module.main()
