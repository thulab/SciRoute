#!/usr/bin/env python3
"""Non-root multi-source discovery permission oracle on an isolated mount."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import struct
import subprocess
import sys
import time
import uuid

import h5py
import numpy as np
import geos_pilot as pilot


CHILD = r'''
import json,socket,struct,sys
j=json.load(sys.stdin)
body=json.dumps(j['request'],separators=(',',':')).encode()
with socket.socket(socket.AF_UNIX) as s:
 s.settimeout(300);s.connect(j['socket']);s.sendall(struct.pack('>I',len(body))+body)
 def receive(n):
  out=bytearray()
  while len(out)<n:
   part=s.recv(n-len(out))
   if not part: raise RuntimeError('short response')
   out.extend(part)
  return bytes(out)
 size,=struct.unpack('>I',receive(4))
 if not 0<size<=1048576:raise RuntimeError('invalid frame size')
 result=json.loads(receive(size))
print(json.dumps(result))
'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", type=Path, required=True)
    parser.add_argument("--profile-config", type=Path, required=True)
    parser.add_argument("--source-pid", type=int, default=1001546)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    pilot.check(sys.platform == "linux" and os.geteuid() == 0, "requires root on Linux")
    args.output.mkdir(parents=True, exist_ok=False)
    fixture = args.output / "fixture.h5"
    with h5py.File(fixture, "w") as f:
        data = f.create_dataset("/array", data=np.arange(48,dtype=np.float32).reshape(6,8),
                                chunks=(3,4), compression="gzip", shuffle=True)
        data.attrs["standard_name"] = np.bytes_("permission_probe")
    env = pilot.connection_environment(args.source_pid, "h09probe1790939889dbfc1f")
    env["IOTDB_CONFIG_FILE"] = str(args.profile_config)
    env["IOTDBFS_NAMESPACE_LOCK"] = str(args.output / "namespace.lock")
    socket_path = Path(f"/tmp/cdisc-permission-{os.getpid()}.sock")
    env["IOTDBFS_QUERY_SOCKET"] = str(socket_path)
    env["IOTDBFS_QUERY_SOCKET_MODE"] = "0666"
    env["IOTDBFS_QUERY_PROFILE_JSONL"] = str(args.output / "profile.jsonl")
    mount = args.output / "mount"
    mount.mkdir()
    prefix = "/discovery-permission-" + uuid.uuid4().hex
    parents = [mount / prefix[1:], mount / prefix[1:] / "upper", mount / prefix[1:] / "upper/leaf"]
    sources = [parents[-1] / "a.h5", parents[-1] / "b.h5"]
    report = {"protocol": "iotdbfs-discovery-permission-smoke/1", "ok": False,
              "checks": {}, "prefix": prefix, "daemon_sha256": hashlib.sha256(args.daemon.read_bytes()).hexdigest()}
    with (args.output / "mount.log").open("wb") as log:
        daemon = subprocess.Popen([str(args.daemon), "-f", "-o", "allow_other,attr_timeout=0,entry_timeout=0", str(mount)],
                                  env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic()+90
            while not (os.path.ismount(mount) and socket_path.exists()):
                pilot.check(daemon.poll() is None and time.monotonic()<deadline,"mount failed; see mount.log")
                time.sleep(.1)
            for parent in parents:
                parent.mkdir(mode=0o755); os.chmod(parent,0o755)
            for source in sources:
                shutil.copyfile(fixture, source)
                os.chown(source,0,40077); os.chmod(source,0o640)
                pilot.check(os.getxattr(source,"user.hdf5.layout_version")==b"5","fixture not indexed")
            paths=["/"+str(p.relative_to(mount)) for p in sources]
            def query(label, groups, denied=False, scope="exact", no_match=False):
                selector = ([{"kind":"exact","path":p} for p in paths] if scope=="exact"
                            else [{"kind":"subtree","path":prefix}])
                request={"kind":"dataset_catalog","request":{"sources":{"paths":selector},
                          "dataset_path":"/absent" if no_match else "/array"}}
                def drop_identity():
                    os.setgroups(groups);os.setgid(65534);os.setuid(65534)
                child=subprocess.run([sys.executable,"-c",CHILD],input=json.dumps({"socket":str(socket_path),"request":request}),
                                     text=True,capture_output=True,timeout=330,cwd="/",preexec_fn=drop_identity)
                pilot.check(child.returncode==0,"permission child failed: "+child.stderr[-1000:])
                result=json.loads(child.stdout)
                report["checks"][label]={"denied":denied,"groups":groups,"response":result}
                if denied:
                    pilot.check(not result.get("ok") and result.get("code")==2,"expected PermissionDenied")
                else:
                    pilot.check(result.get("ok") and len(result["datasets"])==(0 if no_match else 2),"allowed discovery failed")
            query("supplementary_group",[40077])
            query("duplicate_and_primary_group",[65534,40077,40077])
            query("no_group_denied",[],True)
            query("subtree_parent_fallback",[40077],scope="subtree")
            query("authorized_no_match",[40077],no_match=True)
            os.chmod(sources[1],0o600)
            query("one_unreadable_source_denies_all",[40077],True)
            query("unreadable_source_not_hidden_by_dataset_filter",[40077],True,no_match=True)
            os.chmod(sources[1],0o640)
            query("chmod_restore",[40077])
            os.chmod(parents[-1],0o700)
            query("direct_parent_denied",[40077],True)
            os.chmod(parents[-1],0o755)
            os.chmod(parents[0],0o700)
            query("upper_parent_denied",[40077],True)
            os.chmod(parents[0],0o755)
            for source in sources:
                os.chown(source,65534,0);os.chmod(source,0o600)
            query("owner_allowed",[])
            sources[1].unlink()
            response,_=pilot.control(socket_path,{"kind":"dataset_catalog","request":{"sources":{"paths":[{"kind":"subtree","path":prefix}]},"dataset_path":"/array"}})
            pilot.check(len(response["datasets"])==1,"removed source still discovered")
            report["checks"]["unlink"]={"remaining":1,"path":response["datasets"][0]["path"]}
            profiles=[json.loads(line) for line in (args.output/"profile.jsonl").read_text().splitlines()]
            pilot.check(len(profiles)==len(report["checks"]),"missing permission profiles")
            for (label,check),profile in zip(report["checks"].items(),profiles):
                check["counters"]=profile["counters"]
                check["iotdb_delta"]=profile["iotdb_delta"]
                if label in ("supplementary_group","duplicate_and_primary_group","authorized_no_match","chmod_restore","owner_allowed"):
                    pilot.check(profile["counters"].get("authorization_source_catalog_reuse",0)==2,"did not reuse both permissions")
                    pilot.check(profile["counters"].get("authorization_source_metadata_calls",0)==0,"unnecessary source reread")
                    pilot.check(profile["counters"].get("authorization_parent_metadata_calls",0)==0,"unnecessary parent reread")
                if label=="subtree_parent_fallback":
                    pilot.check(profile["counters"].get("authorization_source_catalog_reuse",0)==2,"subtree source hints lost")
                    pilot.check(profile["counters"].get("authorization_parent_metadata_calls",0)==1,"subtree parent bulk fallback missing")
            report["ok"]=True
        finally:
            for source in sources:
                if source.exists():source.unlink()
            for parent in reversed(parents):
                if parent.exists():os.chmod(parent,0o755);parent.rmdir()
            if os.path.ismount(mount):subprocess.run(["fusermount","-u",str(mount)],check=True,timeout=30)
            elif daemon.poll() is None:daemon.terminate()
            daemon.wait(timeout=30)
            (args.output/"report.json").write_text(json.dumps(report,indent=2,sort_keys=True)+"\n")
    print(json.dumps({"ok":True,"checks":list(report["checks"]),"report":str(args.output/"report.json")}))


if __name__=="__main__":
    main()
