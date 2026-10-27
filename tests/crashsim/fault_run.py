#!/usr/bin/env python3
"""I/O-error injection run: server alone on .141 with its data dir on loop -> dm -> ext4; 8 SYNC client threads from node .134 log
every ack; at +20 s the device is switched to the dm 'error' target (mode perm: stays; mode trans: healthy again after 2 s; mode none:
control). Then: stop the server (SIGKILL if it hangs), fsck, remount, restart (WAL replay), and verify that every acked call is present.
usage: fault_run.py <out> <name>:<head|new|old>:<mount opts or ->:<none|perm|trans>[:<rows per call>[:<SYNC|ASYNC>]] ...
rows per call defaults to 1000 (direct path); 20 stays on the WAL ring even under ring pressure. ASYNC: missing calls are only reported."""
import os, re, subprocess, sys, time
SSH=os.environ.get("XS_SSH","ssh -o BatchMode=yes").split()  # "<cmd> <host> '<remote command>'"; the Mystic jump-host wrapper in our setup
SERVER="172.16.5.141"; NODE="172.16.5.134"; PORT=9498; MNT="/mnt/xsfault"
D="/NVME_RAID-0/ab_ndebug/build/"; BIN={"head":D+"XStore_fix8","new":D+"XStore_pathlock","old":D+"XStore_spin"}
CTL="sudo /NVME_RAID-0/faultimg/fault_ctl.sh "; CB="cd ~/zclient_bundle && LD_LIBRARY_PATH=$PWD/lib "
OUT=sys.argv[1]
def ssh(ip,cmd,timeout=900): return subprocess.run(SSH+[ip,cmd],capture_output=True,text=True,timeout=timeout)
def log(l):
    with open(OUT,"a") as f: f.write(l+"\n")
def foreign(): return int(ssh(SERVER,"ps -eo comm | grep -E '^(xstore_|XStore)' | wc -l").stdout.strip() or "0")
def start_server(b):
    ssh(SERVER,"tmux kill-session -t zfault 2>/dev/null; true")
    ssh(SERVER,"tmux new-session -d -s zfault -c %s '%s --host 0.0.0.0 --port %d --threads 16 > /NVME_RAID-0/faultimg/server.log 2>&1'"%(MNT,BIN[b],PORT))
    for _ in range(90):
        if "up" in ssh(SERVER,"(echo > /dev/tcp/127.0.0.1/%d) 2>/dev/null && echo up"%PORT).stdout: time.sleep(5); return True
        time.sleep(1)
    return False
def stop_server():
    ssh(SERVER,"tmux send-keys -t zfault C-c")
    for _ in range(40):
        if foreign()==0: break
        time.sleep(1)
    how="graceful"
    if foreign()!=0:
        ssh(SERVER,"pkill -9 -f '[X]Store_.* --port %d'"%PORT); how="SIGKILL"; time.sleep(3)
    ssh(SERVER,"tmux kill-session -t zfault 2>/dev/null; true"); return how
def scenario(spec):
    name,b,opts,mode,*extra=spec.split(":"); opts="" if opts=="-" else opts
    rows=extra[0] if extra else "1000"; dur=extra[1] if len(extra)>1 else "SYNC"
    if foreign()!=0: log("%s SKIPPED-not-idle"%name); return
    try:
        r=ssh(SERVER,CTL+"setup '%s'"%opts); log("%s setup: %s"%(name,r.stdout.strip()[-40:]+r.stderr.strip()[-120:]))
        if "setup-ok" not in r.stdout: return
        if not start_server(b): log("%s SERVER-FAIL"%name); return
        c=ssh(NODE,CB+"./xstore_zmq_insert_client create %s:%d ZB"%(SERVER,PORT)); log("%s create: %s"%(name,c.stdout.strip()[:60]))
        import concurrent.futures as cf
        with cf.ThreadPoolExecutor(2) as ex:
            run=ex.submit(ssh,NODE,CB+"./xstore_zmq_fault_client run %s:%d ZB 8 45 ~/fault_%s.log %s %s"%(SERVER,PORT,name,rows,dur))
            time.sleep(20); t_fault=0
            if mode!="none":
                t_fault=int(ssh(SERVER,CTL+"error").stdout.strip().split()[-1]); log("%s fault injected at %d (%s)"%(name,t_fault,mode))
                if mode=="trans":
                    time.sleep(2); ssh(SERVER,CTL+"linear"); log("%s device healthy again"%name)
            res=run.result(); log("%s %s"%(name,res.stdout.strip()))
        sl=ssh(SERVER,"grep -ciE 'poison|critical' /NVME_RAID-0/faultimg/server.log; grep -iE 'poison|critical|overloaded|read-only|failed' /NVME_RAID-0/faultimg/server.log | cut -c1-170 | sort | uniq -c | sort -rn | head -6").stdout
        log("%s server log: %s"%(name," | ".join(sl.strip().splitlines())))
        log("%s stop: %s"%(name,stop_server()))
        ssh(SERVER,CTL+"linear")
        st=ssh(SERVER,"mount | grep xsfault | cut -c1-120; sudo dmesg | grep -iE 'EXT4-fs|xsfault|dm-[0-9]' | tail -6 | cut -c1-170").stdout
        log("%s state before fsck: %s"%(name," | ".join(st.strip().splitlines())))
        f=ssh(SERVER,CTL+"fsck '%s'"%opts); log("%s fsck: %s"%(name," | ".join(f.stdout.strip().splitlines()[-12:])))
        if not start_server(b):
            d=ssh(SERVER,"mount | grep xsfault | cut -c1-120; tail -6 /NVME_RAID-0/faultimg/server.log | cut -c1-200; ls -la /mnt/xsfault | head -8").stdout
            log("%s RESTART-FAIL: %s"%(name," | ".join(d.strip().splitlines()))); return
        v=ssh(NODE,CB+"./xstore_zmq_fault_client verify %s:%d ZB ~/fault_%s.log %d"%(SERVER,PORT,name,t_fault),timeout=3600)
        log("%s %s%s"%(name,v.stdout.strip(),(" rc=%d"%v.returncode)))
    finally:
        stop_server(); r=ssh(SERVER,CTL+"teardown"); log("%s teardown: %s"%(name,r.stdout.strip()[-30:]))
open(OUT,"w").close()
for spec in sys.argv[2:]: scenario(spec)
log("FAULT_DONE")
