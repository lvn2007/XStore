#!/usr/bin/env python3
"""Crash-replay run: XStore server alone on .141 with its data dir on ext4 over dm-log-writes; SYNC clients from node .134 log every
ack; a root loop drops a timestamp mark into the dm log every ~20 ms. After a graceful stop the log is replayed to K crash points; each
point gives states A (all writes before the mark), B (only the writes up to the last flush/FUA before it) and C0/C1 (B plus a random
half of the writes in flight); see replay.py. For every state: fsck, mount,
restart the server (WAL replay), and verify that every call acked at least MARGIN ms before the mark is present with the right values.
usage: logw_run.py <out> <name> <new|old> <rows list, e.g. 20,1000> <seconds> <K points>"""
import json, os, re, subprocess, sys, time
SSH=os.environ.get("XS_SSH","ssh -o BatchMode=yes").split()  # "<cmd> <host> '<remote command>'"; the Mystic jump-host wrapper in our setup
SERVER="172.16.5.141"; NODE="172.16.5.134"; PORT=9498; MNT="/mnt/xsfault"
W="/NVME_RAID-0/faultimg/logw"; CTL="sudo %s/logw_ctl.sh "%W; MARGIN=100
D="/NVME_RAID-0/ab_ndebug/build/"; BIN={"new":D+"XStore_pathlock","old":D+"XStore_spin"}
CB="cd ~/zclient_bundle && LD_LIBRARY_PATH=$PWD/lib "
OUT, NAME, B, ROWS, SECS, K = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5]), int(sys.argv[6])
def ssh(ip,cmd,timeout=1800): return subprocess.run(SSH+[ip,cmd],capture_output=True,text=True,timeout=timeout)
def log(l):
    with open(OUT,"a") as f: f.write(l+"\n")
def foreign(): return int(ssh(SERVER,"ps -eo comm | grep -E '^(xstore_|XStore)' | wc -l").stdout.strip() or "0")
def start_server():
    ssh(SERVER,"tmux kill-session -t zlogw 2>/dev/null; true")
    ssh(SERVER,"tmux new-session -d -s zlogw -c %s '%s --host 0.0.0.0 --port %d --threads 16 > %s/server.log 2>&1'"%(MNT,BIN[B],PORT,W))
    for _ in range(120):
        if "up" in ssh(SERVER,"(echo > /dev/tcp/127.0.0.1/%d) 2>/dev/null && echo up"%PORT).stdout: time.sleep(5); return True
        time.sleep(1)
    return False
def stop_server():
    ssh(SERVER,"tmux send-keys -t zlogw C-c")
    for _ in range(60):
        if foreign()==0: break
        time.sleep(1)
    how="graceful"
    if foreign()!=0:
        ssh(SERVER,"pkill -9 -f '[X]Store_.* --port %d'"%PORT); how="SIGKILL"; time.sleep(3)
    ssh(SERVER,"tmux kill-session -t zlogw 2>/dev/null; true"); return how
def main():
    open(OUT,"w").close()
    if foreign()!=0: log("ABORT not idle"); return
    clog="~/logw_%s.log"%NAME
    try:
        ssh(SERVER,CTL+"teardown"); r=ssh(SERVER,CTL+"setup"); log("setup: "+r.stdout.strip()[-40:]+r.stderr.strip()[-120:])
        if "setup-ok" not in r.stdout: return
        ssh(SERVER,"sudo rm -f %s/mark.pid %s/mark.pid.stop; sudo setsid nohup bash %s/mark_loop.sh %s/mark.pid >/dev/null 2>&1 < /dev/null &"%(W,W,W,W))
        if not start_server(): log("SERVER-FAIL"); return
        log("create: "+ssh(NODE,CB+"./xstore_zmq_insert_client create %s:%d ZB"%(SERVER,PORT)).stdout.strip()[:60])
        res=ssh(NODE,CB+"./xstore_zmq_fault_client run %s:%d ZB 8 %d %s %s SYNC"%(SERVER,PORT,SECS,clog,ROWS)); log("run: "+res.stdout.strip())
        ssh(SERVER,"touch %s/mark.pid.stop"%W); time.sleep(1)
        log("stop: "+stop_server()); r=ssh(SERVER,CTL+"finish"); log("finish: "+r.stdout.strip()[-40:])
        if "finish-ok" not in r.stdout: return
        log("logw: "+ssh(SERVER,"python3 %s/replay.py info %s/log.img"%(W,W)).stdout.strip())
        ms=[int(x) for x in ssh(NODE,"awk '$1==\"ACK\"{print $4}' %s | sort -n | sed -n '1p;$p'"%clog).stdout.split()]
        lo,hi=ms[0]+1500,ms[-1]-300
        targets=[lo+(hi-lo)*i//(K-1) for i in range(K)]
        rp=ssh(SERVER,"rm -rf %s/snaps; python3 %s/replay.py %s/base.img %s/log.img %s/snaps %s"%(W,W,W,W,W," ".join(map(str,targets))),timeout=7200)
        pts=[json.loads(l) for l in rp.stdout.splitlines() if l.startswith("{")]; log("replayed %d points, rc=%d %s"%(len(pts),rp.returncode,rp.stderr.strip()[-200:]))
        for p in pts:
            if "error" in p: log("point %s"%p); continue
            cutoff=p["mark_ms"]-MARGIN
            ssh(NODE,"awk -v c=%d 'NR==1||($1==\"ACK\"&&$4<=c)' %s > ~/logw_f.log"%(cutoff,clog))
            for st in (("A","B","C0","C1") if p["k"]==0 else ("B","C0","C1")):
                tag="%s p%d%s"%(NAME,p["k"],st)
                if not verify_state(tag+(" kept=%s"%p["kept"][int(st[1])] if st[0]=="C" else ""),p[st],"~/logw_f.log",p["mark_ms"],p["writes_"+("a" if st=="A" else "b")]): break
                if st!="A": ssh(SERVER,"rm -f %s"%p[st])
            ssh(SERVER,"rm -f %s"%p["A"]) if p["k"]!=0 else None
        # negative control: the earliest state, all acks (must show missing calls, else the check could not fail)
        verify_state(NAME+" NEGCTL(A0 vs all acks)",pts[0]["A"],clog,pts[0]["mark_ms"],pts[0]["writes_a"])
    finally:
        stop_server(); ssh(SERVER,"touch %s/mark.pid.stop"%W); r=ssh(SERVER,CTL+"teardown"); log("teardown: "+r.stdout.strip()[-30:])
        log("LOGW_DONE")
def verify_state(tag,img,aclog,mark_ms,writes):
    r=ssh(SERVER,CTL+"mountimg %s"%img); fs=re.findall(r"fsck_rc=\d+",r.stdout)
    if "mount-ok" not in r.stdout: log("%s MOUNT-FAIL fsck: %s"%(tag," | ".join(r.stdout.strip().splitlines()[-4:]))); ssh(SERVER,CTL+"umountimg"); return False
    ok=start_server()
    if ok:
        v=ssh(NODE,CB+"./xstore_zmq_fault_client verify %s:%d ZB %s"%(SERVER,PORT,aclog),timeout=3600)
        log("%s mark=%d writes=%d %s %s rc=%d"%(tag,mark_ms,writes,fs[0] if fs else "fsck=?",v.stdout.strip(),v.returncode))
    else:
        log("%s RESTART-FAIL %s"%(tag,ssh(SERVER,"tail -4 %s/server.log | cut -c1-200"%W).stdout.strip().replace("\n"," | ")))
    stop_server(); ssh(SERVER,CTL+"umountimg"); return True
main()
