#!/bin/bash
# root: a timestamp mark into the dm-log-writes log every ~20 ms until the stop file appears. pid in $1.
echo $$ > "$1"
while [ ! -e "$1.stop" ]; do dmsetup message xslog 0 "mark t$(date +%s%3N)" >/dev/null 2>&1; sleep 0.02; done
