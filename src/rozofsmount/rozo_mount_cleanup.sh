#!/bin/bash
#set -x
syntax() {
  "usage : ${base} <full path>"
  exit 1
}

#
# Get rozofsmount PID
#
get_pid() {
  PID=`ps -o pid,cmd -C rozofsmount | awk '{if ($3==MNT) print $1;}' MNT=${mnt}`
}


base=`basename $0`
case "$1" in
  "") syntax;;
esac

mnt=$1

# Unmount
umount ${mnt} 2> /dev/null
sleep 0.6
umount -l ${mnt}  2> /dev/null
sleep 0.6


get_pid
# No PID => done
case "$PID" in
  "") exit 0;;
esac  

# Kill rozofsmount
kill ${PID}
sleep 0.4

get_pid
# No PID => done
case "$PID" in
  "") exit 0;;
esac  

# Kill rozofsmount
kill -9 ${PID}
