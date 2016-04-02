#!/bin/bash
syntax() {
  echo "usage : $UTIL source <source site #>"
  echo "  This command will restart geo replication from scratch"
  echo "  from <source site #> toward the other site."
  exit 1
}
UTIL=`basename $0`

case "$1" in
  "source");;
  *) syntax;;
esac

case "$2" in
  0|1);;
  *) syntax;;
esac
  
for slave in {1..8}
do   
  rozodiag -T export:$slave -c geoReplicationRestartFromScratch $2
done  
