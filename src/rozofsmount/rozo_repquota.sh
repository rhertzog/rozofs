#!/bin/bash

#  Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
#  This file is part of Rozofs.
#  Rozofs is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published
#  by the Free Software Foundation, version 2.
#  Rozofs is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#  General Public License for more details.

#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see
#  <http://www.gnu.org/licenses/>.

#
# setup.sh will generates a full working rozofs locally
# it's a useful tool for testing and debugging purposes. 
#
#set -x 
local=/tmp/rozo_rep_quota.$$
status=1

where=`pwd`
res=`attr -qg rozofs.export .`
hosts=`echo $res | awk '{print $1}'`
case "$hosts" in
  "") echo "$where is not a rozofs mount point !"; exit $status;;
esac
eid=`echo $res | awk '{print $2}'`
id=`id -u`


for host in `echo $hosts | awk -F'/' '{print $1" "$2" "$3" "$4" "$5" "$6" "$7" "$8}'`
do

  rozodiag -i $host -T export:0 -c export_conf_path > $local
  success=`grep "\[exportd-M\]" $local`
  case "$success" in
    "") continue;;
    *)  path=`grep -v "\[exportd-M\]" $local` 
  esac
  
  case "$path" in
    "") continue;;
  esac 
   
  rozodiag -i $host -T export:0 -c system "rozo_repquota -n -i $EUID -f $path $eid" | grep -v "\[exportd-M\]"
  status=0
  break
done  

rm -f $local
exit $status
