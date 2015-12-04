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
where=`pwd`
res=`attr -qg rozofs.export .`
ip=`echo $res | awk '{print $1}'`
case "$ip" in
  "") echo "$where is not a rozofs mount point !"; exit;;
esac
eid=`echo $res | awk '{print $2}'`
id=`id -u`


path=`rozodiag -i $ip -T export:0 -c export_conf_path | grep -v "\[exportd-M\]"`

rozodiag -i $ip -T export:0 -c system "rozo_repquota -n -i $EUID -f $path $eid" | grep -v "\[exportd-M\]"
