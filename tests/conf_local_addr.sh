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
# set_local_addr.sh will set local IP address for test a full working
# rozofs locally, it's a useful tool for testing and debugging purposes. 
#

usage ()
{
    echo >&2 "Usage:"
    echo >&2 "$0 set <nb_ip_to_set> <interface_to_use>"
    echo >&2 "$0 unset <nb_ip_to_del> <interface>"
    exit 0;
}

main (){

    [ $# -lt 3 ] && usage

    NB_IP_TO_SET=$2
    INTERFACE_TO_USE=$3
    IP_BASE=192.168
    NETMASK=24

    if [ "$1" == "set" ]
    then

        for k in $(seq ${NB_IP_TO_SET}); do

            for n in {10..13}; do

                ip addr add ${IP_BASE}.${n}.${k}/${NETMASK} dev ${INTERFACE_TO_USE}

            done;

        done;

    elif [ "$1" == "unset" ]
    then

        for k in $(seq ${NB_IP_TO_SET}); do

            for n in {10..13}; do

                ip addr del ${IP_BASE}.${n}.${k}/${NETMASK} dev ${INTERFACE_TO_USE}

            done;

        done;

    else
        usage
    fi

    exit 0;
}

main $@
