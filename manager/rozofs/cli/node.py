# -*- coding: utf-8 -*-
#
# Copyright (c) 2013 Fizians SAS. <http://www.fizians.com>
# This file is part of Rozofs.
#
# Rozofs is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published
# by the Free Software Foundation, version 2.
#
# Rozofs is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see
# <http://www.gnu.org/licenses/>.

import sys
from rozofs.core.platform import Platform, Role
from rozofs.core.agent import ServiceStatus
from rozofs.core.constants import STORAGED_MANAGER, EXPORTD_MANAGER, \
ROZOFSMOUNT_MANAGER
import json
from rozofs.cli.output import ordered_puts
from collections import OrderedDict

ROLES_STR = {Role.EXPORTD: "EXPORTD", Role.STORAGED: "STORAGED", Role.ROZOFSMOUNT: "ROZOFSMOUNT"}
STR_ROLES = {"exportd": Role.EXPORTD, "storaged": Role.STORAGED, "rozofsmount": Role.ROZOFSMOUNT}

def __roles_to_strings(roles):
    strs = []
    if roles & Role.EXPORTD == Role.EXPORTD:
        strs.append("EXPORTD")
    if roles & Role.STORAGED == Role.STORAGED:
        strs.append("STORAGED")
    if roles & Role.ROZOFSMOUNT == Role.ROZOFSMOUNT:
        strs.append("ROZOFSMOUNT")
    return strs

def __args_to_roles(args):
    roles = 0
    if args.roles:
        for r in args.roles:
            roles |= STR_ROLES[r]
    else:
        roles = Role.EXPORTD | Role.STORAGED | Role.ROZOFSMOUNT
    return roles

def list(platform, args):
    list_l = {}
    for h, r in platform.list_nodes(__args_to_roles(args)).items():
        role_l = []
        role_l.append(__roles_to_strings(r))
        list_l.update({h: role_l})
    ordered_puts(list_l)

def status(platform, args):
    statuses = platform.get_statuses(args.nodes, __args_to_roles(args))
    if statuses is None:
        ordered_puts(OrderedDict({h:'down'}))
    else:
        status_l = {}
        for h, s in statuses.items():
            role_l = []
            if s is not None:
                for role, status in s.items():
                    if status:
                        role_l.append({ROLES_STR[role]: 'running'})
                    else:
                        role_l.append({ROLES_STR[role]: 'not running'})
            else:
                status_l.update({h:"not reachable"})
            if role_l:
                status_l.update({h:role_l})
        ordered_puts(status_l)

def start(platform, args):
    platform.start(args.nodes, __args_to_roles(args))

def stop(platform, args):
    platform.stop(args.nodes, __args_to_roles(args))

def rebuild(platform, args):
    platform.rebuild_storage_node(args.node[0])

def config(platform, args):
    if not args.roles:
        args.roles = [EXPORTD_MANAGER, STORAGED_MANAGER, ROZOFSMOUNT_MANAGER]
    configurations = platform.get_configurations(args.nodes, __args_to_roles(args))
    host_l = {}
    for h, c in configurations.items():
        
        # Why?
        #if c is not None and not c:
        #    return

        if c is None:
            host_l.update({'NODE: ' + str(h) : "not reachable"})
            continue

        role_l=[]
        for role, config in c.items():
            if (role & Role.EXPORTD == Role.EXPORTD):
                exportd_l = []
                volume_l = []
                for v in config.volumes.values():
                    cluster_l = []
                    for cluster in v.clusters.values():
                        s_l = []
                        for s, hhh in cluster.storages.items():
                            s_l.append({'sid ' + str(s): hhh})
                        cluster_l.append({'cluster ' + str(cluster.cid): s_l})
                    volume_l.append({'volume ' + str(v.vid): cluster_l})
                exportd_l.append({'VOLUME':volume_l})
                if len(config.exports) != 0:
                    for e in config.exports.values():
                        export_l = OrderedDict([
                            ('vid', e.vid),
                            ('root', e.root),
                            ('md5', e.md5),
                            ('squota', e.squota),
                            ('hquota', e.hquota)])
                        exportd_l.append({'EXPORT':export_l})
                role_l.append({'EXPORTD':exportd_l})

            if (role & Role.STORAGED == Role.STORAGED):
                storage_l = []
                interface_l = []
                for lconfig in config.listens:
                    interface_l.append({lconfig.addr : lconfig.port})
                storage_l.append({'INTERFACE':interface_l})
                keylist = config.storages.keys()
                keylist.sort()
                st_l = []
                for key in keylist:
                    st = config.storages[key]
                    st_l.append({'cid ' + str(st.cid) + ', sid '+
                        str(st.sid) : st.root})
                storage_l.append({'STORAGE': st_l})
                role_l.append({'STORAGED': storage_l})

            if (role & Role.ROZOFSMOUNT == Role.ROZOFSMOUNT):
                exp_l = []
                for c in config:
                    exp_l.append({'node ' +
                        str(c.export_host) : c.export_path})
                role_l.append({'ROZOFSMOUNT': exp_l})

            host_l.update({'NODE: ' + str(h) : role_l})

    ordered_puts(host_l)

def dispatch(args):
    p = Platform(args.exportd)
    globals()[args.action.replace('-', '_')](p, args)
