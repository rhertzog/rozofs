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
from rozofs.cli.output import ordered_puts
from rozofs.cli.output import puts
from collections import OrderedDict


def list(platform, args):
    e_host = platform._active_export_host
    configurations = platform.get_configurations([e_host], Role.EXPORTD)
    if configurations[e_host] is None:
        raise Exception("%s is not reachable" % e_host)

    configuration = configurations[e_host][Role.EXPORTD]

    list_l = {}
    exports_l = []
    for eid, econfig in configuration.exports.items():
        export_l = []
        export_l.append({'vid':econfig.vid})
        export_l.append({'root':econfig.root})
        export_l.append({'md5':econfig.md5})
        export_l.append({'squota':econfig.squota})
        export_l.append({'hquota':econfig.hquota})

        exports_l.append({'EXPORT '+ str(eid):export_l})
    list_l.update({'EXPORTS': exports_l})
    ordered_puts(list_l)

def create(platform, args):
    platform.create_export(args.vid[0], args.name, args.passwd, args.squota, args.hquota)


def update(platform, args):
    platform.update_export(args.eid[0], args.current, args.passwd, args.squota, args.hquota)


def remove(platform, args):
    if not args.eids:
        args.eids = None

    platform.remove_export(args.eids, args.force)

def get(platform, args):
    e_host = platform._active_export_host
    configurations = platform.get_configurations([e_host], Role.EXPORTD)

    if configurations[e_host] is None:
        raise Exception("exportd node is off line.")

    configuration = configurations[e_host][Role.EXPORTD]

    list_l = {}
    exports_l = []
    for eid in args.eids:

        if eid not in configuration.exports:
            raise Exception("Unknown export with eid=%d." % eid)

        econfig = configuration.exports[eid]
        export_l = []
        export_l.append({'vid':econfig.vid})
        export_l.append({'root':econfig.root})
        export_l.append({'md5':econfig.md5})
        export_l.append({'squota':econfig.squota})
        export_l.append({'hquota':econfig.hquota})

        exports_l.append({'EXPORT '+ str(eid):export_l})
    list_l.update({'EXPORTS': exports_l})
    ordered_puts(list_l)

def mount(platform, args):
    if not args.eids:
        args.eids = None

    platform.mount_export(args.eids, args.nodes, args.options)


def umount(platform, args):
    if not args.eids:
        args.eids = None

    platform.umount_export(args.eids, args.nodes)


def dispatch(args):
    only_export_actions = {'list', 'create', 'update', 'get'}

    if (args.action in only_export_actions):
        p = Platform(args.exportd, Role.EXPORTD)
    else:
        p = Platform(args.exportd)

    globals()[args.action.replace('-', '_')](p, args)



