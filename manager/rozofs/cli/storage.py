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
from rozofs.core.storaged import ListenConfig
from rozofs.core.agent import ServiceStatus
from rozofs.cli.output import puts
from rozofs.cli.output import ordered_puts
from collections import OrderedDict

def list(platform, args):
    
    configurations = {}
    
    for h, n in platform._nodes.items():
        if n.has_one_of_roles(Role.STORAGED):
            configurations[h] = n.get_configurations(Role.STORAGED)

    sid_l={}
    for stor in configurations:
        sid_l[stor] = []
        lid_l={}
        if configurations[stor] is None:
            sid_l.update({stor:"not reachable"})
            continue
        for lconfig in configurations[stor][Role.STORAGED].listens:
            lid_l = OrderedDict([
                ('addr', lconfig.addr),
                ('port', lconfig.port)
            ])
            sid_l[stor].append(lid_l)

    ordered_puts(sid_l)

def get(platform, args):

    for host in args.nodes:
        if not host in platform.list_nodes(Role.STORAGED):
                raise Exception('%s: invalid storaged server.' % host)

    sid_l={}
    for host, configuration in platform.get_configurations(args.nodes,
            Role.STORAGED).items():

            sid_l[host]=[]
            lid_l={}
            if configuration is None:
                sid_l.update({host:"not reachable"})
                continue
            for lconfig in configuration[Role.STORAGED].listens:
                lid_l = OrderedDict([
                                     ('addr', lconfig.addr),
                                     ('port', lconfig.port)
                                     ])
                sid_l[host].append(lid_l)

    ordered_puts(sid_l)

def add(platform, args):
    e_host = platform._active_export_host

    for host in args.nodes:
        if not host in platform.list_nodes(Role.STORAGED):
            raise Exception('%s: invalid storaged server.' % host)

    for host in args.nodes:
        
        configurations = platform._get_nodes(e_host)[host].get_configurations(Role.STORAGED)
        configuration = configurations[Role.STORAGED]
        for listener in configuration.listens:
            # if given interface is '*', remove existing interfaces
            if args.interface == "*":
                configuration.listens = []
                continue
            elif args.interface == listener.addr:
                if args.port == listener.port:
                    raise Exception('entry %s:%s already exists.' %
                            (args.interface, args.port))
            if listener.addr == '*':
                configuration.listens = []
        sid_l={}
        sid_l[host]=[]
        lid_l={}
        lconfig = ListenConfig(args.interface, args.port)
        configuration.listens.append(lconfig)
        configurations[Role.STORAGED] = configuration
        platform._get_nodes(e_host)[host].set_configurations(configurations)

        for lconfig in configuration.listens:
            lid_l = OrderedDict([
                ('addr', lconfig.addr),
                ('port', lconfig.port)
            ])
            sid_l[host].append(lid_l)

        ordered_puts(sid_l)

def remove(platform, args):
    e_host = platform._active_export_host

    for host in args.nodes:
        if not host in platform.list_nodes(Role.STORAGED):
            raise Exception('%s: invalid storaged server.' % host)
    
    for host in args.nodes:
        configurations = platform._get_nodes(e_host)[host].get_configurations(Role.STORAGED)
        configuration = configurations[Role.STORAGED]
        check = True

        for listener in configuration.listens:
            if args.interface == listener.addr:
                if args.port == listener.port:
                    # listen entry is found
                    check = False
                    # Check if it's the last listen entry
                    # Replaced by default address
                    if len(configuration.listens) == 1:
                        listener.addr = "*"
                        listener.port = 41001
                    else:
                        configuration.listens.remove(listener)
        if check:
            raise Exception('entry %s:%s does not exist.' % (args.interface,
            args.port))
        sid_l={}
        sid_l[host]=[]
        lid_l={}
        configurations[Role.STORAGED] = configuration

        platform._get_nodes(e_host)[host].set_configurations(configurations)

        for lconfig in configuration.listens:
            lid_l = OrderedDict([
                ('addr', lconfig.addr),
                ('port', lconfig.port)
            ])
            sid_l[host].append(lid_l)

        ordered_puts(sid_l)


def dispatch(args):
    p = Platform(args.exportd, Role.EXPORTD | Role.STORAGED)
    globals()[args.action.replace('-', '_')](p, args)
