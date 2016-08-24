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


from rozofs.core.platform import Platform, Role
from rozofs.core.storaged import ListenConfig
from rozofs.core.agent import ServiceStatus
from rozofs.cli.output import puts
from rozofs.cli.output import ordered_puts
from rozofs.cli.exceptions import MultipleError
from collections import OrderedDict
from rozofs.core.constants import SELF_HEALING, EXPORT_HOSTS


def __check_bool_value(value_str):
    if value_str == 'true':
        value = True
    elif value_str == 'false':
        value = False
    else:
        raise Exception(
            'invalid value: \'%s\' (valid values: true or false).' % value_str)
    return value


def rebuild_start(platform, args):
    platform.rebuild_storage_node(
        args.node[0], args.cid, args.sid, args.device)


def option_list(platform, args):

    if args.nodes is not None:
        for host in args.nodes:
            if host not in platform.list_nodes(Role.STORAGED):
                    raise Exception('%s: invalid storaged server.' % host)

    sid_l = {}
    errors_l = {}

    for host, configuration in platform.get_configurations(args.nodes, Role.STORAGED).items():

        # Node without storaged role
        if not configuration:
            continue

        sid_l[host] = []

        config = configuration[Role.STORAGED]

        # Check exception
        if isinstance(config, Exception):
            # Get error msg
            err_str = type(config).__name__ + ' (' + str(config) + ')'
            # Update standard output dict
            sid_l.update({host: err_str})
            # Update errors dict
            errors_l.update({host: err_str})
            continue

        options_l = []
        if config.self_healing is not None:
            options_l.append({'self-healing': config.self_healing})
        if config.export_hosts is not None:
            options_l.append({'export-hosts': config.export_hosts})

        sid_l[host].append({'OPTIONS': options_l})

    ordered_puts(sid_l)

    if errors_l:
        raise MultipleError(errors_l)


def option_get(platform, args):

    if args.nodes is not None:
        for host in args.nodes:
            if host not in platform.list_nodes(Role.STORAGED):
                raise Exception('%s: invalid storaged server.' % host)

    # Check given option
    valid_opts = [SELF_HEALING, EXPORT_HOSTS]
    if args.option not in valid_opts:
        raise Exception('invalid option: \'%s\' (valid values: %s).'
                        % (args.option, ', '.join(valid_opts)))

    sid_l = {}
    errors_l = {}

    for host, configuration in platform.get_configurations(args.nodes, Role.STORAGED).items():

        # Node without storaged role
        if not configuration:
            continue

        sid_l[host] = []
        config = configuration[Role.STORAGED]

        # Check exception
        if isinstance(config, Exception):
            # Get error msg
            err_str = type(config).__name__ + ' (' + str(config) + ')'
            # Update standard output dict
            sid_l.update({host: err_str})
            # Update errors dict
            errors_l.update({host: err_str})
            continue

        if args.option == SELF_HEALING:
            sid_l[host].append({SELF_HEALING: config.self_healing})

        if args.option == EXPORT_HOSTS:
            sid_l[host].append({EXPORT_HOSTS: config.export_hosts})

    ordered_puts(sid_l)

    if errors_l:
        raise MultipleError(errors_l)


def option_set(platform, args):

    e_host = platform._active_export_host

    if args.nodes is not None:
        for host in args.nodes:
            if host not in platform.list_nodes(Role.STORAGED):
                raise Exception('%s: invalid storaged server.' % host)

    if args.option not in [SELF_HEALING]:
        raise Exception('%s: invalid option.' % args.option)

    sid_l = {}
    errors_l = {}

    for host, configuration in platform.get_configurations(args.nodes, Role.STORAGED).items():

        # Node without storaged role
        if not configuration:
            continue

        sid_l[host] = []
        config = configuration[Role.STORAGED]

        # Check exception
        if isinstance(config, Exception):
            # Get error msg
            err_str = type(config).__name__ + ' (' + str(config) + ')'
            # Update standard output dict
            sid_l.update({host: err_str})
            # Update errors dict
            errors_l.update({host: err_str})
            continue

        if args.option == SELF_HEALING:
            if int(args.value) < 1:
                raise Exception('invalid value: \'%s\' (The lowest possible value is 1).' % args.value)
            config.self_healing = args.value

        # TO DO: not send value option if it's not necessary
        configuration[Role.STORAGED] = config
        try:
            platform._get_nodes(e_host)[host].set_configurations(configuration)
        except Exception as e:
            err = type(e).__name__ + ' (' + str(e) + ')'
            sid_l.update({host: err})
            continue

        sid_l[host].append({args.option: args.value})

    ordered_puts(sid_l)

    if errors_l:
        raise MultipleError(errors_l)


def listen_get(platform, args):

    if args.nodes is not None:
        for host in args.nodes:
            if host not in platform.list_nodes(Role.STORAGED):
                    raise Exception('%s: invalid storaged server.' % host)

    errors_l = {}
    sid_l = {}
    for host, configuration in platform.get_configurations(args.nodes, Role.STORAGED).items():

            # Node without storaged role
            if not configuration:
                continue

            sid_l[host] = []
            lid_l = {}

            config = configuration[Role.STORAGED]

            if isinstance(config, Exception):
                # Get error msg
                err_str = type(config).__name__ + ' (' + str(config) + ')'
                # Update standard output dict
                sid_l.update({host: err_str})
                # Update errors dict
                errors_l.update({host: err_str})
                continue

            for lconfig in configuration[Role.STORAGED].listens:
                lid_l = OrderedDict([
                                     ('addr', lconfig.addr),
                                     ('port', lconfig.port)
                                     ])
                sid_l[host].append(lid_l)

    ordered_puts(sid_l)

    if errors_l:
        raise MultipleError(errors_l)


def listen_add(platform, args):
    e_host = platform._active_export_host

    for host in args.nodes:
        if host not in platform.list_nodes(Role.STORAGED):
            raise Exception('%s: invalid storaged server.' % host)

    for host in args.nodes:

        configurations = platform._get_nodes(e_host)[host].get_configurations(Role.STORAGED)
        config = configurations[Role.STORAGED]

        errors_l = {}
        sid_l = {}
        sid_l[host] = []
        lid_l = {}

        # Check exception
        if isinstance(config, Exception):
            # Get error msg
            err_str = type(config).__name__ + ' (' + str(config) + ')'
            # Update standard output dict
            sid_l.update({host: err_str})
            ordered_puts(sid_l)
            # Update errors dict
            errors_l.update({host: err_str})
            continue

        for listener in config.listens:
            # if given interface is '*', remove existing interfaces
            if args.interface == "*":
                config.listens = []
                continue
            elif args.interface == listener.addr:
                if args.port == listener.port:
                    raise Exception('entry %s:%s already exists.' %
                                    (args.interface, args.port))
            if listener.addr == '*':
                config.listens = []

        lconfig = ListenConfig(args.interface, args.port)
        config.listens.append(lconfig)
        configurations[Role.STORAGED] = config

        try:
            platform._get_nodes(e_host)[host].set_configurations(configurations)
        except Exception as e:
            err = type(e).__name__ + ' (' + str(e) + ')'
            sid_l.update({host: err})
            ordered_puts(sid_l)
            continue

        for lconfig in config.listens:
            lid_l = OrderedDict([
                ('addr', lconfig.addr),
                ('port', lconfig.port)
            ])
            sid_l[host].append(lid_l)

        ordered_puts(sid_l)

    if errors_l:
        raise MultipleError(errors_l)


def listen_remove(platform, args):
    e_host = platform._active_export_host

    for host in args.nodes:
        if host not in platform.list_nodes(Role.STORAGED):
            raise Exception('%s: invalid storaged server.' % host)

    for host in args.nodes:
        configurations = platform._get_nodes(e_host)[host].get_configurations(Role.STORAGED)
        config = configurations[Role.STORAGED]
        check = True

        sid_l = {}
        sid_l[host] = []
        lid_l = {}
        errors_l = {}

        # Check exception
        if isinstance(config, Exception):
            # Get error msg
            err_str = type(config).__name__ + ' (' + str(config) + ')'
            # Update standard output dict
            sid_l.update({host: err_str})
            ordered_puts(sid_l)
            # Update errors dict
            errors_l.update({host: err_str})
            continue

        for listener in config.listens:
            if args.interface == listener.addr:
                if args.port == listener.port:
                    # listen entry is found
                    check = False
                    # Check if it's the last listen entry
                    # Replaced by default address
                    if len(config.listens) == 1:
                        listener.addr = "*"
                        listener.port = 41001
                    else:
                        config.listens.remove(listener)
        if check:
            raise Exception('entry %s:%s does not exist.' % (args.interface,
                            args.port))

        configurations[Role.STORAGED] = config

        try:
            platform._get_nodes(e_host)[host].set_configurations(configurations)
        except Exception as e:
            err = type(e).__name__ + ' (' + str(e) + ')'
            sid_l.update({host: err})
            ordered_puts(sid_l)
            continue

        for lconfig in config.listens:
            lid_l = OrderedDict([
                ('addr', lconfig.addr),
                ('port', lconfig.port)
            ])
            sid_l[host].append(lid_l)

        ordered_puts(sid_l)

    if errors_l:
        raise MultipleError(errors_l)


def dispatch(args):
    p = Platform(args.exportd, Role.EXPORTD | Role.STORAGED)
    globals()["_".join([args.subtopic.replace('-', '_'), args.action.replace('-', '_')])](p, args)
