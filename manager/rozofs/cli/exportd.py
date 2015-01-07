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
from rozofs.core.constants import LAYOUT_VALUES
from rozofs.cli.output import ordered_puts
from collections import OrderedDict
from rozofs.core.constants import NBCORES

def layout_set(platform, args):
    platform.set_layout(args.layout[0])


def layout_get(platform, args):
    layout = platform.get_layout()
    ordered_puts({'layout '+str(layout): OrderedDict([
          ("inverse", LAYOUT_VALUES[layout][0]),
          ("forward", LAYOUT_VALUES[layout][1]),
          ("safe", LAYOUT_VALUES[layout][2])
        ])})


def option_list(platform, args):

    e_host = platform._active_export_host
    configurations = platform.get_configurations([e_host], Role.EXPORTD)
    
    config = configurations[e_host][Role.EXPORTD]

    options_l = []

    options_l.append({'nbcores' : config.nbcores})

    ordered_puts({'OPTIONS':options_l})


def option_get(platform, args):

    # Check given option
    valid_opts = [NBCORES]
    if args.option not in valid_opts:
        raise Exception('invalid option: \'%s\' (valid value: %s).'
                        % (args.option, ', '.join(valid_opts)))

    e_host = platform._active_export_host
    configurations = platform.get_configurations([e_host], Role.EXPORTD)

    config = configurations[e_host][Role.EXPORTD]

    options_l = []

    if args.option == NBCORES:
        options_l.append({'nbcores' : config.nbcores})

    ordered_puts({'OPTIONS':options_l})


def option_set(platform, args):

    # Check given option
    valid_opts = [NBCORES]
    if args.option not in valid_opts:
        raise Exception('invalid option: \'%s\' (valid value: %s).'
                        % (args.option, ', '.join(valid_opts)))

    e_host = platform._active_export_host
    configurations = platform.get_configurations([e_host], Role.EXPORTD)

    config = configurations[e_host][Role.EXPORTD]

    options_l = []

    if args.option == NBCORES:
        config.nbcores = args.value

    configurations[e_host][Role.EXPORTD] = config

    platform._get_nodes(e_host)[e_host].set_configurations(configurations[e_host])
    options_l.append({args.option : args.value})

    ordered_puts({'OPTIONS':options_l})


def dispatch(args):
    p = Platform(args.exportd, Role.EXPORTD)
    globals()[ "_".join([args.subtopic.replace('-', '_'), args.action.replace('-', '_')])](p, args)
