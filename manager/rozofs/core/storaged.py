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

import os
from rozofs.core.configuration import ConfigurationParser, ConfigurationReader, \
    ConfigurationWriter
from rozofs.core.libconfig import config_setting_add, CONFIG_TYPE_INT, \
    config_setting_set_int, CONFIG_TYPE_LIST, CONFIG_TYPE_GROUP, CONFIG_TYPE_STRING, \
    config_setting_set_string, config_lookup, config_setting_get_int, \
    config_setting_length, config_setting_get_elem, config_setting_get_member, \
    config_setting_get_string, config_setting_set_int_elem, \
    config_setting_get_int_elem
from rozofs.core.daemon import DaemonManager
from rozofs.core.constants import LAYOUT, STORAGES, STORAGE_SID, STORAGE_CID, STORAGE_ROOT, \
    LAYOUT_2_3_4, STORAGED_MANAGER, LAYOUT_4_6_8, LAYOUT_8_12_16, LISTEN, \
    LISTEN_ADDR, LISTEN_PORT, THREADS, NBCORES, STORIO
from rozofs.core.agent import Agent, ServiceStatus
from rozofs import __sysconfdir__
import collections
import syslog


class StorageConfig():
    def __init__(self, cid=0, sid=0, root=""):
        self.cid = cid
        self.sid = sid
        self.root = root

class ListenConfig():
    def __init__(self, addr="*", port=41001):
        self.addr = addr
        self.port = port

class StoragedConfig():
    def __init__(self, threads=None, nbcores=None, storio=None, listens=[], storages={}):
        self.threads = threads
        self.nbcores = nbcores
        self.storio = storio
        # keys is a tuple (cid, sid)
        self.storages = storages
        self.listens = listens

class StoragedConfigurationParser(ConfigurationParser):

    def parse(self, configuration, config):

        if configuration.threads is not None:
            threads_setting = config_setting_add(config.root, THREADS, CONFIG_TYPE_INT)
            config_setting_set_int(threads_setting, int(configuration.threads))

        if configuration.nbcores is not None:
            nbcores_setting = config_setting_add(config.root, NBCORES, CONFIG_TYPE_INT)
            config_setting_set_int(nbcores_setting, int(configuration.nbcores))

        if configuration.storio is not None:
             storio_setting = config_setting_add(config.root, STORIO, CONFIG_TYPE_STRING)
             config_setting_set_string(storio_setting, configuration.storio)

        listen_settings = config_setting_add(config.root, LISTEN, CONFIG_TYPE_LIST)
        for listen in configuration.listens:
            listen_setting = config_setting_add(listen_settings, '', CONFIG_TYPE_GROUP)
            addr_setting = config_setting_add(listen_setting, LISTEN_ADDR, CONFIG_TYPE_STRING)
            config_setting_set_string(addr_setting, listen.addr)
            port_setting = config_setting_add(listen_setting, LISTEN_PORT, CONFIG_TYPE_INT)
            config_setting_set_int(port_setting, listen.port)

        storage_settings = config_setting_add(config.root, STORAGES, CONFIG_TYPE_LIST)
        for storage in configuration.storages.values():
            storage_setting = config_setting_add(storage_settings, '', CONFIG_TYPE_GROUP)
            cid_setting = config_setting_add(storage_setting, STORAGE_CID, CONFIG_TYPE_INT)
            config_setting_set_int(cid_setting, storage.cid)
            sid_setting = config_setting_add(storage_setting, STORAGE_SID, CONFIG_TYPE_INT)
            config_setting_set_int(sid_setting, storage.sid)
            root_setting = config_setting_add(storage_setting, STORAGE_ROOT, CONFIG_TYPE_STRING)
            config_setting_set_string(root_setting, storage.root)

    def unparse(self, config, configuration):

        threads_setting = config_lookup(config, THREADS)
        if threads_setting is not None:
            configuration.threads = config_setting_get_int(threads_setting)

        nbcores_setting = config_lookup(config, NBCORES)
        if nbcores_setting is not None:
            configuration.nbcores = config_setting_get_int(nbcores_setting)

        storio_setting = config_lookup(config, STORIO)
        if storio_setting is not None:
            configuration.storio = config_setting_get_string(storio_setting)

        listen_settings = config_lookup(config, LISTEN)
        configuration.listens = []
        for i in range(config_setting_length(listen_settings)):
            listen_setting = config_setting_get_elem(listen_settings, i)
            addr_setting = config_setting_get_member(listen_setting, LISTEN_ADDR)
            addr = config_setting_get_string(addr_setting)
            port_setting = config_setting_get_member(listen_setting, LISTEN_PORT)
            port = config_setting_get_int(port_setting)
            configuration.listens.append(ListenConfig(addr, port))

        storage_settings = config_lookup(config, STORAGES)
        configuration.storages = {}
        for i in range(config_setting_length(storage_settings)):
            storage_setting = config_setting_get_elem(storage_settings, i)
            cid_setting = config_setting_get_member(storage_setting, STORAGE_CID)
            cid = config_setting_get_int(cid_setting)
            sid_setting = config_setting_get_member(storage_setting, STORAGE_SID)
            sid = config_setting_get_int(sid_setting)
            root_setting = config_setting_get_member(storage_setting, STORAGE_ROOT)
            root = config_setting_get_string(root_setting)
            configuration.storages[(cid, sid)] = StorageConfig(cid, sid, root)


class StoragedAgent(Agent):

    def __init__(self, config="%s%s" % (__sysconfdir__, '/rozofs/storage.conf'), daemon='storaged'):
        Agent.__init__(self, STORAGED_MANAGER)
        self._daemon_manager = DaemonManager(daemon, ["-c", config], 5)
        self._reader = ConfigurationReader(config, StoragedConfigurationParser())
        self._writer = ConfigurationWriter(config, StoragedConfigurationParser())

    def get_service_config(self):
        configuration = StoragedConfig()
        return self._reader.read(configuration)

    def set_service_config(self, configuration):
        for r in [s.root for s in configuration.storages.values()]:
            if not os.path.isabs(r):
                raise Exception('%s: not absolute.' % r)
            if not os.path.exists(r):
                os.makedirs(r)
            if not os.path.isdir(r):
                raise Exception('%s: not a directory.' % r)

        self._writer.write(configuration)
        self._daemon_manager.restart()

    def get_service_status(self):
        return self._daemon_manager.status()

    def set_service_status(self, status):
        current_status = self._daemon_manager.status()
        if status == ServiceStatus.STARTED and not current_status:
            self._daemon_manager.start()
        if status == ServiceStatus.STOPPED and current_status:
            self._daemon_manager.stop()

    def restart_with_rebuild(self, exports_list):
        self._daemon_manager.restart(["-r", "/".join(exports_list)])
