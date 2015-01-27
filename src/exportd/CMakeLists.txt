# Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
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


add_definitions(-DD_REENTRANT -D_FILE_OFFSET_BITS=64)
include_directories(${CMAKE_BINARY_DIR}) # for config.h
include_directories(${CMAKE_SOURCE_DIR}) # for librozofs headers
include_directories(${PTHREAD_INCLUDE_DIR})
link_directories(${CMAKE_BINARY_DIR}/rozofs)

add_executable(exportd
    volume.h
    volume.c
    mreg.h
    mreg.c
    mdir.h
    mdir.c
    mslnk.h
    mslnk.c
    dirent_journal.h
    mdirent.h
    dirent_file_repair.c
    writeback_cache.c
    dirent_file.c
    dirent_cache.c
    dirent_search.c
    dirent_insert.c
    dirent_enum2String_file_repair_cause_e.h
    cache.h
    cache.c

    export.h
    export.c
    eproto.c
    monitor.h
    monitor.c
    econfig.h
    econfig.c
    exportd.c
    exp_cache_dirty_mgt.c
    export_expgw_conf.c
    export_expgw_conf.h
    export_internal_channel.c
    export_internal_channel.h
    export_nblock_init.c
    export_expgateway_conf.c
    export_expgateway_conf.h
    export_expgateway_sup.c
    export_gwprotoclt.c   
)
target_link_libraries(exportd rozofs ${PTHREAD_LIBRARY} ${UUID_LIBRARY} ${CONFIG_LIBRARY})


#add_executable(expgateway
#    expgw_attr_cache.c
#    expgw_attr_cache.h
#    expgw_eprotosvc.c
#    expgw_export.h
#    expgw_fid_cache.c
#    expgw_fid_cache.h
#    expgw_lookup.c
#    expgw_attr.c
#    expgw_create_dir_file.c
#    expgw_delete_dir_file.c
#    expgw_mgt.c
#    expgw_rozofs_north_intf.c
#    expgw_send.c
#    expgw_proto.h
#    expgw_exportd_north_intf.c
#    expgw_main.c
#    expgw_main.h
#    expgw_nblock_init.c
#    expgw_gwprotosvc.c
#    expgw_gwprotosvc.h
#    expgw_gw_poll.c
#)
#target_link_libraries(expgateway rozofs ${PTHREAD_LIBRARY} ${UUID_LIBRARY} ${CONFIG_LIBRARY})


# Install
install(CODE "EXECUTE_PROCESS (COMMAND mkdir -p ${ROZOFS_CONFIG_DIR})" COMPONENT export)

install(TARGETS exportd
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT export
)

install(FILES export.conf.sample DESTINATION ${ROZOFS_CONFIG_DIR} COMPONENT export)
install(FILES export.conf DESTINATION ${ROZOFS_CONFIG_DIR} COMPONENT export)
