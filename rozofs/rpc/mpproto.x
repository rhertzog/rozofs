/*
 Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
 This file is part of Rozofs.

 Rozofs is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published
 by the Free Software Foundation, version 2.

 Rozofs is distributed in the hope that it will be useful, but
 WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see
 <http://www.gnu.org/licenses/>.
 */

/* rozofsmount profiling protocol
 * profile rozofsmount process
 */

%#include <rozofs/rozofs.h>

enum mpp_status_t {
    MPP_SUCCESS = 0,
    MPP_FAILURE = 1
};

union mpp_status_ret_t switch (mpp_status_t status) {
    case MPP_FAILURE:  int error;
    default:            void;
};

struct mpp_profiler_t {
    uint64_t uptime;
    uint64_t now;
    uint8_t  vers[20];
    uint64_t rozofs_ll_lookup[2];
    uint64_t rozofs_ll_lookup_agg[2];
    uint64_t rozofs_ll_forget[2];
    uint64_t rozofs_ll_getattr[2];
    uint64_t rozofs_ll_setattr[2];
    uint64_t rozofs_ll_readlink[2];
    uint64_t rozofs_ll_mknod[2];
    uint64_t rozofs_ll_mkdir[2];
    uint64_t rozofs_ll_unlink[2];
    uint64_t rozofs_ll_rmdir[2];
    uint64_t rozofs_ll_symlink[2];
    uint64_t rozofs_ll_rename[2];
    uint64_t rozofs_ll_open[2];
    uint64_t rozofs_ll_link[2];
    uint64_t rozofs_ll_read[3];
    uint64_t rozofs_ll_write[3];
    uint64_t rozofs_ll_flush[2];
    uint64_t rozofs_ll_release[2];
    uint64_t rozofs_ll_opendir[2];
    uint64_t rozofs_ll_readdir[2];
    uint64_t rozofs_ll_releasedir[2];
    uint64_t rozofs_ll_fsyncdir[2];
    uint64_t rozofs_ll_statfs[2];
    uint64_t rozofs_ll_setxattr[2];
    uint64_t rozofs_ll_getxattr[2];
    uint64_t rozofs_ll_listxattr[2];
    uint64_t rozofs_ll_removexattr[2];
    uint64_t rozofs_ll_access[2];
    uint64_t rozofs_ll_create[2];
    uint64_t rozofs_ll_getlk[2];
    uint64_t rozofs_ll_setlk[2];
    uint64_t rozofs_ll_setlk_int[2];
    uint64_t rozofs_ll_ioctl[2];
    uint64_t rozofs_ll_clearlkowner[2];
};

union mpp_profiler_ret_t switch (mpp_status_t status) {
    case MPP_SUCCESS:  mpp_profiler_t profiler;
    case MPP_FAILURE:  int error;
    default:            void;
};

program ROZOFSMOUNT_PROFILE_PROGRAM {
    version ROZOFSMOUNT_PROFILE_VERSION {
        void
        MPP_NULL(void)         = 0;

        mpp_profiler_ret_t
        MPP_GET_PROFILER(void)  = 1;

        mpp_status_ret_t
        MPP_CLEAR(void)        = 2;

    }=1;
} = 0x20000006;

