/*
  Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
  This file is part of Rozofs.

  Rozofs is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published
  by the Free Software Foundation; either version 3 of the License,
  or (at your option) any later version.

  Rozofs is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see
  <http://www.gnu.org/licenses/>.
*/

/* Storaged profiling protocol
 * profile storaged main process (aka internal monitor process)
 */
%#include <rozofs/rozofs.h>

enum stcpp_status_t {
    STCPP_SUCCESS = 0,
    STCPP_FAILURE = 1
};

union stcpp_status_ret_t switch (stcpp_status_t status) {
    case STCPP_FAILURE:    int error;
    default:            void;
};


struct stcpp_profiler_t {
    uint64_t    uptime;
    uint64_t    now;
    uint8_t     vers[20];
    uint64_t    stat[2];
    uint64_t    ports[2];
    uint64_t    remove[2];
    /* io processes only */
    uint64_t    read_req[3];
    uint64_t    read[3];
    uint64_t    trans_inv[3];
    uint64_t    write[3];
    uint64_t    write_req[3];
    uint64_t    trans_forward[3];
    uint64_t    truncate[3];
    uint64_t    truncate_prj[3];
    uint64_t    truncate_prj_tmo[2];
    uint64_t    truncate_prj_err[2];
    uint64_t    read_prj[3];
    uint64_t    write_prj[3];
    uint64_t    read_prj_tmo[2];
    uint64_t    read_prj_err[2];
    uint64_t    read_prj_enoent[2];
    uint64_t    write_prj_tmo[2];
    uint64_t    write_prj_err[2];

    uint64_t    repair[3];
    uint64_t    repair_prj[3];
    uint64_t    repair_prj_tmo[2];
    uint64_t    repair_prj_err[2];
    
    uint16_t    io_process_ports[32];
};

union stcpp_profiler_ret_t switch (stcpp_status_t status) {
    case STCPP_SUCCESS:    stcpp_profiler_t profiler;
    case STCPP_FAILURE:    int error;
    default:            void;
};

program STORCLI_PROFILE_PROGRAM {
    version STORCLI_PROFILE_VERSION {
        void
        STCPP_NULL(void)          = 0;

        stcpp_profiler_ret_t
        STCPP_GET_PROFILER(void)  = 1;

        stcpp_status_ret_t
        STCPP_CLEAR(void)         = 2;
    }=1;
} = 0x20000008;
