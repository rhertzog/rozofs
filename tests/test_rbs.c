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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <errno.h>

#include <rozofs/rozofs_srv.h>
#include <rozofs/common/profile.h>

#include "rbs.h"

DEFINE_PROFILING(spp_profiler_t) = {0};

int main(int argc, char *argv[]) {
    char storage_root[FILENAME_MAX];
    char export_host[ROZOFS_HOSTNAME_MAX];
    sid_t sid = 0;
    cid_t cid = 0;
    int status = -1;

    /*
    ** init of the crc32
    */
    crc32c_init(0 /*generate_enable*/,0/* check_enable*/,0 /*hw_forced*/);
    
    // Check args
    if (argc < 4) {
        fprintf(stderr,
                "Usage: rbs_test <export_host> <cid-of-storage-to-rebuild>"
                " <sid-to-rebuild> <storage_root_path>\n");
        exit(EXIT_FAILURE);
    }

    // Copy args
    strncpy(export_host, argv[1], ROZOFS_HOSTNAME_MAX);
    cid = atoi(argv[2]);
    sid = atoi(argv[3]);
    strncpy(storage_root, argv[4], FILENAME_MAX);

    // Initialize rozofs constants
    rozofs_layout_initialize();

    // Sanity check
    if (rbs_sanity_check(export_host, cid, sid, storage_root) != 0) {
        fprintf(stderr, "rbs_sanity_check failed: %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

    // Rebuild storage
    if (rbs_rebuild_storage(export_host, cid, sid, storage_root, 0) != 0) {
        fprintf(stderr, "rbs_rebuild_storage failed: %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

    rozofs_layout_release();

    status = 0;

    exit(status);
}
