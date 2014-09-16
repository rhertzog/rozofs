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

#include <stdio.h>
#include <errno.h>
#include <inttypes.h>
#include <unistd.h>

#include <rozofs/rpc/export_profiler.h>
#include <rozofs/common/profile.h>
#include <rozofs/rpc/epproto.h>
#include <rozofs/rozofs_srv.h>

#include "export.h"

export_one_profiler_t  * export_profiler[EXPGW_EXPORTD_MAX_IDX] = { 0 };
uint32_t		 export_profiler_eid = 0;

DEFINE_PROFILING(epp_profiler_t);

void print_cache(lv2_cache_t *cache) {
    char str[37];
    list_t *p;

    puts("============= cache =============");
    printf("size: %d (%d)\n", cache->size, cache->max);

    list_for_each_forward(p, &cache->lru) {
        lv2_entry_t *entry = list_entry(p, lv2_entry_t, list);
        uuid_unparse(entry->attributes.fid, str);
        printf("%s\n", str);
    }
    puts("=================================");
}

void print_mattr(mattr_t *mattr) {
    char str[37];
    uuid_unparse(mattr->fid, str);
    printf("\tfid: %s\n", str);
    printf("\tsize: %"PRIu64"\n", mattr->size);
    printf("\tnlink: %d\n", mattr->nlink);
    printf("\tmode: %"PRIu32"\n", mattr->mode);
    printf("\tuid: %"PRIu32"\n", mattr->uid);
    printf("\tgid: %"PRIu32"\n", mattr->gid);
    printf("\tctime: %"PRIu64"\n", mattr->ctime);
    printf("\tatime: %"PRIu64"\n", mattr->atime);
    printf("\tmtime: %"PRIu64"\n", mattr->mtime);
}

void print_estat(estat_t *estat) {
    printf("\tbsize: %d\n", estat->bsize);
    printf("\tblocks: %"PRIu64"\n", estat->blocks);
    printf("\tbfree: %"PRIu64"\n", estat->bfree);
    printf("\tfiles: %"PRIu64"\n", estat->files);
    printf("\tffree: %"PRIu64"\n", estat->ffree);
    printf("\tnamemax: %d\n", estat->namemax);
}

int main(int argc, char **argv) {
    export_t export;
    lv2_cache_t cache;
    //estat_t estat;
    /* FAKE FOR TESTING ONLY */
    volume_t volume;
    mattr_t mattrs;
    mattr_t pmattrs; 
    //fid_t fid;
    //char link_name[PATH_MAX];

    printf("rozofs:\t\t\t");
    rozofs_layout_initialize();
    printf("initialized\n");

    printf("export:\t\t\t");
    if (export_create("./export_test_directory") != 0) {
        perror("can't create export directory.");
        return errno;
    }
    printf("created\n");

    printf("volume:\t\t\t");
    if (volume_initialize(&volume, 1, 0) != 0) {
        perror("can't initialize volume.");
        return errno;
    }
    printf("initialized\n");

    printf("lv2_cache:\t\t");
    lv2_cache_initialize(&cache);
    printf("initialized\n");

    printf("export:\t\t\t");
    if (export_initialize(&export, &volume, &cache, 1, "./export_test_directory", "", 0, 0) != 0) {
        perror("can't initialize export");
        return errno;
    }
    printf("initialized\n");
    print_cache(&cache);
    /*
        printf("export_stat:\n");
        export_stat(&export, &estat);
        print_estat(&estat);

        printf("export_lookup (/.):\n");
        if (export_lookup(&export, export.rfid, ".", &mattrs) != 0) {
            perror("can't lookup /.");
            return errno;
        }
        print_mattr(&mattrs);
     */
    printf("export_mknod (/node1):\n");
    if (export_mknod(&export, export.rfid, "node1", getuid(), getgid(), S_IFREG | S_IRWXU, &mattrs,&pmattrs) != 0) {
        perror("can't make node /node1");
        return errno;
    }
    if (export_getattr(&export, mattrs.fid, &mattrs) != 0) {
        perror("can't get attrs");
        return errno;
    }
    print_cache(&cache);

    printf("export_mknod (/node2):\n");
    if (export_mknod(&export, export.rfid, "node2", getuid(), getgid(), S_IFREG | S_IRWXU, &mattrs,&pmattrs) != 0) {
        perror("can't make node /node1");
        return errno;
    }
    if (export_getattr(&export, mattrs.fid, &mattrs) != 0) {
        perror("can't get attrs");
        return errno;
    }
    print_cache(&cache);

    printf("export_mknod (/node3):\n");
    if (export_mknod(&export, export.rfid, "node3", getuid(), getgid(), S_IFREG | S_IRWXU, &mattrs,&pmattrs) != 0) {
        perror("can't make node /node1");
        return errno;
    }
    if (export_getattr(&export, mattrs.fid, &mattrs) != 0) {
        perror("can't get attrs");
        return errno;
    }
    print_cache(&cache);

    printf("export_mknod (/node4):\n");
    if (export_mknod(&export, export.rfid, "node4", getuid(), getgid(), S_IFREG | S_IRWXU, &mattrs,&pmattrs) != 0) {
        perror("can't make node /node1");
        return errno;
    }
    if (export_getattr(&export, mattrs.fid, &mattrs) != 0) {
        perror("can't get attrs");
        return errno;
    }
    print_cache(&cache);

    printf("export_mknod (/node5):\n");
    if (export_mknod(&export, export.rfid, "node5", getuid(), getgid(), S_IFREG | S_IRWXU, &mattrs,&pmattrs) != 0) {
        perror("can't make node /node1");
        return errno;
    }
    if (export_getattr(&export, mattrs.fid, &mattrs) != 0) {
        perror("can't get attrs");
        return errno;
    }
    print_cache(&cache);
    /*
        printf("export_stat:\n");
        export_stat(&export, &estat);
        print_estat(&estat);

        printf("export_lookup (/node1):\n");
        if (export_lookup(&export, export.rfid, "node1", &mattrs) != 0) {
            perror("can't lookup /node1");
            return errno;
        }
        print_mattr(&mattrs);

        printf("export_getattr (node1):\n");
        if (export_getattr(&export, mattrs.fid, &mattrs) != 0) {
            perror("can't get attributes /node1");
            return errno;
        }
        print_mattr(&mattrs);

        printf("export_setattr (node1, size: 4096):\t");
        mattrs.size = 4096;
        if (export_setattr(&export, mattrs.fid, &mattrs) != 0) {
            perror("can't set attributes /node1");
            return errno;
        }
        printf("set\n");


        printf("export_getattr (node1):\n");
        if (export_getattr(&export, mattrs.fid, &mattrs) != 0) {
            perror("can't get attributes /node1");
            return errno;
        }
        print_mattr(&mattrs);

        printf("export_lookup (/node1):\n");
        if (export_lookup(&export, export.rfid, "node1", &mattrs) != 0) {
            perror("can't lookup /node1");
            return errno;
        }
        print_mattr(&mattrs);

        printf("export_getattr (node1):\n");
        if (export_getattr(&export, mattrs.fid, &mattrs) != 0) {
            perror("can't get attributes /node1");
            return errno;
        }
        print_mattr(&mattrs);

        printf("export_mkdir (/node2):\n");
        if (export_mkdir(&export, export.rfid, "node2", getuid(), getgid(), S_IFDIR|S_IRWXU, &mattrs) != 0) {
            perror("can't make directory /node2");
            return errno;
        }
        print_mattr(&mattrs);

        printf("export_mkdir (/node2/node3):\n");
        if (export_mkdir(&export, mattrs.fid, "node3", getuid(), getgid(), S_IFDIR|S_IRWXU, &mattrs) != 0) {
            perror("can't make directory /node2/node3");
            return errno;
        }
        print_mattr(&mattrs);

        printf("export_mknod (/node2/node3/node4):\n");
        if (export_mknod(&export, mattrs.fid, "node4", getuid(), getgid(), S_IFREG|S_IRWXU, &mattrs) != 0) {
            perror("can't make node /node2/node3/node4");
            return errno;
        }
        print_mattr(&mattrs);

        printf("export_symlink (/link1 -> /link2):\n");
        if (export_symlink(&export, "link1", export.rfid, "link2", &mattrs) != 0) {
            perror("can't link /link1 to /link2");
            return errno;
        }
        print_mattr(&mattrs);

        printf("read_link (/link2):\t\t\t");
        if (export_readlink(&export, mattrs.fid, link_name) != 0) {
            perror("can't read link /link2 ");
            return errno;
        }
        printf("%s\n", link_name);

        printf("export_symlink (/link2 -> /link1):\n");
        if (export_symlink(&export, "link2", export.rfid, "link1", &mattrs) != 0) {
            perror("can't link /link2 to /link1");
            return errno;
        }
        print_mattr(&mattrs);

        printf("read_link (/link1):\t\t\t");
        if (export_readlink(&export, mattrs.fid, link_name) != 0) {
            perror("can't read link /link2 ");
            return errno;
        }
        printf("%s\n", link_name);

        printf("export_lookup (/node2):\n");
        if (export_lookup(&export, export.rfid, "node2", &mattrs) != 0) {
            perror("can't lookup /node2");
            return errno;
        }
        print_mattr(&mattrs);

        printf("export_mkdir (/node2/node5):\n");
        if (export_mkdir(&export, mattrs.fid, "node5", getuid(), getgid(), S_IFDIR|S_IRWXU, &mattrs) != 0) {
            perror("can't make directory /node2/node5");
            return errno;
        }
        print_mattr(&mattrs);

        printf("export_lookup (/node2):\n");
        if (export_lookup(&export, export.rfid, "node2", &mattrs) != 0) {
            perror("can't lookup /node2");
            return errno;
        }
        print_mattr(&mattrs);

        printf("export_rmdir (/node2/node5):");
        if (export_rmdir(&export, mattrs.fid, "node5", fid) != 0) {
            perror("can't remove directory /node2/node5");
            return errno;
        }
        printf("done\n");

        printf("export_lookup (/node2):\n");
        if (export_lookup(&export, export.rfid, "node2", &mattrs) != 0) {
            perror("can't lookup /node2");
            return errno;
        }
        print_mattr(&mattrs);

        printf("export_rmdir (/node2/node3):\t\t");
        if (export_rmdir(&export, mattrs.fid, "node3", fid) == 0) {
            perror("removed directory not empty: /node2/node3");
            return errno;
        }
        printf("not removed while not empty\n");

        printf("export:\t\t\t");
        export_release(&export);
        printf("released\n");

        printf("lv2_cache:\t\t");
        lv2_cache_release(&cache);
        printf("released\n");
     */
    return 0;
}
