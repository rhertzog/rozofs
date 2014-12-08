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


#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <libconfig.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/log.h>
#include <rozofs/core/rozofs_host2ip.h>

#include "sconfig.h"

/* Settings names for storage configuration file */
#define SSTORAGES   "storages"
#define SSID        "sid"
#define SCID        "cid"
#define SROOT       "root"
#define STHREADS    "threads"
#define SIOLISTEN   "listen"
#define SIOADDR     "addr"
#define SIOPORT     "port"
#define SCORES      "nbcores"
#define SSTORIO     "storio"
#define SCRC32_C     "crc32c_check"
#define SCRC32_G     "crc32c_generate"
#define SCRC32_H     "crc32c_hw_forced"

int storage_config_initialize(storage_config_t *s, cid_t cid, sid_t sid,
        const char *root) {
    DEBUG_FUNCTION;

    s->sid = sid;
    s->cid = cid;
    strncpy(s->root, root, PATH_MAX);
    list_init(&s->list);
    return 0;
}

void storage_config_release(storage_config_t *s) {
    return;
}

int sconfig_initialize(sconfig_t *sc) {
    DEBUG_FUNCTION;
    memset(sc, 0, sizeof (sconfig_t));
    list_init(&sc->storages);
    return 0;
}

void sconfig_release(sconfig_t *config) {
    list_t *p, *q;
    DEBUG_FUNCTION;

    list_for_each_forward_safe(p, q, &config->storages) {
        storage_config_t *entry = list_entry(p, storage_config_t, list);
        storage_config_release(entry);
        list_remove(p);
        free(entry);
    }
}

int sconfig_read(sconfig_t *config, const char *fname) {
    int status = -1;
    config_t cfg;
    int my_bool;
    struct config_setting_t *stor_settings = 0;
    struct config_setting_t *ioaddr_settings = 0;
    int i = 0;
    const char              *char_value = NULL;    
#if (((LIBCONFIG_VER_MAJOR == 1) && (LIBCONFIG_VER_MINOR >= 4)) \
               || (LIBCONFIG_VER_MAJOR > 1))
    int threads, port, nb_cores;
#else
    long int threads, port, nb_cores;
#endif      
    DEBUG_FUNCTION;

    config_init(&cfg);

    if (config_read_file(&cfg, fname) == CONFIG_FALSE) {
        errno = EIO;
        severe("can't read %s : %s.", fname, config_error_text(&cfg));
        goto out;
    }

    if (!config_lookup_int(&cfg, STHREADS, &threads)) {
        config->nb_disk_threads = 2;
    } else {
        config->nb_disk_threads = threads;
    }

    if (!config_lookup_bool(&cfg, SCRC32_C, &my_bool)) {
        config->crc32c_check = 0;
    } else {
        config->crc32c_check = my_bool;
    }

    if (!config_lookup_bool(&cfg, SCRC32_G, &my_bool)) {
        config->crc32c_generate = 0;
    } else {
        config->crc32c_generate = my_bool;
    }
    if (!config_lookup_bool(&cfg, SCRC32_H, &my_bool)) {
        config->crc32c_hw_forced = 0;
    } else {
        config->crc32c_hw_forced = my_bool;
    }
    if (!config_lookup_int(&cfg, SCORES, &nb_cores)) {
        config->nb_cores = 2;
    } else {
        config->nb_cores = nb_cores;
    }
    
    /*
    ** Default is single storio 
    */
    config->multiio = 0;
    if (config_lookup_string(&cfg, SSTORIO, &char_value)) {
        if (strcasecmp(char_value, "multiple") == 0) {
            config->multiio = 1;
        } else if (strcasecmp(char_value, "single") != 0) {
            severe("%s has unexpected value \"%s\". Assume single storio.",
                    SSTORIO, char_value);
        }
    }

    if (!(ioaddr_settings = config_lookup(&cfg, SIOLISTEN))) {
        errno = ENOKEY;
        severe("can't fetch listen settings.");
        goto out;
    }

    config->io_addr_nb = config_setting_length(ioaddr_settings);

    if (config->io_addr_nb > STORAGE_NODE_PORTS_MAX) {
        errno = EINVAL;
        severe("too many IO listen addresses defined. %d while max is %d.",
                config->io_addr_nb, STORAGE_NODE_PORTS_MAX);
        goto out;
    }

    if (config->io_addr_nb == 0) {
        errno = EINVAL;
        severe("no IO listen address defined.");
        goto out;
    }

    for (i = 0; i < config->io_addr_nb; i++) {
        struct config_setting_t * io_addr = NULL;
        const char * io_addr_str = NULL;

        if (!(io_addr = config_setting_get_elem(ioaddr_settings, i))) {
            errno = ENOKEY;
            severe("can't fetch IO listen address(es) settings %d.", i);
            goto out;
        }

        if (config_setting_lookup_string(io_addr, SIOADDR, &io_addr_str)
                == CONFIG_FALSE) {
            errno = ENOKEY;
            severe("can't lookup address in IO listen address %d.", i);
            goto out;
        }

        // Check if the io address is specified by a single * character
        // if * is specified storio will listen on any of the interfaces
        if (strcmp(io_addr_str, "*") == 0) {
            config->io_addr[i].ipv4 = INADDR_ANY;
        } else {
            if (rozofs_host2ip((char*) io_addr_str, &config->io_addr[i].ipv4)
                    < 0) {
                severe("bad address \"%s\" in IO listen address %d",
                        io_addr_str, i);
                goto out;
            }
        }

        if (config_setting_lookup_int(io_addr, SIOPORT, &port)
                == CONFIG_FALSE) {
            errno = ENOKEY;
            severe("can't lookup port in IO address %d.", i);
            goto out;
        }

        config->io_addr[i].port = port;
    }

    if (!(stor_settings = config_lookup(&cfg, SSTORAGES))) {
        errno = ENOKEY;
        severe("can't fetch storages settings.");
        goto out;
    }

    for (i = 0; i < config_setting_length(stor_settings); i++) {
        storage_config_t *new = 0;
        struct config_setting_t *ms = 0;

        // Check version of libconfig
#if (((LIBCONFIG_VER_MAJOR == 1) && (LIBCONFIG_VER_MINOR >= 4)) \
               || (LIBCONFIG_VER_MAJOR > 1))
        int sid;
        int cid;
#else
        long int sid;
        long int cid;
#endif
        const char *root = 0;

        if (!(ms = config_setting_get_elem(stor_settings, i))) {
            errno = ENOKEY;
            severe("can't fetch storage.");
            goto out;
        }

        if (config_setting_lookup_int(ms, SSID, &sid) == CONFIG_FALSE) {
            errno = ENOKEY;
            severe("can't lookup sid for storage %d.", i);
            goto out;
        }

        if (config_setting_lookup_int(ms, SCID, &cid) == CONFIG_FALSE) {
            errno = ENOKEY;
            severe("can't lookup cid for storage %d.", i);
            goto out;
        }

        if (config_setting_lookup_string(ms, SROOT, &root) == CONFIG_FALSE) {
            errno = ENOKEY;
            severe("can't lookup root path for storage %d.", i);
            goto out;
        }

        // Check root path length
        if (strlen(root) > PATH_MAX) {
            errno = ENAMETOOLONG;
            severe("root path for storage %d must be lower than %d.", i,
                    PATH_MAX);
            goto out;
        }

        new = xmalloc(sizeof (storage_config_t));
        if (storage_config_initialize(new, (cid_t) cid, (sid_t) sid,
                root) != 0) {
            if (new)
                free(new);
            goto out;
        }
        list_push_back(&config->storages, &new->list);
    }

    status = 0;
out:
    config_destroy(&cfg);
    return status;
}

int sconfig_validate(sconfig_t *config) {
    int status = -1;
    int i = -1;
    int j = -1;
    list_t *p;
    int storages_nb = 0;
    uint32_t ip = 0;
    DEBUG_FUNCTION;

    // Check if IO addresses are duplicated
    for (i = 0; i < config->io_addr_nb; i++) {

        if ((config->io_addr[i].ipv4 == INADDR_ANY) &&
                (config->io_addr_nb != 1)) {
            severe("only one IO listen address can be configured if '*'"
                    " character is specified");
            errno = EINVAL;
            goto out;
        }

        for (j = i + 1; j < config->io_addr_nb; j++) {

            if ((config->io_addr[i].ipv4 == config->io_addr[j].ipv4)
                    && (config->io_addr[i].port == config->io_addr[j].port)) {

                ip = config->io_addr[i].ipv4;
                severe("duplicated IO listen address (addr: %u.%u.%u.%u ;"
                        " port: %"PRIu32")",
                        ip >> 24, (ip >> 16)&0xFF, (ip >> 8)&0xFF, ip & 0xFF,
                        config->io_addr[i].port);
                errno = EINVAL;
                goto out;
            }
        }
    }

    list_for_each_forward(p, &config->storages) {
        list_t *q;
        storage_config_t *e1 = list_entry(p, storage_config_t, list);
        if (access(e1->root, F_OK) != 0) {
            severe("invalid root for storage (cid: %u ; sid: %u) %s: %s.",
                    e1->cid, e1->sid, e1->root, strerror(errno));
            errno = EINVAL;
            goto out;
        }

        list_for_each_forward(q, &config->storages) {
            storage_config_t *e2 = list_entry(q, storage_config_t, list);
            if (e1 == e2)
                continue;
            if ((e1->sid == e2->sid) && (e1->cid == e2->cid)) {
                severe("duplicated couple (cid: %u ; sid: %u)", e1->cid,
                        e1->sid);
                errno = EINVAL;
                goto out;
            }

            if (strcmp(e1->root, e2->root) == 0) {
                severe("duplicated root: %s", e1->root);
                errno = EINVAL;
                goto out;
            }
        }

        // Compute the nb. of storage(s) for this storage node
        storages_nb++;
    }

    // Check the nb. of storage(s) for this storage node
    if (storages_nb > STORAGES_MAX_BY_STORAGE_NODE) {
        severe("too many number of storages for this storage node: %d"
                " storages register (maximum is %d)",
                storages_nb, STORAGES_MAX_BY_STORAGE_NODE);
        errno = EINVAL;
        goto out;
    }

    status = 0;
out:
    return status;
}
