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
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <pthread.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <sys/resource.h>
#include <unistd.h>
#include <libintl.h>
#include <sys/poll.h>
#include <rpc/rpc.h>
#include <rpc/pmap_clnt.h>
#include <libconfig.h>
#include <getopt.h>
#include <inttypes.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <netdb.h>

#include <rozofs/rozofs_srv.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/list.h>
#include <rozofs/common/daemon.h>
#include <rozofs/common/profile.h>
#include <rozofs/rpc/mproto.h>
#include <rozofs/rpc/sproto.h>
#include <rozofs/rpc/spproto.h>
#include <rozofs/core/rozofs_core_files.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/rozofs_timer_conf.h>

#include "config.h"
#include "sconfig.h"
#include "storage.h"
#include "storaged.h"
#include "sconfig.h"
#include "rbs.h"
#include "storaged_nblock_init.h"

#define STORAGED_PID_FILE "storaged"

static char storaged_config_file[PATH_MAX] = STORAGED_DEFAULT_CONFIG;

sconfig_t storaged_config;

static storage_t storaged_storages[STORAGES_MAX_BY_STORAGE_NODE] = { { 0 } };

static char *storaged_hostname = NULL;

static uint16_t storaged_nrstorages = 0;

static SVCXPRT *storaged_monitoring_svc = 0;

static SVCXPRT *storaged_profile_svc = 0;

uint8_t storio_nb_threads = 0;
uint8_t storaged_nb_ports = 0;
uint8_t storaged_nb_io_processes = 0;

DEFINE_PROFILING(spp_profiler_t) = {0};

// Rebuild storage variables

/* Need to start rebuild storage process */
uint8_t rbs_start_process = 0;
/* Export hostname */
static char rbs_export_hostname[ROZOFS_HOSTNAME_MAX];
/* Time in seconds between two attemps of rebuild */
#define TIME_BETWEEN_2_RB_ATTEMPS 30

static int storaged_initialize() {
    int status = -1;
    list_t *p = NULL;
    DEBUG_FUNCTION;

    /* Initialize rozofs constants (redundancy) */
    rozofs_layout_initialize();

    storaged_nrstorages = 0;

    storaged_nb_io_processes = 1;
    
    storio_nb_threads = storaged_config.nb_disk_threads;

    storaged_nb_ports = storaged_config.io_addr_nb;

    /* For each storage on configuration file */
    list_for_each_forward(p, &storaged_config.storages) {
        storage_config_t *sc = list_entry(p, storage_config_t, list);
        /* Initialize the storage */
        if (storage_initialize(storaged_storages + storaged_nrstorages++,
                sc->cid, sc->sid, sc->root) != 0) {
            severe("can't initialize storage (cid:%d : sid:%d) with path %s",
                    sc->cid, sc->sid, sc->root);
            goto out;
        }
    }

    status = 0;
out:
    return status;
}

/** Check each storage to rebuild
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
static int rbs_check() {
    list_t *p = NULL;
    int status = -1;
    DEBUG_FUNCTION;

    // For each storage present on configuration file

    list_for_each_forward(p, &storaged_config.storages) {
        storage_config_t *sc = list_entry(p, storage_config_t, list);

        // Sanity check for rebuild this storage
        if (rbs_sanity_check(rbs_export_hostname, sc->cid, sc->sid,
                sc->root) != 0)
            goto out;
    }
    status = 0;
out:
    return status;
}

/** Structure used to store configuration for each storage to rebuild */
typedef struct rbs_stor_config {
    char export_hostname[ROZOFS_HOSTNAME_MAX]; ///< export hostname or IP.
    cid_t cid; //< unique id of cluster that owns this storage.
    sid_t sid; ///< unique id of this storage for one cluster.
    uint8_t stor_idx; ///< storage index used for display statistics.
    char root[PATH_MAX]; ///< absolute path.
} rbs_stor_config_t;

/** Starts a thread for rebuild given storage(s)
 *
 * @param v: table of storages configurations to rebuild.
 */
static void * rebuild_storage_thread(void *v) {

    DEBUG_FUNCTION;
    int i = 0;

    // Get storage(s) configuration(s)
    rbs_stor_config_t *stor_confs = (rbs_stor_config_t*) v;

    for (i = 0; i < STORAGES_MAX_BY_STORAGE_NODE; i++) {

        // Check if storage conf is empty
        if (stor_confs[i].cid == 0 && stor_confs[i].sid == 0) {
            continue;
        }

        info("Start rebuild process for storage (cid=%u;sid=%u).",
                stor_confs[i].cid, stor_confs[i].sid);

        // Try to rebuild the storage until it's over
        while (rbs_rebuild_storage(stor_confs[i].export_hostname,
                stor_confs[i].cid, stor_confs[i].sid, stor_confs[i].root,
                stor_confs[i].stor_idx) != 0) {

            // Probably a problem when connecting with other members
            // of this cluster
            severe("can't rebuild storage (cid:%u;sid:%u) with path %s,"
                    " next attempt in %d seconds",
                    stor_confs[i].cid, stor_confs[i].sid, stor_confs[i].root,
                    TIME_BETWEEN_2_RB_ATTEMPS);

            sleep(TIME_BETWEEN_2_RB_ATTEMPS);
        }

        // Here the rebuild process is finish, so exit
        info("The rebuild process for storage (cid=%u;sid=%u)"
                " was completed successfully.",
                stor_confs[i].cid, stor_confs[i].sid);
    }

    return 0;

}

rbs_stor_config_t rbs_stor_configs[STORAGES_MAX_BY_STORAGE_NODE];

/** Start one rebuild process for each storage to rebuild
 */
static void rbs_process_initialize() {
    list_t *p = NULL;
    int i = 0;

    memset(&rbs_stor_configs, 0,
            STORAGES_MAX_BY_STORAGE_NODE * sizeof(rbs_stor_config_t));

    DEBUG_FUNCTION;

    // For each storage in configuration file

    list_for_each_forward(p, &storaged_config.storages) {

        storage_config_t *sc = list_entry(p, storage_config_t, list);

        // Copy the configuration for the storage to rebuild
        strncpy(rbs_stor_configs[i].export_hostname, rbs_export_hostname,
        ROZOFS_HOSTNAME_MAX);
        rbs_stor_configs[i].cid = sc->cid;
        rbs_stor_configs[i].sid = sc->sid;
        rbs_stor_configs[i].stor_idx = i;
        strncpy(rbs_stor_configs[i].root, sc->root, PATH_MAX);

        // Set profiling values
        SET_PROBE_VALUE(rbs_cids[i], sc->cid);
        SET_PROBE_VALUE(rbs_sids[i], sc->sid);
        SET_PROBE_VALUE(rb_files_current[i], 0);
        SET_PROBE_VALUE(rb_files_total[i], 0);
        SET_PROBE_VALUE(rb_status[i], 0);

        i++;
    }

    // Create pthread for rebuild storage(s)
    pthread_t thread;

    if ((errno = pthread_create(&thread, NULL, rebuild_storage_thread,
            &rbs_stor_configs)) != 0) {
        severe("can't create thread for rebuild storage(s): %s",
                strerror(errno));
    }
}

static void storaged_release() {
    DEBUG_FUNCTION;
    int i;
    list_t *p, *q;

    for (i = 0; i < storaged_nrstorages; i++) {
        storage_release(&storaged_storages[i]);
    }
    storaged_nrstorages = 0;

    // Free config

    list_for_each_forward_safe(p, q, &storaged_config.storages) {

        storage_config_t *s = list_entry(p, storage_config_t, list);
        free(s);
    }
}

storage_t *storaged_lookup(cid_t cid, sid_t sid) {
    storage_t *st = 0;
    DEBUG_FUNCTION;

    st = storaged_storages;
    do {
        if ((st->cid == cid) && (st->sid == sid))
            goto out;
    } while (st++ != storaged_storages + storaged_nrstorages);
    errno = EINVAL;
    st = 0;
out:
    return st;
}

static void on_stop() {
    DEBUG_FUNCTION;
    char cmd[128];
    int ret = -1;
   
    /*
    ** Kill every instance of storio of this host
    */
    if (storaged_hostname) 
      sprintf(cmd,"storio_killer.sh -H %s", storaged_hostname);
    else 
      sprintf(cmd,"storio_killer.sh"); 
    
    ret = system(cmd);
    if (-1 == ret) {
        DEBUG("system command failed: %s", strerror(errno));
    }

    svc_exit();

    if (storaged_monitoring_svc) {
        svc_unregister(MONITOR_PROGRAM, MONITOR_VERSION);
        pmap_unset(MONITOR_PROGRAM, MONITOR_VERSION);
        svc_destroy(storaged_monitoring_svc);
        storaged_monitoring_svc = NULL;
    }

    if (storaged_profile_svc) {
        svc_unregister(STORAGED_PROFILE_PROGRAM, STORAGED_PROFILE_VERSION);
        pmap_unset(STORAGED_PROFILE_PROGRAM, STORAGED_PROFILE_VERSION);
        svc_destroy(storaged_profile_svc);
        storaged_profile_svc = NULL;
    }

    rozofs_layout_release();

    storaged_release();

    info("stopped.");
    closelog();
}

char storage_process_filename[NAME_MAX];

static void on_start() {
    char cmd[128];
    char * p;
    storaged_start_conf_param_t conf;
    int ret = -1;
        
    DEBUG_FUNCTION;

    af_unix_socket_set_datagram_socket_len(128);

    storage_process_filename[0] = 0;

    // Initialization of the storage configuration
    if (storaged_initialize() != 0) {
        fatal("can't initialize storaged: %s.", strerror(errno));
        return;
    }

    // Start rebuild storage process(es) if necessary
    if (rbs_start_process == 1) {
        rbs_process_initialize();
        SET_PROBE_VALUE(nb_rb_processes, list_size(&storaged_config.storages));
    } else {
        SET_PROBE_VALUE(nb_rb_processes, 0);
    }

    SET_PROBE_VALUE(uptime, time(0));
    strncpy((char*) gprofiler.vers, VERSION, 20);
    SET_PROBE_VALUE(nb_io_processes, storio_nb_threads);
    
    // Create storio process(es)

    // Set monitoring values just for the master process
    //SET_PROBE_VALUE(io_process_ports[i],(uint16_t) storaged_storage_ports[i] + 1000);
    
    /*
    ** 1rst kill storio in case it is already running
    */
    p = cmd;
    if (storaged_hostname) 
      sprintf(cmd,"storio_killer.sh -H %s", storaged_hostname);
    else 
      sprintf(cmd,"storio_killer.sh"); 

    // Launch killer script
    ret = system(cmd);
    if (-1 == ret) {
        DEBUG("system command failed: %s", strerror(errno));
    }

    conf.io_port = 0;

    /*
    ** Then start storio
    */
    if (storaged_config.multiio==0) {
      p = cmd;
      p += sprintf(p, "storio_starter.sh storio -i 0 -c %s ", storaged_config_file);
      if (storaged_hostname) p += sprintf (p, "-H %s", storaged_hostname);
      p += sprintf(p, "&");

      // Launch storio_starter script
      ret = system(cmd);
      if (-1 == ret) {
          DEBUG("system command failed: %s", strerror(errno));
      }
      conf.io_port++;
    }
    else {
      int idx;
      for (idx = 0; idx < storaged_nb_ports; idx++) {
        p = cmd;
        p += sprintf(p, "storio_starter.sh storio -i %d -c %s ", idx+1, storaged_config_file);
        if (storaged_hostname) p += sprintf (p, "-H %s", storaged_hostname);
        p += sprintf(p, "&");

        // Launch storio_starter script
        ret = system(cmd);
        if (-1 == ret) {
            DEBUG("system command failed: %s", strerror(errno));
        }
        conf.io_port++;
      }
    }

    // Create the debug thread of the parent
    conf.instance_id = 0;
   conf.debug_port = rozofs_get_service_port_storaged_diag();

    if (storaged_hostname != NULL) strcpy(conf.hostname, storaged_hostname);
    else conf.hostname[0] = 0;
    
    storaged_start_nb_th(&conf);
}

void usage() {

    printf("RozoFS storage daemon - %s\n", VERSION);
    printf("Usage: storaged [OPTIONS]\n\n");
    printf("   -h, --help\t\t\tprint this message.\n");
    printf("   -H, --host=storaged-host\tspecify the hostname to use for build pid name (default: none).\n");
    printf("   -c, --config=config-file\tspecify config file to use (default: %s).\n",
            STORAGED_DEFAULT_CONFIG);
    printf("   -r, --rebuild=exportd-host\trebuild data for this storaged and get information from exportd-host.\n");
    printf("                             \texportd-host is a list of \'/\' separated host where exportd is running\n");
    printf("   -m, --multiio\t\twhen set, the storaged starts as many storio as listening port exist in the config file.\n");
}

int main(int argc, char *argv[]) {
    int c;
    int  multiio=0; /* Default is one storio */
    char pid_name[256];
    static struct option long_options[] = {
        { "help", no_argument, 0, 'h'},
        { "config", required_argument, 0, 'c'},
        { "rebuild", required_argument, 0, 'r'},
        { "host", required_argument, 0, 'H'},
        { "multiio", no_argument, 0, 'm'},
        { 0, 0, 0, 0}
    };

    // Init of the timer configuration
    rozofs_tmr_init_configuration();

    storaged_hostname = NULL;

    while (1) {

        int option_index = 0;
        c = getopt_long(argc, argv, "hmc:r:H:", long_options, &option_index);

        if (c == -1)
            break;

        switch (c) {

            case 'h':
                usage();
                exit(EXIT_SUCCESS);
                break;

            case 'm':
                multiio = 1;
                break;
            case 'c':
                if (!realpath(optarg, storaged_config_file)) {
                    fprintf(stderr, "storaged failed: %s %s\n", optarg,
                            strerror(errno));
                    exit(EXIT_FAILURE);
                }
                break;
            case 'r':
                if (strncpy(rbs_export_hostname, optarg, ROZOFS_HOSTNAME_MAX)
                        == NULL) {
                    fprintf(stderr, "storaged failed: %s %s\n", optarg,
                            strerror(errno));
                    exit(EXIT_FAILURE);
                }
                rbs_start_process = 1;
                break;
            case 'H':
                storaged_hostname = strdup(optarg);
                break;
            case '?':
                usage();
                exit(EXIT_SUCCESS);
                break;
            default:
                usage();
                exit(EXIT_FAILURE);
                break;
        }
    }
    openlog("storaged", LOG_PID, LOG_DAEMON);

    // Initialize the list of storage config
    if (sconfig_initialize(&storaged_config) != 0) {
        fprintf(stderr, "Can't initialize storaged config: %s.\n",
                strerror(errno));
        goto error;
    }
    // Read the configuration file
    if (sconfig_read(&storaged_config, storaged_config_file) != 0) {
        fprintf(stderr, "Failed to parse storage configuration file: %s.\n",
                strerror(errno));
        goto error;
    }
    // Check the configuration
    if (sconfig_validate(&storaged_config) != 0) {
        fprintf(stderr, "Inconsistent storage configuration file: %s.\n",
                strerror(errno));
        goto error;
    }
    // Check rebuild storage configuration if necessary
    if (rbs_start_process == 1) {
        if (rbs_check() != 0)
            goto error;
    }

    char *pid_name_p = pid_name;
    if (storaged_hostname != NULL) {
        sprintf(pid_name_p, "%s_%s.pid", STORAGED_PID_FILE, storaged_hostname);
    } else {
        sprintf(pid_name_p, "%s.pid", STORAGED_PID_FILE);
    }
    
    /*
    ** When -m is set force multiple storio mode
    */
    if (multiio) {
      storaged_config.multiio = 1; 
    }

    daemon_start("storaged", storaged_config.nb_cores, pid_name, on_start,
            on_stop, NULL);

    exit(0);
error:
    fprintf(stderr, "Can't start storaged. See logs for more details.\n");
    exit(EXIT_FAILURE);
}
