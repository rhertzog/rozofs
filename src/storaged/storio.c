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

#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/list.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/common/daemon.h>
#include <rozofs/common/profile.h>
#include <rozofs/rpc/mproto.h>
#include <rozofs/rpc/sproto.h>
#include <rozofs/rpc/spproto.h>
#include <rozofs/core/rozofs_core_files.h>
#include <rozofs/core/rozofs_ip_utilities.h>

#include "config.h"
#include "sconfig.h"
#include "storage.h"
#include "storaged.h"
#include <rozofs/rozofs_timer_conf.h>
#include "storio_nblock_init.h"
#include "storio_crc32.h"

#define STORIO_PID_FILE "storio"
int     storio_instance = 0;
static char storaged_config_file[PATH_MAX] = STORAGED_DEFAULT_CONFIG;

sconfig_t storaged_config;

static storage_t storaged_storages[STORAGES_MAX_BY_STORAGE_NODE] = {
    {0}
};

static char *storaged_hostname = NULL;

static uint16_t storaged_nrstorages = 0;


extern void storage_program_1(struct svc_req *rqstp, SVCXPRT *ctl_svc);

extern void storaged_profile_program_1(struct svc_req *rqstp, SVCXPRT *ctl_svc);

//uint32_t storaged_storage_ports[STORAGE_NODE_PORTS_MAX] = {0};
uint8_t storaged_nb_ports = 0;
uint8_t storaged_nb_io_processes = 0;

DEFINE_PROFILING(spp_profiler_t) = {0};

    uint32_t ipadd_hostformat ; /**< storio IP address in host format */
    uint16_t  port_hostformat ;/**< storio port in host format */

static int storaged_initialize() {
    int status = -1;
    list_t *p = NULL;
    DEBUG_FUNCTION;

    /* Initialize rozofs constants (redundancy) */
    rozofs_layout_initialize();

    storaged_nrstorages = 0;

    storaged_nb_io_processes = 1;
    storaged_nb_ports        = storaged_config.io_addr_nb;

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

char storage_process_filename[NAME_MAX];

/**
 *  Signal catching
 */

static void on_stop(int sig) {
    if (storage_process_filename[0] != 0) {
      unlink(storage_process_filename);
    }  
    closelog();
}

static void on_start(void) {
    storaged_start_conf_param_t conf;

    DEBUG_FUNCTION;


    rozofs_signals_declare("storio", storaged_config.nb_cores);
    rozofs_attach_crash_cbk(on_stop);

    /*
    ** Save the process PID in PID directory 
    */

    storage_process_filename[0] = 0;
    char *pid_name_p = storage_process_filename;
    if (storaged_hostname != NULL) {
        sprintf(pid_name_p, "%s%s_%s.%d.pid", DAEMON_PID_DIRECTORY, STORIO_PID_FILE, storaged_hostname,storio_instance);
    } else {
        sprintf(pid_name_p, "%s%s.%d.pid", DAEMON_PID_DIRECTORY,STORIO_PID_FILE,storio_instance);
    }
    int ppfd;
    if ((ppfd = open(storage_process_filename, O_RDWR | O_CREAT, 0640)) < 0) {
        severe("can't open process file");
    } else {
        char str[10];
        sprintf(str, "%d\n", getpid());
        if (write(ppfd, str, strlen(str))<0) {
            severe("can't write process file");
	}
        close(ppfd);
    }

    /**
     * start the non blocking thread
     */
    
    conf.instance_id = storio_instance;
    conf.debug_port = rozofs_get_service_port_storio_diag(storio_instance); 
    if (storaged_hostname != NULL) 
      strcpy(conf.hostname, storaged_hostname);
    else 
      conf.hostname[0] = 0;
      
    storio_start_nb_th(&conf);
}


void usage() {

    printf("Rozofs storage daemon - %s\n", VERSION);
    printf("Usage: storaged [OPTIONS]\n\n");
    printf("\t-h, --help\tprint this message.\n");
    printf("\t-i,--instance instance number\t\tstorage io instance number \n");
    printf("\t-H,--host storaged hostname\t\tused to build to the pid name (default: none) \n");
    printf("\t-c, --config\tconfig file to use (default: %s).\n",
            STORAGED_DEFAULT_CONFIG);
}

int main(int argc, char *argv[]) {
    int c;
    int val;
    static struct option long_options[] = {
        { "help", no_argument, 0, 'h'},
        {"config", required_argument, 0, 'c'},
        { "host", required_argument, 0, 'H'},
        { "instance", required_argument, 0, 'i'},
        { 0, 0, 0, 0}
    };

    /*
     ** init of the timer configuration
     */
    rozofs_tmr_init_configuration();

    storaged_hostname = NULL;

    while (1) {

        int option_index = 0;
        c = getopt_long(argc, argv, "hc:r:H:i:", long_options, &option_index);

        if (c == -1)
            break;

        switch (c) {

            case 'h':
                usage();
                exit(EXIT_SUCCESS);
                break;
            case 'c':
                if (!realpath(optarg, storaged_config_file)) {
                    fprintf(stderr, "storaged failed: %s %s\n", optarg,
                            strerror(errno));
                    exit(EXIT_FAILURE);
                }
                break;
            case 'H':
                storaged_hostname = strdup(optarg);
                break;
	    case 'i':	
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                storio_instance = val;
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
    
    openlog("storio", LOG_PID, LOG_DAEMON);
        
    // Initialize the list of storage config
    if (sconfig_initialize(&storaged_config) != 0) {
        fatal( "Can't initialize storaged config: %s.\n",strerror(errno));
    }
    // Read the configuration file
    if (sconfig_read(&storaged_config, storaged_config_file) != 0) {
        fatal("Failed to parse storage configuration file: %s.\n",strerror(errno));
    }
    // Check the configuration
    if (sconfig_validate(&storaged_config) != 0) {
        fatal( "Inconsistent storage configuration file: %s.\n",strerror(errno));
    }
    /*
    ** init of the crc32c
    */
    crc32c_init(storaged_config.crc32c_generate,
                storaged_config.crc32c_check,
                storaged_config.crc32c_hw_forced);
		
    // Initialization of the storage configuration
    if (storaged_initialize() != 0) {
        fatal("can't initialize storaged: %s.", strerror(errno));
    }

    SET_PROBE_VALUE(uptime, time(0));
    strncpy((char*) gprofiler.vers, VERSION, 20);
    SET_PROBE_VALUE(nb_io_processes, storaged_nb_io_processes);
    
    
    on_start();
    return 0;
}
