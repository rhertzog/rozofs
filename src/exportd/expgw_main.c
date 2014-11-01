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
#include <inttypes.h>
#include <pthread.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <sys/resource.h>
#include <unistd.h>
#include <libintl.h>
#include <rpc/rpc.h>
#include <rpc/pmap_clnt.h>
#include <getopt.h>
#include <time.h>

#include <rozofs/rozofs_srv.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/list.h>
#include <rozofs/rpc/export_profiler.h>
#include <rozofs/rpc/eproto.h>
#include <rozofs/rpc/epproto.h>
#include <rozofs/rpc/mclient.h>
#include <rozofs/rpc/eclient.h>
#include <rozofs/core/north_lbg_api.h>
#include "expgw_main.h"
#include "expgw_export.h"
#include <rozofs/core/expgw_common.h>
#include <rozofs/rozofs_debug_ports.h>
#include <rozofs/core/rozofs_timer_conf_dbg.h>
#include <rozofs/core/rozofs_core_files.h>


#define EXPGW_PID_FILE "expgw"

uint32_t expgw_local_ipaddr = INADDR_ANY;
char storage_process_filename[NAME_MAX];


DEFINE_PROFILING(epp_profiler_t) = {0};

/**
 * data structure used to store the configuration parameter of a expgw process
 */
typedef struct expgw_conf {
    char *host; /**< hostname of the export from which the expgw will get the mstorage configuration  */
    char *localhost; /**< local host name  */
    char *export; /**< pathname of the exportd (unique) */
    char *passwd; /**< user password */
    char *mount; /**< mount point */
    int module_index; /**< expgw instance number within the exportd: more that one expgw processes can be started */
    unsigned buf_size;
    unsigned max_retry;
    unsigned listening_port_base;
    unsigned rozofsmount_instance;
} expgw_conf;


#define SHOW_PROFILER_PROBE(probe) pChar += sprintf(pChar," %18s | %15"PRIu64"  | %9"PRIu64"  | %18"PRIu64"  |\n",\
					#probe,\
					gprofiler.probe[P_COUNT],\
					gprofiler.probe[P_COUNT]?gprofiler.probe[P_ELAPSE]/gprofiler.probe[P_COUNT]:0,\
					gprofiler.probe[P_ELAPSE]);

#define SHOW_PROFILER_PROBE_BYTE(probe) pChar += sprintf(pChar," %14s | %15"PRIu64"  | %9"PRIu64"  | %18"PRIu64"  | %15"PRIu64" \n",\
					#probe,\
					gprofiler.probe[P_COUNT],\
					gprofiler.probe[P_COUNT]?gprofiler.probe[P_ELAPSE]/gprofiler.probe[P_COUNT]:0,\
					gprofiler.probe[P_ELAPSE],\
                    gprofiler.probe[P_BYTES]);

static char * show_profiler_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"profiler reset       : reset statistics\n");
  pChar += sprintf(pChar,"profiler             : display statistics\n");  
  return pChar; 
}
void show_profiler(char * argv[], uint32_t tcpRef, void *bufRef) {

    char *pChar = uma_dbg_get_buffer();

    pChar += sprintf(pChar, "GPROFILER version %s uptime = %llu\n", gprofiler.vers, (long long unsigned int) gprofiler.uptime);
    pChar += sprintf(pChar, "   procedure        |     count        |  time(us)  | cumulated time(us)  |     bytes       \n");
    pChar += sprintf(pChar, "--------------------+------------------+------------+---------------------+-----------------\n");
#if 1
    SHOW_PROFILER_PROBE(ep_lookup);
    SHOW_PROFILER_PROBE(ep_getattr);
    SHOW_PROFILER_PROBE(ep_setattr);
    SHOW_PROFILER_PROBE(ep_readlink);
    SHOW_PROFILER_PROBE(ep_mknod);
    SHOW_PROFILER_PROBE(ep_unlink);
    SHOW_PROFILER_PROBE(ep_mkdir);
    SHOW_PROFILER_PROBE(ep_rmdir);
    SHOW_PROFILER_PROBE(ep_symlink);
    SHOW_PROFILER_PROBE(ep_rename);
    SHOW_PROFILER_PROBE(ep_readdir);
    SHOW_PROFILER_PROBE(ep_read_block);
    SHOW_PROFILER_PROBE(ep_write_block);
    SHOW_PROFILER_PROBE(ep_link);
    SHOW_PROFILER_PROBE(ep_setxattr);
    SHOW_PROFILER_PROBE(ep_getxattr);
    SHOW_PROFILER_PROBE(ep_removexattr);
    SHOW_PROFILER_PROBE(ep_listxattr);
	SHOW_PROFILER_PROBE(gw_invalidate);
	SHOW_PROFILER_PROBE(gw_invalidate_all);
	SHOW_PROFILER_PROBE(gw_configuration);
	SHOW_PROFILER_PROBE(gw_poll);

#endif
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}



void show_fid_cache(char * argv[], uint32_t tcpRef, void *bufRef) {

    char *pChar = uma_dbg_get_buffer();
    
    com_cache_show_cache_stats(pChar,expgw_fid_cache_p,"FID CACHE");

    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}


void show_attr_cache(char * argv[], uint32_t tcpRef, void *bufRef) {

    char *pChar = uma_dbg_get_buffer();
    
    com_cache_show_cache_stats(pChar,expgw_attr_cache_p,"ATTRIBUTES CACHE");

    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}




void show_exp_routing_table(char * argv[], uint32_t tcpRef, void *bufRef) {

    char *pChar = uma_dbg_get_buffer();
    
    expgw_display_all_exportd_routing_table(pChar);

    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}


void show_eid_exportd_assoc(char * argv[], uint32_t tcpRef, void *bufRef) {

    char *pChar = uma_dbg_get_buffer();
    
    expgw_display_all_eid(pChar);

    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}


/*__________________________________________________________________________
 */
/**
 *  Global and local datas
 */
static expgw_conf conf;
uint32_t *rozofs_expgw_cid_table[ROZOFS_CLUSTERS_MAX];



/**
 *  Signal catching
 */

static void expgw_handle_signal(int sig) {
    if (storage_process_filename[0] != 0) {
      unlink(storage_process_filename);
    }  
}




void usage() {
    printf("Rozofs storage client daemon - %s\n", VERSION);
    printf("Usage: expgw -i <instance> [OPTIONS]\n\n");
    printf("\t-h, --help\tprint this message.\n");
    printf("\t-P,--port LISTENING_PORT_BASE\t\trozofsmount,exportd (default: none) \n");
    printf("\t-D,--debug_port DEBUG_PORT_BASE\t\tdebug port (default: none) \n");
    printf("\t-L,--local LOCAL_HOST\t\tdefine address (or dns name) of the local host \n");
}

int main(int argc, char *argv[]) {
    int c;
    int ret;
    int val;
    int debug_port;
    static struct option long_options[] = {
        { "help", no_argument, 0, 'h'},
        { "local", required_argument, 0, 'L'},
        { "port", required_argument, 0, 'P'},
        { "debug_port", required_argument, 0, 'D'},
        { 0, 0, 0, 0}
    };

   storage_process_filename[0] = 0;

    conf.host = NULL;
    conf.localhost = NULL;
    conf.passwd = NULL;
    conf.export = NULL;
    conf.mount = NULL;
    conf.module_index = -1;
    conf.buf_size = 256;
    conf.max_retry = 3;
    conf.listening_port_base = 0;
    conf.rozofsmount_instance = 0;
    debug_port = rzdbg_default_base_port;
   
    while (1) {

        int option_index = 0;
        c = getopt_long(argc, argv, "hP:L:", long_options, &option_index);

        if (c == -1)
            break;

        switch (c) {
            case 'h':
                usage();
                exit(EXIT_SUCCESS);
                break;
            case 'L':
                conf.localhost = strdup(optarg);
                break;

            case 'P':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                conf.listening_port_base = val;
                break;
            case 'D':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                debug_port = val;
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
    /*
    ** check the listening base port is configured
    */
    if (conf.listening_port_base == 0)
    {
        severe("Listening Base port configuration missing: ");
        goto error;               
    }
    /*
    ** get the IP address of the local host
    */
    if (conf.localhost != NULL)
    {
       if (expgw_host2ip(conf.localhost,&expgw_local_ipaddr) < 0)
       {
        severe("Cannot get local IP address: %s\n",conf.localhost);
        goto error;       
       
       }
    }
    char name[128];
    {
      if (conf.localhost != NULL)
      {
        sprintf(name, "expgw %s", conf.localhost);
      }
      else
      {
        sprintf(name, "expgw");        
      }
      uma_dbg_set_name(name);
    }
    openlog(name, LOG_PID, LOG_DAEMON);

    rozofs_signals_declare("expgw", 1);
    rozofs_attach_crash_cbk(expgw_handle_signal);

    char *pid_name_p = storage_process_filename;
    if (conf.localhost != NULL) {
        sprintf(pid_name_p, "%s%s_%s:%d.pid", DAEMON_PID_DIRECTORY, EXPGW_PID_FILE, conf.localhost , conf.listening_port_base);
    } else {
        sprintf(pid_name_p, "%s%s:%d.pid", DAEMON_PID_DIRECTORY, EXPGW_PID_FILE, conf.listening_port_base);
    }
    int ppfd;
    if ((ppfd = open(storage_process_filename, O_RDWR | O_CREAT, 0640)) < 0) {
        severe("can't open process file");
    } else {
        char str[10];
        sprintf(str, "%d\n", getpid());
        write(ppfd, str, strlen(str));
        close(ppfd);
    }

    /*
     ** init of the non blocking part
     */
    ret = expgw_non_blocking_init(debug_port+RZDBG_EXPGW_PORT, expgw_local_ipaddr);
    if (ret < 0) {
        severe("Fatal error while initializing non blocking entity\n");
        goto error;
    }
    
    rozofs_timer_conf_dbg_init();

    /*
     ** Init of the north interface (read/write request processing)
     */
    ret = expgw_rozofs_north_interface_init(
            expgw_local_ipaddr, conf.listening_port_base+EXPGW_PORT_ROZOFSMOUNT_IDX,
            EXPGW_NORTH_LBG_BUF_RECV_CNT, EXPGW_NORTH_LBG_BUF_RECV_SZ);
    if (ret < 0) {
        severe("Fatal error on expgw_rozofs_north_interface_init()\n");
        goto error;
    }

    /*
     ** Init of the north interface (interface with exportd for configuration and cache consistency)
     */
    ret = expgw_exportd_north_interface_init(
            expgw_local_ipaddr, conf.listening_port_base+EXPGW_PORT_EXPORTD_IDX,
            EXPGW_NORTH_LBG_BUF_RECV_CNT, EXPGW_NORTH_LBG_BUF_RECV_SZ);
    if (ret < 0) {
        severe("Fatal error on expgw_exportd_north_interface_init()\n");
        goto error;
    }




    /*
    ** init of fid and attribute cache
    */
    ret = expgw_fid_cache_init();
    if (ret < 0) {
        severe("Fatal error on expgw_fid_cache_init()\n");
        goto error;
    }

    ret = expgw_attr_cache_init();
    if (ret < 0) {
        severe("Fatal error on expgw_fid_cache_init()\n");
        goto error;
    } 
    /*
    ** init of the exportd/eid routing table
    */
    expgw_export_tableInit();
    /*
     ** add the topic for the local profiler
     */
    uma_dbg_addTopic_option("profiler", show_profiler,UMA_DBG_OPTION_RESET);
    /*
    ** add debug entry for fid and attributes caches
    */
    uma_dbg_addTopic("fid_cache", show_fid_cache);
    uma_dbg_addTopic("attr_cache", show_attr_cache);
    uma_dbg_addTopic("exp_route", show_exp_routing_table);
    uma_dbg_addTopic("exp_eid", show_eid_exportd_assoc);
    
    /*
     ** main loop
     */
    info("Export Gateway non blocking thread started: debug port %d",
                                conf.listening_port_base+EXPGW_PORT_DEBUG_IDX);

    while (1) {
        ruc_sockCtrl_selectWait();
    }

error:
    severe("see log for more details.\n");
    exit(EXIT_FAILURE);
}
