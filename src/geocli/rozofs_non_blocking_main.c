/*
  Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
  This file is part of Rozofs.

  Rozofs is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published
  by the Free Software Foundation, version 2.

  Rozofs is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for morozofs_fuse_initre details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see
  <http://www.gnu.org/licenses/>.
 */
#include <rozofs/core/ruc_common.h>
#include <rozofs/core/uma_tcp_main_api.h>
#include <rozofs/core/ruc_tcpServer_api.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/north_lbg_api.h>
#include <rozofs/core/rozofs_tx_api.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/rozofs_core_files.h>
#include "rozofsmount.h"
#include <rozofs/rpc/geo_replica_proto.h>
#include <rozofs/rpc/storcli_lbg_prototypes.h>
#include "geocli_srv.h"
#include "rzcp_file_ctx.h"

// For trace purpose
struct timeval Global_timeDay;
unsigned long long Global_timeBefore, Global_timeAfter;
static rozofs_fuse_conf_t *args_p;


uint32_t ruc_init(uint32_t test, uint16_t debug_port) {
    int ret;


    uint32_t mx_tcp_client = 10;
    uint32_t mx_tcp_server = 10;
    uint32_t mx_tcp_server_cnx = 10;
    uint32_t mx_af_unix_ctx = ROZO_AFUNIX_CTX_GEOCLI;
    uint32_t mx_lbg_north_ctx = 32;

    //#warning TCP configuration ressources is hardcoded!!
    /*
     ** trace buffer initialization
     */
    ruc_traceBufInit();
#if 0
    /*
     ** Not needed since there is already done
     ** by libUtil
     */

    /* catch the sigpipe signal for socket 
     ** connections with RELC(s) in this way when a RELC
     ** connection breaks an errno is set on a recv or send 
     **  socket primitive 
     */
    sigAction.sa_flags = SA_RESTART;
    sigAction.sa_handler = SIG_IGN; /* Mask SIGPIPE */
    if (sigaction(SIGPIPE, &sigAction, NULL) < 0) {
        exit(0);
    }
    sigAction.sa_flags = SA_RESTART;
    sigAction.sa_handler = hand; /*  */
    if (sigaction(SIGUSR1, &sigAction, NULL) < 0) {
        exit(0);
    }
#endif
    /*
     ** initialize the socket controller:
     **   4 connections per Relc and 32
     **   for: NPS, Timer, Debug, etc...
     */
    ret = ruc_sockctl_init(ROZO_SOCKCTRL_CTX_GEOCLI);
    if (ret != RUC_OK) {
        fatal( " socket controller init failed" );
    }

    /*
     **  Timer management init
     */
    ruc_timer_moduleInit(FALSE);

    while (1) {
        /*
         **--------------------------------------
         **  configure the number of TCP connection
         **  supported
         **--------------------------------------   
         **  
         */
        ret = uma_tcp_init(mx_tcp_client + mx_tcp_server + mx_tcp_server_cnx);
        if (ret != RUC_OK) break;

        /*
         **--------------------------------------
         **  configure the number of TCP server
         **  context supported
         **--------------------------------------   
         **  
         */
        ret = ruc_tcp_server_init(mx_tcp_server);
        if (ret != RUC_OK) break;

        /*
         **--------------------------------------
         **  configure the number of TCP client
         **  context supported
         **--------------------------------------   
         **  
         */
        //     ret = ruc_tcp_clientinit(mx_tcp_client);
        //     if (ret != RUC_OK) break;   


        /*
         **--------------------------------------
         **  configure the number of AF_UNIX
         **  context supported
         **--------------------------------------   
         **  
         */
        ret = af_unix_module_init(mx_af_unix_ctx,
                32, 1024, // xmit(count,size)
                32, 1024 // recv(count,size)
                );
        if (ret != RUC_OK) break;

        /*
         ** init of th emodule that handles the load balancing group
         ** there is one load balancing group for the exportd
         ** and one load balancing group per storaged
         */
#if 1
        ret = north_lbg_module_init(mx_lbg_north_ctx);
        if (ret != RUC_OK) break;

        ret = rozofs_tx_module_init(args_p->max_transactions+32, // fuse trx + internal trx
                args_p->max_transactions, 2048, // xmit small [count,size]
                args_p->max_transactions, (1024 * 258), // xmit large [count,size]
                args_p->max_transactions, 1024, // recv small [count,size]
                args_p->max_transactions, (1024 * 258)); // recv large [count,size];  

        if (ret != RUC_OK) break;
#endif    
        break;

    }



    /*
     ** internal debug init
     */
    //ruc_debug_init();


    /*
     **--------------------------------------
     **   D E B U G   M O D U L E
     **--------------------------------------
     */
    uma_dbg_init(10, INADDR_ANY, debug_port);

    return ret;
}

/*
 *_______________________________________________________________________
 */

/**
 *  This function is the entry point for setting rozofs in non-blocking mode

   @param args->ch: reference of the fuse channnel
   @param args->se: reference of the fuse session
   @param args->max_transactions: max number of transactions that can be handled in parallel
   
   @retval -1 on error
   @retval : no retval -> only on fatal error

 */
int rozofs_stat_start(void *args) {


    int ret;
    //sem_t semForEver;    /* semaphore for blocking the main thread doing nothing */
    args_p = args;
    
    exportclt_t *exportclt_p;
    
    exportclt_p = args_p->exportclt;

    uint16_t debug_port = args_p->debug_port;

    ret = ruc_init(FALSE, debug_port);
    if (ret != RUC_OK) {
        /*
         ** fatal error
         */
        return -1;
    }

    {
        char name[32];
        sprintf(name, "geocli %d", args_p->instance);
        uma_dbg_set_name(name);
    }

    /*
     ** Perform the init with exportd--> setup of the TCP connection associated with the load balancing group
     */
    if (georep_lbg_initialize((exportclt_t*) args_p->exportclt, GEO_PROGRAM, GEO_VERSION, rozofs_get_service_port_export_master_geo_replica()) != 0) {
        severe("Cannot setup the load balancing group towards geo replication module");
    }
    //#warning storcli instances are hardcoded
    if (storcli_lbg_initialize((exportclt_t*) args_p->exportclt,"geocli", args_p->instance, 1, 2) != 0) {
        severe("Cannot setup the load balancing group towards StorCli");
    }
    
    
    /*
    ** start the geo-replication service
    */
    ret = geocli_init(GEO_DEF_PERIO_MS,exportclt_p->eid,rozofs_site_number);
    if (ret < 0) return -1;
    /*
    ** init of the copy file module
    */
    ret = rzcp_module_init(RZCP_MAX_CTX);
    if (ret < 0) return -1;
    /*
     ** main loop
     */
    while (1) {
        ruc_sockCtrl_selectWait();
    }
    fatal( "main() code is rotten" );
}
