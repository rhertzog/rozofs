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



#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <errno.h>
#include <string.h>
#include <rpc/pmap_clnt.h>
#include <unistd.h>

#include <rozofs/core/rozofs_socket_family.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/core/ruc_common.h>
#include <rozofs/core/ruc_sockCtl_api.h>
#include <rozofs/core/rozofs_tx_api.h>
#include <rozofs/core/north_lbg_api.h>
#include <rozofs/core/rozofs_host_list.h>
#include <rozofs/rpc/eclient.h>
#include <rozofs/rpc/eproto.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include "rpcclt.h"
#include "storcli_lbg_prototypes.h"

static north_remote_ip_list_t my_list[32];  /**< list of the connection for the exportd */

 /**
 *  socket configuration for the family
 */
static af_unix_socket_conf_t  af_inet_exportd_conf =
{
  1,  //           family: identifier of the socket family    */
  0,         /**< instance number within the family   */
  sizeof(uint32_t),  /* headerSize  -> size of the header to read                 */
  0,                 /* msgLenOffset->  offset where the message length fits      */
  sizeof(uint32_t),  /* msgLenSize  -> size of the message length field in bytes  */
  
  (1024*256), /*  bufSize        -> length of buffer (xmit and received)        */
  (300*1024), /*  so_sendbufsize -> length of buffer (xmit and received)        */
  rozofs_tx_userRcvAllocBufCallBack, /*  userRcvAllocBufCallBack -> user callback for buffer allocation             */
  rozofs_tx_recv_rpc_cbk,            /*  userRcvCallBack         -> callback provided by the connection owner block */
  rozofs_tx_xmit_abort_rpc_cbk,      /*  userDiscCallBack        ->callBack for TCP disconnection detection         */
  NULL,                              /* userConnectCallBack     -> callback for client connection only              */
  NULL,  //    userXmitDoneCallBack; /**< optional call that must be set when the application when to be warned when packet has been sent */
  NULL,  //    userRcvReadyCallBack; /* NULL for default callback                    */
  NULL,  //    userXmitReadyCallBack; /* NULL for default callback                    */
  NULL,  //    userXmitEventCallBack; /* NULL for default callback                    */
  rozofs_tx_get_rpc_msg_len_cbk,        /* userHdrAnalyzerCallBack ->NULL by default, function that analyse the received header that returns the payload  length  */
  ROZOFS_RPC_SRV,       /* recv_srv_type ---> service type for reception : ROZOFS_RPC_SRV or ROZOFS_GENERIC_SRV  */
  0,       /*   rpc_recv_max_sz ----> max rpc reception buffer size : required for ROZOFS_RPC_SRV only */
  NULL,  //    *userRef;           /* user reference that must be recalled in the callbacks */
  NULL,  //    *xmitPool; /* user pool reference or -1 */
  NULL   //    *recvPool; /* user pool reference or -1 */
};
/*
**____________________________________________________
*/
/*
  Request port mapper to give the port number of a TCP rpc service on a remote host
  @param host  The remote host name
  @param prog  The program number
  @param vers  The version number of the program
*/
static int get_service_tcp_port(char *host ,unsigned long prog, unsigned long vers) {
  struct sockaddr_in server;
  int port = 0;

  server.sin_family = AF_INET;

  if (rozofs_host2ip_netw((char*)host, &server.sin_addr.s_addr) != 0) {
      severe("rozofs_host2ip failed for host : %s, %s", host,
              strerror(errno));
      return 0;
  }

  if ((port = pmap_getport(&server, prog, vers, IPPROTO_TCP)) == 0) {
    //warning("pmap_getport failed %s (%x:%d) %s",  host, (unsigned int)prog, (int)vers, clnt_spcreateerror(""));
    errno = EPROTO;
    return 0;
  }
  
  info("rpc service %s (%x:%d) available on port %d",  host, (unsigned int)prog, (int)vers, port);
  return port;
}     
/*
**____________________________________________________
*/
/*
  Periodic timer expiration
*/
static void export_lbg_periodic_ticker(void * param) {
  int status;
  uint16_t port;
  exportclt_t *exportclt = (exportclt_t *) param;
   
  /* Check whether the export LBG is up ,*/
  status = north_lbg_get_state(exportclt->rpcclt.lbg_id);
//  info("export_lbg_periodic_ticker status %d\n", status);
  
  if (status == NORTH_LBG_UP) return;

  /* Try to find out whether the export service is up again */
  port = get_service_tcp_port(exportclt->host,EXPORT_PROGRAM, EXPORT_VERSION);
  if (port == 0) {
    return; /* Still unavailable */
  }
 
  /* The service is back again on a new port */
  my_list[0].remote_port_host = port;
  north_lbg_re_configure_af_inet_destination_port(exportclt->rpcclt.lbg_id, my_list, 1); 
}
/*
**____________________________________________________
*/
/*
  start a periodic timer to chech wether the export LBG is down
  When the export is restarted its port may change, and so
  the previous configuration of the LBG is not valid any more
*/
static void export_lbg_start_timer(exportclt_t *exportclt) {
  struct timer_cell * export_lbg_periodic_timer;

  export_lbg_periodic_timer = ruc_timer_alloc(0,0);
  if (export_lbg_periodic_timer == NULL) {
    severe("export_lbg_start_timer");
    return;
  }
  ruc_periodic_timer_start (export_lbg_periodic_timer, 
                            4000,
 	                    export_lbg_periodic_ticker,
 			    exportclt);

}

/*
**__________________________________________________________________________
*/
/**
*  Init of the load balancing group associated with the exportd
   (used by rozofsmount only)
   
   @param exportclt: data structure that describes the remote exportd
   @param prog: export program name
   @param vers: exportd program version
   @param port_num: tcp port of the exportd (0 for dynamic port )
   @param supervision_callback: supervision callback (NULL if none)
   
   @retval 0 on success
   @retval < 0 on error (see errno for details)
*/
int export_lbg_initialize(exportclt_t *exportclt ,unsigned long prog,
        		  unsigned long vers,uint32_t port_num,
			  af_stream_poll_CBK_t supervision_callback) 
{
    int status = -1;
    struct sockaddr_in server;
    int port = 0;
    int lbg_size;
    int export_index=0;
    char * pHost;
    int ret;
    
    DEBUG_FUNCTION;    
    rpcclt_t * client = &exportclt->rpcclt;

    server.sin_family = AF_INET;

    lbg_size = 0;
    for (export_index=0; export_index < ROZOFS_HOST_LIST_MAX_HOST; export_index++) { 

        pHost = rozofs_host_list_get_host(export_index);
	    if (pHost == NULL) break;
	if (rozofs_host2ip_netw((char*)pHost, &server.sin_addr.s_addr) != 0) {
            severe("rozofs_host2ip failed for host : %s, %s", pHost,
                    strerror(errno));
            continue;
	}

	 if (port_num == 0) {
         if ((port = pmap_getport(&server, prog, vers, IPPROTO_TCP)) == 0) {
             warning("pmap_getport failed%s", clnt_spcreateerror(""));
             errno = EPROTO;
             goto out;
         }
         server.sin_port = htons(port);
	 } else {
         server.sin_port = htons(port_num);
	 }
	/*
	** store the IP address and port in the list of the endpoint
	*/
	my_list[lbg_size].remote_port_host = ntohs(server.sin_port);
	my_list[lbg_size].remote_ipaddr_host = ntohl(server.sin_addr.s_addr);
	lbg_size++;
    }
    if (lbg_size == 0) goto out;
    
     af_inet_exportd_conf.recv_srv_type = ROZOFS_RPC_SRV;
     af_inet_exportd_conf.rpc_recv_max_sz = rozofs_large_tx_recv_size;
     
     /*
     ** allocate a load balancing group
     */
     client->lbg_id = north_lbg_create_no_conf();
     if (client->lbg_id < 0)
     {     
        /*
	** cannot create the load balancing group
	*/
	severe("Out of lbg context while creating EXPORTD load balancing group");
	goto out;
     }
     /*
     ** put the supervision callback if any declared
     */
 #if 1
     if (supervision_callback != NULL)
     {	
       ret = north_lbg_attach_application_supervision_callback(client->lbg_id,supervision_callback);
       if (ret < 0)
       {
          /*
	  ** cannot create the load balancing group
	  */
	  severe("failure while configuring EXPORTD load balancing group");
	  goto out;     
       }
       ret = north_lbg_set_application_tmo4supervision(client->lbg_id,3);
       if (ret < 0)
       {
          /*
	  ** cannot create the load balancing group
	  */
	  severe("failure while configuring EXPORTD load balancing group");
	  goto out;     
       }
       north_lbg_set_active_standby_mode(client->lbg_id);
     }
#endif
     client->lbg_id = north_lbg_configure_af_inet(client->lbg_id,"METADATA",INADDR_ANY,0,my_list,ROZOFS_SOCK_FAMILY_EXPORT_NORTH,
                                                  lbg_size,&af_inet_exportd_conf,0);
     if (client->lbg_id >= 0)
     {
       status = 0;
       /*
       ** the timer is started only to address the case of a dynamic port
       */
       if (port_num == 0) export_lbg_start_timer (exportclt);      
       return status;    
     }
     severe("Cannot create Load Balancing Group for Exportd");

out:
     return  status;
}     

