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
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <uuid/uuid.h>
#include <netdb.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/rpc/eproto.h>
#include <rozofs/core/expgw_common.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include "export_expgateway_conf.h"


uint32_t export_nb_gateways;  /**< number of gateways in the configuration */
export_expgw_conf_ctx_t export_expgw_conf_table[EXPGW_EXPGW_MAX_IDX];

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



/*__________________________________________________________________________
*/
/**
* init of the configuration gateway 

  @param p : pointer to the context
  @param rank : index of the export gateway
  
  @retval none;
*/
void export_expgw_conf_ctx_init(export_expgw_conf_ctx_t *p,int rank)
{
   ruc_listEltInit(&p->link);
   p->index = rank;
   p->conf_state = EPGW_CONF_UNKNOWN;
   p->poll_conf_tx_state = EPGW_TX_IDLE;
   p->free = TRUE;
   p->port = 0;
   p->ipaddr = 0;
   p->current_conf_idx = 0;
   p->hostname[0] = 0;
   p->gateway_lbg_id = -1;
   memset(&p->stats,0,sizeof(export_expgw_conf_stats_t));
}



/*__________________________________________________________________________
*/
/**
*  Deletion of a context

  @param rank rank of the context to delete

*/
void export_expgw_conf_ctx_delete(int rank)
{
  export_expgw_conf_ctx_t *p = export_expgw_conf_table;
  int ret;

  if (rank >= EXPGW_EXPGW_MAX_IDX) return;

  p+=rank;

  /*
  ** Delete the load balancing group if it exists
  */
  if (p->gateway_lbg_id!= -1)
  {
    ret = north_lbg_delete(p->gateway_lbg_id);
    if (ret < 0)
    {
      severe("error on load bala,cing group deletion: %d ",p->gateway_lbg_id);
    }
    p->gateway_lbg_id = -1;
    export_expgw_conf_ctx_init(p,rank); 
  }
}

/*__________________________________________________________________________
*/
/**
*  Create of a context

  @param rank rank of the context to create
  @param hostname : hostname of the export gateway
  @param port : configuration port of the export gateway

*/
int export_expgw_conf_ctx_create(int rank,char *hostname,uint16_t port)
{
  export_expgw_conf_ctx_t *p = export_expgw_conf_table;
  char bufname[32];
  int ret = 0;

  if (rank >= EXPGW_EXPGW_MAX_IDX) 
  {
    errno = EINVAL;
    return -1;
  }
  p+=rank;
  if (p->gateway_lbg_id!= -1)
  {
    export_expgw_conf_ctx_delete(rank);  
  }
  /*
  ** create the load balancing group
  */
  strncpy(p->hostname, hostname, ROZOFS_HOSTNAME_MAX);
  if (rozofs_host2ip(hostname,&p->ipaddr) < 0)
  {
    return -1;  
  }
  p->port  = port;  
  /*
  ** store the IP address and port in the list of the endpoint
  */
  my_list[0].remote_port_host = p->port;
  my_list[0].remote_ipaddr_host = p->ipaddr ;
  int lbg_size = 1;

   af_inet_exportd_conf.recv_srv_type = ROZOFS_RPC_SRV;
   af_inet_exportd_conf.rpc_recv_max_sz = rozofs_large_tx_recv_size;
   sprintf(bufname,"GWCNF_%d",rank);
   p->gateway_lbg_id = north_lbg_create_af_inet(bufname,INADDR_ANY,0,my_list,ROZOFS_SOCK_FAMILY_EXPORT_NORTH,lbg_size,&af_inet_exportd_conf);
   if (p->gateway_lbg_id >= 0)
   {
     return 0;    
   }
   severe("Cannot create Load Balancing Group for Export Gateway configurator");
   return  ret;
}

/*__________________________________________________________________________
*/
/**
* Init of the export -> export gateway configuration Module
  param none
  
  @retval always RUC_OK
*/
int export_expgw_conf_moduleInit()
{
   int i;
   export_expgw_conf_ctx_t *p;
   
   p = export_expgw_conf_table;
   
   for (i = 0; i < EXPGW_EXPGW_MAX_IDX; i++,p++)
   {
    export_expgw_conf_ctx_init(p,i);   
   }
   export_nb_gateways = 0;
   return RUC_OK;

}

