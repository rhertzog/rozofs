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
#include <sys/types.h> 
#include <sys/socket.h>
#include <fcntl.h> 
#include <sys/un.h>             
#include <errno.h>  
#include <time.h>
#include <pthread.h> 
#include <rozofs/core/ruc_common.h>
#include <rozofs/core/ruc_list.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/rozofs_socket_family.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rpc/rozofs_rpc_util.h>
#include <rozofs/rpc/sproto.h>
#include "storio_disk_thread_intf.h" 
#include "storage.h" 

int af_unix_disk_socket_ref = -1;
 
 #define MICROLONG(time) ((unsigned long long)time.tv_sec * 1000000 + time.tv_usec)



storage_t *storaged_lookup(cid_t cid, sid_t sid) ;

/**
*  Thread table
*/
#define ROZOFS_MAX_DISK_THREADS  32
rozofs_disk_thread_ctx_t rozofs_disk_thread_ctx_tb[ROZOFS_MAX_DISK_THREADS];

/*
**__________________________________________________________________________
*/
/**
   Creation of a AF_UNIX socket Datagram  in non blocking mode

   For the disk the socket is created in blocking mode
     
   @param name0fSocket : name of the AF_UNIX socket
   @param size: size in byte of the xmit buffer (the service double that value
   
    retval: >0 reference of the created AF_UNIX socket
    retval < 0 : error on socket creation 

*/
int af_unix_disk_sock_create_internal(char *nameOfSocket,int size)
{
  int ret;    
  int fd=-1;  
  struct sockaddr_un addr;
  int fdsize;
  unsigned int optionsize=sizeof(fdsize);

  /* 
  ** create a datagram socket 
  */ 
  fd=socket(PF_UNIX,SOCK_DGRAM,0);
  if(fd<0)
  {
    warning("af_unix_disk_sock_create_internal socket(%s) %s", nameOfSocket, strerror(errno));
    return -1;
  }
  /* 
  ** remove fd if it already exists 
  */
  ret = unlink(nameOfSocket);
  /* 
  ** named the socket reception side 
  */
  addr.sun_family= AF_UNIX;
  strcpy(addr.sun_path,nameOfSocket);
  ret=bind(fd,(struct sockaddr*)&addr,sizeof(addr));
  if(ret<0)
  {
    warning("af_unix_disk_sock_create_internal bind(%s) %s", nameOfSocket, strerror(errno));
    return -1;
  }
  /*
  ** change the length for the send buffer, nothing to do for receive buf
  ** since it is out of the scope of the AF_SOCKET
  */
  ret= getsockopt(fd,SOL_SOCKET,SO_SNDBUF,(char*)&fdsize,&optionsize);
  if(ret<0)
  {
    warning("af_unix_disk_sock_create_internal getsockopt(%s) %s", nameOfSocket, strerror(errno));
    return -1;
  }
  /*
  ** update the size, always the double of the input
  */
  fdsize=2*size;
  
  /* 
  ** set a new size for emission and 
  ** reception socket's buffer 
  */
  ret=setsockopt(fd,SOL_SOCKET,SO_SNDBUF,(char*)&fdsize,sizeof(int));
  if(ret<0)
  {
    warning("af_unix_disk_sock_create_internal setsockopt(%s,%d) %s", nameOfSocket, fdsize, strerror(errno));
    return -1;
  }

  return(fd);
}  


/*
**__________________________________________________________________________
*/
/**
* encode the RCP reply
    
  @param p       : pointer to the generic rpc context
  @param arg_ret : returned argument to encode 
  
  @retval none

*/
int storio_encode_rpc_response (rozorpc_srv_ctx_t *p,char * arg_ret) {
   uint8_t *pbuf;           /* pointer to the part that follows the header length */
   uint32_t *header_len_p;  /* pointer to the array that contains the length of the rpc message*/
   XDR xdrs;
   int len;

   if (p->xmitBuf == NULL) {
     // STAT
     severe("no xmit buffer");
     return -1;
   } 
   
   /*
   ** create xdr structure on top of the buffer that will be used for sending the response
   */ 
   header_len_p = (uint32_t*)ruc_buf_getPayload(p->xmitBuf); 
   pbuf = (uint8_t*) (header_len_p+1);            
   len = (int)ruc_buf_getMaxPayloadLen(p->xmitBuf);
   len -= sizeof(uint32_t);
   xdrmem_create(&xdrs,(char*)pbuf,len,XDR_ENCODE); 
   
   if (rozofs_encode_rpc_reply(&xdrs,(xdrproc_t)p->xdr_result,(caddr_t)arg_ret,p->src_transaction_id) != TRUE) {
     ROZORPC_SRV_STATS(ROZORPC_SRV_ENCODING_ERROR);
     severe("rpc reply encoding error");
     return -1;    
   }       
   /*
   ** compute the total length of the message for the rpc header and add 4 bytes more bytes for
   ** the ruc buffer to take care of the header length of the rpc message.
   */
   int total_len = xdr_getpos(&xdrs);
   *header_len_p = htonl(0x80000000 | total_len);
   total_len += sizeof(uint32_t);
   ruc_buf_setPayloadLen(p->xmitBuf,total_len);

   return 0;
}

/*__________________________________________________________________________
*/
/**
*  Read data from a file

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storio_disk_read(rozofs_disk_thread_ctx_t *thread_ctx_p,storio_disk_thread_msg_t * msg) {
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
  storage_t *st = 0;
  sp_read_arg_t          * args;
  rozorpc_srv_ctx_t      * rpcCtx;
  sp_read_ret_t            ret;
    
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);

  ret.status = SP_FAILURE;      
	          
  /*
  ** update statistics
  */
  thread_ctx_p->stat.diskRead_count++;
  
  rpcCtx = msg->rpcCtx;
  args   = (sp_read_arg_t*) ruc_buf_getPayload(rpcCtx->decoded_arg);


  // Get the storage for the couple (cid;sid)
  if ((st = storaged_lookup(args->cid, args->sid)) == 0) {
    ret.sp_read_ret_t_u.error = errno;
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.diskRead_badCidSid++ ;   
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }
    
  /*
  ** set the pointer to the bins
  */
  char *pbuf = ruc_buf_getPayload(rpcCtx->xmitBuf);
  pbuf += rpcCtx->position;     
  /*
  ** clear the length of the bins and set the pointer where data must be returned
  */  
  ret.sp_read_ret_t_u.rsp.bins.bins_val = pbuf;
  ret.sp_read_ret_t_u.rsp.bins.bins_len = 0;
#if 0 // for future usage with distributed cache 
  /*
  ** clear the optimization array
  */
  ret.sp_read_ret_t_u.rsp.optim.optim_val = (char*)sp_optim;
  ret.sp_read_ret_t_u.rsp.optim.optim_len = 0;
#endif   

  // Read projections
  if (storage_read(st, args->layout, (sid_t *) args->dist_set, args->spare,
            (unsigned char *) args->fid, args->bid, args->nb_proj,
            (bin_t *) ret.sp_read_ret_t_u.rsp.bins.bins_val,
            (size_t *) & ret.sp_read_ret_t_u.rsp.bins.bins_len,
            &ret.sp_read_ret_t_u.rsp.file_size) != 0) 
  {
    ret.sp_read_ret_t_u.error = errno;
    storio_encode_rpc_response(rpcCtx,(char*)&ret);
    if (errno == ENOENT)    thread_ctx_p->stat.diskRead_nosuchfile++;
    else if (!args->spare)  thread_ctx_p->stat.diskRead_error++;
    else                    thread_ctx_p->stat.diskRead_error_spare++;
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }  
 
  ret.status = SP_SUCCESS;  
  msg->size = ret.sp_read_ret_t_u.rsp.bins.bins_len;        
  storio_encode_rpc_response(rpcCtx,(char*)&ret);  
  thread_ctx_p->stat.diskRead_Byte_count += ret.sp_read_ret_t_u.rsp.bins.bins_len;
  storio_send_response(thread_ctx_p,msg,0);

  /*
  ** Update statistics
  */
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.diskRead_time +=(timeAfter-timeBefore);  
}
/*__________________________________________________________________________
*/
/**
*  Write data to a file

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storio_disk_write(rozofs_disk_thread_ctx_t *thread_ctx_p,storio_disk_thread_msg_t * msg) {
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
  storage_t *st = 0;
  sp_write_arg_no_bins_t * args;
  rozorpc_srv_ctx_t      * rpcCtx;
  sp_write_ret_t           ret;
  uint8_t                  version = 0;
  int                      size;
  
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);

  ret.status = SP_FAILURE;          
  
  /*
  ** update statistics
  */
  thread_ctx_p->stat.diskWrite_count++;
  
  rpcCtx = msg->rpcCtx;
  args   = (sp_write_arg_no_bins_t*) ruc_buf_getPayload(rpcCtx->decoded_arg);

  /*
  ** set the pointer to the bins
  */
  char *pbuf = ruc_buf_getPayload(rpcCtx->recv_buf); 
  pbuf += rpcCtx->position;
  
  /*
  ** Use received buffer for the response
  */
  rpcCtx->xmitBuf  = rpcCtx->recv_buf;
  rpcCtx->recv_buf = NULL;
  

  // Get the storage for the couple (cid;sid)
  if ((st = storaged_lookup(args->cid, args->sid)) == 0) {
    ret.sp_write_ret_t_u.error = errno;
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.diskWrite_badCidSid++ ;   
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }
  


  // Write projections
  size =  storage_write(st, args->layout, (sid_t *) args->dist_set, args->spare,
          (unsigned char *) args->fid, args->bid, args->nb_proj, version,
          &ret.sp_write_ret_t_u.file_size,
          (bin_t *) pbuf);
  if (size <= 0)  {
    ret.sp_write_ret_t_u.error = errno;
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.diskWrite_error++; 
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }
  
  ret.status = SP_SUCCESS;          
  msg->size = size;        
  storio_encode_rpc_response(rpcCtx,(char*)&ret);  
  thread_ctx_p->stat.diskWrite_Byte_count += size;
  storio_send_response(thread_ctx_p,msg,0);

  /*
  ** Update statistics
  */
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.diskWrite_time +=(timeAfter-timeBefore);  
}    
/**
*  Truncate a file

  @param thread_ctx_p: pointer to the thread context
  @param msg         : address of the message received
  
  @retval: none
*/
static inline void storio_disk_truncate(rozofs_disk_thread_ctx_t *thread_ctx_p,storio_disk_thread_msg_t * msg) {
  struct timeval     timeDay;
  unsigned long long timeBefore, timeAfter;
  storage_t *st = 0;
  sp_truncate_arg_no_bins_t      * args;
  rozorpc_srv_ctx_t      * rpcCtx;
  sp_status_ret_t          ret;
  uint8_t                  version = 0;
  int                      result;
  
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeBefore = MICROLONG(timeDay);

  ret.status = SP_FAILURE;          
  
  /*
  ** update statistics
  */
  thread_ctx_p->stat.diskTruncate_count++;
  
  rpcCtx = msg->rpcCtx;
  args   = (sp_truncate_arg_no_bins_t*) ruc_buf_getPayload(rpcCtx->decoded_arg);

  /*
  ** set the pointer to the bins
  */
  char *pbuf = ruc_buf_getPayload(rpcCtx->recv_buf); 
  pbuf += rpcCtx->position;
    
  /*
  ** Use received buffer for the response
  */
  rpcCtx->xmitBuf  = rpcCtx->recv_buf;
  rpcCtx->recv_buf = NULL;
  

  // Get the storage for the couple (cid;sid)
  if ((st = storaged_lookup(args->cid, args->sid)) == 0) {
    ret.sp_status_ret_t_u.error = errno;
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.diskTruncate_badCidSid++ ;   
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }
  

  // Truncate bins file
  result = storage_truncate(st, args->layout, (sid_t *) args->dist_set,
        		    args->spare, (unsigned char *) args->fid, args->proj_id,
        		    args->bid,version,args->last_seg,args->last_timestamp,
			    args->len, pbuf);
  if (result != 0) {
    ret.sp_status_ret_t_u.error = errno;
    storio_encode_rpc_response(rpcCtx,(char*)&ret);  
    thread_ctx_p->stat.diskTruncate_error++; 
    storio_send_response(thread_ctx_p,msg,-1);
    return;
  }
  
  ret.status = SP_SUCCESS;          
  storio_encode_rpc_response(rpcCtx,(char*)&ret);  
  storio_send_response(thread_ctx_p,msg,0);

  /*
  ** Update statistics
  */
  gettimeofday(&timeDay,(struct timezone *)0);  
  timeAfter = MICROLONG(timeDay);
  thread_ctx_p->stat.diskTruncate_time +=(timeAfter-timeBefore);  
}    


/*
**   D I S K   T H R E A D
*/

void *storio_disk_thread(void *arg) {
  storio_disk_thread_msg_t   msg;
  rozofs_disk_thread_ctx_t * ctx_p = (rozofs_disk_thread_ctx_t*)arg;
  int                        bytesRcvd;

  //info("Disk Thread %d Started !!\n",ctx_p->thread_idx);
  
  while(1) {
  
    /*
    ** read the north disk socket
    */
    bytesRcvd = recvfrom(af_unix_disk_socket_ref,
			 &msg,sizeof(msg), 
			 0,(struct sockaddr *)NULL,NULL);
    if (bytesRcvd == -1) {
      fatal("Disk Thread %d recvfrom %s !!\n",ctx_p->thread_idx,strerror(errno));
      exit(0);
    }
    if (bytesRcvd == 0) {
      fatal("Disk Thread %d socket is dead %s !!\n",ctx_p->thread_idx,strerror(errno));
      exit(0);    
    }
    
    msg.size = 0;
    
    switch (msg.opcode) {
    
      case STORIO_DISK_THREAD_READ:
        storio_disk_read(ctx_p,&msg);
        break;
	
      case STORIO_DISK_THREAD_WRITE:
        storio_disk_write(ctx_p,&msg);
        break;
	
      case STORIO_DISK_THREAD_TRUNCATE:
        storio_disk_truncate(ctx_p,&msg);
        break;
       	
      default:
        fatal(" unexpected opcode : %d\n",msg.opcode);
        exit(0);       
    }
    sched_yield();
  }
}
/*
** Create the threads that will handle all the disk requests

* @param hostname    storio hostname (for tests)
* @param nb_threads  number of threads to create
*  
* @retval 0 on success -1 in case of error
*/
int storio_disk_thread_create(char * hostname, int nb_threads, int instance_id) {
   int                        i;
   int                        err;
   pthread_attr_t             attr;
   rozofs_disk_thread_ctx_t * thread_ctx_p;
   char                       socketName[128];

   /*
   ** clear the thread table
   */
   memset(rozofs_disk_thread_ctx_tb,0,sizeof(rozofs_disk_thread_ctx_tb));
   /*
   ** create the common socket to receive requests on
   */
   sprintf(socketName,"%s_%d_%s",ROZOFS_SOCK_FAMILY_DISK_NORTH,instance_id, hostname);
   af_unix_disk_socket_ref = af_unix_disk_sock_create_internal(socketName,1024*32);
   if (af_unix_disk_socket_ref < 0) {
      fatal("af_unix_disk_thread_create af_unix_disk_sock_create_internal(%s) %s",socketName,strerror(errno));
      return -1;   
   }
   /*
   ** Now create the threads
   */
   thread_ctx_p = rozofs_disk_thread_ctx_tb;
   for (i = 0; i < nb_threads ; i++) {
   
     thread_ctx_p->hostname = hostname;
     /*
     ** create the thread specific socket to send the response from 
     */
     sprintf(socketName,"%s_%d_%s_%d",ROZOFS_SOCK_FAMILY_DISK_NORTH,instance_id,hostname,i);
     thread_ctx_p->sendSocket = af_unix_disk_sock_create_internal(socketName,1024*32);
     if (thread_ctx_p->sendSocket < 0) {
	fatal("af_unix_disk_thread_create af_unix_disk_sock_create_internal(%s) %s",socketName, strerror(errno));
	return -1;   
     }   
   
     err = pthread_attr_init(&attr);
     if (err != 0) {
       fatal("af_unix_disk_thread_create pthread_attr_init(%d) %s",i,strerror(errno));
       return -1;
     }  

     thread_ctx_p->thread_idx = i;
     err = pthread_create(&thread_ctx_p->thrdId,&attr,storio_disk_thread,thread_ctx_p);
     if (err != 0) {
       fatal("af_unix_disk_thread_create pthread_create(%d) %s",i, strerror(errno));
       return -1;
     }  
     
     thread_ctx_p++;
  }
  return 0;
}
 
