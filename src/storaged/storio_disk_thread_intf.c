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
#include <stddef.h>
#include <stdint.h>
#include <errno.h>
#include <sched.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/profile.h>
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/core/rozofs_rpc_non_blocking_generic_srv.h>
#include <rozofs/core/ruc_buffer_debug.h>

#include "storio_disk_thread_intf.h"
#include "sproto_nb.h"
#include "config.h"

DECLARE_PROFILING(spp_profiler_t); 
 
static int transactionId = 1; 
int        af_unix_disk_south_socket_ref = -1;
int        af_unix_disk_thread_count=0;
int        af_unix_disk_pending_req_count = 0;

struct  sockaddr_un storio_south_socket_name;
struct  sockaddr_un storio_north_socket_name;

 
int storio_disk_thread_create(char * hostname, int nb_threads, int instance_id) ;
 


void * af_unix_disk_pool_send = NULL;
void * af_unix_disk_pool_recv = NULL;

/*__________________________________________________________________________
  Trace level debug function
  ==========================================================================
  PARAMETERS: 
  - 
  RETURN: none
  ==========================================================================*/
#define new_line(title)  pChar += sprintf(pChar,"\n%-24s |", title)
#define display_val(val) pChar += sprintf(pChar," %16lld |", (long long unsigned int) val)
#define display_div(val1,val2) if (val2==0) display_val(0);else display_val(val1/val2)
#define display_txt(txt) pChar += sprintf(pChar," %16s |", (char *) txt)

#define display_line_topic(title) \
  new_line(title);\
  for (i=startIdx; i<(stopIdx+last); i++) {\
    pChar += sprintf(pChar,"__________________|");\
  }
    
#define display_line_val(title,val) \
  new_line(title);\
  for (i=startIdx; i<stopIdx; i++) {\
    sum.val += p[i].stat.val;\
    display_val(p[i].stat.val);\
  }\
  if (last) { display_val(sum.val); }
  

#define display_line_div(title,val1,val2) \
  new_line(title);\
  for (i=startIdx; i<stopIdx; i++) {\
    display_div(p[i].stat.val1,p[i].stat.val2);\
  }\
  if (last) { display_div(sum.val1,sum.val2); }

 
static char * disk_thread_debug_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"diskThreads reset       : reset statistics\n");
  pChar += sprintf(pChar,"diskThreads             : display statistics\n");  
  return pChar; 
}  
#define THREAD_PER_LINE 6
void disk_thread_debug(char * argv[], uint32_t tcpRef, void *bufRef) {
  char           *pChar=uma_dbg_get_buffer();
  int i;
  rozofs_disk_thread_ctx_t *p = rozofs_disk_thread_ctx_tb;
  int startIdx,stopIdx;
  rozofs_disk_thread_stat_t sum;
  int                       last=0;
  
  if (argv[1] != NULL) {
    if (strcmp(argv[1],"reset")==0) {
      for (i=0; i<af_unix_disk_thread_count; i++) {
	memset(&p[i].stat,0,sizeof(p[i].stat));
      }          
      uma_dbg_send(tcpRef,bufRef,TRUE,"Reset Done");
      return;
    }
    pChar = disk_thread_debug_help(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
    return;      
  }
  
  memset(&sum, 0, sizeof(sum));
  stopIdx  = 0;
  last = 0;
   
  while (last == 0) {
  
    startIdx = stopIdx;
    if ((af_unix_disk_thread_count - startIdx) > THREAD_PER_LINE) {
      stopIdx = startIdx + THREAD_PER_LINE;
    }  
    else {
      stopIdx = af_unix_disk_thread_count;
      last = 1;
    }  
    
    new_line("Thread number");
    for (i=startIdx; i<stopIdx; i++) {
      display_val(p[i].thread_idx);
    } 
    if (last) {
      display_txt("Total");
    }   
  
    display_line_topic("Read Requests");  
    display_line_val("   number", diskRead_count);
    display_line_val("   No such file",diskRead_nosuchfile);
    display_line_val("   Unknown cid/sid",diskRead_badCidSid);  
    display_line_val("   error spare",diskRead_error_spare);  
    display_line_val("   error",diskRead_error);  
    display_line_val("   Bytes",diskRead_Byte_count);      
    display_line_val("   Cumulative Time (us)",diskRead_time);
    display_line_div("   Average Bytes",diskRead_Byte_count,diskRead_count);  
    display_line_div("   Average Time (us)",diskRead_time,diskRead_count);
    display_line_div("   Throughput (MBytes/s)",diskRead_Byte_count,diskRead_time);  

    display_line_topic("Write Requests");  
    display_line_val("   number", diskWrite_count);
    display_line_val("   Unknown cid/sid",diskWrite_badCidSid);  
    display_line_val("   error",diskWrite_error);  
    display_line_val("   Bytes",diskWrite_Byte_count);      
    display_line_val("   Cumulative Time (us)",diskWrite_time);
    display_line_div("   Average Bytes",diskWrite_Byte_count,diskWrite_count); 
    display_line_div("   Average Time (us)",diskWrite_time,diskWrite_count);
    display_line_div("   Throughput (MBytes/s)",diskWrite_Byte_count,diskWrite_time);  

    display_line_topic("Truncate Requests");  
    display_line_val("   number", diskTruncate_count);
    display_line_val("   Unknown cid/sid",diskTruncate_badCidSid);  
    display_line_val("   error",diskTruncate_error);  
    display_line_val("   Cumulative Time (us)",diskTruncate_time);
    display_line_div("   Average Time (us)",diskTruncate_time,diskTruncate_count);

    display_line_topic("");  
    pChar += sprintf(pChar,"\n");
  }

  uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());
}

/*__________________________________________________________________________
*/
/**
* test function for allocatiing a buffer in the client space

 The service might reject the buffer allocation because the pool runs
 out of buffer or because there is no pool with a buffer that is large enough
 for receiving the message because of a out of range size.

 @param userRef : pointer to a user reference: not used here
 @param socket_context_ref: socket context reference
 @param len : length of the incoming message
 
 @retval <>NULL pointer to a receive buffer
 @retval == NULL no buffer
*/
void * af_unix_disk_userRcvAllocBufCallBack(void *userRef,uint32_t socket_context_ref,uint32_t len) {
  return ruc_buf_getBuffer(af_unix_disk_pool_recv);   
}


 /**
 * prototypes
 */
uint32_t af_unix_disk_rcvReadysock(void * af_unix_disk_ctx_p,int socketId);
uint32_t af_unix_disk_rcvMsgsock(void * af_unix_disk_ctx_p,int socketId);
uint32_t af_unix_disk_xmitReadysock(void * af_unix_disk_ctx_p,int socketId);
uint32_t af_unix_disk_xmitEvtsock(void * af_unix_disk_ctx_p,int socketId);

#define DISK_SO_SENDBUF  (300*1024)
#define DISK_SOCKET_NICKNAME "disk_resp_th"
/*
**  Call back function for socket controller
*/
ruc_sockCallBack_t af_unix_disk_callBack_sock=
  {
     af_unix_disk_rcvReadysock,
     af_unix_disk_rcvMsgsock,
     af_unix_disk_xmitReadysock,
     af_unix_disk_xmitEvtsock
  };
  
  /*
**__________________________________________________________________________
*/
/**
  Application callBack:

  Called from the socket controller. 


  @param unused: not used
  @param socketId: reference of the socket (not used)
 
  @retval : always FALSE
*/

uint32_t af_unix_disk_xmitReadysock(void * unused,int socketId)
{

    return FALSE;
}


/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   Called from the socket controller upon receiving a xmit ready event
   for the associated socket. That callback is activeted only if the application
   has replied TRUE in rozofs_fuse_xmitReadysock().
   
   It typically the processing of a end of congestion on the socket

    
  @param unused: not used
  @param socketId: reference of the socket (not used)
 
   @retval :always TRUE
*/
uint32_t af_unix_disk_xmitEvtsock(void * unused,int socketId)
{
   
    return TRUE;
}
/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   receiver ready function: called from socket controller.
   The module is intended to return if the receiver is ready to receive a new message
   and FALSE otherwise

    
  @param unused: not used
  @param socketId: reference of the socket (not used)
 
  @retval : TRUE-> receiver ready
  @retval : FALSE-> receiver not ready
*/

uint32_t af_unix_disk_rcvReadysock(void * unused,int socketId)
{
  return TRUE;
}
/*
**__________________________________________________________________________
*/
/**
  Processes a disk response

   Called from the socket controller when there is a response from a disk thread
   the response is either for a disk read or write
    
  @param msg: pointer to disk response message
 
  @retval :none
*/
void af_unix_disk_response(storio_disk_thread_msg_t *msg) 
{

  storio_disk_thread_request_e   opcode;
  rozorpc_srv_ctx_t            * rpcCtx;
  int                            ret;
  uint64_t                       tic, toc;  
  struct timeval                 tv;  
  
  rpcCtx = msg->rpcCtx;
  opcode = msg->opcode;
  tic    = msg->timeStart;

  switch (opcode) {
    case STORIO_DISK_THREAD_READ:
      STOP_PROFILING_IO(read,msg->size);
      update_read_detailed_counters(toc - tic);      
      break;
    case STORIO_DISK_THREAD_WRITE:
      STOP_PROFILING_IO(write,msg->size);
      update_write_detailed_counters(toc - tic);            
      break;     
    case STORIO_DISK_THREAD_TRUNCATE:
      STOP_PROFILING(truncate);
      break;   
    default:
      severe("Unexpected opcode %d", opcode);
  }
  /*
  ** send the response towards the storcli process that initiates the disk operation
  */
  ret = af_unix_generic_send_stream_with_idx((int)rpcCtx->socketRef,rpcCtx->xmitBuf);  
  if (ret == 0) {
    /**
    * success so remove the reference of the xmit buffer since it is up to the called
    * function to release it
    */
    ROZORPC_SRV_STATS(ROZORPC_SRV_SEND);
    rpcCtx->xmitBuf = NULL;
  }
  else {
    ROZORPC_SRV_STATS(ROZORPC_SRV_SEND_ERROR);
  }
    
  rozorpc_srv_release_context(rpcCtx);          
}

/*
**__________________________________________________________________________
*/
/**
  Application callBack:

   Called from the socket controller when there is a message pending on the
   socket associated with the context provide in input arguments.
   
   That service is intended to process a response sent by a disk thread

    
  @param unused: user parameter not used by the application
  @param socketId: reference of the socket 
 
   @retval : TRUE-> xmit ready event expected
  @retval : FALSE-> xmit  ready event not expected
*/

uint32_t af_unix_disk_rcvMsgsock(void * unused,int socketId)
{
  storio_disk_thread_msg_t   msg;
  int                        bytesRcvd;
  int eintr_count = 0;
  


  /*
  ** disk responses have the highest priority, loop on the socket until
  ** the socket becomes empty
  */
  while(1) {  
    /*
    ** check if there are some pending requests
    */
    if (af_unix_disk_pending_req_count == 0)
    {
     return TRUE;
    }
    /*
    ** read the north disk socket
    */
    bytesRcvd = recvfrom(socketId,
			 &msg,sizeof(msg), 
			 0,(struct sockaddr *)NULL,NULL);
    if (bytesRcvd == -1) {
     switch (errno)
     {
       case EAGAIN:
        /*
        ** the socket is empty
        */
        return TRUE;

       case EINTR:
         /*
         ** re-attempt to read the socket
         */
         eintr_count++;
         if (eintr_count < 3) continue;
         /*
         ** here we consider it as a error
         */
         severe ("Disk Thread Response error too many eintr_count %d",eintr_count);
         return TRUE;

       case EBADF:
       case EFAULT:
       case EINVAL:
       default:
         /*
         ** We might need to double checl if the socket must be killed
         */
         fatal("Disk Thread Response error on recvfrom %s !!\n",strerror(errno));
         exit(0);
     }

    }
    if (bytesRcvd == 0) {
      fatal("Disk Thread Response socket is dead %s !!\n",strerror(errno));
      exit(0);    
    } 
    af_unix_disk_pending_req_count--;
    if (  af_unix_disk_pending_req_count < 0) af_unix_disk_pending_req_count = 0;
    af_unix_disk_response(&msg); 
  }       
  return TRUE;
}


/*
**__________________________________________________________________________
*/
/**
* fill the storio  AF_UNIX name in the global data

  @param hostname
  @param socketname : pointer to a sockaddr_un structure
  
  @retval none
*/
void storio_set_socket_name_with_hostname(struct sockaddr_un *socketname,char *name,char *hostname,int instance_id)
{
  socketname->sun_family = AF_UNIX;  
  sprintf(socketname->sun_path,"%s_%d_%s",name,instance_id,hostname);
}

/*
**__________________________________________________________________________
*/
/**
*  Thar API is intended to be used by a disk thread for sending back a 
   disk response (read/write or truncate) towards the main thread
   
   @param thread_ctx_p: pointer to the thread context (contains the thread source socket )
   @param msg: pointer to the message that contains the disk response
   @param status : status of the disk operation
   
   @retval none
*/
void storio_send_response (rozofs_disk_thread_ctx_t *thread_ctx_p, storio_disk_thread_msg_t * msg, int status) 
{
  int                     ret;
  
  msg->status = status;
  
  /*
  ** send back the response
  */  
  ret = sendto(thread_ctx_p->sendSocket,msg, sizeof(*msg),0,(struct sockaddr*)&storio_south_socket_name,sizeof(storio_south_socket_name));
  if (ret <= 0) {
     fatal("storio_send_response %d sendto(%s) %s", thread_ctx_p->thread_idx, storio_south_socket_name.sun_path, strerror(errno));
     exit(0);  
  }
}

/*__________________________________________________________________________
*/
/**
*  Send a disk request to the disk threads
*
* @param opcode     the request operation code
* @param rpcCtx     pointer to the generic rpc context
* @param timeStart  time stamp when the request has been decoded
*
* @retval 0 on success -1 in case of error
*  
*/
int storio_disk_thread_intf_send(storio_disk_thread_request_e   opcode, 
                                 rozorpc_srv_ctx_t            * rpcCtx,
				                 uint64_t                       timeStart) 
{
  int                         ret;
  storio_disk_thread_msg_t    msg;
 
  /* Fill the message */
  msg.msg_len         = sizeof(storio_disk_thread_msg_t)-sizeof(msg.msg_len);
  msg.opcode          = opcode;
  msg.status          = 0;
  msg.transaction_id  = transactionId++;
  msg.timeStart       = timeStart;
  msg.size            = 0;
  msg.rpcCtx          = rpcCtx;
  
  /* Send the buffer to its destination */
  ret = sendto(af_unix_disk_south_socket_ref,&msg, sizeof(msg),0,(struct sockaddr*)&storio_north_socket_name,sizeof(storio_north_socket_name));
  if (ret <= 0) {
     fatal("storio_disk_thread_intf_send  sendto(%s) %s", storio_north_socket_name.sun_path, strerror(errno));
     exit(0);  
  }
  
  af_unix_disk_pending_req_count++;
  sched_yield();
  return 0;
}

/*
**__________________________________________________________________________
*/

/**
* creation of the AF_UNIX socket that is attached on the socket controller

  That socket is used to receive back the response from the threads that
  perform disk operation (read/write/truncate)
  
  @param socketname : name of the socket
  
  @retval >= 0 : reference of the socket
  @retval < 0 : error
*/
int af_unix_disk_response_socket_create(char *socketname)
{
  int len;
  int fd = -1;
  void *sockctrl_ref;

   len = strlen(socketname);
   if (len >= AF_UNIX_SOCKET_NAME_SIZE)
   {
      /*
      ** name is too big!!
      */
      severe("socket name %s is too long: %d (max is %d)",socketname,len,AF_UNIX_SOCKET_NAME_SIZE);
      return -1;
   }
   while (1)
   {
     /*
     ** create the socket
     */
     fd = af_unix_sock_create_internal(socketname,DISK_SO_SENDBUF);
     if (fd == -1)
     {
       break;
     }
     /*
     ** OK, we are almost done, just need to connect with the socket controller
     */
     sockctrl_ref = ruc_sockctl_connect(fd,  // Reference of the socket
                                                DISK_SOCKET_NICKNAME,   // name of the socket
                                                16,                  // Priority within the socket controller
                                                (void*)NULL,      // user param for socketcontroller callback
                                                &af_unix_disk_callBack_sock);  // Default callbacks
      if (sockctrl_ref == NULL)
      {
         /*
         ** Fail to connect with the socket controller
         */
         fatal("error on ruc_sockctl_connect");
         break;
      }
      /*
      ** All is fine
      */
      break;
    }    
    return fd;
}

/*__________________________________________________________________________
*/
/**
*   entry point for disk response socket polling
*

   @param current_time : current time provided by the socket controller
   
   
   @retval none
*/
void af_unix_disk_scheduler_entry_point(uint64_t current_time)
{
  af_unix_disk_rcvMsgsock(NULL,af_unix_disk_south_socket_ref);
}

/*__________________________________________________________________________
* Initialize the disk thread interface
*
* @param hostname    storio hostname (for tests)
* @param nb_threads  Number of threads that can process the disk requests
* @param nb_buffer   Number of buffer for sending and number of receiving buffer
*
*  @retval 0 on success -1 in case of error
*/
int storio_disk_thread_intf_create(char * hostname, int instance_id, int nb_threads, int nb_buffer) {

  af_unix_disk_thread_count = nb_threads;

  af_unix_disk_pool_send = ruc_buf_poolCreate(nb_buffer,sizeof(storio_disk_thread_msg_t));
  if (af_unix_disk_pool_send == NULL) {
    fatal("storio_disk_thread_intf_create af_unix_disk_pool_send (%d,%d)", nb_buffer, (int)sizeof(storio_disk_thread_msg_t));
    return -1;
  }
  ruc_buffer_debug_register_pool("diskSendPool",af_unix_disk_pool_send);   
  
  af_unix_disk_pool_recv = ruc_buf_poolCreate(1,sizeof(storio_disk_thread_msg_t));
  if (af_unix_disk_pool_recv == NULL) {
    fatal("storio_disk_thread_intf_create af_unix_disk_pool_recv (1,%d)", (int)sizeof(storio_disk_thread_msg_t));
    return -1;
  }
  ruc_buffer_debug_register_pool("diskRecvPool",af_unix_disk_pool_recv);   

  /*
  ** init of the AF_UNIX sockaddr associated with the south socket (socket used for disk response receive)
  */
  storio_set_socket_name_with_hostname(&storio_south_socket_name,ROZOFS_SOCK_FAMILY_DISK_SOUTH,hostname, instance_id);
    
  /*
  ** hostname is required for the case when several storaged run on the same server
  ** as is the case of test on one server only
  */   
  af_unix_disk_south_socket_ref = af_unix_disk_response_socket_create(storio_south_socket_name.sun_path);
  if (af_unix_disk_south_socket_ref < 0) {
    fatal("storio_create_disk_thread_intf af_unix_sock_create(%s) %s",storio_south_socket_name.sun_path, strerror(errno));
    return -1;
  }
 /*
  ** init of the AF_UNIX sockaddr associated with the north socket (socket used for disk request receive)
  */
  storio_set_socket_name_with_hostname(&storio_north_socket_name,ROZOFS_SOCK_FAMILY_DISK_NORTH,hostname,instance_id);
  
  uma_dbg_addTopic_option("diskThreads", disk_thread_debug,UMA_DBG_OPTION_RESET); 
  /*
  ** attach the callback on socket controller
  */
  ruc_sockCtrl_attach_applicative_poller(af_unix_disk_scheduler_entry_point);  
   
  return storio_disk_thread_create(hostname, nb_threads, instance_id);
}



