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

#include <limits.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <inttypes.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/common/profile.h>
#include <rozofs/rpc/spproto.h>
#include <rozofs/rpc/sproto.h>
#include <rozofs/core/uma_dbg_api.h>

#include "storage.h"
#include "storaged.h"
#include "sproto_nb.h"
#include "storaged_north_intf.h"
#include "storage_fd_cache.h"
#include "storio_disk_thread_intf.h"

/*
** Detailed time counters for read and write operation
*/
#define STORIO_DETAILED_COUNTER_MAX  30
#define STORIO_DETAILED_READ_SLICE   64
#define STORIO_DETAILED_WRITE_SLICE 128
typedef struct _STORIO_DETAILED_COUNTERS_T {
  uint64_t     write[STORIO_DETAILED_COUNTER_MAX+1];
  uint64_t     read[STORIO_DETAILED_COUNTER_MAX+1];  
} STORIO_DETAILED_COUNTERS_T;
static STORIO_DETAILED_COUNTERS_T storio_detailed_counters;

/*_______________________________________________________________________
* Update detailed time counters for wrtite operation
* @param delay delay in us of the write operation
*/
void update_write_detailed_counters(uint64_t delay) {
  delay = delay / STORIO_DETAILED_READ_SLICE;
  if (delay >= STORIO_DETAILED_COUNTER_MAX) delay = STORIO_DETAILED_COUNTER_MAX;
  storio_detailed_counters.write[delay]++;
}

/*_______________________________________________________________________
* Update detailed time counters for wrtite operation
* @param delay delay in us of the write operation
*/
void update_read_detailed_counters(uint64_t delay) {
  delay = delay / STORIO_DETAILED_WRITE_SLICE;
  if (delay > STORIO_DETAILED_COUNTER_MAX) delay = STORIO_DETAILED_COUNTER_MAX;
  storio_detailed_counters.read[delay]++;
}

/*_______________________________________________________________________
* Update detailed time counters for wrtite operation
* @param delay delay in us of the write operation
*/
static inline void reset_detailed_counters(void) {
  memset(&storio_detailed_counters,0,sizeof(storio_detailed_counters));
}

/*_______________________________________________________________________
* Update detailed time counters for wrtite operation
* @param delay delay in us of the write operation
*/
static char * display_detailed_counters_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"detailedTiming reset       : reset statistics\n");
  pChar += sprintf(pChar,"detailedTiming             : display statistics\n");  
  return pChar; 
}
void display_detailed_counters (char * argv[], uint32_t tcpRef, void *bufRef) {
  char          * p = uma_dbg_get_buffer();
  int             i;
  int             start_read,stop_read;
  int             start_write,stop_write;
  
  if (argv[1] != NULL) {
    if (strcmp(argv[1],"reset")==0) {
      reset_detailed_counters();
      uma_dbg_send(tcpRef,bufRef,TRUE,"Reset Done");
      return;
    }
    p = display_detailed_counters_help(p);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
    return;    
  }  
  
  start_read = start_write = 0;
  stop_read  = STORIO_DETAILED_READ_SLICE;
  stop_write  = STORIO_DETAILED_WRITE_SLICE;

  p += sprintf(p, "    READ                                 WRITE\n");  
  for (i=0; i<=STORIO_DETAILED_COUNTER_MAX; i++) {
  
    p += sprintf(p, "%4d..%4d : %-16"PRIu64"        %4d..%4d : %-16"PRIu64"\n", 
                start_read,stop_read,storio_detailed_counters.read[i],
                start_write,stop_write,storio_detailed_counters.write[i]); 

    start_read = stop_read;
    stop_read += STORIO_DETAILED_READ_SLICE; 
    start_write = stop_write;
    stop_write += STORIO_DETAILED_WRITE_SLICE;       
  }
  uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());    
}

/*_______________________________________________________________________
* Initialize detailed time counters service
*/
void detailed_counters_init(void) {
  reset_detailed_counters();
  uma_dbg_addTopic_option("detailedTiming", display_detailed_counters,UMA_DBG_OPTION_RESET); 
}

DECLARE_PROFILING(spp_profiler_t);

int rozofs_storcli_fake_encode(xdrproc_t encode_fct,void *msg2encode_p)
{
    uint8_t *buf= NULL;
    XDR               xdrs;    
    struct rpc_msg   call_msg;
    int              opcode = 0;
    uint32_t         null_val = 0;
    int              position = -1;
    
    buf = malloc(2048);
    if (buf == NULL)
    {
       severe("Out of memory");
       goto out;
    
    }
    xdrmem_create(&xdrs,(char*)buf,2048,XDR_ENCODE);
    /*
    ** fill in the rpc header
    */
    call_msg.rm_direction = CALL;
    /*
    ** allocate a xid for the transaction 
    */
	call_msg.rm_xid             = 1; 
	call_msg.rm_call.cb_rpcvers = RPC_MSG_VERSION;
	/* XXX: prog and vers have been long historically :-( */
	call_msg.rm_call.cb_prog = (uint32_t)0;
	call_msg.rm_call.cb_vers = (uint32_t)0;
	if (! xdr_callhdr(&xdrs, &call_msg))
    {
       /*
       ** THIS MUST NOT HAPPEN
       */
       severe("fake encoding error");
       goto out;	
    }    
    /*
    ** insert the procedure number, NULL credential and verifier
    */
    XDR_PUTINT32(&xdrs, (int32_t *)&opcode);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
    XDR_PUTINT32(&xdrs, (int32_t *)&null_val);
        
    /*
    ** ok now call the procedure to encode the message
    */
    if ((*encode_fct)(&xdrs,msg2encode_p) == FALSE)
    {
       severe("fake encoding error");
       goto out;
    }
    /*
    ** Now get the current length and fill the header of the message
    */
    position = XDR_GETPOS(&xdrs);
    /*
    ** add the length that corresponds to the field that contains the length part
    ** of the rpc message
    */
    position += sizeof(uint32_t);
out:
    if (buf != NULL) free(buf);
    return position;    
}


int storage_bin_write_first_bin_to_read = 0;

/*
**__________________________________________________________________________
*/
/**
*  That service is intended to be called by the write service
*  The goal is to provide the position of the first byte to write on disk

  @param none
  @retval position of the first byte
*/
int storage_get_position_of_first_byte2write_from_write_req()
{
  sp_write_arg_no_bins_t *request; 
  sp_write_arg_no_bins_t  write_prj_args;
  int position;
  
  
  if (storage_bin_write_first_bin_to_read == 0)
  {
    request = &write_prj_args;
    memset(request,0,sizeof(sp_write_arg_no_bins_t));
    position = rozofs_storcli_fake_encode((xdrproc_t) xdr_sp_write_arg_no_bins_t, (caddr_t) request);
    if (position < 0)
    {
      fatal("Cannot get the size of the rpc header for writing");
      return 0;    
    }
    storage_bin_write_first_bin_to_read = position;
  }
  return storage_bin_write_first_bin_to_read;

}
/*
**__________________________________________________________________________
*/

int storage_bin_read_first_bin_to_write = 0;

int storage_get_position_of_first_byte2write_from_read_req()

{
   int position;
   if (storage_bin_read_first_bin_to_write != 0) return storage_bin_read_first_bin_to_write;
   {
      /*
      ** now get the current position in the buffer for loading the first byte of the bins 
      */  
      position =  sizeof(uint32_t); /* length header of the rpc message */
      position += rozofs_rpc_get_min_rpc_reply_hdr_len();
      position += sizeof(uint32_t);   /* length of the storage status field */
      position += sizeof(uint32_t);   /* length of the alignment field (FDL) */
      position += sizeof(uint32_t);   /* length of the bins len field */

      storage_bin_read_first_bin_to_write = position;
    }
    return storage_bin_read_first_bin_to_write;
}

/*
**__________________________________________________________________________
*/
/**
* send a rpc reply: the encoding function MUST be found in xdr_result 
 of the gateway context

  It is assumed that the xmitBuf MUST be found in xmitBuf field
  
  In case of a success it is up to the called function to release the xmit buffer
  
  @param p : pointer to the root transaction context used for the read
  @param arg_ret : returned argument to encode 
  
  @retval none

*/
void storaged_srv_forward_read_success (rozorpc_srv_ctx_t *p,sp_read_ret_t * arg_ret)
{
   int ret;
   uint8_t *pbuf;           /* pointer to the part that follows the header length */
   uint32_t *header_len_p;  /* pointer to the array that contains the length of the rpc message*/
   XDR xdrs;
   int len;
   int cur_len;

   if (p->xmitBuf == NULL)
   {
      ROZORPC_SRV_STATS(ROZORPC_SRV_NO_BUFFER_ERROR);
      severe("no xmit buffer");
      goto error;
   } 
    /*
    ** create xdr structure on top of the buffer that will be used for sending the response
    */
    header_len_p = (uint32_t*)ruc_buf_getPayload(p->xmitBuf); 
    pbuf = (uint8_t*) (header_len_p+1);            
    len = (int)ruc_buf_getMaxPayloadLen(p->xmitBuf);
    len -= sizeof(uint32_t);
    xdrmem_create(&xdrs,(char*)pbuf,len,XDR_ENCODE); 
    /*
    ** just encode the header
    */
    if (rozofs_encode_rpc_reply(&xdrs,(xdrproc_t)NULL,(caddr_t)NULL,p->src_transaction_id) != TRUE)
    {
      ROZORPC_SRV_STATS(ROZORPC_SRV_ENCODING_ERROR);
      severe("rpc reply encoding error");
      goto error;     
    }
    /*
    ** OK now starts encoding the response
    */
    xdr_sp_status_t (&xdrs, &arg_ret->status);
    xdr_uint32_t (&xdrs, &arg_ret->sp_read_ret_t_u.rsp.filler);          
    xdr_uint32_t (&xdrs, &arg_ret->sp_read_ret_t_u.rsp.bins.bins_len); 
    /*
    ** skip the bins
    */
    cur_len =  xdr_getpos(&xdrs) ;     
    cur_len +=  arg_ret->sp_read_ret_t_u.rsp.bins.bins_len;
    xdr_setpos(&xdrs,cur_len);
    /*
    ** encode the length of the file
    */
    xdr_uint64_t (&xdrs, &arg_ret->sp_read_ret_t_u.rsp.file_size);
#if 0 // for future usage with distributed cache 
    /*
    ** encode the optim array
    */
    xdr_bytes (&xdrs, (char **)&arg_ret->sp_read_ret_t_u.rsp.optim.optim_val, 
                       (u_int *) &arg_ret->sp_read_ret_t_u.rsp.optim.optim_len, ~0);  
#endif
    /*
    ** compute the total length of the message for the rpc header and add 4 bytes more bytes for
    ** the ruc buffer to take care of the header length of the rpc message.
    */
    int total_len = xdr_getpos(&xdrs) ;
    *header_len_p = htonl(0x80000000 | total_len);
    total_len +=sizeof(uint32_t);
    ruc_buf_setPayloadLen(p->xmitBuf,total_len);

    /*
    ** Get the callback for sending back the response:
    ** A callback is needed since the request for read might be local or remote
    */
    ret =  af_unix_generic_send_stream_with_idx((int)p->socketRef,p->xmitBuf);  
    if (ret == 0)
    {
      /**
      * success so remove the reference of the xmit buffer since it is up to the called
      * function to release it
      */
      ROZORPC_SRV_STATS(ROZORPC_SRV_SEND);
      p->xmitBuf = NULL;
    }
    else
    {
      ROZORPC_SRV_STATS(ROZORPC_SRV_SEND_ERROR);
    }
error:
    return;
}

/*
**___________________________________________________________
*/

void sp_null_1_svc_nb(void *args, rozorpc_srv_ctx_t *req_ctx_p) {
    DEBUG_FUNCTION;
    static void *result = NULL;
     req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
     req_ctx_p->recv_buf = NULL;
     
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&result); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);     
    return ;
}

/*
**___________________________________________________________
*/

void sp_write_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    sp_write_arg_t * args = (sp_write_arg_t *) pt;
    static sp_write_ret_t ret;
    storage_t *st = 0;
    // Variable to be used in a later version.
    uint8_t version = 0;
    char *buf_bins;
    
    /*
    ** put  the pointer to the bins (still in received buffer
    */
    int position = storage_get_position_of_first_byte2write_from_write_req();
    buf_bins = (char*)ruc_buf_getPayload(req_ctx_p->recv_buf);
    buf_bins+= position;


    DEBUG_FUNCTION;

    START_PROFILING_IO(write, args->nb_proj * rozofs_get_max_psize(args->layout)
            * sizeof (bin_t));

    ret.status = SP_FAILURE;

    // Get the storage for the couple (cid;sid)
    if ((st = storaged_lookup(args->cid, args->sid)) == 0) {
        ret.sp_write_ret_t_u.error = errno;
        goto out;
    }

    // Write projections
    if (storage_write(st, args->layout, (sid_t *) args->dist_set, args->spare,
            (unsigned char *) args->fid, args->bid, args->nb_proj, version,
            &ret.sp_write_ret_t_u.file_size,
            (bin_t *) buf_bins) <= 0) {
        ret.sp_write_ret_t_u.error = errno;
        goto out;
    }

    ret.status = SP_SUCCESS;
out:
 
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    STOP_PROFILING(write);
    return ;
}
/*
**___________________________________________________________
*/

void sp_write_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_write_ret_t ret;

    START_PROFILING(write);
     
    if (storio_disk_thread_intf_send(STORIO_DISK_THREAD_WRITE, req_ctx_p,tic) == 0) {
      return;
    }
    
    severe("sp_write_1_svc_disk_thread storio_disk_thread_intf_send %s", strerror(errno));
    
    ret.status                = SP_FAILURE;            
    ret.sp_write_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    STOP_PROFILING(write);
    return ;
}
/*
**___________________________________________________________
*/

//static char sp_optim[512];

void storage_check_readahead()
{
   
   if (current_storage_fd_cache_p == NULL) return;
   

   posix_fadvise(current_storage_fd_cache_p->fd,                      
                    current_storage_fd_cache_p->offset,
                    current_storage_fd_cache_p->len_read,
                    POSIX_FADV_WILLNEED);


   current_storage_fd_cache_p->len_read = 0;

}

/*
**___________________________________________________________
*/

void sp_read_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    sp_read_arg_t * args = (sp_read_arg_t *) pt;
    static sp_read_ret_t ret;
    storage_t *st = 0;

    START_PROFILING_IO(read, args->nb_proj * rozofs_get_max_psize(args->layout)
            * sizeof (bin_t));
            
    ret.status = SP_FAILURE;            
    /*
    ** allocate a buffer for the response
    */
    req_ctx_p->xmitBuf = ruc_buf_getBuffer(storage_xmit_buffer_pool_p);
    if (req_ctx_p->xmitBuf == NULL)
    {
      severe("Out of memory STORAGE_NORTH_LARGE_POOL");
      ret.sp_read_ret_t_u.error = ENOMEM;
      req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
      req_ctx_p->recv_buf = NULL;
      goto error;         
    }


    // Get the storage for the couple (cid;sid)
    if ((st = storaged_lookup(args->cid, args->sid)) == 0) {
        ret.sp_read_ret_t_u.error = errno;
        goto error;
    }

    /*
    ** set the pointer to the bins
    */
    int position = storage_get_position_of_first_byte2write_from_read_req();
    uint8_t *pbuf = (uint8_t*)ruc_buf_getPayload(req_ctx_p->xmitBuf);     
    /*
    ** clear the length of the bins and set the pointer where data must be returned
    */  
    ret.sp_read_ret_t_u.rsp.bins.bins_val =(char *)(pbuf+position);  ;
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
            &ret.sp_read_ret_t_u.rsp.file_size) != 0) {
        ret.sp_read_ret_t_u.error = errno;
        goto error;
    }

    ret.status = SP_SUCCESS;
    storaged_srv_forward_read_success(req_ctx_p,&ret);
    /*
    ** check the case of the readahead
    */
    storage_check_readahead();
    goto out;
    
error:
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
out:
    rozorpc_srv_release_context(req_ctx_p);
    STOP_PROFILING(read);
    return ;
}
/*
**___________________________________________________________
*/

void sp_read_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_read_ret_t ret;

    START_PROFILING(read);
            
    /*
    ** allocate a buffer for the response
    */
    req_ctx_p->xmitBuf = ruc_buf_getBuffer(storage_xmit_buffer_pool_p);
    if (req_ctx_p->xmitBuf == NULL)
    {
      severe("sp_read_1_svc_disk_thread Out of memory STORAGE_NORTH_LARGE_POOL");
      errno = ENOMEM;
      req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
      req_ctx_p->recv_buf = NULL;
      goto error;         
    }

 
    /*
    ** Set the position where the data have to be written in the xmit buffer 
    */
    req_ctx_p->position = storage_get_position_of_first_byte2write_from_read_req();
     
    if (storio_disk_thread_intf_send(STORIO_DISK_THREAD_READ, req_ctx_p, tic) == 0) {
      return;
    }
    severe("storio_disk_thread_intf_send %s", strerror(errno));
    
error:
    ret.status                = SP_FAILURE;            
    ret.sp_read_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    STOP_PROFILING(read);
    return ;
}
/*
**___________________________________________________________
*/

void sp_truncate_1_svc_nb(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    sp_truncate_arg_t * args = (sp_truncate_arg_t *) pt;
    static sp_status_ret_t ret;
    storage_t *st = 0;
    // Variable to be used in a later version.
    uint8_t version = 0;
    
    DEBUG_FUNCTION;
    
    START_PROFILING(truncate);
    
    ret.status = SP_FAILURE;

    // Get the storage for the couple (cid;sid)
    if ((st = storaged_lookup(args->cid, args->sid)) == 0) {
        ret.sp_status_ret_t_u.error = errno;
        goto out;
    }

    // Truncate bins file
    if (storage_truncate(st, args->layout, (sid_t *) args->dist_set,
            args->spare, (unsigned char *) args->fid, args->proj_id,
            args->bid,version,args->last_seg,args->last_timestamp,
	    args->bins.bins_len, args->bins.bins_val) != 0) {
        ret.sp_status_ret_t_u.error = errno;
        goto out;
    }
    ret.status = SP_SUCCESS;
out:
 
    req_ctx_p->xmitBuf  = req_ctx_p->recv_buf;
    req_ctx_p->recv_buf = NULL;
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    
    STOP_PROFILING(truncate);
    return ;
}
/*
**___________________________________________________________
*/

void sp_truncate_1_svc_disk_thread(void * pt, rozorpc_srv_ctx_t *req_ctx_p) {
    static sp_status_ret_t ret;

    START_PROFILING(truncate);

     
    if (storio_disk_thread_intf_send(STORIO_DISK_THREAD_TRUNCATE, req_ctx_p,tic) == 0) {
      return;
    }
    
    severe("sp_truncate_1_svc_disk_thread storio_disk_thread_intf_send %s", strerror(errno));
    
    ret.status                  = SP_FAILURE;            
    ret.sp_status_ret_t_u.error = errno;
    
    rozorpc_srv_forward_reply(req_ctx_p,(char*)&ret); 
    /*
    ** release the context
    */
    rozorpc_srv_release_context(req_ctx_p);
    STOP_PROFILING(truncate);
    return ;
}
