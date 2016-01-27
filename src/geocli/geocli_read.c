/*
  Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
  This cpy_p is part of Rozofs.

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
#include <inttypes.h>
#include <rozofs/rpc/eproto.h>
#include <rozofs/rpc/storcli_proto.h>
#include "rozofs_sharedmem.h"
#include "rozofs_rw_load_balancing.h"


/**
* Align off as well as len to read on blocksize bundary
  @param[in]  off         : offset to read
  @param[in]  len         : length to read
  @param[out] off_aligned : aligned read offset
  @param[out] len_aligned : aligned length to read  
  
  @retval none
*/
static inline void rozofs_align_off_and_len(uint64_t off, int len, uint64_t * off_aligned, int * len_aligned) {

  *off_aligned = (off/ROZOFS_CACHE_BSIZE)*ROZOFS_CACHE_BSIZE;       
  *len_aligned = len + (off-*off_aligned);
  if ((*len_aligned % ROZOFS_CACHE_BSIZE) == 0) return;
  *len_aligned = ((*len_aligned/ROZOFS_CACHE_BSIZE)+1)*ROZOFS_CACHE_BSIZE;
}

/** Reads the distributions on the export server,
 *  adjust the read buffer to read only whole data blocks
 *  and uses the function read_blocks to read data
 *
 * @param *f: pointer to the cpy_p structure
 * @param off: offset to read from
 * @param *buf: pointer where the data will be stored: buffer associated with the cpy_p_t structure
 * @param len: length to read: (correspond to the max buffer size defined in the exportd parameters
 * @param *last_block_size_p: pointer to store the size of the last block size
 *  read
 *
 * @return: the length read on success, -1 otherwise (errno is set)
 */
 
int geo_read_req(rzcp_copy_ctx_t * cpy_p) 
{
   uint64_t bid = 0;
   uint32_t nb_prj = 0;
   storcli_read_arg_t  args;
   int ret;
   uint32_t *p32;
   /*
   ** get the reference of the first block to read and the number of block to
   ** read
   */
   bid = rw_ctx_p->off_cur / ROZOFS_BSIZE;
   uint64_t len;
   len = rw_ctx_p->initial_len - rw_ctx_p->len_cur;
   if (len > RZCPY_MAX_BUF_LEN) len = RZCPY_MAX_BUF_LEN;
   nb_prj = len / ROZOFS_BSIZE;
   /*
   ** save the requested length
   */
   cpy_p->len2read = len;
   
    if (rozofs_rotation_read_modulo == 0) {
      rw_ctx_p->rotation_idx = 0;
    }
    else {
      rw_ctx_p->rotation_counter++;
      if ((rw_ctx_p->rotation_counter % rozofs_rotation_read_modulo)==0) {
	rw_ctx_p->rotation_idx++;
      }
    }  
    args.sid = rw_ctx_p->rotation_idx;

    // Fill request
    args.cid = rw_ctx_p->cid;
    args.layout = rw_ctx_p->layout;
    memcpy(args.dist_set, rw_ctx_p->sids, sizeof (sid_t) * ROZOFS_SAFE_MAX);
    memcpy(args.fid, rw_ctx_p->fid, sizeof (fid_t));
    args.proj_id = 0; // N.S
    args.bid = bid;
    args.nb_proj = nb_prj;
    /*
    ** clear the first 4 bytes of the array that is supposed to contain
    ** the reference of the transaction
    */
    p32 = (uint32_t *)ruc_buf_getPayload(cpy->shared_buf_ref);
    *p32 = 0;
    args.proj_id = cpy->shared_buf_idx;
    args.spare     = 'S';
    /*
    ** now initiates the transaction towards the remote end
    */
    ret = rozofs_storcli_send_common(NULL,ROZOFS_TMR_GET(TMR_STORCLI_PROGRAM),STORCLI_PROGRAM, STORCLI_VERSION,
                              STORCLI_READ,(xdrproc_t) xdr_storcli_read_arg_t,(void *)&args,
                              rzcp_read_cbk,cpy_p,cpy_p->storcli_idx,rw_ctx_p->fid); 
    if (ret < 0) goto error;
    
    /*
    ** no error just waiting for the answer
    */
    return ret;    
error:
    return ret;

}


/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated pseudo cpy_p context
 
 @return none
 */
void rzcp_read_cbk(void *this,void *param) 
{
   struct rpc_msg  rpc_reply;
   size_t size;
   uint64_t off;
   size_t length;
   int len;
   void *shared_buf_ref;
   rzcp_file_ctx_t *rw_ctx_p;
   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   storcli_read_ret_no_data_t ret;
   xdrproc_t decode_proc = (xdrproc_t)xdr_storcli_read_ret_no_data_t;
   cpy_p_t *cpy_p;
   int position ;
   errno =0;

   rzcp_copy_ctx_t *cpy_p = (rzcp_copy_ctx_t*)param;
   rw_ctx_p = &cpy_p->read_ctx;
   
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   shared_buf_ref = cpy_p->shared_buf_ref;
    /*
    ** get the pointer to the transaction context:
    ** it is required to get the information related to the receive buffer
    */
    rozofs_tx_ctx_t      *rozofs_tx_ctx_p = (rozofs_tx_ctx_t*)this;     
    /*    
    ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
    */
    status = rozofs_tx_get_status(this);
    if (status < 0)
    {
       /*
       ** something wrong happened
       */
       errno = rozofs_tx_get_errno(this);  
       goto error; 
    }
    /*
    ** get the pointer to the receive buffer payload
    */
    recv_buf = rozofs_tx_get_recvBuf(this);
    if (recv_buf == NULL)
    {
       /*
       ** something wrong happened
       */
       errno = EFAULT;  
       goto error;         
    }
    payload  = (uint8_t*) ruc_buf_getPayload(recv_buf);
    payload += sizeof(uint32_t); /* skip length*/
    /*
    ** OK now decode the received message
    */
    bufsize = (int) ruc_buf_getPayloadLen(recv_buf);
    xdrmem_create(&xdrs,(char*)payload,bufsize,XDR_DECODE);
    /*
    ** decode the rpc part
    */
    if (rozofs_xdr_replymsg(&xdrs,&rpc_reply) != TRUE)
    {
     TX_STATS(ROZOFS_TX_DECODING_ERROR);
     errno = EPROTO;
     goto error;
    }
    /*
    ** ok now call the procedure to decode the message
    */
    memset(&ret,0, sizeof(ret));                    
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       xdr_free((xdrproc_t) decode_proc, (char *) &ret);
       goto error;
    }   
    if (ret.status == STORCLI_FAILURE) {
        errno = ret.storcli_read_ret_no_data_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    /*
    ** no error, so get the length of the data part: need to consider the case
    ** of the shared memory since for its case, the length of the returned data
    ** is not in the rpc buffer. It is detected by th epresence of the 0x53535353
    ** pattern in the alignment field of the rpc buffer
    */
    int received_len = ret.storcli_read_ret_no_data_t_u.len.len;
    uint32_t alignment = (uint32_t) ret.storcli_read_ret_no_data_t_u.len.alignment;
    xdr_free((xdrproc_t) decode_proc, (char *) &ret); 
    if (alignment == 0x53535353)
    {
      /*
      ** case of the shared memory
      */
      uint32_t *p32 = (uint32_t*)ruc_buf_getPayload(shared_buf_ref);;
      received_len = p32[1];
      position = 0;
      payload = (uint8_t*)&p32[4096/4];      
    }
    else
    { 
      /*
      ** case without shared memory
      */
      position = XDR_GETPOS(&xdrs);
    }
out:
    /*
    ** release the transaction context 
    */
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);  
    /*
    ** release the shared buffer
    */ 
    if (cpy_p->shared_buf_ref!= NULL) 
    {
      uint32_t *p32 = (uint32_t*)ruc_buf_getPayload(cpy_p->shared_buf_ref);    
      /*
      ** clear the timestamp
      */
      *p32 = 0;
    }
    /*
    ** inform the caller thanks its callback
    */
    status = 0;
    (*cpy_p->rzcp_caller_cbk)(cpy_p,status);
    return;
error:
    status = -1;
    goto out;
    return;
}

