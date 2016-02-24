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

/* need for crypt */
#define _XOPEN_SOURCE 500

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stddef.h>
#include <fcntl.h>
#include <unistd.h>
#include <getopt.h>
#include <pthread.h>
#include <assert.h>

#include <rozofs/rozofs.h>
#include "config.h"
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
//#include "rozofs_stats.h"
#include <rozofs/core/rozofs_tx_common.h>
#include <rozofs/core/rozofs_tx_api.h>
#include "rozofs_storcli.h"
#include "storage_proto.h"
#include <rozofs/core/af_unix_socket_generic_api.h>
#include <rozofs/rozofs_srv.h>
#include "rozofs_storcli_rpc.h"
#include <rozofs/rpc/sproto.h>
#include "storcli_main.h"
#include <rozofs/rozofs_timer_conf.h>
#include "rozofs_storcli_mojette_thread_intf.h"


static int storcli_storage_supports_repair2 = 1;

DECLARE_PROFILING(stcpp_profiler_t);

/*
**__________________________________________________________________________
*/
/**
* PROTOTYPES
*/


/**
* allocate a sequence number for the read. The sequence number is associated to
* the read context and is common to all the request concerning the projections of a particular set of distribution
 @retval sequence number
*/
extern uint32_t rozofs_storcli_allocate_read_seqnum();
extern int rozofs_storcli_fake_encode(xdrproc_t encode_fct,void *msg2encode_p);

void show_repair2(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();

  if (argv[1] != NULL) {
    if (strcasecmp(argv[1],"on")==0) {
      storcli_storage_supports_repair2 = 1;
    }
    else if (strcasecmp(argv[1],"off")==0) {
      storcli_storage_supports_repair2 = 0;
    }
    else {
	  pChar += sprintf(pChar,"repair2 [on|off]\n");
	}
  }

  switch(storcli_storage_supports_repair2) {
    case 0:
	  pChar += sprintf(pChar,"storages DO NOT support REPAIR2\n");    	 
	  break;
	case 1:
	  pChar += sprintf(pChar,"storages SHOULD support REPAIR2\n");    	 
 	  break;
	default:   
	  pChar += sprintf(pChar,"storages DO support REPAIR2\n");
  }
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}

/**
*  END PROTOTYPES
*/
/*
**__________________________________________________________________________
*/

/**
* Local prototypes
*/
void rozofs_storcli_write_repair_req_processing_cbk(void *this,void *param) ;
void rozofs_storcli_write_repair_req_processing(rozofs_storcli_ctx_t *working_ctx_p);

/*
**_________________________________________________________________________
*      LOCAL FUNCTIONS
**_________________________________________________________________________
*/

int storcli_write_repair_bin_first_byte = 0;

int rozofs_storcli_repair_get_position_of_first_byte2write()
{
  sp_write_repair_arg_no_bins_t *request; 
  sp_write_repair_arg_no_bins_t  repair_prj_args;
  int position;
  
  
  if (storcli_write_repair_bin_first_byte == 0)
  {
    request = &repair_prj_args;
    memset(request,0,sizeof(sp_write_repair_arg_no_bins_t));
    position = rozofs_storcli_fake_encode((xdrproc_t) xdr_sp_write_repair_arg_no_bins_t, (caddr_t) request);
    if (position < 0)
    {
      fatal("Cannot get the size of the rpc header for repair");
      return 0;    
    }
    storcli_write_repair_bin_first_byte = position;
  }
  return storcli_write_repair_bin_first_byte;

}
int storcli_write_repair2_bin_first_byte = 0;

int rozofs_storcli_repair2_get_position_of_first_byte2write()
{
  sp_write_repair2_arg_no_bins_t *request; 
  sp_write_repair2_arg_no_bins_t  repair2_prj_args;
  int position;
  
  
  if (storcli_write_repair2_bin_first_byte == 0)
  {
    request = &repair2_prj_args;
    memset(request,0,sizeof(sp_write_repair2_arg_no_bins_t));
    position = rozofs_storcli_fake_encode((xdrproc_t) xdr_sp_write_repair2_arg_no_bins_t, (caddr_t) request);
    if (position < 0)
    {
      fatal("Cannot get the size of the rpc header for repair2");
      return 0;    
    }
    storcli_write_repair2_bin_first_byte = position;
  }
  return storcli_write_repair2_bin_first_byte;

}

/*
**__________________________________________________________________________
*/
/**"
* The purpose of that function is to return TRUE if there are enough projection received for
  rebuilding the associated initial message
  
  @param layout : layout association with the file
  @param prj_cxt_p: pointer to the projection context (working array)
  
  @retval 1 if there are enough received projection
  @retval 0 when there is enough projection
*/
static inline int rozofs_storcli_all_prj_write_repair_check(uint8_t layout,rozofs_storcli_projection_ctx_t *prj_cxt_p)
{
  /*
  ** Get the rozofs_forward value for the layout
  */
  uint8_t   rozofs_forward = rozofs_get_rozofs_forward(layout);
  int i;
  
  for (i = 0; i <rozofs_forward; i++,prj_cxt_p++)
  {
    if (prj_cxt_p->prj_state == ROZOFS_PRJ_WR_IN_PRG) 
    {
      return 0;
    }
  }
  return 1;
}
/*
**__________________________________________________________________________
*/
/** 
  Apply the transform to a buffer starting at "data". That buffer MUST be ROZOFS_BSIZE
  aligned.
  The first_block_idx is the index of a ROZOFS_BSIZE array in the output buffer
  The number_of_blocks is the number of ROZOFS_BSIZE that must be transform
  Notice that the first_block_idx offset applies to the output transform buffer only
  not to the input buffer pointed by "data".
  
 * 
 * @param *working_ctx_p: storcli working context
 * @param number_of_blocks: number of blocks to write
 * @param *data: pointer to the source data that must be transformed
 *
 * @return: the length written on success, -1 otherwise (errno is set)
 */
 void rozofs_storcli_transform_forward_repair(rozofs_storcli_ctx_t *working_ctx_p,
                                	      uint8_t layout,
                                	      uint32_t number_of_blocks,
                                	      char *data) 
 {
    projection_t rozofs_fwd_projections[ROZOFS_SAFE_MAX_STORCLI];
    projection_t *projections; // Table of projections used to transform data
    uint16_t projection_id = 0;
    uint32_t i = 0;    
    uint8_t rozofs_forward = rozofs_get_rozofs_forward(layout);
    uint8_t rozofs_safe    = rozofs_get_rozofs_forward(layout);
    uint8_t rozofs_inverse = rozofs_get_rozofs_inverse(layout);
    rozofs_storcli_projection_ctx_t *prj_ctx_p = &working_ctx_p->prj_ctx[0];
    int empty_block = 0;
    uint8_t sid;
    int moj_prj_id;
    int block_idx;
    int k;
    storcli_read_arg_t *storcli_read_rq_p = (storcli_read_arg_t*)&working_ctx_p->storcli_read_arg;
    uint8_t  bsize  = storcli_read_rq_p->bsize;
    uint32_t bbytes = ROZOFS_BSIZE_BYTES(bsize);
    int prj_size_in_msg = rozofs_get_max_psize_in_msg(layout,bsize);
              
    projections = rozofs_fwd_projections;

    // For each projection
    for (projection_id = 0; projection_id < rozofs_forward; projection_id++) {
        projections[projection_id].angle.p =  rozofs_get_angles_p(layout,projection_id);
        projections[projection_id].angle.q =  rozofs_get_angles_q(layout,projection_id);
        projections[projection_id].size    =  rozofs_get_128bits_psizes(layout,bsize,projection_id);
    }
    /*
    ** now go through all projection set to find out if there is something to regenerate
    */
    for (k = 0; k < rozofs_safe; k++)
    {
	block_idx = 0;
       if (ROZOFS_BITMAP64_TEST_ALL0(prj_ctx_p[k].crc_err_bitmap)) continue;
       /*
       **  Get the sid associated with the projection context
       */
       sid = (uint8_t) rozofs_storcli_lbg_prj_get_sid(working_ctx_p->lbg_assoc_tb,
                                                      prj_ctx_p[k].stor_idx);
       /*
       ** Get the reference of the Mojette projection_id
       */
       moj_prj_id = rozofs_storcli_get_mojette_proj_id(storcli_read_rq_p->dist_set,sid,rozofs_forward);
       if  (moj_prj_id < 0)
       {
          /*
	  ** it is the reference of a spare sid, so go to the next projection context
	  */
	  continue;
       }
       for (i = 0; i < number_of_blocks; i++) 
       {
          if (ROZOFS_BITMAP64_TEST0(i,prj_ctx_p[k].crc_err_bitmap)) 
	  {
	    /*
	    ** nothing to generate for that block
	    */
	    continue;
	  }
	  /*
	  ** check for empty block
	  */
          empty_block = rozofs_data_block_check_empty(data + (i * bbytes), bbytes);
	  /**
	  * regenerate the projection for the block for which a crc error has been detected
	  */
//CRC     projections[moj_prj_id].bins = prj_ctx_p[moj_prj_id].bins + 
          projections[moj_prj_id].bins = prj_ctx_p[k].bins + 
                                         (prj_size_in_msg/sizeof(bin_t)* (0+block_idx));
          rozofs_stor_bins_hdr_t *rozofs_bins_hdr_p = (rozofs_stor_bins_hdr_t*)projections[moj_prj_id].bins;
          /*
          ** check if the user data block is empty: if the data block is empty no need to transform
          */
          if (empty_block)
          {
            rozofs_bins_hdr_p->s.projection_id = 0;
            rozofs_bins_hdr_p->s.timestamp     = 0;          
            rozofs_bins_hdr_p->s.effective_length = 0;    
            rozofs_bins_hdr_p->s.filler = 0;    
            rozofs_bins_hdr_p->s.version = 0;
	    block_idx++;    
            continue;   
          }	 
          /*
          ** fill the header of the projection
          */
          rozofs_bins_hdr_p->s.projection_id     = moj_prj_id;
//CRC     rozofs_bins_hdr_p->s.timestamp         = working_ctx_p->block_ctx_table[block_idx].timestamp;       
          rozofs_bins_hdr_p->s.timestamp         = working_ctx_p->block_ctx_table[i].timestamp; 
//CRC     rozofs_bins_hdr_p->s.effective_length  = working_ctx_p->block_ctx_table[block_idx].effective_length;
          rozofs_bins_hdr_p->s.effective_length  = working_ctx_p->block_ctx_table[i].effective_length;
          rozofs_bins_hdr_p->s.filler = 0;    
          rozofs_bins_hdr_p->s.version = 0;    	 
          /*
          ** update the pointer to point out the first bins
          */
          projections[moj_prj_id].bins += sizeof(rozofs_stor_bins_hdr_t)/sizeof(bin_t);
          /*
          ** do not apply transform for empty block
          */
          if (empty_block == 0)
          {
	  	    
            /*
            ** Apply the erasure code transform for the block i
            */
            transform128_forward_one_proj((pxl_t *) (data + (i * bbytes)),
                    rozofs_inverse,
                    bbytes / rozofs_inverse / sizeof (pxl_t),
                    moj_prj_id, projections);
            /*
	    ** add the footer at the end of the repaired projection
	    */
            rozofs_stor_bins_footer_t *rozofs_bins_foot_p;
            rozofs_bins_foot_p = (rozofs_stor_bins_footer_t*) (projections[moj_prj_id].bins
	                                                      + rozofs_get_psizes(layout,bsize,moj_prj_id));
//CRC       rozofs_bins_foot_p->timestamp      = working_ctx_p->block_ctx_table[block_idx].timestamp;
            rozofs_bins_foot_p->timestamp      = rozofs_bins_hdr_p->s.timestamp;	
          }
	  block_idx++;    	  
        }
    }
}
 
/*
**_________________________________________________________________________
*      PUBLIC FUNCTIONS
**_________________________________________________________________________
*/

/*
**__________________________________________________________________________
*/
/**
*  Get the Mojette projection identifier according to the distribution

   @param dist_p : pointer to the distribution set
   @param sid : reference of the sid within the cluster
   @param fwd : number of projections for a forward
   
   @retval >= 0 : Mojette projection id
   @retval < 0 the sid belongs to the spare part of the distribution set
*/

int rozofs_storcli_get_mojette_proj_id(uint8_t *dist_p,uint8_t sid,uint8_t fwd)
{
   int prj_id;
   
   for (prj_id = 0; prj_id < fwd; prj_id++)
   {
      if (dist_p[prj_id] == sid) return prj_id;
   }
   return -1;
}
/*
**__________________________________________________________________________
*/
/**
*   That function check if a repair block procedure has to be launched after a successfull read
    The goal is to detect the block for which the storage node has reported a crc error
    
    @param working_ctx_p: storcli working context of the read request
    @param rozofs_safe : max number of context to check
    
    @retval 0 : no crc error 
    @retval 1 : there is at least one block with a crc error
*/
int rozofs_storcli_check_repair(rozofs_storcli_ctx_t *working_ctx_p,int rozofs_safe)
{

    rozofs_storcli_projection_ctx_t *prj_ctx_p   = working_ctx_p->prj_ctx;   
    int prj_ctx_idx;
     
	// Storages support repair2 procedure 
    if (storcli_storage_supports_repair2) { 	 
      for (prj_ctx_idx = 0; prj_ctx_idx < rozofs_safe; prj_ctx_idx++,prj_ctx_p++)
      {
    	/*
    	** check for crc error
    	*/
    	if (!ROZOFS_BITMAP64_TEST_ALL0(prj_ctx_p->crc_err_bitmap)) {
          return 1;
    	}	
      }
      return 0;
    }

    // Storage only support repair procedure
	for (prj_ctx_idx = 0; prj_ctx_idx < rozofs_safe; prj_ctx_idx++,prj_ctx_p++)
    {
      /*
      ** check for crc error
      */
      if (prj_ctx_p->crc_err_bitmap[0]!=0) {
        return 1;
      }	
    }	
    return 0;		  
}
/*
**__________________________________________________________________________
*/
/**
  Initial write repair request


  Here it is assumed that storclo is working with the context that has been allocated 
  @param  working_ctx_p: pointer to the working context of a read transaction
 
   @retval : TRUE-> xmit ready event expected
  @retval : FALSE-> xmit  ready event not expected
*/
void rozofs_storcli_repair_req_init(rozofs_storcli_ctx_t *working_ctx_p)
{
   int i;
   storcli_read_arg_t *storcli_read_rq_p = (storcli_read_arg_t*)&working_ctx_p->storcli_read_arg;

   STORCLI_START_NORTH_PROF(working_ctx_p,repair,0);

   /*
   ** set the pointer to to first available data (decoded data)
   */
   working_ctx_p->data_write_p  = working_ctx_p->data_read_p; 
   /*
   ** set now the working variable specific for handling the write
   ** We need one large buffer per projection that will be written on storage
   ** we keep the buffer that have been allocated for the read.
   */
   uint8_t forward_projection = rozofs_get_rozofs_forward(storcli_read_rq_p->layout);
   for (i = 0; i < forward_projection; i++)
   {
     working_ctx_p->prj_ctx[i].prj_state = ROZOFS_PRJ_WR_IDLE;
     if (working_ctx_p->prj_ctx[i].prj_buf == NULL)
     {
       working_ctx_p->prj_ctx[i].prj_buf   = ruc_buf_getBuffer(ROZOFS_STORCLI_SOUTH_LARGE_POOL);
       if (working_ctx_p->prj_ctx[i].prj_buf == NULL)
       {
	 /*
	 ** that situation MUST not occur since there the same number of receive buffer and working context!!
	 */
	 severe("out of large buffer");
	 goto failure;
       }
     }
     /*
     ** set the pointer to the bins
     */
     int position;
	 // For compatibility between new clients and old storages
	 if (storcli_storage_supports_repair2) {
 	   position = rozofs_storcli_repair2_get_position_of_first_byte2write();
	 }
	 else {
 	   position = rozofs_storcli_repair_get_position_of_first_byte2write();	   
	 }	
     uint8_t *pbuf = (uint8_t*)ruc_buf_getPayload(working_ctx_p->prj_ctx[i].prj_buf); 

     working_ctx_p->prj_ctx[i].bins       = (bin_t*)(pbuf+position);   
   }	
   /*
   **  now regenerate the projections that were in error
   */
   rozofs_storcli_transform_forward_repair(working_ctx_p,
                                           storcli_read_rq_p->layout,
                                           storcli_read_rq_p->nb_proj,
                                           (char *)working_ctx_p->data_write_p);    			
   /*
   ** starts the sending of the repaired projections
   */
   rozofs_storcli_write_repair_req_processing(working_ctx_p);
   return;


failure:
  /*
  ** send back the response of the read request towards rozofsmount
  */
  rozofs_storcli_read_reply_success(working_ctx_p);
  /*
  ** release the root transaction context
  */
  STORCLI_STOP_NORTH_PROF(working_ctx_p,repair,0);
  rozofs_storcli_release_context(working_ctx_p);  
}

/*
**__________________________________________________________________________
*/
/*
** That function is called when all the projection are ready to be sent

 @param working_ctx_p: pointer to the root context associated with the top level write request

*/
void rozofs_storcli_write_repair_req_processing(rozofs_storcli_ctx_t *working_ctx_p)
{

  storcli_read_arg_t *storcli_read_rq_p = (storcli_read_arg_t*)&working_ctx_p->storcli_read_arg;
  uint8_t layout = storcli_read_rq_p->layout;
  uint8_t   rozofs_forward;
  uint8_t   projection_id;
  int       error=0;
  int       ret;
  rozofs_storcli_projection_ctx_t *prj_cxt_p   = working_ctx_p->prj_ctx;   
  uint8_t  bsize  = storcli_read_rq_p->bsize;
  int prj_size_in_msg = rozofs_get_max_psize_in_msg(layout,bsize);
  sp_write_repair_arg_no_bins_t  *request; 
  sp_write_repair_arg_no_bins_t   repair_prj_args;
  sp_write_repair2_arg_no_bins_t *request2; 
  sp_write_repair2_arg_no_bins_t  repair2_prj_args;
      
  rozofs_forward = rozofs_get_rozofs_forward(layout);
  
  /*
  ** check if the buffer is still valid: we might face the situation where the rozofsmount
  ** time-out and re-allocate the write buffer located in shared memory for another
  ** transaction (either read or write:
  ** the control must take place only where here is the presence of a shared memory for the write
  */
  error  = 0;
  if (working_ctx_p->shared_mem_p!= NULL)
  {
      uint32_t *xid_p = (uint32_t*)working_ctx_p->shared_mem_p;
      if (*xid_p !=  working_ctx_p->src_transaction_id)
      {
        /*
        ** the source has aborted the request
        */
        error = EPROTO;
      }      
  } 
  /*
  ** send back the response of the read request towards rozofsmount
  */
  rozofs_storcli_read_reply_success(working_ctx_p);
   /*
   ** allocate a sequence number for the working context:
   **   This is mandatory to avoid any confusion with a late response of the previous read request
   */
   working_ctx_p->read_seqnum = rozofs_storcli_allocate_read_seqnum();
  /*
  ** check if it make sense to send the repaired blocks
  */
  if (error)
  {
    /*
    ** the requester has released the buffer and it could be possible that the
    ** rozofsmount uses it for another purpose, so the data that have been repaired
    ** might be wrong, so don't take the right to write wrong data for which we can can 
    ** a good crc !!
    */
    goto fail;
  }
  
  /*
  ** We have enough storage, so initiate the transaction towards the storage for each
  ** projection
  */
  for (projection_id = 0; projection_id < rozofs_forward; projection_id++)
  {
     void  *xmit_buf;  
     int ret;  
	 
     /*
     ** skip the projections for which no error has been detected 
     */
     if (storcli_storage_supports_repair2) {
	   if (ROZOFS_BITMAP64_TEST_ALL0(working_ctx_p->prj_ctx[projection_id].crc_err_bitmap)) continue;
	 }
	 else {
	   if (working_ctx_p->prj_ctx[projection_id].crc_err_bitmap[0] == 0)  continue;
	 } 
	 
	 
     xmit_buf = prj_cxt_p[projection_id].prj_buf;
     if (xmit_buf == NULL)
     {
       /*
       ** fatal error since the ressource control already took place
       */       
       error = EIO;
       goto fail;     
     }
     /*
     ** fill partially the common header
     */
	 if (storcli_storage_supports_repair2) {
       request2   = &repair2_prj_args;
       request2->cid = storcli_read_rq_p->cid;
       request2->sid = (uint8_t) rozofs_storcli_lbg_prj_get_sid(working_ctx_p->lbg_assoc_tb,prj_cxt_p[projection_id].stor_idx);
       request2->layout        = storcli_read_rq_p->layout;
       request2->bsize         = storcli_read_rq_p->bsize;
       /*
       ** the case of spare 1 must not occur because repair is done for th eoptimal distribution only
       */
       if (prj_cxt_p[projection_id].stor_idx >= rozofs_forward) request2->spare = 1;
       else request2->spare = 0;
       memcpy(request2->dist_set, storcli_read_rq_p->dist_set, ROZOFS_SAFE_MAX_STORCLI*sizeof (uint8_t));
       memcpy(request2->fid, storcli_read_rq_p->fid, sizeof (sp_uuid_t));
  //CRCrequest->proj_id = projection_id;
       request2->proj_id = rozofs_storcli_get_mojette_proj_id(storcli_read_rq_p->dist_set,request2->sid,rozofs_forward);
       request2->bid     = storcli_read_rq_p->bid;
       request2->bitmap[0]  = working_ctx_p->prj_ctx[projection_id].crc_err_bitmap[0];     
       request2->bitmap[1]  = working_ctx_p->prj_ctx[projection_id].crc_err_bitmap[1];     
       request2->bitmap[2]  = working_ctx_p->prj_ctx[projection_id].crc_err_bitmap[2];     
       int nb_blocks       = ROZOFS_BITMAP64_NB_BIT1(request2->bitmap);
       request2->nb_proj    = nb_blocks;     

       /*
       ** set the length of the bins part: need to compute the number of blocks
       */

       int bins_len = (prj_size_in_msg * nb_blocks);
       request2->len = bins_len; /**< bins length MUST be in bytes !!! */
       uint32_t  lbg_id = rozofs_storcli_lbg_prj_get_lbg(working_ctx_p->lbg_assoc_tb,prj_cxt_p[projection_id].stor_idx);
       STORCLI_START_NORTH_PROF((&working_ctx_p->prj_ctx[projection_id]),repair_prj,bins_len);
       /*
       ** caution we might have a direct reply if there is a direct error at load balancing group while
       ** ateempting to send the RPC message-> typically a disconnection of the TCP connection 
       ** As a consequence the response fct 'rozofs_storcli_write_repair_req_processing_cbk) can be called
       ** prior returning from rozofs_sorcli_send_rq_common')
       ** anticipate the status of the xmit state of the projection and lock the section to
       ** avoid a reply error before returning from rozofs_sorcli_send_rq_common() 
       ** --> need to take care because the write context is released after the reply error sent to rozofsmount
       */
       working_ctx_p->write_ctx_lock = 1;
       prj_cxt_p[projection_id].prj_state = ROZOFS_PRJ_WR_IN_PRG;

       ret =  rozofs_sorcli_send_rq_common(lbg_id,ROZOFS_TMR_GET(TMR_STORAGE_PROGRAM),STORAGE_PROGRAM,STORAGE_VERSION,SP_WRITE_REPAIR2,
                                           (xdrproc_t) xdr_sp_write_repair2_arg_no_bins_t, (caddr_t) request2,
                                        	xmit_buf,
                                        	working_ctx_p->read_seqnum,
                                        	(uint32_t) projection_id,
                                        	bins_len,
                                        	rozofs_storcli_write_repair_req_processing_cbk,
                                           (void*)working_ctx_p);
     }
	 else {
	 
       request   = &repair_prj_args;
       request->cid = storcli_read_rq_p->cid;
       request->sid = (uint8_t) rozofs_storcli_lbg_prj_get_sid(working_ctx_p->lbg_assoc_tb,prj_cxt_p[projection_id].stor_idx);
       request->layout        = storcli_read_rq_p->layout;
       request->bsize         = storcli_read_rq_p->bsize;
       /*
       ** the case of spare 1 must not occur because repair is done for th eoptimal distribution only
       */
       if (prj_cxt_p[projection_id].stor_idx >= rozofs_forward) request->spare = 1;
       else request->spare = 0;
       memcpy(request->dist_set, storcli_read_rq_p->dist_set, ROZOFS_SAFE_MAX_STORCLI*sizeof (uint8_t));
       memcpy(request->fid, storcli_read_rq_p->fid, sizeof (sp_uuid_t));
  //CRCrequest->proj_id = projection_id;
       request->proj_id = rozofs_storcli_get_mojette_proj_id(storcli_read_rq_p->dist_set,request->sid,rozofs_forward);
       request->bid     = storcli_read_rq_p->bid;
       request->bitmap  = working_ctx_p->prj_ctx[projection_id].crc_err_bitmap[0];     
       int nb_blocks       = ROZOFS_BITMAP64_NB_BIT1_FUNC((uint8_t*)&request->bitmap,8);
       request->nb_proj    = nb_blocks;     

       /*
       ** set the length of the bins part: need to compute the number of blocks
       */

       int bins_len = (prj_size_in_msg * nb_blocks);
       request->len = bins_len; /**< bins length MUST be in bytes !!! */
       uint32_t  lbg_id = rozofs_storcli_lbg_prj_get_lbg(working_ctx_p->lbg_assoc_tb,prj_cxt_p[projection_id].stor_idx);
       STORCLI_START_NORTH_PROF((&working_ctx_p->prj_ctx[projection_id]),repair_prj,bins_len);
       /*
       ** caution we might have a direct reply if there is a direct error at load balancing group while
       ** ateempting to send the RPC message-> typically a disconnection of the TCP connection 
       ** As a consequence the response fct 'rozofs_storcli_write_repair_req_processing_cbk) can be called
       ** prior returning from rozofs_sorcli_send_rq_common')
       ** anticipate the status of the xmit state of the projection and lock the section to
       ** avoid a reply error before returning from rozofs_sorcli_send_rq_common() 
       ** --> need to take care because the write context is released after the reply error sent to rozofsmount
       */
       working_ctx_p->write_ctx_lock = 1;
       prj_cxt_p[projection_id].prj_state = ROZOFS_PRJ_WR_IN_PRG;

       ret =  rozofs_sorcli_send_rq_common(lbg_id,ROZOFS_TMR_GET(TMR_STORAGE_PROGRAM),STORAGE_PROGRAM,STORAGE_VERSION,SP_WRITE_REPAIR,
                                           (xdrproc_t) xdr_sp_write_repair_arg_no_bins_t, (caddr_t) request,
                                        	xmit_buf,
                                        	working_ctx_p->read_seqnum,
                                        	(uint32_t) projection_id,
                                        	bins_len,
                                        	rozofs_storcli_write_repair_req_processing_cbk,
                                           (void*)working_ctx_p);	   
	 }										   

     working_ctx_p->write_ctx_lock = 0;
     if (ret < 0)
     {
        /*
	** there is no retry, just keep on with a potential other projection to repair
	*/
        STORCLI_ERR_PROF(repair_prj_err);
        STORCLI_STOP_NORTH_PROF((&working_ctx_p->prj_ctx[projection_id]),repair_prj,0);
	prj_cxt_p[projection_id].prj_state = ROZOFS_PRJ_WR_ERROR;
	continue;
     } 
     else
     {
       /*
       ** check if the state has not been changed: -> it might be possible to get a direct error
       */
       if (prj_cxt_p[projection_id].prj_state == ROZOFS_PRJ_WR_ERROR)
       {
          /*
	  ** it looks like that we cannot repair that preojection, check if there is some other
	  */
          STORCLI_STOP_NORTH_PROF((&working_ctx_p->prj_ctx[projection_id]),repair_prj,0);

       }      
     }
   }
   /*
   ** check if there some write repair request pending, in such a case we wait for the end of the repair
   ** (answer from the storage node
   */
    ret = rozofs_storcli_all_prj_write_repair_check(storcli_read_rq_p->layout,
                                                    working_ctx_p->prj_ctx);
    if (ret == 0)
    {
       /*
       ** there is some pending write
       */
       return;
    }   
  
fail:
     /*
     ** release the root transaction context
     */
     STORCLI_STOP_NORTH_PROF(working_ctx_p,repair,0);
     rozofs_storcli_release_context(working_ctx_p);  
  return;

}

/*
**__________________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure on a projection write request
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */

void rozofs_storcli_write_repair_req_processing_cbk(void *this,void *param) 
{
   uint32_t   seqnum;
   uint32_t   projection_id;
   rozofs_storcli_projection_ctx_t  *repair_prj_work_p = NULL;   
   rozofs_storcli_ctx_t *working_ctx_p = (rozofs_storcli_ctx_t*) param ;
   XDR       xdrs;       
   uint8_t  *payload;
   int      bufsize;
   sp_status_ret_t   rozofs_status;
   struct rpc_msg  rpc_reply;
   storcli_read_arg_t *storcli_read_rq_p = NULL;
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   int lbg_id;

   
   int status;
   void     *recv_buf = NULL;   
   int      ret;
   int error = 0;

    storcli_read_rq_p = (storcli_read_arg_t*)&working_ctx_p->storcli_read_arg;
    /*
    ** get the sequence number and the reference of the projection id form the opaque user array
    ** of the transaction context
    */
    rozofs_tx_read_opaque_data(this,0,&seqnum);
    rozofs_tx_read_opaque_data(this,1,&projection_id);
    rozofs_tx_read_opaque_data(this,2,(uint32_t*)&lbg_id);
    /*
    ** check if the sequence number of the transaction matches with the one saved in the tranaaction
    ** that control is required because we can receive a response from a late transaction that
    ** it now out of sequence since the system is waiting for transaction response on a next
    ** set of distribution
    ** In that case, we just drop silently the received message
    */
    if (seqnum != working_ctx_p->read_seqnum)
    {
      /*
      ** not the right sequence number, so drop the received message
      */
      goto drop_msg;    
    }
    /*
    ** check if the write is already doen: this might happen in the case when the same projection
    ** is sent twoards 2 different LBG
    */    
    if (working_ctx_p->prj_ctx[projection_id].prj_state == ROZOFS_PRJ_WR_DONE)
    {
      /*
      ** The reponse has already been received for that projection so we don't care about that
      ** extra reponse
      */
      goto drop_msg;       
    }
    /*    
    ** get the status of the transaction -> 0 OK, -1 error (need to get errno for source cause
    */

    STORCLI_STOP_NORTH_PROF((&working_ctx_p->prj_ctx[projection_id]),repair_prj,0);
    status = rozofs_tx_get_status(this);
    if (status < 0)
    {

       /*
       ** something wrong happened: assert the status in the associated projection id sub-context
       ** now, double check if it is possible to retry on a new storage
       */
       working_ctx_p->prj_ctx[projection_id].prj_state = ROZOFS_PRJ_WR_ERROR;
       working_ctx_p->prj_ctx[projection_id].errcode   = rozofs_tx_get_errno(this);
       errno = rozofs_tx_get_errno(this);  
       if (errno == ETIME)
       {
         storcli_lbg_cnx_sup_increment_tmo(lbg_id);
         STORCLI_ERR_PROF(repair_prj_tmo);
       }
       else
       {
	     if (storcli_storage_supports_repair2!=2) {
		   if ((errno==EPROTO)||(errno==ENOTSUP)) storcli_storage_supports_repair2 = 0;
		 }  
         STORCLI_ERR_PROF(repair_prj_err);
       } 
       error = 1;      
    }
    else
    {    
      storcli_lbg_cnx_sup_clear_tmo(lbg_id);
      /*
      ** get the pointer to the receive buffer payload
      */
      recv_buf = rozofs_tx_get_recvBuf(this);
      if (recv_buf == NULL)
      {
	 /*
	 ** something wrong happened
	 */
	 error = EFAULT;  
	 working_ctx_p->prj_ctx[projection_id].prj_state = ROZOFS_PRJ_WR_ERROR;
	 working_ctx_p->prj_ctx[projection_id].errcode = error;
	 STORCLI_ERR_PROF(repair_prj_err);
	 goto fatal;         
      }
      /*
      ** set the useful pointer on the received message
      */
      payload  = (uint8_t*) ruc_buf_getPayload(recv_buf);
      payload += sizeof(uint32_t); /* skip length*/
      /*
      ** OK now decode the received message
      */
      bufsize = ruc_buf_getPayloadLen(recv_buf);
      bufsize -= sizeof(uint32_t); /* skip length*/
      xdrmem_create(&xdrs,(char*)payload,bufsize,XDR_DECODE);
      while (1)
      {
	/*
	** decode the rpc part
	*/
	if (rozofs_xdr_replymsg(&xdrs,&rpc_reply) != TRUE)
	{
	   TX_STATS(ROZOFS_TX_DECODING_ERROR);
	   errno = EPROTO;
	   STORCLI_ERR_PROF(repair_prj_err);
           error = 1;
           break;
	}
	/*
	** decode the status of the operation
	*/
	if (xdr_sp_status_ret_t(&xdrs,&rozofs_status)!= TRUE)
	{
          errno = EPROTO;
  	  STORCLI_ERR_PROF(repair_prj_err);
          error = 1;
          break;    
	}
	/*
	** check th estatus of the operation
	*/
	if ( rozofs_status.status != SP_SUCCESS )
	{
           errno = rozofs_status.sp_status_ret_t_u.error;
           error = 1;
   	   STORCLI_ERR_PROF(repair_prj_err);
           break;    
	}
	break;
      }
    }
    /*
    ** check the status of the operation
    */
    if (error)
    {
       /*
       ** there was an error on the remote storage while attempt to write the file
       ** try to write the projection on another storaged
       */
       working_ctx_p->prj_ctx[projection_id].prj_state = ROZOFS_PRJ_WR_ERROR;
       working_ctx_p->prj_ctx[projection_id].errcode   = errno;  
    }
    else
    {
	   if (storcli_storage_supports_repair2==1) storcli_storage_supports_repair2 = 2;
       /*
       ** set the pointer to the read context associated with the projection for which a response has
       ** been received
       */
       repair_prj_work_p = &working_ctx_p->prj_ctx[projection_id];
       /*
       ** set the status of the transaction to done for that projection
       */
       repair_prj_work_p->prj_state = ROZOFS_PRJ_WR_DONE;
       repair_prj_work_p->errcode   = errno;
    }
    /*
    ** OK now check if we have send enough projection
    ** if it is the case, the distribution will be valid
    */
    ret = rozofs_storcli_all_prj_write_repair_check(storcli_read_rq_p->layout,
                                                    working_ctx_p->prj_ctx);
    if (ret == 0)
    {
       /*
       ** no enough projection 
       */
       goto wait_more_projection;
    }
    /*
    ** caution lock can be asserted either by a write retry attempt or an initial attempt
    */
    if (working_ctx_p->write_ctx_lock != 0) goto wait_more_projection;
    /*
    ** repair is finished,
    ** release the root context and the transaction context
    */
    if(recv_buf!= NULL) ruc_buf_freeBuffer(recv_buf);       

    STORCLI_STOP_NORTH_PROF(working_ctx_p,repair,0);
    rozofs_storcli_release_context(working_ctx_p);    
    rozofs_tx_free_from_ptr(this);
    return;
    
    /*
    **_____________________________________________
    **  Exception cases
    **_____________________________________________
    */    
drop_msg:
    /*
    ** the message has not the right sequence number,so just drop the received message
    ** and release the transaction context
    */  
     if(recv_buf!= NULL) ruc_buf_freeBuffer(recv_buf);       
     rozofs_tx_free_from_ptr(this);
     return;

fatal:
    /*
    ** unrecoverable error : mostly a bug!!
    */  
    if(recv_buf!= NULL) ruc_buf_freeBuffer(recv_buf);       
    rozofs_tx_free_from_ptr(this);
    /*
    ** caution lock can be asserted either by a write retry attempt or an initial attempt
    */
    if (working_ctx_p->write_ctx_lock != 0) return;
    /*
    ** release the root transaction context
    */
    STORCLI_STOP_NORTH_PROF(working_ctx_p,repair,0);
    rozofs_storcli_release_context(working_ctx_p);  
    return;

        
wait_more_projection:    
    /*
    ** need to wait for some other write transaction responses
    ** 
    */
    if(recv_buf!= NULL) ruc_buf_freeBuffer(recv_buf);           
    rozofs_tx_free_from_ptr(this);
    return;


}

