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
#include <rozofs/common/xmalloc.h>
#include <errno.h>
#include <inttypes.h>
#include <rozofs/common/log.h>
#include "geocli_srv.h"
#include "rozofsmount.h"
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/common/geo_replica_str.h>
#include "rzcp_file_ctx.h"
#include "rzcp_copy.h"

void geo_cli_geo_file_sync_remove_end_cbk(void *param,int status);
int geo_cli_no_abort = 1;
/*
**____________________________________________________
*/
/**
*  Start of a copy process for a bunch of files

   That function is intended be called after the
   reception of either a sync_req or a sync_getnext_req
   response

   - start the file synchro process
   
*/
void geo_cli_geo_file_sync_processing(geocli_ctx_t *p)
{
    geo_fid_entry_t *info_p = (geo_fid_entry_t*)p->data_record;  
    geo_fid_entry_t *file_p;
    rzcp_copy_ctx_t *cpy_p=NULL;
    uint64_t off_aligned;
    uint64_t len_aligned;
    int ret;
    int status = -1;
    /*
    ** check the number of file to synchronize
    */
    if ((p->nb_records == 0) || (p->cur_record == p->nb_records))
    {
      if (p->last ) p->state_sync = GEOSYNC_ST_GETDEL;    
      else p->state_sync = GEOSYNC_ST_GETNEXT;
      p->state = GEOCLI_ST_IDLE;      
      return;
    }
    /*
    ** OK there is some file to synchronize
    ** get the first record and initiate a copy
    */

    
    file_p = info_p+p->cur_record;
    
    p->cur_record++;
    /*
    ** align off and len on a boundary block size
    */
    if ((0!=file_p->off_start) || (0 != file_p->off_end))
    {
      START_RZCPY_PROFILING(copy_file);
      rzcp_align_off_and_len(file_p->off_start,(file_p->off_end -file_p->off_start),&off_aligned,&len_aligned);
      /*
      ** allocate a copy context
      */
      cpy_p = rzcp_copy_init(file_p->fid,file_p->cid,file_p->sids,file_p->layout,
                             off_aligned,len_aligned,
			     file_p->fid,file_p->cid,file_p->sids,file_p->layout,
			     p,geo_cli_geo_file_sync_read_end_cbk);
      if (cpy_p == NULL)
      {
	/*
	** out of context
	*/
	p->state_sync = GEOSYNC_ST_IDLE;
	p->state = GEOCLI_ST_IDLE;
	STOP_RZCPY_PROFILING(copy_file,status);      
	return;
      }
      /*
      ** now initiate the first read
      */
      cpy_p->storcli_idx = 0;
      ret = rzcp_read_req(cpy_p);
      if (ret < 0)
      {
	 /*
	 ** abort the copy
	 */
	RZCP_CTX_STATS(RZCP_CTX_CPY_ABORT_ERR);
	p->state_sync = GEOSYNC_ST_IDLE;
	p->state = GEOCLI_ST_IDLE;
	/*
	** release the copy context
	*/
	rzcp_free_from_ptr(cpy_p);
	STOP_RZCPY_PROFILING(copy_file,status);      
	return;
      }
      /*
      ** it looks good wait for the read response
      */
      return;
    }
    /*
    **  case of the file delete
    */
    START_RZCPY_PROFILING(remove_file);
    cpy_p = rzcp_copy_init(file_p->fid,file_p->cid,file_p->sids,file_p->layout,
                           0,0,
			   file_p->fid,file_p->cid,file_p->sids,file_p->layout,
			   p,geo_cli_geo_file_sync_remove_end_cbk);
    if (cpy_p == NULL)
    {
      /*
      ** out of context
      */
      p->state_sync = GEOSYNC_ST_IDLE;
      p->state = GEOCLI_ST_IDLE;
      STOP_RZCPY_PROFILING(remove_file,status);      
      return;
    }
    /*
    ** now initiate the file remove
    */
    cpy_p->rzcp_caller_cbk = geo_cli_geo_file_sync_remove_end_cbk;
    cpy_p->storcli_idx = 0;
    ret = rzcp_remove_req(cpy_p);
    if (ret < 0)
    {
       /*
       ** abort the copy
       */
      RZCP_CTX_STATS(RZCP_CTX_CPY_ABORT_ERR);
      p->state_sync = GEOSYNC_ST_IDLE;
      p->state = GEOCLI_ST_IDLE;
      /*
      ** release the copy context
      */
      rzcp_free_from_ptr(cpy_p);
      STOP_RZCPY_PROFILING(remove_file,status);      
      return;
    }
    /*
    ** it looks good wait for the read response
    */
    return;
}       

/*
**____________________________________________________
*/
 /**
 *   end of read event
 
    @param param: pointer to the copy context
    @param status: status of the operation : 0 success / < 0 error
    
    in case of error, errno contains the reason
    
    @retval none
*/
void geo_cli_geo_file_sync_read_end_cbk(void *param,int status)
{
    rzcp_copy_ctx_t *cpy_p=(rzcp_copy_ctx_t*)param;
    geocli_ctx_t *p = cpy_p->opaque;
    int ret;
    
    /*
    ** check the read status: in case of error the copy is aborted
    */
    if (status < 0)
    {
       /*
       ** check the case of ENOENT
       */
       if (errno != ENOENT) goto abort_check;         
       /*
       ** the file is empty: release the copy context
       */
       rzcp_free_from_ptr(cpy_p);
      /*
      ** nothing has been read, so it is the end of the copy
      */
      p->status_bitmap &= ~(1<< (p->cur_record-1));
      status = 0;
      STOP_RZCPY_PROFILING(copy_file,status);      
      geo_cli_geo_file_sync_processing(p);
      return;
    }
    cpy_p->write_ctx.off_cur = cpy_p->read_ctx.off_cur;
    cpy_p->write_ctx.len_cur = cpy_p->received_len;
    if (cpy_p->received_len == 0)
    {
       /*
       ** release the copy context
       */
       rzcp_free_from_ptr(cpy_p);
      /*
      ** nothing has been read, so it is the end of the copy
      */
      p->status_bitmap &= ~(1<< (p->cur_record-1));
      status = 0;
      STOP_RZCPY_PROFILING(copy_file,status);      
      geo_cli_geo_file_sync_processing(p);
      return;
    }
    /*
    ** update the length 
    */
    int bbytes = ROZOFS_BSIZE_BYTES(exportclt.bsize);
    cpy_p->read_ctx.initial_len -= cpy_p->received_len;
    cpy_p->read_ctx.off_cur += cpy_p->received_len;
    if ((cpy_p->received_len%bbytes)!=0)
    {
       /*
       ** something wrong in copy
       */
       RZCP_CTX_STATS(RZCP_CTX_CPY_BAD_READ_LEN);
       goto abort_check;
    }  
    /*
    ** initiate the write:
    ** we need to copy read buffer towards write buffer
    */
    {
       rzcp_file_ctx_t *rw_ctx_p;
       int alignment;

       uint8_t *from = (uint8_t*)ruc_buf_getPayload(cpy_p->shared_buf_ref[SHAREMEM_IDX_READ]) + (4096);
       rw_ctx_p = &cpy_p->write_ctx;
       uint32_t *to = (uint32_t*)ruc_buf_getPayload(cpy_p->shared_buf_ref[SHAREMEM_IDX_WRITE]);
       alignment = rw_ctx_p->off_cur%16;
       to[2] = alignment;
       /*
       ** Set pointer to the buffer start and adjust with alignment
       */
       uint8_t * buf_start = (uint8_t *)&to[4];
       buf_start += alignment;
       memcpy(buf_start,from,cpy_p->received_len);    
    }

    RZCPY_PROFILING_BYTES(copy_file,cpy_p->received_len);
    cpy_p->rzcp_caller_cbk = geo_cli_geo_file_sync_write_end_cbk;
    cpy_p->storcli_idx = 1;
    ret = rzcp_write_req(cpy_p);
    if (ret < 0)
    {
        RZCP_CTX_STATS(RZCP_CTX_CPY_WRITE_ERR);
       goto abort;    
    }
    /*
    ** OK, now wait for the end of the writing
    */
    return;
 abort:
    /*
    ** abort the copy
    */
    RZCP_CTX_STATS(RZCP_CTX_CPY_ABORT_ERR);
    p->state_sync = GEOSYNC_ST_IDLE;
    p->state = GEOCLI_ST_IDLE;
    /*
    ** release the copy context
    */
    rzcp_free_from_ptr(cpy_p);
    STOP_RZCPY_PROFILING(copy_file,status);      
    return;         

 abort_check:
    /*
    ** abort the copy for the current record
    */
    rzcp_free_from_ptr(cpy_p);
    p->status_bitmap |= 1<< (p->cur_record-1);
    p->delete_forbidden = 1;
    STOP_RZCPY_PROFILING(copy_file,status);      
    geo_cli_geo_file_sync_processing(p);
    return;     

}

/*
**____________________________________________________
*/
 /**
 *   end of write event
 
    @param param: pointer to the copy context
    @param status: status of the operation : 0 success / < 0 error
    
    in case of error, errno contains the reason
    
    @retval none
*/
void geo_cli_geo_file_sync_write_end_cbk(void *param,int status)
{
    rzcp_copy_ctx_t *cpy_p=(rzcp_copy_ctx_t*)param;
    geocli_ctx_t *p = cpy_p->opaque;
    int ret;
    /*
    ** check the read status: in case of error the copy is aborted
    */
    if (status < 0)
    {
      RZCP_CTX_STATS(RZCP_CTX_CPY_WRITE_ERR);
      goto abort_check;         
    }
    /*
    ** check if a new read must be initiated
    */
    if (cpy_p->read_ctx.initial_len <= 0)
    {
      /*
      ** this is the end of the copy for that file
      ** attempt to copy a new one
      */
      p->status_bitmap &= ~(1<< (p->cur_record-1));
      rzcp_free_from_ptr(cpy_p);
      status = 0;
      STOP_RZCPY_PROFILING(copy_file,status);      
      return geo_cli_geo_file_sync_processing(p);
    }
    /*
    ** it is not the end, so read more
    */
    cpy_p->rzcp_caller_cbk = geo_cli_geo_file_sync_read_end_cbk;
    cpy_p->storcli_idx = 0;
    ret = rzcp_read_req(cpy_p);
    if (ret < 0)
    {
       /*
       ** abort the copy
       */
       RZCP_CTX_STATS(RZCP_CTX_CPY_READ_ERR);
       goto abort;
    }
    /*
    ** wait for the end of read
    */
    return;
    
 abort:
    /*
    ** abort the copy
    */
    p->state_sync = GEOSYNC_ST_IDLE;
    p->state = GEOCLI_ST_IDLE;
    RZCP_CTX_STATS(RZCP_CTX_CPY_ABORT_ERR);
    /*
    ** release the copy context
    */
    rzcp_free_from_ptr(cpy_p);
    STOP_RZCPY_PROFILING(copy_file,status);      
    return;      

 abort_check:
    /*
    ** abort the copy for the current record
    */
    rzcp_free_from_ptr(cpy_p);
    p->status_bitmap |= 1<< (p->cur_record-1);
    p->delete_forbidden = 1;
    STOP_RZCPY_PROFILING(copy_file,status);      
    geo_cli_geo_file_sync_processing(p);
    return;     

}

/*
**____________________________________________________
*/
 /**
 *   end of remove event
 
    @param param: pointer to the copy context
    @param status: status of the operation : 0 success / < 0 error
    
    in case of error, errno contains the reason
    
    @retval none
*/
void geo_cli_geo_file_sync_remove_end_cbk(void *param,int status)
{
    rzcp_copy_ctx_t *cpy_p=(rzcp_copy_ctx_t*)param;
    geocli_ctx_t *p = cpy_p->opaque;
    int ret;
    
    /*
    ** check the read status: in case of error the copy is aborted
    */
    if (status < 0)
    {
      goto abort_check;         
    }
    if (cpy_p->storcli_idx == 0)
    {
      /*
      ** now initiate the file remove on the other side
      */
      cpy_p->rzcp_caller_cbk = geo_cli_geo_file_sync_remove_end_cbk;
      cpy_p->storcli_idx = 1;
      ret = rzcp_remove_req(cpy_p);
      if (ret < 0)
      {
	goto abort;
      }
      return;
    }
     /*
     ** all is fine,release the copy context
     */
     p->status_bitmap &= ~(1<< (p->cur_record-1));
     rzcp_free_from_ptr(cpy_p);
     /*
     ** nothing has been read, so it is the end of the copy
     */
     status = 0;
     STOP_RZCPY_PROFILING(remove_file,status);      
     geo_cli_geo_file_sync_processing(p);
     return;


 abort:
    /*
    ** abort the copy
    */
    RZCP_CTX_STATS(RZCP_CTX_CPY_ABORT_ERR);
    p->state_sync = GEOSYNC_ST_IDLE;
    p->state = GEOCLI_ST_IDLE;
    /*
    ** release the copy context
    */
    rzcp_free_from_ptr(cpy_p);
    STOP_RZCPY_PROFILING(remove_file,status);      
    return;         

 abort_check:
    /*
    ** abort the copy for the current record
    */
    rzcp_free_from_ptr(cpy_p);
    p->status_bitmap |= 1<< (p->cur_record-1);
    p->delete_forbidden = 1;
    STOP_RZCPY_PROFILING(remove_file,status);      
    geo_cli_geo_file_sync_processing(p);
    return; 

}
