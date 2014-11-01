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

//#define TRACE_FS_READ_WRITE 1
//#warning TRACE_FS_READ_WRITE active

#include <rozofs/rpc/eproto.h>
#include <rozofs/rpc/storcli_proto.h>

#include "rozofs_fuse_api.h"
#include "rozofs_modeblock_cache.h"
#include "rozofs_rw_load_balancing.h"
#include "rozofs_sharedmem.h"

DECLARE_PROFILING(mpp_profiler_t);

void export_write_block_nb(void *fuse_ctx_p, file_t *file_p);

static int64_t write_buf_nb(void *buffer_p,file_t * f, uint64_t off, const char *buf, uint32_t len);

void rozofs_ll_write_cbk(void *this,void *param);
void rozofs_clear_file_lock_owner(file_t * f);
/*
** write alignment statistics
*/
uint64_t    rozofs_write_buf_flush = 0;
uint64_t    rozofs_aligned_write_start[2];
uint64_t    rozofs_aligned_write_end[2];

#define CLEAR_WRITE(p) \
{ \
  p->write_pos = 0;\
  p->write_from = 0;\
  p->buf_write_wait = 0;\
}


#define CLEAR_READ(p) \
{ \
  p->read_pos = 0;\
  p->read_from = 0;\
  p->buf_read_wait = 0;\
}

int ROZOFS_MAX_WRITE_PENDING = 1;
typedef struct _WRITE_FLUSH_STAT_T {
  uint64_t non_synchroneous;
  uint64_t synchroneous;
  uint64_t synchroneous_success;
  uint64_t synchroneous_error;
} WRITE_FLUSH_STAT_T;

static WRITE_FLUSH_STAT_T write_flush_stat;
/*
**__________________________________________________________________
*/
/**
   Reset write synch stats
*/
static inline void reset_write_flush_stat(void) {
  memset(&write_flush_stat,0,sizeof(write_flush_stat));
}
/*
**__________________________________________________________________
*/
/**
   Display write synch stats
*/
static char * display_write_flush_stat_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"write_flush set <value> : set new write pending maximum value\n");
  pChar += sprintf(pChar,"write_flush reset       : reset statistics\n");
  pChar += sprintf(pChar,"write_flush             : display statistics\n");  
  return pChar; 
}
#define SHOW_STAT_WRITE_FLUSH(name) pChar += sprintf(pChar,"%-26s :  %10llu\n","  "#name ,(long long unsigned int) write_flush_stat.name);
static void display_write_flush_stat(char * argv[], uint32_t tcpRef, void *bufRef){
  char *pChar = uma_dbg_get_buffer();
  int   new_val;

  if (argv[1] != NULL) {
    if (strcmp(argv[1],"reset")==0) {
      reset_write_flush_stat();
      uma_dbg_send(tcpRef, bufRef, TRUE, "Reset done\n");  
      return;         
    }
    if (strcmp(argv[1],"set")==0) {
      errno = 0;       
      new_val = (int) strtol(argv[2], (char **) NULL, 10);   
      if (errno != 0) {
        pChar += sprintf(pChar, "bad value %s\n",argv[2]);
        pChar = display_write_flush_stat_help(pChar);
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
        return;
      } 
      ROZOFS_MAX_WRITE_PENDING = new_val;
      reset_write_flush_stat();
      uma_dbg_send(tcpRef, bufRef, TRUE, "New write pending maximum value is %d\n",ROZOFS_MAX_WRITE_PENDING); 
      return;   
    } 
    /*
    ** Help
    */
    pChar = display_write_flush_stat_help(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
    return;
  }
  pChar += sprintf(pChar,"Write pending maximum value is %d\n",ROZOFS_MAX_WRITE_PENDING);
  SHOW_STAT_WRITE_FLUSH(non_synchroneous);
  SHOW_STAT_WRITE_FLUSH(synchroneous);
  SHOW_STAT_WRITE_FLUSH(synchroneous_success);
  SHOW_STAT_WRITE_FLUSH(synchroneous_error);    
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());  
}  

/*
**__________________________________________________________________
*/
/**
   Bugwatch flag set/reset
*/
int rozofs_bugwatch = 0;

static char * bugwatch_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"bugwatch enable  : set bugwatch_flag\n");
  pChar += sprintf(pChar,"bugwatch disable : reset bugwatch flag\n");
  pChar += sprintf(pChar,"bugwatch         : display current state\n");  
  return pChar; 
}


static void bugwatch_proc(char * argv[], uint32_t tcpRef, void *bufRef){
  char *pChar = uma_dbg_get_buffer();

  if (argv[1] != NULL) {
    if (strcmp(argv[1],"enable")==0) {
      rozofs_bugwatch = 1;
      uma_dbg_send(tcpRef, bufRef, TRUE, "bugwatch enabled\n");  
      return;         
    }
    if (strcmp(argv[1],"disable")==0) {
      rozofs_bugwatch = 1;
      uma_dbg_send(tcpRef, bufRef, TRUE, "bugwatch disabled\n");   
      return;     
    }   
    /*
    ** Help
    */
    pChar = bugwatch_help(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
    return;
  }
  uma_dbg_send(tcpRef, bufRef, TRUE, "BUGROZOFSWATCH is %s\n",(rozofs_bugwatch==0)?"disabled":"enabled"); 
}  
/*
**__________________________________________________________________
*/
/**
   Initialize write synch stat service
*/
void init_write_flush_stat(int max_write_pending){
  ROZOFS_MAX_WRITE_PENDING = max_write_pending;
  reset_write_flush_stat();
  uma_dbg_addTopic_option("write_flush", display_write_flush_stat,UMA_DBG_OPTION_RESET);  
  uma_dbg_addTopic("bugwatch", bugwatch_proc);  
}

/*
**__________________________________________________________________
*/
/**
   API to clear the buffer after a flush
   If some data is pending in the buffer the clear is not done
   
   @param *f : pointer to the file structure where read buffer information can be retrieved
   
   @retval -1 some data to write is pending
   @retval 0 if the read buffer is not empty
*/
int clear_read_data(file_t *f)
{
  f->read_from = f->read_pos = 0;
  return 0;
}
/*
**__________________________________________________________________
*/
/**
   API to check if the read section is empty
   
   @param *p : pointer to the file structure where read buffer information can be retrieved
   
   @retval 1 if the read buffer is empty
   @retval 0 if the read buffer is not empty
*/
static inline int read_section_empty(file_t *p)
{
   return (p->read_from == p->read_pos)?1:0;
}

/*
**__________________________________________________________________
*/
/**
   API to check if the write section is empty
   
   @param *p : pointer to the file structure where write buffer information can be retrieved
   
   @retval 1 if the write buffer is empty
   @retval 0 if the write buffer is not empty
*/
static inline int write_section_empty(file_t *p)
{
   return (p->write_from == p->write_pos)?1:0;
}

/*
**__________________________________________________________________
*/
/**
*  flush the content of the buffer to the disk

  @param fuse_ctx_p: pointer to the fuse transaction context
  @param p : pointer to the file structure that contains buffer information
  
  @retval len_write >= total data length push to the disk
  @retval < 0 --> error while attempting to initiate a write request towards storcli 
*/
int buf_flush(void *fuse_ctx_p,file_t *p)
{
//  if (p->buf_write_wait == 0) return 0;
  
  uint64_t flush_off = p->write_from;
  uint32_t flush_len = (uint32_t)(p->write_pos - p->write_from);
  uint32_t flush_off_buf = (uint32_t)(p->write_from - p->read_from);
  char *buffer;
  ientry_t *ie;
  /*
  ** stats
  */
  rozofs_fuse_read_write_stats_buf.flush_buf_cpt++;
  
  buffer = p->buffer+flush_off_buf;
  /*
  ** Push the data in the cache
  */
  rozofs_mbcache_insert(p->fid,flush_off,(uint32_t)flush_len,(uint8_t*)buffer);  
  /*
  ** trigger the write
  */
  if ((write_buf_nb(fuse_ctx_p,p, flush_off, buffer, flush_len)) < 0) {
    return -1;
  }
  p->buf_write_wait = 0;
  ie = (ientry_t *) p->ie;
  if (ie->write_pending == p) ie->write_pending = NULL;
  return 0;


}

/*
**__________________________________________________________________
*/
/**
*  API to align the write_from pointer to a ROZOFS_BSIZE boundary

  @param p : pointer to the file structure that contains buffer information
  
  @retval none
*/
static inline void buf_align_write_from(file_t *p)
{
   uint64_t off2start_w;
   uint64_t off2start;

   off2start_w = (p->write_from/ROZOFS_BSIZE);
   off2start = off2start_w*ROZOFS_BSIZE;
   if ( off2start >= p->read_from)
   {
     p->write_from = off2start;
   }      
}
/*
**__________________________________________________________________
*/
/**
*  API to align the write_pos pointer to a ROZOFS_BSIZE boundary
  it is assumed that write_pos,write_from, read_from and read_pos are already computed
  
  @param p : pointer to the file structure that contains buffer information
  
  @retval none
*/
static inline void buf_align_write_pos(file_t *p)
{        
   uint64_t off2end;

   /*
   ** attempt to align write_pos on a block boundary
   */
   if (p->write_pos%ROZOFS_BSIZE)
   {
      /*
      ** not on a block boundary
      */
      off2end = ((p->write_pos/ROZOFS_BSIZE)+1)*ROZOFS_BSIZE;
      if (off2end <= p->read_pos)
      {
        /*
        ** align on block boundary
        */
        p->write_pos = off2end;           
      }
   }
}
/*
**__________________________________________________________________
*/
/**
*  API to write data towards disks in asynchronous mode
  
  The asynchronous write is triggered for the following case
    - the data to write does not fit in the current buffer-> a previous write is pending and a write
      is trigger for that pending write while the new data are inserted in the cache buffer associated
      with the file_t structure.
    - the data to write fits partially in the buffer. SO we ned to flush the data that are been filled
      at the end of the current buffer before copying the remaining data in the buffer

  @param ie: ientry of the file
  @param fuse_ctx_p: pointer to the fuse transaction context
  @param p : pointer to the file structure that contains buffer information
  @param off : first byte to write
  @param len : data write length
  @param buf : user buffer where write data are found
  @param status_p: pointer to the reported status
     BUF_STATUS_DONE: the data has be written in the cache buffer without triggered any write to disk
     BUF_STATUS_WR_IN_PRG: the data has be written in the cache buffer and a write request is in progress (async)
     BUF_STATUS_FAILURE : error while writing data (see errcode in structure rozo_buf_rw_status_t)
      
  @retval none
*/
void buf_file_write_nb(ientry_t * ie,
                    void *fuse_ctx_p,
                    rozo_buf_rw_status_t *status_p,
                    file_t * p,
                    uint64_t off, 
                    const char *buf, 
                    uint32_t len) 
{ 

  uint64_t off_requested = off;
  uint64_t pos_requested = off+len;
  int state;
  int action;
  uint64_t off2end;
  uint32_t buf_off;
  int ret;
  char *dest_p;
  char *src_p;
  uint32_t len_alignment;

  /*
  ** Check whether some pending data written by an other application
  ** on the same file is pending. In that case flush it immediatly.
  */
  if ((ie->write_pending != NULL) && (ie->write_pending != p)) {
    flush_write_ientry(ie);
  }
    
  /*
  ** Check whether the buffer content is valid or if it must be forgotten
  */
  if (p->read_consistency != ie->read_consistency) {
    /* The file has been modified since this buffer has been read. The data
    ** it contains are questionable. Better forget them.
    */
    p->read_from = p->read_pos = 0;
  }
    
  ie->read_consistency++;
  p->read_consistency = ie->read_consistency;
  

 status_p->status = BUF_STATUS_FAILURE;
 status_p->errcode = EINVAL ;
  /*
  ** check the start point 
  */
  while(1)
  {
    if (!read_section_empty(p))
    {
      /*
      ** we have either a read or write section, need to check read section first
      */
      if (off_requested >= p->read_from) 
      {
         if (off_requested <= p->read_pos)
         {
           state = BUF_ST_READ_INSIDE;  
           break;   
         } 
         state = BUF_ST_READ_AFTER;
         break;    
      }
      /*
      ** the write start off is out side of the cache buffer
      */
      state = BUF_ST_READ_BEFORE;
      break;
    }
    /*
    ** the buffer is empty (neither read nor write occurs
    */
    state = BUF_ST_EMPTY;
    break;
  }
  /*
  ** ok, now check the end
  */
//  printf("state %s\n",rozofs_buf_read_write_state_e2String(state));
  switch (state)
  {
    case BUF_ST_EMPTY:
      action = BUF_ACT_COPY_EMPTY;
      break;
    case BUF_ST_READ_BEFORE:
        action = BUF_ACT_FLUSH_THEN_COPY_NEW;
        break;    

    case BUF_ST_READ_INSIDE:
      /*
      ** the start is inside the write buffer check if the len does not exceed the buffer length
      */
      if ((pos_requested - p->read_from) >= p->export->bufsize) 
      {
        if (write_section_empty(p)== 0)
        {        
          action = BUF_ACT_FLUSH_ALIGN_THEN_COPY_NEW;
          break;      
        }
        action = BUF_ACT_COPY_OVERLAP;
        break;
      }
      action = BUF_ACT_COPY;
      break;
      
    case BUF_ST_READ_AFTER:
        action = BUF_ACT_FLUSH_THEN_COPY_NEW;
        break;      
  }
//  printf("action %s\n",rozofs_buf_read_write_action_e2String(action));
  /*
  ** OK now perform the required action
  */
  switch (action)
  {
    case BUF_ACT_COPY_EMPTY:
#ifdef TRACE_FS_READ_WRITE
      info("BUF_ACT_COPY_EMPTY state %d write[%llx:%llx],wrb%d[%llx:%llx],rdb[%llx:%llx]",
            state,(long long unsigned int)off,(long long unsigned int)(off+len),
            p->buf_write_wait,
            (long long unsigned int)p->write_from,(long long unsigned int)p->write_pos,
            (long long unsigned int)p->read_from,(long long unsigned int)p->read_pos);
#endif            
      p->write_from = off_requested; 
      p->write_pos  = pos_requested; 
      p->buf_write_wait = 1;
      p->read_from = off_requested; 
      p->read_pos  = pos_requested; 
      /*
      ** copy the buffer
      */
      memcpy(p->buffer,buf,len);
      /*
      ** fill the status section
      */
      status_p->status = BUF_STATUS_DONE;
      status_p->errcode = 0;
      break;

    case BUF_ACT_COPY:
        /** 
        *      off_req                        pos_req
        *            +-----------------------------+

        *      off_req                 pos_req
        *            +-------------------+
        *
        *   +--------------------------------+
        *   rd_f                             rd_p
        *   +-------------------------------------------+  BUFSIZE
        *  
        *  
        */
#ifdef TRACE_FS_READ_WRITE
      info("BUF_ACT_COPY state %d write[%llx:%llx],wrb%d[%llx:%llx],rdb[%llx:%llx]",
            state,(long long unsigned int)off,(long long unsigned int)(off+len),
            p->buf_write_wait,
            (long long unsigned int)p->write_from,(long long unsigned int)p->write_pos,
            (long long unsigned int)p->read_from,(long long unsigned int)p->read_pos);
#endif
      if (write_section_empty(p))
      {
        p->write_from = off_requested; 
        p->write_pos  = pos_requested; 
      }
      else
      {
        if (p->write_from > off_requested) p->write_from = off_requested;      
        if (p->write_pos  < pos_requested) p->write_pos  = pos_requested;      
      }
      buf_align_write_from(p);
      if (p->write_pos > p->read_pos) p->read_pos = p->write_pos;
      else
      {
        /*
        ** attempt to align write_pos on a block boundary
        */
        buf_align_write_pos(p);
      }
      p->buf_write_wait = 1;
      /*
      ** copy the buffer
      */
      dest_p = p->buffer + (off_requested - p->read_from);
      memcpy(dest_p,buf,len);
      /*
      ** fill the status section
      */
      status_p->status = BUF_STATUS_DONE;
      status_p->errcode = 0;
      break;  

      /*
      **  there is no write_wait in the buffer
      */
    case BUF_ACT_COPY_OVERLAP:
        /** 
        *      off_req                        pos_req
        *            +-----------------------------+
        *   +-------------------+
        *   rd_f               rd_p
        *   +----------------------------------+  BUFSIZE
        *  
        *  
        */

      /*
      ** check for off_requested alignment on a block size boundary 
      */
#ifdef TRACE_FS_READ_WRITE
      info("BUF_ACT_COPY_OVERLAP state %d write[%llx:%llx],wrb%d[%llx:%llx],rdb[%llx:%llx]",
            state,(long long unsigned int)off,(long long unsigned int)(off+len),
            p->buf_write_wait,
            (long long unsigned int)p->write_from,(long long unsigned int)p->write_pos,
            (long long unsigned int)p->read_from,(long long unsigned int)p->read_pos);      
#endif
      p->write_from = off_requested;
      buf_align_write_from(p);
      if (off_requested == p->write_from)
      {
        /**
        * forget previous buffer content, replace with new content
        */
        dest_p = p->buffer;
        memcpy(dest_p,buf,len);
        /*
        ** update the offsets
        */
        p->write_pos  = pos_requested; 
        p->buf_write_wait = 1;
        p->read_from = p->write_from; 
        p->read_pos  = p->write_pos;        
        /*
        ** fill the status section
        */
        status_p->status = BUF_STATUS_DONE;
        status_p->errcode = 0;
        break;      
      }
      /*
      ** check the length need for alignment
      */
      len_alignment =  off_requested - p->write_from;
      if ((len+ len_alignment) > p->export->bufsize)
      {
         /**
         *  we cannot align the data to write so copy it unaligned
        * forget previous buffer content, replace with new content
        */
        dest_p = p->buffer;
        memcpy(dest_p,buf,len);
        /*
        ** update the offsets
        */
        p->write_from = off_requested;
        p->write_pos  = pos_requested; 
        p->buf_write_wait = 1;
        p->read_from = p->write_from; 
        p->read_pos  = p->write_pos;        
        /*
        ** fill the status section
        */
        status_p->status = BUF_STATUS_DONE;
        status_p->errcode = 0;
        break;                     
      }  
      /*
      ** there is enough room in the buffer so move the alignment portion
      ** to the front of the buffer
      */ 
      dest_p = p->buffer;
      src_p = p->buffer + (off_requested - len_alignment -p->read_from);
      memcpy(dest_p,src_p,len_alignment); 
      /*
      ** now copy the write buffer
      */
      dest_p = p->buffer+len_alignment;
      memcpy(dest_p,buf,len);                 
      /*
      ** update the offsets
      */
      p->write_from = off_requested - len_alignment;
      p->write_pos  = pos_requested; 
      p->buf_write_wait = 1;
      p->read_from = p->write_from; 
      p->read_pos  = p->write_pos;        
      /*
      ** fill the status section
      */
      status_p->status = BUF_STATUS_DONE;
      status_p->errcode = 0;
      break;                     
  
  
    case BUF_ACT_FLUSH_THEN_COPY_NEW:
        /** 
        *  Before:
        *  -------
        *  off_req                                             pos_req
        *    +----------------------------------------------------+
        *  off_req                 pos_req
        *    +----------------------+
        *   off_req   pos_req
        *    +----------+
        *                 +--------------------------------+
        *               rd_f                             rd_p
        *   
        *  
        *  After:
        *  -------
        *                                 off_req   pos_req
        *                                    +----------+
        *        +----------------------+
        *        rd_f                  rd_p
        *   
        *  
        */

      /*
      ** flush
      */
#ifdef TRACE_FS_READ_WRITE
      info("BUF_ACT_FLUSH_THEN_COPY_NEW state %d write[%llx:%llx],wrb%d[%llx:%llx],rdb[%llx:%llx]",
            state,(long long unsigned int)off,(long long unsigned int)(off+len),
            p->buf_write_wait,
            (long long unsigned int)p->write_from,(long long unsigned int)p->write_pos,
            (long long unsigned int)p->read_from,(long long unsigned int)p->read_pos);     
#endif
      if (p->buf_write_wait != 0) 
      {
        ret = buf_flush(fuse_ctx_p,p);
        if (ret < 0)
        {
          status_p->status = BUF_STATUS_FAILURE;
          status_p->errcode = errno;
          break;                  
        }
        /*
        ** write is in progress
        */
        status_p->status = BUF_STATUS_WR_IN_PRG;
        status_p->errcode = 0;            
      }
      else
      {
        /*
        ** fill the status section
        */
        status_p->status = BUF_STATUS_DONE;
        status_p->errcode = 0;            
      }
      p->write_from = off_requested; 
      p->write_pos  = pos_requested; 
      p->read_from = off_requested; 
      p->read_pos  = pos_requested; 
      p->buf_write_wait = 1;
      /*
      ** copy the buffer
      */
      memcpy(p->buffer,buf,len);

      break;    
  
    case BUF_ACT_FLUSH_ALIGN_THEN_COPY_NEW:
        /** 
        *  There is some pending data to write in the current buffer
        *  -------
        *                 off_req                           pos_req
        *                  +------------------------------------+
        *      +--------------------------------+
        *     rd_f                             rd_p
        *      +-------------------------------------------+  BUFSIZE
        *          +-----------------------+ 
        *         wr_f                    wr_pos
        *   
        *  
        */

      /*
      ** partial flush
      */
#ifdef TRACE_FS_READ_WRITE
      info("BUF_ACT_FLUSH_ALIGN_THEN_COPY_NEW state %d write[%llx:%llx],wrb%d[%llx:%llx],rdb[%llx:%llx]",
            state,(long long unsigned int)off,(long long unsigned int)(off+len),
            p->buf_write_wait,
            (long long unsigned int)p->write_from,(long long unsigned int)p->write_pos,
            (long long unsigned int)p->read_from,(long long unsigned int)p->read_pos);     
#endif
      if (write_section_empty(p))
      {
        p->write_from = off_requested; 
      }
      else
      {
        if (p->write_from > off_requested) p->write_from = off_requested;      
      }
      buf_align_write_from(p);
      off2end = p->read_from + p->export->bufsize;
      
      if (off2end < p->write_from)
      {
       severe ("Bug!!!! off2start %llu off2end: %llu\n",(long long unsigned int)p->write_from,
                                                        (long long unsigned int)off2end);      
      }
      else
      {
//        printf("offset start %llu len %u\n",p->write_from,(off2end-p->write_from));            
      }
      /*
      ** partial copy of the buffer in the cache
      */
      buf_off = (uint32_t)(off_requested - p->read_from);
      memcpy(p->buffer+buf_off,buf,(size_t)(off2end - off_requested));
            
      p->write_pos  = off2end; 
      p->buf_write_wait = 1;
      ret = buf_flush(fuse_ctx_p,p);
      if (ret < 0)
      {
        status_p->status = BUF_STATUS_FAILURE;
        status_p->errcode = errno;
        break;                  
      }      
     /*
      ** write in progress
      */
      status_p->status = BUF_STATUS_WR_IN_PRG;
      status_p->errcode = 0;
      /*
      ** copy the buffer
      */
      buf_off = (uint32_t)(p->write_pos - off_requested);
      memcpy(p->buffer,buf+buf_off,(len-buf_off));

      p->write_from = off2end; 
      p->write_pos  = pos_requested; 
      p->read_from = off2end; 
      p->read_pos  = pos_requested; 
      if (write_section_empty(p) == 0) p->buf_write_wait = 1;      
 
      break;  
    default:
#ifdef TRACE_FS_READ_WRITE
      info("UNKNOWN %d state %d write[%llx:%llx],wrb%d[%llx:%llx],rdb[%llx:%llx]",
            action,state,(long long unsigned int)off,(long long unsigned int)(off+len),
            p->buf_write_wait,
            (long long unsigned int)p->write_from,(long long unsigned int)p->write_pos,
            (long long unsigned int)p->read_from,(long long unsigned int)p->read_pos);     
#endif
      break;    
  
  }

  /*
  ** At the end of the write procedure, if some data is waiting to be
  ** written on disk, save the file descriptor reference in the ie.
  ** The flush will occur on a further read or write.
  */
  if (p->buf_write_wait == 1) ie->write_pending = p;
} 



/*
**__________________________________________________________________
*/
/** Send a request to the export server to know the file size
 *  adjust the write buffer to write only whole data blocks,
 *  reads blocks if necessary (incomplete blocks)
 *  and uses the function write_blocks to write data
 *
 * @param *f: pointer to the file structure
 * @param off: offset to write from
 * @param *buf: pointer where the data are be stored
 * @param len: length to write
*/
 
static int64_t write_buf_nb(void *buffer_p,file_t * f, uint64_t off, const char *buf, uint32_t len) 
{
   storcli_write_arg_t  args;
   int ret;
   fuse_end_tx_recv_pf_t  callback;
   int storcli_idx;
   ientry_t *ie;

    // Fill request
    ie = f->ie;
    args.cid = ie->attrs.cid;
    args.layout = f->export->layout;
    memcpy(args.dist_set, ie->attrs.sids, sizeof (sid_t) * ROZOFS_SAFE_MAX);
    memcpy(args.fid, f->fid, sizeof (fid_t));
    args.off = off;
    args.data.data_len = len;
    /**
    * write alignement stats
    */
    rozofs_aligned_write_start[(off%ROZOFS_BSIZE==0)?0:1]++;
    rozofs_aligned_write_end[((off+len)%ROZOFS_BSIZE==0)?0:1]++;
    
    args.data.data_val = (char*)buf;  
    
    /* If file was empty at opening tell it to storcli at 1rts write */
    if (f->file2create == 1) {

      args.empty_file = 1;
      f->file2create = 0;
    }
    else {
      args.empty_file = 0;
    }
    /*
    ** get the storcli to use for the transaction
    */
    storcli_idx = stclbg_storcli_idx_from_fid(f->fid);
//    lbg_id = storcli_lbg_get_lbg_from_fid(f->fid);
    /*
    ** allocate a shared buffer for writing
    */
#if 1
    uint32_t *p32;
    int shared_buf_idx = -1;
    uint32_t length;
    void *shared_buf_ref = rozofs_alloc_shared_storcli_buf(storcli_idx);
    if (shared_buf_ref != NULL)
    {
      /*
      ** clear the first 4 bytes of the array that is supposed to contain
      ** the reference of the transaction
      */
       p32 = (uint32_t *)ruc_buf_getPayload(shared_buf_ref);
       *p32 = 0;
       /*
       ** store the length to write 
       */
       p32[1] = args.data.data_len;
       /*
       ** get the index of the shared payload in buffer
       */
       shared_buf_idx = rozofs_get_shared_storcli_payload_idx(shared_buf_ref,storcli_idx,&length);
       if (shared_buf_idx != -1)
       {
	 /*
	 ** indicate to the transmitter that shared memory request type must be used
	 */
	 args.data.data_len = 0x80000000 | shared_buf_idx;
         /*
         ** save the reference of the shared buffer in the fuse context
         */
         SAVE_FUSE_PARAM(buffer_p,shared_buf_ref);
       }
    }
#endif 
    /*
    ** now initiates the transaction towards the remote end
    */
    GET_FUSE_CALLBACK(buffer_p,callback);
    f->buf_write_pending++;
    f->write_block_pending = 1;
    ret = rozofs_storcli_send_common(NULL,ROZOFS_TMR_GET(TMR_STORCLI_PROGRAM),STORCLI_PROGRAM, STORCLI_VERSION,
                              STORCLI_WRITE,
			      (shared_buf_idx!=-1)?(xdrproc_t) xdr_storcli_write_arg_no_data_t: (xdrproc_t)xdr_storcli_write_arg_t,
			      (void *)&args,
                              callback,buffer_p,storcli_idx,f->fid); 
    if (ret < 0) goto error;
    
    /*
    ** no error just waiting for the answer
    */
    uint64_t buf_flush_offset = off;
    uint32_t buf_flush_len = len;
    SAVE_FUSE_PARAM(buffer_p,buf_flush_offset);
    SAVE_FUSE_PARAM(buffer_p,buf_flush_len);

    return ret;    
error:
    f->buf_write_pending--;
    return ret;

}



/*
**__________________________________________________________________
*/
/**
*  data write

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param ino : inode  provided by rozofsmount
 @param buf : pointer to the buffer that contains the data to write
 @param off : absolute offset in the file
 @param size : size of the data to write
 @param fi : file info structure where information related to the file can be found (file_t structure)
 
 @retval none
*/
void rozofs_ll_write_nb(fuse_req_t req, fuse_ino_t ino, const char *buf,
        size_t size, off_t off, struct fuse_file_info *fi) 
{
    ientry_t *ie = 0;
    void *buffer_p = NULL;
    rozo_buf_rw_status_t status;
   int deferred_fuse_write_response;
   errno = 0;

    file_t *file = (file_t *) (unsigned long) fi->fh;

    int trc_idx = rozofs_trc_req_io(srv_rozofs_ll_write,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,size,off);

    DEBUG("write to inode %lu %llu bytes at position %llu\n",
            (unsigned long int) ino, (unsigned long long int) size,
            (unsigned long long int) off);



   /*
   ** stats
   */
   ROZOFS_WRITE_STATS_ARRAY(size);

    /*
    ** allocate a context for saving the fuse parameters
    */
    buffer_p = _rozofs_fuse_alloc_saved_context("rozofs_ll_write_nb");
    if (buffer_p == NULL)
    {
      severe("out of fuse saved context");
      errno = ENOMEM;
      goto error;
    }
    START_PROFILING_IO_NB(buffer_p,rozofs_ll_write, size);

    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,size);
    SAVE_FUSE_PARAM(buffer_p,off);
    SAVE_FUSE_PARAM(buffer_p,ino);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);
    SAVE_FUSE_STRUCT(buffer_p,fi,sizeof( struct fuse_file_info)); 
    
    /*
    ** install the callback 
    */
    SAVE_FUSE_CALLBACK(buffer_p,rozofs_ll_write_cbk);

    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }
    
    if (ie->attrs.size < (off + size)) {

        /*
	** Check whether the size extension is compatible 
	** with the export hard quota
	*/
        if (! eid_check_free_quota(ie->attrs.size,off + size)) {
          goto error; // errno is already set	  
	}
	/*
	** if the size is 0, assert file2create: it will be checked by storcli in order
	** to exclude any other request concerning the same fid: fix a potential issue
	** when storcli is configure to handle request in parallel
	*/
	if (ie->attrs.size == 0) 
	{
	  file->file2create = 1;
        }	
	ie->file_extend_pending = 1;
        ie->attrs.size = (off + size);
    }

    /*
    ** check the status of the last write operation
    */
    if (file->wr_error!= 0)
    {
      /*
      ** that error is permanent, once can read the file but any
      ** attempt to write will return that error code
      */
      errno = file->wr_error;
      goto error;    
    }
    /*
    ** check if the application is attempting to write atfer a close (_ll_release)
    */
    if (rozofs_is_file_closing(file))
    {
      errno = EBADF;
      goto error;        
    }
    buf_file_write_nb(ie,buffer_p,&status,file,off,buf,size);
    /*
    ** check the returned status
    */
    if (status.status == BUF_STATUS_FAILURE)
    {
      errno = status.errcode;
      goto error;
    }  
    
    file->current_pos = (off+size);
 
    /*
    **  The data have been written in the buffer and 
    **  no write is pending toward the STORCLI
    */
    if (status.status == BUF_STATUS_DONE)
    {
      rozofs_trc_rsp(srv_rozofs_ll_write,(fuse_ino_t)file,file->fid,(errno==0)?0:1,trc_idx);
      fuse_reply_write(req, size);
      goto out;
    }
    
    /*
    ** A write toward the STORCLI is pending 
    ** so we must keep the the fuse context until the STORCLI response
    */
    if (file->buf_write_pending <= ROZOFS_MAX_WRITE_PENDING) {
      deferred_fuse_write_response = 0;
      SAVE_FUSE_PARAM(buffer_p,deferred_fuse_write_response);
      rozofs_trc_rsp(srv_rozofs_ll_write,(fuse_ino_t)file,file->fid,(errno==0)?0:1,trc_idx);
      fuse_reply_write(req, size);
      write_flush_stat.non_synchroneous++;
      buffer_p = NULL;
      goto out;
    }        
    /*
    ** Maximum number of write pending is reached. 
    ** Let's differ the FUSE response until the STORCLI response
    */
    write_flush_stat.synchroneous++;
    deferred_fuse_write_response = 1;
    SAVE_FUSE_PARAM(buffer_p,deferred_fuse_write_response);    
    buffer_p = NULL;
    goto out;
    
error:
    rozofs_trc_rsp(srv_rozofs_ll_write,(fuse_ino_t)file,file->fid,(errno==0)?0:1,trc_idx);
    fuse_reply_err(req, errno);
out:
    STOP_PROFILING_NB(buffer_p,rozofs_ll_write);
    if (buffer_p != NULL) rozofs_fuse_release_saved_context(buffer_p);
    return;
}



/*
**__________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_write_cbk(void *this,void *param) 
{
   struct rpc_msg  rpc_reply;
   struct fuse_file_info  file_info;
   struct fuse_file_info  *fi = &file_info;
   fuse_req_t req; 
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   storcli_status_ret_t ret;
   xdrproc_t decode_proc = (xdrproc_t)xdr_storcli_status_ret_t;
   file_t *file = NULL;
   int deferred_fuse_write_response;
   size_t size;
   errno = 0;
   int trc_idx;
   rpc_reply.acpted_rply.ar_results.proc = NULL;

   RESTORE_FUSE_STRUCT(param,fi,sizeof( struct fuse_file_info));    
   RESTORE_FUSE_PARAM(param,trc_idx);
       
   file = (file_t *) (unsigned long)  fi->fh;   
   file->buf_write_pending--;
   if (file->buf_write_pending < 0)
   {
     severe("buf_write_pending mismatch, %d",file->buf_write_pending);
     file->buf_write_pending = 0;     
   }
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
       severe(" transaction error %s",strerror(errno));
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
       severe(" transaction error %s",strerror(errno));
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
     severe(" transaction error %s",strerror(errno));
     goto error;
    }
    /*
    ** ok now call the procedure to encode the message
    */
    memset(&ret,0, sizeof(ret));                    
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       xdr_free((xdrproc_t) decode_proc, (char *) &ret);
       severe(" transaction error %s",strerror(errno));
       goto error;
    }   
    if (ret.status == STORCLI_FAILURE) {
        errno = ret.storcli_status_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    
    /*
    ** When a fuse response is expected send it
    */
    rozofs_trc_rsp(srv_rozofs_ll_write,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,(errno==0)?0:1,trc_idx);
    RESTORE_FUSE_PARAM(param,deferred_fuse_write_response);
    if (deferred_fuse_write_response) {
      RESTORE_FUSE_PARAM(param,req);
      RESTORE_FUSE_PARAM(param,size);
      fuse_reply_write(req, size);
      write_flush_stat.synchroneous_success++;      
    }    
    /*
    ** no error, so get the length of the data part
    */
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);
    /*
    ** Keep the fuse context since we need to trigger the update of 
    ** the metadata of the file
    */
    rozofs_tx_free_from_ptr(rozofs_tx_ctx_p); 
    ruc_buf_freeBuffer(recv_buf);  
    /*
    ** Update the exportd with the filesize if that one has changed
    */ 
    export_write_block_nb(param,file);
    /*
    ** check if there is some request waiting for the last pending write to complete
    */
    goto check_last_write_pending;

error:
    if (file != NULL)
    {
       file->wr_error = errno; 
    }
    
    /*
    ** When a fuse response is expected send it
    */
    rozofs_trc_rsp(srv_rozofs_ll_write,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,(errno==0)?0:1,trc_idx);
    RESTORE_FUSE_PARAM(param,deferred_fuse_write_response);
    if (deferred_fuse_write_response) {
      RESTORE_FUSE_PARAM(param,req);
      fuse_reply_err(req, errno);
      write_flush_stat.synchroneous_error++;      
    }
      
    /*
    ** release the transaction context and the fuse context
    */
 //   STOP_PROFILING_NB(param,rozofs_ll_write);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    
check_last_write_pending:
    /*
    ** Check if there some request (flush or release waiting fro the last write to take place
    */
    if (file->buf_write_pending == 0)
    {
      fuse_end_tx_recv_pf_t  callback;
      void *pending_fuse_ctx_p;
      
      pending_fuse_ctx_p = fuse_ctx_write_pending_queue_get(file);
      if (pending_fuse_ctx_p != NULL)
      {
        GET_FUSE_CALLBACK(pending_fuse_ctx_p,callback);
        (*callback)(NULL,pending_fuse_ctx_p);
      }
    }
    return;
}



void rozofs_ll_flush_cbk(void *this,void *param) ;
void rozofs_ll_flush_defer(void *ns,void *param) ;
void rozofs_asynchronous_flush_cbk(void *ns,void *param) ;
/*
**__________________________________________________________________
*/
/**
*  data flush

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param ino : inode  provided by rozofsmount
 @param fi : file info structure where information related to the file can be found (file_t structure)
 
 @retval none
*/

void rozofs_ll_flush_nb(fuse_req_t req, fuse_ino_t ino,
        struct fuse_file_info *fi) {
    file_t *f;
    ientry_t *ie = 0;
    void *buffer_p= NULL;
    int ret;
    int trc_idx=0;
    errno = 0;

    DEBUG_FUNCTION;
    if (!(f = (file_t *) (unsigned long) fi->fh)) {
        rozofs_trc_req(srv_rozofs_ll_flush,(fuse_ino_t)f,(f==NULL)?NULL:f->fid);
        errno = EBADF;
        goto error;
    }
    {
      uint64_t flush_off = 0;
      uint32_t flush_len = 0;
      if (f->buf_write_wait!= 0)
      {      
        flush_off = f->write_from;
        flush_len = (uint32_t)(f->write_pos - f->write_from);
      }
      trc_idx = rozofs_trc_req_io(srv_rozofs_ll_flush,(fuse_ino_t)f,(f==NULL)?NULL:f->fid,flush_len,flush_off);
    }
    /*
    ** allocate a context for saving the fuse parameters
    */
    buffer_p = _rozofs_fuse_alloc_saved_context("rozofs_ll_flush_nb");
    if (buffer_p == NULL)
    {
      severe("out of fuse saved context");
      errno = ENOMEM;
      goto error;
    }
    START_PROFILING_NB(buffer_p,rozofs_ll_flush);
    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);
    SAVE_FUSE_PARAM(buffer_p,ino);
    SAVE_FUSE_STRUCT(buffer_p,fi,sizeof( struct fuse_file_info));    

    /*
    ** install the callback 
    */
    SAVE_FUSE_CALLBACK(buffer_p,rozofs_ll_flush_cbk);
    
    // Sanity check
    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }
    /*
    ** check the status of the last write operation
    */
    if (f->wr_error!= 0)
    {
      /*
      ** that error is permanent, once can read the file but any
      ** attempt to write will return that error code
      */
      errno = f->wr_error;
      goto error;    
    }


    /*
    ** check if there some pendinag data to write in the buffer
    */
    if (f->buf_write_wait!= 0)
    {
      /*
      ** flush to disk and wait for the response
      */
      ret = buf_flush(buffer_p,f);
      if (ret < 0) goto error;
    /*
    ** wait for the end of the flush: just return evreything will take palce
    ** upon receiving the flush response (see rozofs_ll_flush_cbk())
    */
      return;
    }
    /*
    ** nothing to flush to disk, but we might some write transactions running
    ** since it is a sync. We have to wait up to the end of the last running transaction
    */
    if (f->buf_write_pending > 0) 
    {
      /*
      ** install the call for the flush response deferred until receiving the last write response
      */ 
      SAVE_FUSE_CALLBACK(buffer_p,rozofs_ll_flush_defer); 
      fuse_ctx_write_pending_queue_insert(f,buffer_p);
      /*
      ** the response is deferred until the last write transaction is responding 
      */
      return;
    }
    /*
    ** nothing pending, so we can reply immediately and we force the update of the
    ** metadata server
    */
    rozofs_trc_rsp(srv_rozofs_ll_flush,(fuse_ino_t)f,(f==NULL)?NULL:f->fid,(errno==0)?0:1,trc_idx);
    STOP_PROFILING_NB(buffer_p,rozofs_ll_flush);
    
    f->write_block_req = 1;  
    export_write_block_nb(buffer_p,f);
    fuse_reply_err(req, 0);
    return;
    
error:
    fuse_reply_err(req, errno);
    rozofs_trc_rsp(srv_rozofs_ll_flush,(fuse_ino_t)f,(f==NULL)?NULL:f->fid,(errno==0)?0:1,trc_idx);
    STOP_PROFILING_NB(buffer_p,rozofs_ll_flush);
    if (buffer_p != NULL) rozofs_fuse_release_saved_context(buffer_p);
    return;
}

/*
**__________________________________________________________________
*/
/**
*  Some request may trigger an internal flush before beeing executed.

   That's the case of a read request while the file buffer contains
   some data that have not yet been saved on disk, but do not contain 
   the data that the read wants. 

   No fuse reply is expected

 @param fi   file info structure where information related to the file can be found (file_t structure)
 
 @retval 0 in case of failure 1 on success
*/

int rozofs_asynchronous_flush(struct fuse_file_info *fi) {
  file_t * f;
  void   * buffer_p= 0;
  int      ret;

  DEBUG_FUNCTION;

  /*
  ** Retrieve file context
  */
  if (!(f = (file_t *) (unsigned long) fi->fh)) {
    errno = EBADF;
    return 0;
  }

  /*
  ** check the status of the last write operation
  */
  if (f->wr_error!= 0) {
    /*
    ** that error is permanent, once can read the file but any
    ** attempt to write will return that error code
    */
    errno = f->wr_error;
    return 0;
  }

  /*
  ** check that there is actually some pending data to write in the buffer
  */
  if (f->buf_write_wait == 0) return 1; 

  /*
  ** allocate a context for processing the internal flush
  */
  buffer_p = _rozofs_fuse_alloc_saved_context("rozofs_asynchronous_flush");
  if (buffer_p == NULL) {
    severe("out of fuse context");
    errno = ENOMEM;
    return 0;
  }    

  /*
  ** Initialize the so called fuse context
  */
  SAVE_FUSE_STRUCT(buffer_p,fi,sizeof( struct fuse_file_info));    
  SAVE_FUSE_CALLBACK(buffer_p,rozofs_asynchronous_flush_cbk)


  /*
  ** flush to disk and wait for the response
  */
  ret = buf_flush(buffer_p,f);
  if (ret < 0) {
    rozofs_fuse_release_saved_context(buffer_p);
    return 0;
  }
  return 1;
}
/*
**__________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_asynchronous_flush_cbk(void *this,void *param) 
{
//   fuse_req_t req; 
   struct rpc_msg  rpc_reply;
   struct fuse_file_info  file_info;
   struct fuse_file_info  *fi = &file_info;
   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   storcli_status_ret_t ret;
   xdrproc_t decode_proc = (xdrproc_t)xdr_storcli_status_ret_t;
   file_t *file = NULL;

   rpc_reply.acpted_rply.ar_results.proc = NULL;
//   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_STRUCT(param,fi,sizeof( struct fuse_file_info));    

   file = (file_t *) (unsigned long)  fi->fh;   
   file->buf_write_pending--;
   if (file->buf_write_pending < 0)
   {
     severe("buf_write_pending mismatch, %d",file->buf_write_pending);
     file->buf_write_pending = 0;     
   }

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
       severe(" transaction error %s",strerror(errno));
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
       severe(" transaction error %s",strerror(errno));
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
     severe(" transaction error %s",strerror(errno));
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
       severe(" transaction error %s",strerror(errno));
       goto error;
    }   
    if (ret.status == STORCLI_FAILURE) {
        errno = ret.storcli_status_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    /*
    ** no error, so get the length of the data part
    */
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);
//    fuse_reply_err(req, 0);
    /*
    ** Keep the fuse context since we need to trigger the update of 
    ** the metadata of the file
    */
    rozofs_tx_free_from_ptr(rozofs_tx_ctx_p); 
    ruc_buf_freeBuffer(recv_buf);   
    /*
    ** Update the exportd with the filesize if that one has changed
    */ 
    export_write_block_nb(param,file);
    /*
    ** check if there is some request waiting for the last pending write to complete
    */
    goto check_last_write_pending;
    
error:
    if (file != NULL)
    {
       file->wr_error = errno; 
    }
//    fuse_reply_err(req, errno);
    /*
    ** release the transaction context and the fuse context
    */
//    STOP_PROFILING_NB(param,rozofs_ll_flush);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);     
    
check_last_write_pending:
    /*
    ** Check if there some request (flush or release waiting fro the last write to take place
    */
    if (file->buf_write_pending == 0)
    {
      fuse_end_tx_recv_pf_t  callback;
      void *pending_fuse_ctx_p;
      
      pending_fuse_ctx_p = fuse_ctx_write_pending_queue_get(file);
      if (pending_fuse_ctx_p != NULL)
      {
        GET_FUSE_CALLBACK(pending_fuse_ctx_p,callback);
        (*callback)(NULL,pending_fuse_ctx_p);
      }
    }
    return;
}
/*
**__________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_flush_cbk(void *this,void *param) 
{
   fuse_req_t req; 
   struct rpc_msg  rpc_reply;
   struct fuse_file_info  file_info;
   struct fuse_file_info  *fi = &file_info;
   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   storcli_status_ret_t ret;
   xdrproc_t decode_proc = (xdrproc_t)xdr_storcli_status_ret_t;
   file_t *file = NULL;
   int trc_idx;
   errno = 0;
   
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,trc_idx);
   RESTORE_FUSE_STRUCT(param,fi,sizeof( struct fuse_file_info));    

   file = (file_t *) (unsigned long)  fi->fh;   
   file->buf_write_pending--;
   if (file->buf_write_pending < 0)
   {
     severe("buf_write_pending mismatch, %d",file->buf_write_pending);
     file->buf_write_pending = 0;     
   }

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
       severe(" transaction error %s",strerror(errno));
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
       severe(" transaction error %s",strerror(errno));
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
     severe(" transaction error %s",strerror(errno));
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
       severe(" transaction error %s",strerror(errno));
       goto error;
    }   
    if (ret.status == STORCLI_FAILURE) {
        errno = ret.storcli_status_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    /*
    ** no error, so get the length of the data part
    */
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);
    fuse_reply_err(req, 0);
    /*
    ** Keep the fuse context since we need to trigger the update of 
    ** the metadata of the file
    */
    rozofs_trc_rsp(srv_rozofs_ll_flush,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,(errno==0)?0:1,trc_idx);
    rozofs_tx_free_from_ptr(rozofs_tx_ctx_p); 
    ruc_buf_freeBuffer(recv_buf); 
    /*
    ** force the flush on disk
    */
    file->write_block_req = 1;  
    return export_write_block_nb(param,file);
    
error:
    if (file != NULL)
    {
       file->wr_error = errno; 
    }
    fuse_reply_err(req, errno);
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_flush,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,(errno==0)?0:1,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_flush);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    
    return;
}


/*
**__________________________________________________________________
*/
/**
*  Call back function associated to a flsh waiting for end of write
*
 @param ns : Not significant
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_flush_defer(void *ns,void *param) 
{
   fuse_req_t req; 
   struct fuse_file_info  file_info;
   struct fuse_file_info  *fi = &file_info;
   int trc_idx;
   errno = 0;
   
   file_t *file = NULL;

   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,trc_idx);
   RESTORE_FUSE_STRUCT(param,fi,sizeof( struct fuse_file_info));    

   file = (file_t *) (unsigned long) fi->fh;
    /*
    ** check the status of the last write operation
    */
    errno = 0;
    if (file->wr_error!= 0)
    {
      /*
      ** that error is permanent, once can read the file but any
      ** attempt to write will return that error code
      */
      errno = file->wr_error;
    }
    fuse_reply_err(req, errno) ;    
    /*
    ** udpate the metadata service 
    */
    rozofs_trc_rsp(srv_rozofs_ll_flush,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,(errno==0)?0:1,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_flush);
    file->write_block_req = 1;  
    export_write_block_nb(param,file);    
//    rozofs_fuse_release_saved_context(param);    
    return;
}

/*
**__________________________________________________________________
*/
/**
*  release

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param ino : inode  provided by rozofsmount
 @param fi : file info structure where information related to the file can be found (file_t structure)
 
 @retval none
*/
void rozofs_ll_release_cbk(void *this,void *param) ;
void rozofs_ll_release_defer(void *ns,void *param); 

void rozofs_ll_release_nb(fuse_req_t req, fuse_ino_t ino,
        struct fuse_file_info *fi) {
    file_t *f = NULL;
    ientry_t *ie = 0;
    void *buffer_p = NULL;
    int ret;
    errno = 0;  
    
    int trc_idx = rozofs_trc_req(srv_rozofs_ll_release,(fuse_ino_t)fi->fh,NULL);

    DEBUG("release (%lu)\n", (unsigned long int) ino);
    /*
    ** allocate a context for saving the fuse parameters
    */
    buffer_p = _rozofs_fuse_alloc_saved_context("rozofs_ll_release_nb");
    if (buffer_p == NULL)
    {
      severe("out of fuse saved context");
      errno = ENOMEM;
      goto error;
    }
    START_PROFILING_NB(buffer_p,rozofs_ll_release);
    SAVE_FUSE_PARAM(buffer_p,req);
    SAVE_FUSE_PARAM(buffer_p,trc_idx);
    SAVE_FUSE_STRUCT(buffer_p,fi,sizeof( struct fuse_file_info));    
    /*
    ** install the callback 
    */
    SAVE_FUSE_CALLBACK(buffer_p,rozofs_ll_release_cbk);
    
    // Sanity check
    if (!(ie = get_ientry_by_inode(ino))) {
        errno = ENOENT;
        goto error;
    }

    if (!(f = (file_t *) (unsigned long) fi->fh)) {
        errno = EBADF;
        goto error;
    }
    /*
    ** check the status of the last write operation
    */
    if (f->wr_error!= 0)
    {
      /*
      ** that error is permanent, once can read the file but any
      ** attempt to write will return that error code
      */
      errno = f->wr_error;
      goto error;    
    }
    
    /*
    ** Clear all the locks eventually pending on the file for this owner
    */
    if (f->lock_owner_ref != 0) {
      /*
      ** Call file lock service to clear everything about this file descriptor
      */
      rozofs_clear_file_lock_owner(f);
    }

     if (rozofs_bugwatch) severe("BUGROZOFSWATCH release(%p) , buf_write_wait=%d, buf_write_pending=%d,",
                                   f,f->buf_write_wait,f->buf_write_pending);

    /*
    ** check if there some pendinag data to write in the buffer
    */
    if (f->buf_write_wait!= 0)
    {
      /*
      ** flush to disk and wait for the response
      */
      ret = buf_flush(buffer_p,f);
      if (ret < 0) goto error;
    /*
    ** wait for the end of the flush: just return evreything will take palce
    ** upon receiving the flush response (see rozofs_ll_flush_cbk())
    */
      return;
    }
    /*
    ** nothing to flush to disk, but we might some write transactions running
    ** since it is a sync. We have to wait up to the end of the last running transaction
    */
    if (f->buf_write_pending > 0) 
    {
      /*
      ** install the call for the flush response deferred until receiving the last write response
      */ 
      SAVE_FUSE_CALLBACK(buffer_p,rozofs_ll_release_defer); 
      fuse_ctx_write_pending_queue_insert(f,buffer_p);
      /*
      ** the response is deferred until the last write transaction is responding 
      */
      return;
    }
    /*
    ** release the data structure associated with the file descriptor
    */
    STOP_PROFILING_NB(buffer_p,rozofs_ll_release);
    f->write_block_req = 1;  
    export_write_block_nb(buffer_p,f);    
    file_close(f);
    fuse_reply_err(req, 0);
    rozofs_trc_rsp(srv_rozofs_ll_release,(fuse_ino_t)f,(ie==NULL)?NULL:ie->attrs.fid,1,trc_idx);
//    STOP_PROFILING_NB(buffer_p,rozofs_ll_release);
    return;

error:
    /*
    ** release the data structure associated with the file descriptor
    */
    file_close(f);
    fuse_reply_err(req, errno);
    rozofs_trc_rsp(srv_rozofs_ll_release,(fuse_ino_t)f,(ie==NULL)?NULL:ie->attrs.fid,1,trc_idx);
    STOP_PROFILING_NB(buffer_p,rozofs_ll_release);
    if (buffer_p != NULL) rozofs_fuse_release_saved_context(buffer_p);   
    return;
}

/*
**__________________________________________________________________
*/
/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_release_cbk(void *this,void *param) 
{
   fuse_req_t req; 
   struct rpc_msg  rpc_reply;
   struct fuse_file_info  file_info;
   struct fuse_file_info  *fi = &file_info;
   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   storcli_status_ret_t ret;
   xdrproc_t decode_proc = (xdrproc_t)xdr_storcli_status_ret_t;
   file_t *file = NULL;
   errno = 0;
   int trc_idx;
   
   rpc_reply.acpted_rply.ar_results.proc = NULL;
   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,trc_idx);
   RESTORE_FUSE_STRUCT(param,fi,sizeof( struct fuse_file_info));    

   file = (file_t *) (unsigned long)  fi->fh;   
   file->buf_write_pending--;
   if (file->buf_write_pending < 0)
   {
     severe("buf_write_pending mismatch, %d",file->buf_write_pending);
     file->buf_write_pending = 0;     
   }
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
       severe(" transaction error %s",strerror(errno));
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
       severe(" transaction error %s",strerror(errno));
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
     severe(" transaction error %s",strerror(errno));
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
       severe(" transaction error %s",strerror(errno));
       goto error;
    }   
    if (ret.status == STORCLI_FAILURE) {
        errno = ret.storcli_status_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    /*
    ** no error, so get the length of the data part
    */
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);
    fuse_reply_err(req, 0);

    /*
    ** Keep the fuse context since we need to trigger the update of 
    ** the metadata of the file
    */
    rozofs_trc_rsp(srv_rozofs_ll_release,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,status,trc_idx);    
    rozofs_tx_free_from_ptr(rozofs_tx_ctx_p); 
    ruc_buf_freeBuffer(recv_buf); 
    STOP_PROFILING_NB(param,rozofs_ll_release); 
    file->write_block_req = 1;     
    export_write_block_nb(param,file);
    file_close(file);
    return;
    
error:
    if (file != NULL)
    {
       file->wr_error = errno; 
    }
    fuse_reply_err(req, errno);

    /*
    ** release the transaction context and the fuse context
    ** and release the file descriptor
    */
    rozofs_trc_rsp(srv_rozofs_ll_release,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,status,trc_idx);    
    file_close(file);    
    STOP_PROFILING_NB(param,rozofs_ll_release);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    
    return;
}


/*
**__________________________________________________________________
*/
/**
*  Call back function associated to a flsh waiting for end of write
*
 @param ns : Not significant
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
void rozofs_ll_release_defer(void *ns,void *param) 
{
   fuse_req_t req; 
   struct fuse_file_info  file_info;
   struct fuse_file_info  *fi = &file_info;
   int trc_idx;
   
   file_t *file = NULL;

   RESTORE_FUSE_PARAM(param,req);
   RESTORE_FUSE_PARAM(param,trc_idx);
   RESTORE_FUSE_STRUCT(param,fi,sizeof( struct fuse_file_info));    

    file = (file_t *) (unsigned long) fi->fh;
    /*
    ** check the status of the last write operation
    */
    errno = 0;
    if (file->wr_error!= 0)
    {
      /*
      ** that error is permanent, once can read the file but any
      ** attempt to write will return that error code
      */
      errno = file->wr_error;
    }
    fuse_reply_err(req, errno) ; 
    /*
    ** trigger the update of the metadata server
    */
    rozofs_trc_rsp(srv_rozofs_ll_release,(fuse_ino_t)file,(file==NULL)?NULL:file->fid,(errno==0)?0:1,trc_idx);    
    file->write_block_req = 1;     
    STOP_PROFILING_NB(param,rozofs_ll_release);
    export_write_block_nb(param,file);   
    /*
    ** release the transaction context and the fuse context
    ** and release the file descriptor
    */
    file_close(file);
    /*
    ** do not release the transaction context and the fuse context; this is done by export_write_block_cbk()
    */
//    rozofs_fuse_release_saved_context(param);    
    return;
}


/**
*  metadata update -> need to update the file size

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param parent : inode parent provided by rozofsmount
 @param name : name to search in the parent directory
 
 @retval none
*/
int export_write_block_asynchrone(void *fuse_ctx_p, file_t *file_p, sys_recv_pf_t recv_cbk) 
{
    epgw_write_block_arg_t arg;
    int    ret;        
    uint64_t buf_flush_offset ;
    uint32_t buf_flush_len ;
    
    RESTORE_FUSE_PARAM(fuse_ctx_p,buf_flush_offset);
    RESTORE_FUSE_PARAM(fuse_ctx_p,buf_flush_len);


    /*
    ** Invalidate cache entry
    */ 
    ientry_t * ie = file_p->ie;
    if (ie) {
      ie->timestamp = 0;
    }  

    /*
    ** adjust the size of the attributes of the local file
    */
    if ((buf_flush_offset + buf_flush_len) > ie->attrs.size)
    {
      ie->attrs.size = buf_flush_offset + buf_flush_len;
      ie->file_extend_pending = 1;
    }
    int trc_idx = rozofs_trc_req_io(srv_rozofs_ll_ioctl,(fuse_ino_t)file_p,file_p->fid, ie->attrs.size,0);
    SAVE_FUSE_PARAM(fuse_ctx_p,trc_idx);
    /*
    ** fill up the structure that will be used for creating the xdr message
    */    
    arg.arg_gw.eid = exportclt.eid;
    memcpy(arg.arg_gw.fid, file_p->fid, sizeof (fid_t));
    arg.arg_gw.bid = 0;
    arg.arg_gw.nrb = 1;
    arg.arg_gw.length = 0; //buf_flush_len;
    arg.arg_gw.offset = ie->attrs.size; //buf_flush_offset;
    arg.arg_gw.dist = 0;
    /*
    ** now initiates the transaction towards the remote end
    */
    
#if 1
    ret = rozofs_expgateway_send_routing_common(arg.arg_gw.eid,file_p->fid,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_WRITE_BLOCK,(xdrproc_t) xdr_epgw_write_block_arg_t,(void *)&arg,
                              recv_cbk,fuse_ctx_p); 
#else
    ret = rozofs_export_send_common(&exportclt,EXPORT_PROGRAM, EXPORT_VERSION,
                              EP_WRITE_BLOCK,(xdrproc_t) xdr_epgw_write_block_arg_t,(void *)&arg,
                              recv_cbk,fuse_ctx_p); 
#endif
    if (ret >= 0)
    {
       /*
       ** indicates that the revelant file size will be found in the ientry
       */
       if (ie->file_extend_pending )       
       {
         ie->file_extend_pending = 0;
         ie->file_extend_running = 1;
      }
    }
    return ret;  
}

/**
*  metadata update -> need to update the file size

 Under normal condition the service ends by calling : fuse_reply_entry
 Under error condition it calls : fuse_reply_err

 @param req: pointer to the fuse request context (must be preserved for the transaction duration
 @param parent : inode parent provided by rozofsmount
 @param name : name to search in the parent directory
 
 @retval none
*/
void export_write_block_cbk(void *this,void *param);

void export_write_block_nb(void *fuse_ctx_p, file_t *file_p) 
{
    int    ret;        
    void *buffer_p = fuse_ctx_p;
    int trc_idx;
    uint64_t buf_flush_offset ;
    uint32_t buf_flush_len ;
    ientry_t *ie;

    RESTORE_FUSE_PARAM(fuse_ctx_p,buf_flush_offset);
    RESTORE_FUSE_PARAM(fuse_ctx_p,buf_flush_len);
    
    ie = file_p->ie;
    /*
    ** adjust the size of the attributes of the local file
    */
    if ((buf_flush_offset + buf_flush_len) > ie->attrs.size)
    {
      ie->attrs.size = buf_flush_offset + buf_flush_len;
      ie->file_extend_pending = 1;
    }
    /*
    ** check if the update is requested
    */
    if (file_p->write_block_req == 0) 
    {
      /*
      ** assert the pending flag in order to trigger the writeblock on flush and release
      */
      file_p->write_block_pending = 1;
      goto out;
    }
    /*
    ** there is a writeblock request check if there is a pending request
    ** without pending request nothing takes place
    */
    if (file_p->write_block_pending ==0) 
    {
      file_p->write_block_req = 0;
      goto out;
    }
    /*
    ** clear the flag and update the metadata server
    */
    START_PROFILING_NB(buffer_p,rozofs_ll_ioctl);
    ret = export_write_block_asynchrone(fuse_ctx_p, file_p, export_write_block_cbk);
    if (ret < 0) goto error; 
    file_p->write_block_pending = 0;
    file_p->write_block_req = 0;
    return;
error:
    /*
    ** release the buffer if has been allocated
    */
    RESTORE_FUSE_PARAM(fuse_ctx_p,trc_idx);
    rozofs_trc_rsp(srv_rozofs_ll_ioctl,(fuse_ino_t)file_p,file_p->fid,(errno==0)?0:1,trc_idx);
    STOP_PROFILING_NB(fuse_ctx_p,rozofs_ll_ioctl);
out:
    if (fuse_ctx_p != NULL) rozofs_fuse_release_saved_context(fuse_ctx_p);
    return;
}



/**
*  Call back function call upon a success rpc, timeout or any other rpc failure
*
 @param this : pointer to the transaction context
 @param param: pointer to the associated rozofs_fuse_context
 
 @return none
 */
 
void export_write_block_cbk(void *this,void *param) 
{
   epgw_mattr_ret_t ret ;
   struct rpc_msg  rpc_reply;

   
   int status;
   uint8_t  *payload;
   void     *recv_buf = NULL;   
   XDR       xdrs;    
   int      bufsize;
   xdrproc_t decode_proc = (xdrproc_t)xdr_epgw_mattr_ret_t;
   rozofs_fuse_save_ctx_t *fuse_ctx_p;
   errno = 0;
   int trc_idx;
   ientry_t *ie = 0;
    
   GET_FUSE_CTX_P(fuse_ctx_p,param);    
   RESTORE_FUSE_PARAM(param,trc_idx);

   rpc_reply.acpted_rply.ar_results.proc = NULL;
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
    bufsize = rozofs_tx_get_small_buffer_size();
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
    ** ok now call the procedure to encode the message
    */
    memset(&ret,0, sizeof(ret));                    
    if (decode_proc(&xdrs,&ret) == FALSE)
    {
       TX_STATS(ROZOFS_TX_DECODING_ERROR);
       errno = EPROTO;
       xdr_free((xdrproc_t) decode_proc, (char *) &ret);
       goto error;
    }   
    /*
    **  This gateway do not support the required eid 
    */    
    if (ret.status_gw.status == EP_FAILURE_EID_NOT_SUPPORTED) {    

        /*
        ** Do not try to select this server again for the eid
        ** but directly send to the exportd
        */
        expgw_routing_expgw_for_eid(&fuse_ctx_p->expgw_routing_ctx, ret.hdr.eid, EXPGW_DOES_NOT_SUPPORT_EID);       

        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    

        /* 
        ** Attempt to re-send the request to the exportd and wait being
        ** called back again. One will use the same buffer, just changing
        ** the xid.
        */
        status = rozofs_expgateway_resend_routing_common(rozofs_tx_ctx_p, NULL,param); 
        if (status == 0)
        {
          /*
          ** do not forget to release the received buffer
          */
          ruc_buf_freeBuffer(recv_buf);
          recv_buf = NULL;
          return;
        }           
        /*
        ** Not able to resend the request
        */
        errno = EPROTO; /* What else ? */
        goto error;
         
    }



    if (ret.status_gw.status == EP_FAILURE) {
        errno = ret.status_gw.ep_mattr_ret_t_u.error;
        xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
        goto error;
    }
    
    /*
    ** Update eid free quota
    */
    eid_set_free_quota(ret.free_quota);        
    /*
    ** Update cache entry
    */
    ie = get_ientry_by_inode(fuse_ctx_p->ino);
    if (ie) {
      /*
      ** update the attributes in the ientry
      */
      rozofs_ientry_update(ie,(mattr_t *)&ret.status_gw.ep_mattr_ret_t_u.attrs);  
      ie->file_extend_running = 0;
    }     
        
    xdr_free((xdrproc_t) decode_proc, (char *) &ret);    
//out:

error:
    /*
    ** release the transaction context and the fuse context
    */
    rozofs_trc_rsp(srv_rozofs_ll_ioctl,0/*ino*/,NULL,(errno==0)?0:1,trc_idx);
    STOP_PROFILING_NB(param,rozofs_ll_ioctl);
    rozofs_fuse_release_saved_context(param);
    if (rozofs_tx_ctx_p != NULL) rozofs_tx_free_from_ptr(rozofs_tx_ctx_p);    
    if (recv_buf != NULL) ruc_buf_freeBuffer(recv_buf);   
    
    return;
}
