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

#define _XOPEN_SOURCE 500
#define STORAGE_C

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <inttypes.h>
#include <glob.h>
#include <fnmatch.h>
#include <dirent.h>

#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/list.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/rozofs_string.h>
#include "storio_cache.h"
#include "storio_bufcache.h"
#include "storio_device_mapping.h"
#include "storio_device_mapping.h"
#include "storio_crc32.h"




/*
=================== STORIO LOG SERVICE ====================================

This service logs in a small buffer the first encountered errors.
The logg cn be displayed through rozodiag
The log can be reset through rozodiag

===========================================================================
*/
int storio_error_log_initialized = 0;

/*
** A device access error record
*/
typedef struct _storio_device_error_log_record_t {
  fid_t            fid;
  uint16_t         nb_blocks;
  uint8_t          chunk;
  uint8_t          device;
  uint32_t         line;
  uint32_t         bid;
  uint32_t         error;
  uint64_t         ts;
  char             string[16];
} storio_device_error_log_record_t;

/*
** Dimensionning factor of the log
*/
#define STORIO_DEVICE_ERROR_LOG_MAX_RECORD      128

typedef struct _storio_device_error_log {
  pthread_rwlock_t                  lock; 
  uint32_t                          next_record;
  storio_device_error_log_record_t  record[STORIO_DEVICE_ERROR_LOG_MAX_RECORD];
} storio_device_error_log_t;

storio_device_error_log_t storio_device_error_log;


/*_______________________________________________________________________
* Write an error in the next log entry
*
* @param fid        The FID tha has encountered an error
* @param line       The line where the error is called
* @param device     The device on which the error occured
* @param chunk      The chunk of the FID on which the   error occured
* @param bid        The block where the error occured
* @param nb_blocks  The block of the chunk where the error occured
* @param error      The errno
* @param string     A string
*
*/
void storio_device_error_log_new(fid_t fid, int line, uint8_t device, uint8_t chunk, uint32_t bid, uint16_t nb_blocks, uint32_t error, char * string) {
  uint32_t                          record_nb;
  storio_device_error_log_record_t *p ;
  
  /*
  ** Service must have been initialized
  */ 
  if (storio_error_log_initialized==0) return;
   
  /*
  ** No space left in the log
  */
  if (storio_device_error_log.next_record >= STORIO_DEVICE_ERROR_LOG_MAX_RECORD) return;

  /*
  ** Take the lock
  */
  if (pthread_rwlock_wrlock(&storio_device_error_log.lock) != 0) {
    return;
  }
  
  /*
  ** Get a record number
  */ 
  record_nb = storio_device_error_log.next_record;
  storio_device_error_log.next_record++;
  
  /*
  ** release the lock
  */
  pthread_rwlock_unlock(&storio_device_error_log.lock);  

  /*
  ** Check  the record number again
  */
  if (record_nb >= STORIO_DEVICE_ERROR_LOG_MAX_RECORD) return;
  
  p = &storio_device_error_log.record[record_nb];
  memcpy(p->fid, fid,sizeof(fid_t));
  p->nb_blocks   = nb_blocks;
  p->line        = line;
  p->device      = device;
  p->chunk       = chunk;
  p->bid         = bid;
  p->error       = error;
  p->ts          = time(NULL);
  if (string == NULL) {
    p->string[0] = 0;
  }
  else {
    int i;
    for (i=0; i<15; i++,string++) {
      p->string[i] = *string;
      if (*string == 0) break;
    }
    p->string[i] = 0;
  }
}
/*_______________________________________________________________________
* Reset the error log
*/
void storio_device_error_log_reset() {
   
  /*
  ** Take the lock
  */
  if (pthread_rwlock_wrlock(&storio_device_error_log.lock) != 0) {
    return;
  }
  
  /*
  ** Reset the log
  */ 
  storio_device_error_log.next_record = 0;
  memset(&storio_device_error_log.record[0],0,sizeof(storio_device_error_log_record_t)*STORIO_DEVICE_ERROR_LOG_MAX_RECORD);

  /*
  ** release the lock
  */
  pthread_rwlock_unlock(&storio_device_error_log.lock);  
}


/*_______________________________________________________________________
* Display the error log
*/
void storio_device_error_log_display (char * argv[], uint32_t tcpRef, void *bufRef) {
  char                             * pChar = uma_dbg_get_buffer();
  int                                idx;
  int                                nb_record = storio_device_error_log.next_record;
  storio_device_error_log_record_t * p = storio_device_error_log.record;
  struct tm                          ts;
  char                             * line_sep = "+-----+--------------------------------------+-------+-----+-----+-----------+-------------------+-----------------+--------------------------------+\n";
  
    
  pChar += rozofs_string_append(pChar,"nb log  : ");
  pChar += rozofs_u32_append(pChar,nb_record);
  *pChar++ = '/';
  pChar += rozofs_u32_append(pChar,STORIO_DEVICE_ERROR_LOG_MAX_RECORD);
  pChar += rozofs_eol(pChar);   

  if (nb_record == 0) {
    uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());
    return;
  } 

  pChar += rozofs_string_append(pChar,"git ref : ");
  pChar += rozofs_string_append(pChar,ROZO_GIT_REF);
  pChar += rozofs_eol(pChar);

  pChar += rozofs_string_append(pChar, line_sep);
  pChar += rozofs_string_append(pChar,"|  #  |                FID                   | line  | dev | chk |  block id |   time stamp      |      action     |          error                 |\n");
  pChar += rozofs_string_append(pChar, line_sep);

  for (idx=0; idx < nb_record; idx++,p++) {
    *pChar++ = '|';
    pChar += rozofs_u32_padded_append(pChar, 4, rozofs_right_alignment, idx);   
    pChar += rozofs_string_append(pChar," | ");
    rozofs_uuid_unparse(p->fid, pChar);
    pChar += 36;
    *pChar++ = ' '; *pChar++ = '|';      
    pChar += rozofs_u32_padded_append(pChar, 6, rozofs_right_alignment, p->line);
    *pChar++ = ' '; *pChar++ = '|';  
    pChar += rozofs_u32_padded_append(pChar, 4, rozofs_right_alignment, p->device); 
    *pChar++ = ' '; *pChar++ = '|';
    pChar += rozofs_u32_padded_append(pChar, 4, rozofs_right_alignment, p->chunk); 
    *pChar++ = ' '; *pChar++ = '|';
    if (p->bid==-1) {
      pChar += rozofs_string_append(pChar, "           | ");    
    }
    else {
      pChar += rozofs_u32_padded_append(pChar, 10, rozofs_right_alignment, p->bid);
      *pChar++ = ' '; *pChar++ = '|'; *pChar++ = ' ';
    }

    time_t t = p->ts;
    ts = *localtime(&t);
    pChar += rozofs_u32_append(pChar, ts.tm_year-100);
    *pChar++ = '/';
    pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, ts.tm_mon+1);
    *pChar++ = '/';
    pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, ts.tm_mday);
    *pChar++ = ' ';
    pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, ts.tm_hour);
    *pChar++ = ':';
    pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, ts.tm_min);
    *pChar++ = ':';
    pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, ts.tm_sec);
    *pChar++ = ' '; *pChar++ = '|';*pChar++ = ' ';
    
    pChar += rozofs_string_padded_append(pChar, 16,rozofs_left_alignment,p->string);
    *pChar++ = '|';*pChar++ = ' ';
    
    pChar += rozofs_string_padded_append(pChar, 31, rozofs_left_alignment, strerror(p->error));
    *pChar++ = '|'; *pChar++ = ' ';

    pChar += rozofs_eol(pChar);
    

  } 
  pChar += rozofs_string_append(pChar, line_sep);
  
  
  /*
  ** Reset log when requested
  */
  if ((argv[1] != NULL) && (strcmp(argv[1],"reset")==0)) {
    storio_device_error_log_reset();
    pChar += rozofs_string_append(pChar, "Reset Done\n");
  }

  uma_dbg_send(tcpRef,bufRef,TRUE,uma_dbg_get_buffer());

}
/*_______________________________________________________________________
* Initialize the storio error log service
*
*/
void storio_device_error_log_init(void) {
 
  if (storio_error_log_initialized) return ;
  
  memset((char*)&storio_device_error_log,0,sizeof(storio_device_error_log_t));

  if (pthread_rwlock_init(&storio_device_error_log.lock, NULL) != 0) {
    severe("pthread_rwlock_init %s",strerror(errno));
  }
  
  uma_dbg_addTopic("log", storio_device_error_log_display);
  
  storio_error_log_initialized = 1;
}


#define storio_fid_error(fid,dev,chunk,bid,nb_blocks,string) storio_device_error_log_new(fid, __LINE__, dev, chunk, bid, nb_blocks, errno,string)
#define storio_hdr_error(fid,dev,string)                     storio_device_error_log_new(fid, __LINE__, dev, 0, -1, -1, errno,string) 

/*
=================== END OF STORIO LOG SERVICE ====================================
*/




static inline void storage_get_projection_size(uint8_t spare, 
                                               sid_t sid, 
					       uint8_t layout, 
					       uint32_t bsize,
					       sid_t * dist_set,
					       uint16_t * msg,
				    	       uint16_t * disk) { 
  int prj_id;
  int idx;
  int safe;
  int forward;
  char mylog[128];
  char * p = mylog;
    
  /* Size of a block in a message received from the client */  
  *msg = rozofs_get_max_psize_in_msg(layout,bsize);
  
  /*
  ** On a spare storage, we store the projections as received.
  ** That is one block takes the maximum projection block size.
  */
  if (spare) {
    *disk = *msg;
    return;
  }
    
  /*
  ** On a non spare storage, we store the projections on its exact size.
  */
  
  forward = rozofs_get_rozofs_forward(layout);
  safe    = rozofs_get_rozofs_safe(layout);

  /* Retrieve the current sid in the given distribution */
  for (prj_id=0; prj_id < safe; prj_id++) {
    if (sid == dist_set[prj_id]) break;
  }
  
  /* The sid is within the forward 1rst sids : this is what we expected */
  if (prj_id < forward) {
    *disk = rozofs_get_psizes_on_disk(layout,bsize,prj_id);
    return;
  }	  

  /* This is abnormal. The sid is not supposed to be spare */
  p += rozofs_string_append(p, " safe ");
  p += rozofs_u32_append(p,safe);
  for (idx=0; idx < safe; idx++) {
    *p++ = '/';
    p += rozofs_u32_append(p,dist_set[idx]);    
  }    
  p += rozofs_string_append(p, " storage_get_projection_size spare ");
  p += rozofs_u32_append(p,spare);
  p += rozofs_string_append(p, " sid ");
  p += rozofs_u32_append(p,sid);

  if (prj_id < safe) {
    /* spare should have been set to 1 !? */
    severe("%s",mylog);
    *disk = *msg;
    return;
  }
  
  /* !!! sid not in the distribution !!!! */
  fatal("%s",mylog);	
}  
static inline void storage_get_projid_size(uint8_t spare, 
                                           uint8_t prj_id, 
					   uint8_t layout,
					   uint32_t bsize,
					   uint16_t * msg,
				    	   uint16_t * disk) { 

  *msg = rozofs_get_max_psize_in_msg(layout,bsize);
  
  /*
  ** On a spare storage, we store the projections as received.
  ** That is one block takes the maximum projection block size.
  */
  if (spare) {
    *disk = *msg;
    return;
  }
    
  /*
  ** On a non spare storage, we store the projections on its exact size.
  */
  *disk = rozofs_get_psizes_on_disk(layout,bsize,prj_id);		
} 





/*
 ** Write a header/mapper file on a device

  @param path : pointer to the bdirectory where to write the header file
  @param hdr : header to write in the file
  
  @retval 0 on sucess. -1 on failure
  
 */
int storage_write_header_file(storage_t * st,int dev, char * path, rozofs_stor_bins_file_hdr_t * hdr) {
  size_t                    nb_write;
  int                       fd;
  char                      my_path[FILENAME_MAX];
  char                     *pChar;
 
  /*
  ** Create directory when needed */
  if (storage_create_dir(path) < 0) {   
    storio_hdr_error(hdr->v0.fid,dev,"create dir");
    storage_error_on_device(st,dev);
    return -1;
  }   

   
      
  strcpy(my_path,path); // Not to modify input path
  pChar = my_path;
  pChar += strlen(my_path);
  rozofs_uuid_unparse_no_recycle(hdr->v0.fid, pChar);

  
  // Open bins file
  fd = open(my_path, ROZOFS_ST_BINS_FILE_FLAG, ROZOFS_ST_BINS_FILE_MODE);
  if (fd < 0) {	
    storio_hdr_error(hdr->v0.fid, dev,"open hdr write");
    storage_error_on_device(st,dev);    
    return -1;
  }      

  // Write the header for this bins file
  nb_write = pwrite(fd, hdr, sizeof (*hdr), 0);
  close(fd);

  if (nb_write != sizeof (*hdr)) {
    storio_hdr_error(hdr->v0.fid, dev,"write hdr");
    storage_error_on_device(st,dev);  
    return -1;
  }
  return 0;
}  
/*
 ** Write a header/mapper file on every device
    This function writes the header file of the given FID on every
    device where it should reside on this storage.    
    
  @param st    : storage we are looking on
  @param fid   : fid whose hader file we are looking for
  @param spare : whether this storage is spare for this FID
  @param hdr   : the content to be written in header file
  
  @retval The number of header file that have been written successfuly
  
 */
int storage_write_all_header_files(storage_t * st, fid_t fid, uint8_t spare, rozofs_stor_bins_file_hdr_t * hdr) {
  int                       dev;
  int                       hdrDevice;
  int                       storage_slice;
  char                      path[FILENAME_MAX];
  int                       result=0;
  
  storage_slice = rozofs_storage_fid_slice(fid);
  
  /*
  ** Compute CRC32
  */
  uint32_t crc32 = fid2crc32((uint32_t *)fid);  
  storio_gen_header_crc32(hdr,crc32);

  for (dev=0; dev < st->mapper_redundancy ; dev++) {

    hdrDevice = storage_mapper_device(fid,dev,st->mapper_modulo);
    storage_build_hdr_path(path, st->root, hdrDevice, spare, storage_slice);
                 
    if (storage_write_header_file(st,hdrDevice,path, hdr) == 0) {    
      //dbg("Header written on storage %d/%d device %d", st->cid, st->sid, hdrDevice);
      result++;
    }
  }  
  return result;
} 
/*
** Make a header file version 1 from a header file version 0
** and rewrite it on disk
*/
static inline int update_header_file_version(storage_t * st, fid_t fid, uint8_t spare, rozofs_stor_bins_file_hdr_t * hdr) {

  /*
  ** If header file is in version 1 nothing to do
  */
  if (hdr->v0.version != 0) return 0;
  
  /*
  ** Update header with version1 information
  */
  hdr->v0.version = 1;
  hdr->v1.cid = st->cid;
  hdr->v1.sid = st->sid;
  
  /*
  ** Rewite header file
  */
  storage_write_all_header_files(st, fid, spare, hdr);
  return 1;
}    
/*
** API to be called when an error occurs on a device
 *
 * @param st: the storage to be initialized.
 * @param device_nb: device number
 *
 */
int storage_error_on_device(storage_t * st, uint8_t device_nb) {

  if ((st == NULL) || (device_nb >= STORAGE_MAX_DEVICE_NB)) return 0;     
    
  int active = st->device_errors.active;
    
  // Since several threads can call this API at the same time
  // some count may be lost...
  st->device_errors.errors[active][device_nb]++;
  return st->device_errors.errors[active][device_nb];
}
/**
*   truncate to 0 a file that has been just recycled

  @param st    : storage we are looking on
  @param device    : list of the devices per chunk
  @param storage_slice    : directory number depending on fid
  @param fid   : fid whose hader file we are looking for
  @param spare : whether this storage is spare for this FID
  @param hdr   :  the read header file
  
  @retval  0 on success
  @retval  -1 on error
  
*/  
int storage_truncate_recycle(storage_t * st, uint8_t * device, int storage_slice,uint8_t spare, fid_t fid,rozofs_stor_bins_file_hdr_t *file_hdr) {
    int status = -1;
    char path[FILENAME_MAX];
    int fd = -1;
    int open_flags;
    int chunk;
    int result;
    int chunk_idx;


    open_flags = ROZOFS_ST_BINS_FILE_FLAG;     
    // Build the chunk file name for chunk 0
    chunk = 0;
    /*
    ** A valid device is given as input, so use it
    */
    if ((device[chunk] != ROZOFS_EOF_CHUNK)&&(device[chunk] != ROZOFS_EMPTY_CHUNK)&&(device[chunk] != ROZOFS_UNKNOWN_CHUNK)) {
      /*
      ** Build the chunk file name using the valid device id given in the device array
      */
      storage_build_chunk_full_path(path, st->root, device[chunk], spare, storage_slice, fid, chunk);

      // Open bins file
      fd = open(path, open_flags, ROZOFS_ST_BINS_FILE_MODE);
      if (fd < 0) {
          storio_fid_error(fid, device[chunk], chunk, 0, 0,"open truncate"); 		        
	  storage_error_on_device(st,file_hdr->v0.device[chunk]);  				    
          severe("open failed (%s) : %s", path, strerror(errno));
          goto out;
      }
      
      /* 
      ** truncate the file to the required size
      */      
      if ((spare) || (common_config.recycle_truncate_blocks==0)) {
        if (ftruncate(fd, 0)){}
	goto next_chunks;
      }

      /*
      ** Find out the projection identifier for this sid
      */
      uint8_t  prj_id  = 0;
      uint8_t  forward = rozofs_get_rozofs_forward(file_hdr->v0.layout);
      uint16_t rozofs_msg_psize=0, rozofs_disk_psize=0;

      for (prj_id=0; prj_id< forward; prj_id++) {
        if (file_hdr->v0.dist_set_current[prj_id] == file_hdr->v1.sid) break;
      }

      /*
      ** Retrieve the projection size on disk
      */      
      storage_get_projid_size(spare, prj_id, file_hdr->v0.layout, file_hdr->v0.bsize,
                              &rozofs_msg_psize, &rozofs_disk_psize);
      /*
      ** compute the truncate size
      */
      uint64_t truncate_size = common_config.recycle_truncate_blocks;
      truncate_size *= rozofs_disk_psize;

      /*
      ** compare the truncate size and the file size
      */
      struct stat buf;
      if (fstat(fd, &buf)<0) {     
        if (ftruncate(fd, 0)){}
	goto next_chunks;	  
      }
      
      if (buf.st_size > truncate_size) {
        if (ftruncate(fd, truncate_size)){}            	  
      }
    }
    
next_chunks:    
    /*
    ** Remove the extra chunks
    */
    for (chunk_idx=(chunk+1); chunk_idx<ROZOFS_STORAGE_MAX_CHUNK_PER_FILE; chunk_idx++) {

      if (file_hdr->v0.device[chunk_idx] == ROZOFS_EOF_CHUNK) {
        continue;
      }
      
      if (file_hdr->v0.device[chunk_idx] == ROZOFS_EMPTY_CHUNK) {
        file_hdr->v0.device[chunk_idx] = ROZOFS_EOF_CHUNK;
	continue;
      }
      
      storage_rm_data_chunk(st, file_hdr->v0.device[chunk_idx], fid, spare, chunk_idx,1/*errlog*/);
      file_hdr->v0.device[chunk_idx] = ROZOFS_EOF_CHUNK;
    }     
    /* 
    ** Rewrite file header on disk
    */   
    result = storage_write_all_header_files(st, fid, spare, file_hdr);        
    /*
    ** Failure on every write operation
    */ 
    if (result == 0) goto out;
      
    memcpy(device,file_hdr->v0.device,ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);
       
    status = 0;
out:

    if (fd != -1) close(fd);
    
    return status;
}

/*
 ** Read a header/mapper file
    This function looks for a header file of the given FID on every
    device when it should reside on this storage.

  @param st    : storage we are looking on
  @param fid   : fid whose hader file we are looking for
  @param spare : whether this storage is spare for this FID
  @param hdr   : where to return the read header file
  @param update_recycle : whether the header file is to be updated when recycling occurs
  
  @retval  STORAGE_READ_HDR_ERRORS     on failure
  @retval  STORAGE_READ_HDR_NOT_FOUND  when header file does not exist
  @retval  STORAGE_READ_HDR_OK         when header file has been read
  @retval  STORAGE_READ_HDR_OTHER_RECYCLING_COUNTER         when header file has been read
  
*/
STORAGE_READ_HDR_RESULT_E storage_read_header_file(storage_t                   * st, 
                                                   fid_t                         fid, 
						   uint8_t                       spare, 
						   rozofs_stor_bins_file_hdr_t * hdr, 
						   int                           update_recycle) {
  int  dev;
  int  hdrDevice;
  char path[FILENAME_MAX];
  int  storage_slice;
  int  fd;
  int  nb_read;
  int       device_result[STORAGE_MAX_DEVICE_NB];
  uint64_t  device_time[STORAGE_MAX_DEVICE_NB];
  uint64_t  device_id[STORAGE_MAX_DEVICE_NB];
  uint64_t  swap_time;
  int       swap_device;
  int       nb_devices=0;
  struct stat buf;
  int       idx;
  int       ret;
  uint64_t  crc32_error=0;
  char     *pChar;
  
  memset(device_time,0,sizeof(device_time));
  memset(device_id,0,sizeof(device_id));
  memset(device_result,0,sizeof(device_result));
  
  /*
  ** Compute storage slice from FID
  */
  storage_slice = rozofs_storage_fid_slice(fid);    
 
  /*
  ** Search for the last updated file.
  ** It may occur that a file can not be written any more although 
  ** it can still be read, so we better read the latest file writen
  ** on disk to be sure to get the correct information.
  ** So let's list all the redundant header files in the modification 
  ** date order.
  */
  for (dev=0; dev < st->mapper_redundancy ; dev++) {

    /*
    ** Header file path
    */
    hdrDevice = storage_mapper_device(fid,dev,st->mapper_modulo);
    pChar = storage_build_hdr_path(path, st->root, hdrDevice, spare, storage_slice);
	            
    // Check that this directory already exists, otherwise it will be create
    if (storage_create_dir(path) < 0) {
      storio_hdr_error(fid, hdrDevice,"create dir");   
      device_result[dev] = errno;		    
      storage_error_on_device(st,hdrDevice);
      continue;
    }   
        
    /* 
    ** Fullfill the path with the name of the mapping file
    */
    rozofs_uuid_unparse_no_recycle(fid, pChar);

    /*
    ** Get the file attributes
    */
    ret = stat(path,&buf);
    if (ret < 0) {
      device_result[dev] = errno;
      continue;
    }
    
    /*
    ** Insert in the table in the time order
    */
    for (idx=0; idx < nb_devices; idx++) {
      if (device_time[idx] > buf.st_mtime) continue;
      break;
    }
    nb_devices++;
    for (; idx < nb_devices; idx++) {  
     
      swap_time   = device_time[idx];
      swap_device = device_id[idx];

      device_time[idx] = buf.st_mtime;
      device_id[idx]   = hdrDevice;

      buf.st_mtime = swap_time;
      hdrDevice    = swap_device;  
    } 
  }
  
  /*
  ** Header files do not exist
  */
  if (nb_devices == 0) {
    for (dev=0; dev < st->mapper_redundancy ; dev++) {
      if (device_result[dev] == ENOENT) return STORAGE_READ_HDR_NOT_FOUND;  
    } 
    /*
    ** All devices have problems
    */
    return STORAGE_READ_HDR_ERRORS; 
  }
  
  /*
  ** Some header file is missing but not all
  */
  if (nb_devices != st->mapper_redundancy) {
    for (dev=0; dev < st->mapper_redundancy ; dev++) {
      if (device_result[dev] != 0) {
        hdrDevice = storage_mapper_device(fid,dev,st->mapper_modulo); 
	errno = device_result[dev];
        storio_hdr_error(fid, hdrDevice,"stat hdr");   
        storage_error_on_device(st,hdrDevice);        
      }
    }
  }
  

  /*
  ** Look for the mapping information in one of the redundant mapping devices
  ** which numbers are derived from the fid
  */
  for (dev=0; dev < nb_devices ; dev++) {

    /*
    ** Header file name
    */
    storage_build_hdr_file_path(path, st->root, device_id[dev], spare, storage_slice, fid);

    // Open hdr file
    fd = open(path, ROZOFS_ST_NO_CREATE_FILE_FLAG, ROZOFS_ST_BINS_FILE_MODE);
    if (fd < 0) {
      storio_hdr_error(fid, dev,"open hdr read");       
      device_result[dev] = errno;	
      continue;
    }
    
    nb_read = pread(fd, hdr, sizeof (*hdr), 0);
    close(fd);
    
    if (nb_read < 0) {
      storio_hdr_error(fid, dev, "read hdr");       
      device_result[dev] = EINVAL;	
      storage_error_on_device(st,device_id[dev]);
      continue;
    }
    
    /*
    ** check CRC32
    */
    uint32_t crc32 = fid2crc32((uint32_t *)fid);

    if (storio_check_header_crc32(hdr,&st->crc_error, crc32) != 0) {
      crc32_error |= (1ULL<<dev);
      device_result[dev] = EIO;	
      storio_hdr_error(fid, dev,"crc32 hdr");             
      storage_error_on_device(st,device_id[dev]);   
      continue;      
    }  
    /*
    ** check the recycle case : not the same recycling value
    */
    if (memcmp(hdr->v0.fid,fid,sizeof(fid_t))!= 0)
    {
      /*
      ** need to update the value of the fid in hdr
      */
      if (update_recycle) {
	memcpy(hdr->v0.fid,fid,sizeof(fid_t));
	storage_truncate_recycle(st,hdr->v0.device,storage_slice,spare,fid,hdr);
	return STORAGE_READ_HDR_OK;
      }
      /*
      ** This not the same FID, so the file we are looking for does not exist
      */
      else {
        return STORAGE_READ_HDR_OTHER_RECYCLING_COUNTER;
      }	    
    }
    /*
    ** Check whether header file is in version 1
    */
    if (update_header_file_version(st, fid, spare, hdr) == 1) {
      /* header file was in version 0 and has been update to version 1 */
      return STORAGE_READ_HDR_OK;
    }
    
    /*
    ** Header file has been read successfully. 
    ** Check whether some header file has had 
    ** a CRC32 error detected that should be fixed
    */
    if (crc32_error!=0) {
      int idx;
      for (idx=0; idx < dev; idx++) {
        if (crc32_error &(1ULL<<idx)) {
	  /*
	  ** Rewrite corrupted header file
	  */
	  storage_build_hdr_path(path, st->root, device_id[idx], spare, storage_slice);
	  storage_write_header_file(st,device_id[idx], path, hdr);
	}
      }
    }



    return STORAGE_READ_HDR_OK;	
  }  
  
  /*
  ** All devices have problems
  */
  return STORAGE_READ_HDR_ERRORS;
}



/*
 ** Find the name of the chunk file.
    
  @param st        : storage we are looking on
  @param device_id : the device_id the file resides on or -1 when unknown
  @param chunk     : chunk number to write
  @param fid       : FID of the file to write
  @param layout    : layout to use
  @param dist_set  : the file sid distribution set
  @param spare     : whether this storage is spare for this FID
  @param path      : The returned absolute path
  @param version   : current header file format version

  @retval A value within storage_dev_map_distribution_write_ret_e
  @retval MAP_OK      the path is written in path
  @retval MAP_COPY2CACHE when an error occur that prevent the correct operation
  
 */
typedef enum {

  /* An error occur that prevent access to the file */
  MAP_FAILURE=0, 
  
  /* The path is written in path */
  MAP_OK,
       
  /* The path is written in path. But FID cache has to be updated after file is written
     from information in file header */  
  MAP_COPY2CACHE 
} storage_dev_map_distribution_write_ret_e;

	  
static inline storage_dev_map_distribution_write_ret_e
    storage_dev_map_distribution_write(  storage_t * st, 
					 uint8_t * device,
					 uint8_t chunk,
					 uint32_t bsize, 
					 fid_t fid, 
					 uint8_t layout,
                                	 sid_t dist_set[ROZOFS_SAFE_MAX], 
					 uint8_t spare, 
					 char *path, 
					 int version,
					 rozofs_stor_bins_file_hdr_t * file_hdr) {
    int                         result;
    STORAGE_READ_HDR_RESULT_E   read_hdr_res;
    int                         storage_slice = rozofs_storage_fid_slice(fid); 
    uint8_t                     dev;       

    /*
    ** A valid device is given as input, so use it
    */
    if ((device[chunk] != ROZOFS_EOF_CHUNK)&&(device[chunk] != ROZOFS_EMPTY_CHUNK)&&(device[chunk] != ROZOFS_UNKNOWN_CHUNK)) {
      /*
      ** Build the chunk file name using the valid device id given in the device array
      */
      storage_build_chunk_full_path(path, st->root, device[chunk], spare, storage_slice, fid, chunk);
      return MAP_OK;  
    }   

    /*
    ** When no device id is given as input, let's read the header file 
    */    
    read_hdr_res = storage_read_header_file(
            st,       // cid/sid context
            fid,      // FID we are looking for
	    spare,    // Whether the storage is spare for this FID
	    file_hdr,// Returned header file content
            1 );      // Update header file when not the same recycling value

      
    /*
    ** Error accessing all the devices
    */
    if (read_hdr_res == STORAGE_READ_HDR_ERRORS) {
      return MAP_FAILURE;
    }
    
    /*
    ** Header file has been read
    */
    if (read_hdr_res == STORAGE_READ_HDR_OK) {
       
      dev = file_hdr->v0.device[chunk];
      
      /*
      ** A device is already allocated for this chunk.
      */
      if ((dev != ROZOFS_EOF_CHUNK)&&(dev != ROZOFS_EMPTY_CHUNK)) {
      
	 /*
	 ** Build the chunk file name using the device id read from the header file
	 */
	 storage_build_chunk_full_path(path, st->root, dev, spare, storage_slice, fid, chunk);
	 
         /*
	 ** Update input device array from the read header file
	 */
         memcpy(device,file_hdr->v0.device,ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);
	 
	 return MAP_OK;
      }   
      
      /*
      ** We are extending the file
      */
      if (dev == ROZOFS_EOF_CHUNK) {
        /*
	** All previous chunks that where said EOF must be said EMPTY
	*/
        int idx;
	for (idx=0; idx <= chunk; idx++) {
	  if (file_hdr->v0.device[idx] == ROZOFS_EOF_CHUNK) {
	    file_hdr->v0.device[idx] = ROZOFS_EMPTY_CHUNK;
	  }
	}
      } 
      
    }    
      
    /*
    ** Header file does not exist. This is a brand new file
    */    
    if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) {
      int idx;
      /*
      ** Prepare file header
      */
      memcpy(file_hdr->v0.dist_set_current, dist_set, ROZOFS_SAFE_MAX * sizeof (sid_t));
      file_hdr->v0.layout = layout;
      file_hdr->v0.bsize  = bsize;
      file_hdr->v0.version = 1;
      file_hdr->v1.cid = st->cid;
      file_hdr->v1.sid = st->sid;
      memcpy(file_hdr->v0.fid, fid,sizeof(fid_t)); 
      for (idx=0; idx <= chunk; idx++) {
	file_hdr->v0.device[idx] = ROZOFS_EMPTY_CHUNK;
      }
      for (;idx<ROZOFS_STORAGE_MAX_CHUNK_PER_FILE; idx++) {
	file_hdr->v0.device[idx] = ROZOFS_EOF_CHUNK;      
      }        
    }
    
      
    /*
    ** Allocate a device for this newly written chunk
    */
    dev = storio_device_mapping_allocate_device(st);
    file_hdr->v0.device[chunk] = dev; 
        
    /*
    ** (re)Write the header files on disk 
    */
    result = storage_write_all_header_files(st, fid, spare, file_hdr);        
    /*
    ** Failure on every write operation
    */ 
    if (result == 0) {
      /*
      ** Header file was not existing, so let's remove it from every
      ** device. The inode may have been created although the file
      ** data can not be written
      */
      if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) {
        storage_dev_map_distribution_remove(st, fid, spare);
      }
      return MAP_FAILURE;
    }       
    

    /*
    ** Build the chunk file name using the newly allocated device id
    */
    storage_build_chunk_full_path(path, st->root, dev, spare, storage_slice, fid, chunk);
    
    /*
    ** Update input device array from header file
    */   
    return MAP_COPY2CACHE;            
}

/*
** 
  @param device_number: number of device handled by the sid
  @param mapper_modulo: number of device to hold mapping file
  @param mapper_redundancy: number of mapping file instance to write per FID
**  
*/
int storage_initialize(storage_t *st, 
                       cid_t cid, 
		       sid_t sid, 
		       const char *root, 
                       uint32_t device_number, 
		       uint32_t mapper_modulo, 
		       uint32_t mapper_redundancy,
		       int      selfHealing,
		       char   * export_hosts) {
    int status = -1;
    char path[FILENAME_MAX];
    struct stat s;
    int dev;
    char * pChar, * pChar2;

    DEBUG_FUNCTION;

    storio_device_error_log_init();

    if (!realpath(root, st->root))
        goto out;
	
    if (mapper_modulo > device_number) {
      severe("mapper_modulo is %d > device_number %d",mapper_modulo,device_number)
      goto out;
    }	
		
    if (mapper_redundancy > mapper_modulo) {
      severe("mapper_redundancy is %d > mapper_modulo %d",mapper_redundancy,mapper_modulo)
      goto out;
    }	
    
    st->mapper_modulo     = mapper_modulo;
    st->device_number     = device_number; 
    st->mapper_redundancy = mapper_redundancy;
    st->selfHealing       = selfHealing; 
    st->export_hosts      = export_hosts;
    st->share             = NULL;
    
    st->device_free.active = 0;
    for (dev=0; dev<STORAGE_MAX_DEVICE_NB; dev++) {
      st->device_free.blocks[0][dev] = 20000;
      st->device_free.blocks[1][dev] = 20000;
    }

    /*
    ** Initialize device status
    */
    for (dev=0; dev<device_number; dev++) {
      st->device_ctx[dev].status = storage_device_status_init;
      st->device_ctx[dev].failure = 0;
    }

    memset(&st->device_errors , 0,sizeof(st->device_errors));        
	    
    // sanity checks
    if (stat(st->root, &s) != 0) {
        severe("can not stat %s",st->root);
        goto out;
    }

    if (!S_ISDIR(s.st_mode)) {
        errno = ENOTDIR;
        goto out;
    }		
        	
    for (dev=0; dev < st->device_number; dev++) {		
	

	// sanity checks
	pChar = path;
	pChar += rozofs_string_append(pChar,st->root);
	*pChar++ = '/';
	pChar += rozofs_u32_append(pChar,dev);

	if (stat(path, &s) != 0) {
            severe("can not stat %s",path);
	    continue;
	}
	    
	if (!S_ISDIR(s.st_mode)) {
            severe("can not stat %s",path);
            errno = ENOTDIR;
            goto out;
	}

        /*
	** Build 2nd level directories
	*/
        pChar2 = pChar;
	pChar2 += rozofs_string_append(pChar2,"/hdr_0/");
        if (access(path, F_OK) != 0) {
          if (storage_create_dir(path) < 0) {
            severe("%s creation %s",path, strerror(errno));
          }
	  int slice;
	  for (slice = 0; slice < (common_config.storio_slice_number); slice++) {
	    rozofs_u32_append(pChar2,slice);
            if (storage_create_dir(path) < 0) {
              severe("%s creation %s",path, strerror(errno));
            }	    
	  }
	}  
	
        pChar2 = pChar;
	pChar2 += rozofs_string_append(pChar2,"/hdr_1/");
        if (access(path, F_OK) != 0) {
          if (storage_create_dir(path) < 0) {
            severe("%s creation %s",path, strerror(errno));
          }	
	  int slice;
	  for (slice = 0; slice < (common_config.storio_slice_number); slice++) {
	    rozofs_u32_append(pChar2,slice);	  
            if (storage_create_dir(path) < 0) {
              severe("%s creation %s",path, strerror(errno));
            }	    
	  }
	} 
	
        pChar2 = pChar;	
	pChar2 += rozofs_string_append(pChar2,"/bins_0/");
        if (access(path, F_OK) != 0) {
          if (storage_create_dir(path) < 0) {
            severe("%s creation %s",path, strerror(errno));
          }	
	  int slice;
	  for (slice = 0; slice < (common_config.storio_slice_number); slice++) {
	    rozofs_u32_append(pChar2,slice);	 
            if (storage_create_dir(path) < 0) {
              severe("%s creation %s",path, strerror(errno));
            }	    
	  }
	} 

        pChar2 = pChar;		
	pChar2 += rozofs_string_append(pChar2,"/bins_1/");
        if (access(path, F_OK) != 0) {
          if (storage_create_dir(path) < 0) {
            severe("%s creation %s",path, strerror(errno));
          }	
	  int slice;
	  for (slice = 0; slice < (common_config.storio_slice_number); slice++) {
	    rozofs_u32_append(pChar2,slice);	 
            if (storage_create_dir(path) < 0) {
              severe("%s creation %s",path, strerror(errno));
            }	    
	  } 
	} 	
    }
    st->sid = sid;
    st->cid = cid;

    status = 0;
out:
    return status;
}

void storage_release(storage_t * st) {

    DEBUG_FUNCTION;

    st->sid = 0;
    st->cid = 0;
    st->root[0] = 0;

}

uint64_t buf_ts_storage_write[STORIO_CACHE_BCOUNT];


int storage_relocate_chunk(storage_t * st, uint8_t * device,fid_t fid, uint8_t spare, 
                           uint8_t chunk, uint8_t * old_device) {
    STORAGE_READ_HDR_RESULT_E      read_hdr_res;  
    rozofs_stor_bins_file_hdr_t    file_hdr;
    int                            result;

    /*
    ** Let's read the header file 
    */    
    read_hdr_res = storage_read_header_file(
            st,       // cid/sid context
            fid,      // FID we are looking for
	    spare,    // Whether the storage is spare for this FID
	    &file_hdr,// Returned header file content
            1 );      // Update header file when not the same recycling value   

    /*
    ** Error accessing all the devices
    */
    if (read_hdr_res == STORAGE_READ_HDR_ERRORS) {
      severe("storage_relocate_chunk");
      return -1;
    }

    /*
    ** Header file does not exist! This is a brand new file
    */    
    if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) { 
      memset(device,ROZOFS_UNKNOWN_CHUNK,ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);
      *old_device = ROZOFS_EMPTY_CHUNK;
      return 0;
    }

    /*
    ** Header file has been read
    */
    
    /* Save the previous chunk location and then release it */
    *old_device = file_hdr.v0.device[chunk];
    
    /* Last chunk ? */
    if (chunk == (ROZOFS_STORAGE_MAX_CHUNK_PER_FILE-1)) {
      file_hdr.v0.device[chunk] = ROZOFS_EOF_CHUNK;
    }
    /* End of file ? */
    else if (file_hdr.v0.device[chunk+1] == ROZOFS_EOF_CHUNK) {
      int idx;
      file_hdr.v0.device[chunk] = ROZOFS_EOF_CHUNK;
      idx = chunk-1;
      /* Previous empty chunk is now end of file */
      while (idx>=0) {
        if (file_hdr.v0.device[idx] != ROZOFS_EMPTY_CHUNK) break;
	file_hdr.v0.device[idx] = ROZOFS_EOF_CHUNK;
	idx--;
      }
    }
    /* Inside the file */
    else {
      file_hdr.v0.device[chunk] = ROZOFS_EMPTY_CHUNK;
    }  
    memcpy(device,file_hdr.v0.device,ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);

    /* 
    ** Rewrite file header on disk
    */   
    result = storage_write_all_header_files(st, fid, spare, &file_hdr);        
    /*
    ** Failure on every write operation
    */ 
    if (result == 0) return -1;   
    return 0;
}
int storage_rm_data_chunk(storage_t * st, uint8_t device, fid_t fid, uint8_t spare, uint8_t chunk, int errlog) {
  char path[FILENAME_MAX];
  int  ret;

  uint32_t storage_slice = rozofs_storage_fid_slice(fid);
  storage_build_chunk_full_path(path, st->root, device, spare, storage_slice, fid, chunk);

  ret = unlink(path);   
  if ((ret < 0) && (errno != ENOENT) && (errlog)) {
    severe("storage_rm_data_chunk(%s) %s", path, strerror(errno));
  }
  return ret;  
} 
int storage_restore_chunk(storage_t * st, uint8_t * device,fid_t fid, uint8_t spare, 
                           uint8_t chunk, uint8_t old_device) {
    STORAGE_READ_HDR_RESULT_E      read_hdr_res;  
    rozofs_stor_bins_file_hdr_t    file_hdr;
    int                            result;       
   
    /*
    ** Let's read the header file 
    */    
    read_hdr_res = storage_read_header_file(
            st,       // cid/sid context
            fid,      // FID we are looking for
	    spare,    // Whether the storage is spare for this FID
	    &file_hdr,// Returned header file content
            0 );      // Do not update header file when not the same recycling value
   
    /*
    ** Error accessing all the devices
    */
    if (read_hdr_res == STORAGE_READ_HDR_ERRORS) {
      severe("storage_relocate_chunk");
      return -1;
    }

    /*
    ** Header file does not exist! This is a brand new file
    */    
    if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) { 
      memset(device,ROZOFS_UNKNOWN_CHUNK,ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);
      return 0;
    }

    /*
    ** Header file has been read. 
    */
    
    if (read_hdr_res == STORAGE_READ_HDR_OTHER_RECYCLING_COUNTER) {
      /*
      ** This is not the same recycle counter that the one we were rebuilding
      ** better not modify any thing.
      */
      return 0;
    }
       
    
    /*
    ** Remove new data file which rebuild has failed 
    */
    storage_rm_data_chunk(st, file_hdr.v0.device[chunk], fid, spare, chunk,0/* No errlog*/);        
    
    /*
    ** Restore device in header file
    */
    file_hdr.v0.device[chunk] = old_device;
    if (old_device==ROZOFS_EOF_CHUNK) {
      /* not the last chunk */
      if ((chunk != (ROZOFS_STORAGE_MAX_CHUNK_PER_FILE-1))
      &&  (file_hdr.v0.device[chunk+1] != ROZOFS_EOF_CHUNK)) {
	file_hdr.v0.device[chunk] = ROZOFS_EMPTY_CHUNK;
      }
    }
    else if (old_device==ROZOFS_EMPTY_CHUNK) {  
      /* Last chunk */
      if ((chunk == (ROZOFS_STORAGE_MAX_CHUNK_PER_FILE-1))
      ||  (file_hdr.v0.device[chunk+1] != ROZOFS_EOF_CHUNK)) {
	file_hdr.v0.device[chunk] = ROZOFS_EOF_CHUNK;
      }   
    }
    if (file_hdr.v0.device[chunk] == ROZOFS_EOF_CHUNK) {
      int idx = chunk-1;
      /* Previous empty chunk is now end of file */
      while (idx>=0) {
        if (file_hdr.v0.device[idx] != ROZOFS_EMPTY_CHUNK) break;
	file_hdr.v0.device[idx] = ROZOFS_EOF_CHUNK;
	idx--;
      }      
    }     
    memcpy(device,file_hdr.v0.device,ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);

    /* 
    ** Rewrite file header on disk
    */   
    result = storage_write_all_header_files(st, fid, spare, &file_hdr);        
    /*
    ** Failure on every write operation
    */ 
    if (result == 0) return -1;   
    return 0;
}
int storage_write_chunk(storage_t * st, storio_device_mapping_t * fidCtx, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, uint8_t chunk, bid_t bid, uint32_t nb_proj, uint8_t version,
        uint64_t *file_size, const bin_t * bins, int * is_fid_faulty) {
    int status = -1;
    char path[FILENAME_MAX];
    int fd = -1;
    size_t nb_write = 0;
    size_t length_to_write = 0;
    off_t bins_file_offset = 0;
    uint16_t rozofs_msg_psize;
    uint16_t rozofs_disk_psize;
    struct stat sb;
    int open_flags;
    int    device_id_is_given;
    rozofs_stor_bins_file_hdr_t file_hdr;
    storage_dev_map_distribution_write_ret_e map_result = MAP_FAILURE;
    uint8_t * device = fidCtx->device;

    // No specific fault on this FID detected
    *is_fid_faulty = 0; 

    dbg("%d/%d Write chunk %d : ", st->cid, st->sid, chunk);
   
open:    
    // If the device id is given as input, that proves that the file
    // has been existing with that name on this device sometimes ago. 
    if ((device[chunk] != ROZOFS_EOF_CHUNK)&&(device[chunk] != ROZOFS_EMPTY_CHUNK)&&(device[chunk] != ROZOFS_UNKNOWN_CHUNK)) {
      device_id_is_given = 1;
      open_flags = ROZOFS_ST_NO_CREATE_FILE_FLAG;
    }
    // The file location is not known. It may not exist and should be created 
    else {
      device_id_is_given = 0;
      open_flags = ROZOFS_ST_BINS_FILE_FLAG;
    }        
 
    // Build the chunk file name 
    map_result = storage_dev_map_distribution_write(st, device, chunk, bsize, 
                                        	    fid, layout, dist_set, 
						    spare, path, 0, &file_hdr);
    if (map_result == MAP_FAILURE) {
      goto out;      
    }  

    // Open bins file
    fd = open(path, open_flags, ROZOFS_ST_BINS_FILE_MODE);
    if (fd < 0) {
    
        // Something definitively wrong on device
        if (errno != ENOENT) {
          storio_fid_error(fid, device[chunk], chunk, bid, nb_proj,"open write");	
	  storage_error_on_device(st,device[chunk]); 
	  goto out;
	}
	
        // If device id was not given as input, the file path has been deduced from 
	// the header files or should have been allocated. This is a definitive error !!!
	if (device_id_is_given == 0) {
          storio_fid_error(fid, device[chunk], chunk, bid, nb_proj,"open write");
	  storage_error_on_device(st,device[chunk]); 
	  goto out;
	}
	
	// The device id was given as input so the file did exist some day,
	// but the file may have been deleted without storio being aware of it.
	// Let's try to find the file again without using the given device id.
	device[chunk] = ROZOFS_EOF_CHUNK;
	goto open;    
    }

    
    /*
    ** Retrieve the projection size in the message
    ** and the projection size on disk 
    */
    storage_get_projection_size(spare, st->sid, layout, bsize, dist_set,
                                &rozofs_msg_psize, &rozofs_disk_psize); 
	       
    // Compute the offset and length to write
    
    bins_file_offset = bid * rozofs_disk_psize;
    length_to_write  = nb_proj * rozofs_disk_psize;

    //dbg("write %s bid %d nb %d",path,bid,nb_proj);

    uint32_t crc32 = fid2crc32((uint32_t *)fid) + bid;

    /*
    ** Writting the projection as received directly on disk
    */
    if (rozofs_msg_psize == rozofs_disk_psize) {
    
      /*
      ** generate the crc32c for each projection block
      */
      storio_gen_crc32((char*)bins,nb_proj,rozofs_disk_psize,crc32);

      errno = 0;
      nb_write = pwrite(fd, bins, length_to_write, bins_file_offset);
    }

    /*
    ** Writing the projections on a different size on disk
    */
    else {
      struct iovec       vector[ROZOFS_MAX_BLOCK_PER_MSG+1]; 
      int                i;
      char *             pMsg;
      
      if (nb_proj > (ROZOFS_MAX_BLOCK_PER_MSG+1)) {  
        severe("storage_write more blocks than possible %d vs max %d",
	        nb_proj,ROZOFS_MAX_BLOCK_PER_MSG+1);
        errno = ESPIPE;	
        goto out;
      }
      pMsg  = (char *) bins;
      for (i=0; i< nb_proj; i++) {
        vector[i].iov_base = pMsg;
        vector[i].iov_len  = rozofs_disk_psize;
	pMsg += rozofs_msg_psize;
      }
      
      /*
      ** generate the crc32c for each projection block
      */
      
      storio_gen_crc32_vect(vector,nb_proj,rozofs_disk_psize,crc32);
      
      errno = 0;      
      nb_write = pwritev(fd, vector, nb_proj, bins_file_offset);      
    } 

    if (nb_write != length_to_write) {
	
        if (errno==0) errno = ENOSPC;
	storio_fid_error(fid, device[chunk], chunk, bid, nb_proj,"write");
        
	/*
	** Only few bytes written since no space left on device 
	*/
        if ((errno==0)||(errno==ENOSPC)) {
	  errno = ENOSPC;
	  goto out;
        }
	storage_error_on_device(st,device[chunk]);
	// A fault probably localized to this FID is detected   
	*is_fid_faulty = 1;  
        severe("pwrite(%s) size %llu expecting %llu offset %llu : %s",
	        path, (unsigned long long)nb_write,
	        (unsigned long long)length_to_write, 
		(unsigned long long)bins_file_offset, 
		strerror(errno));
        goto out;
    }
    /**
    * insert in the fid cache the written section
    */
//    storage_build_ts_table_from_prj_header((char*)bins,nb_proj,rozofs_max_psize,buf_ts_storage_write);
//    storio_cache_insert(fid,bid,nb_proj,buf_ts_storage_write,0);
    
    // Stat file for return the size of bins file after the write operation
    if (fstat(fd, &sb) == -1) {
        severe("fstat failed: %s", strerror(errno));
        goto out;
    }

    *file_size = sb.st_size;


    // Write is successful
    status = nb_proj * rozofs_msg_psize;

out:
    if (fd != -1) close(fd);

    /*
    ** Update device array in FID cache from header file
    */    
    if (map_result == MAP_COPY2CACHE) {
      memcpy(device,file_hdr.v0.device,ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);    
    }
        
    return status;
}
int storage_write_repair_chunk(storage_t * st, uint8_t * device, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, uint8_t chunk, bid_t bid, uint32_t nb_proj, uint64_t bitmap, uint8_t version,
        uint64_t *file_size, const bin_t * bins, int * is_fid_faulty) {
    int status = -1;
    char path[FILENAME_MAX];
    int fd = -1;
    size_t nb_write = 0;
    off_t bins_file_offset = 0;
    uint16_t rozofs_msg_psize;
    uint16_t rozofs_disk_psize;
    struct stat sb;
    rozofs_stor_bins_file_hdr_t file_hdr;
    storage_dev_map_distribution_write_ret_e map_result = MAP_FAILURE;

    // No specific fault on this FID detected
    *is_fid_faulty = 0; 

    dbg("%d/%d repair chunk %d : ", st->cid, st->sid, chunk);
    
    /*
    ** This is a repair, so the blocks to repair have been read and the CRC32 
    ** was incorrect, so the chunk must exist.
    */
    if ((device[chunk] == ROZOFS_EOF_CHUNK)||(device[chunk] == ROZOFS_EMPTY_CHUNK)||(device[chunk] == ROZOFS_UNKNOWN_CHUNK)) {
      errno = EADDRNOTAVAIL;
      goto out;
    }   
 
    // Build the chunk file name
    map_result = storage_dev_map_distribution_write(st, device, chunk, bsize, 
                                        	    fid, layout, dist_set, 
						    spare, path, 0, &file_hdr);
    if (map_result == MAP_FAILURE) {
      goto out;      
    }  
        
    // Open bins file
    fd = open(path, ROZOFS_ST_NO_CREATE_FILE_FLAG, ROZOFS_ST_BINS_FILE_MODE);
    if (fd < 0) {
      storio_fid_error(fid, device[chunk], chunk, bid, nb_proj,"open repair"); 		
      storage_error_on_device(st,device[chunk]); 
      goto out;
    }

    /*
    ** Retrieve the projection size in the message
    ** and the projection size on disk 
    */
    storage_get_projection_size(spare, st->sid, layout, bsize, dist_set,
                                &rozofs_msg_psize, &rozofs_disk_psize); 
	       
    char *data_p = (char *)bins;
    int block_idx = 0;
    int block_count = 0;
    int error = 0;   
    uint32_t crc32 = fid2crc32((uint32_t *)fid)+bid;
            
    for (block_idx = 0; block_idx < nb_proj; block_idx++)
    {

       if ((bitmap & (1ULL << block_idx)) == 0) continue;
       
       /*
       ** generate the crc32c for each projection block
       */
       storio_gen_crc32((char*)data_p,1,rozofs_disk_psize,crc32+block_idx);
       /* 
       **  write the projection on disk
       */
       bins_file_offset = (bid+block_idx) * rozofs_disk_psize;
       nb_write = pwrite(fd, data_p, rozofs_disk_psize, bins_file_offset);
       if (nb_write != rozofs_disk_psize) {

 	  storio_fid_error(fid, device[chunk], chunk, bid, nb_proj,"write repair");
	  storage_error_on_device(st,device[chunk]);

	  /*
	  ** Only few bytes written since no space left on device 
	  */
          if ((errno==0)||(errno==ENOSPC)) {
	    errno = ENOSPC;
          }
	  else {
            severe("pwrite failed: %s", strerror(errno));
	  } 
	  error +=1;
       }
       /*
       ** update the data pointer for the next write
       */
       data_p+=rozofs_msg_psize;
       block_count += rozofs_msg_psize;
    }
    if (error != 0) goto out;
    

    // Stat file for return the size of bins file after the write operation
    if (fstat(fd, &sb) == -1) {
        severe("fstat failed: %s", strerror(errno));
        goto out;
    }
    *file_size = sb.st_size;


    // Write is successful
    status = block_count;

out:
    if (fd != -1) close(fd);
    return status;
}

uint64_t buf_ts_storage_before_read[STORIO_CACHE_BCOUNT];
uint64_t buf_ts_storage_after_read[STORIO_CACHE_BCOUNT];
uint64_t buf_ts_storcli_read[STORIO_CACHE_BCOUNT];
char storage_bufall[4096];
uint8_t storage_read_optim[4096];

int storage_read_chunk(storage_t * st, storio_device_mapping_t * fidCtx, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, uint8_t chunk, bid_t bid, uint32_t nb_proj,
        bin_t * bins, size_t * len_read, uint64_t *file_size,int * is_fid_faulty) {

    int status = -1;
    char path[FILENAME_MAX];
    int fd = -1;
    size_t nb_read = 0;
    size_t length_to_read = 0;
    off_t bins_file_offset = 0;
    uint16_t rozofs_msg_psize;
    uint16_t rozofs_disk_psize;
    int    device_id_is_given = 1;
    int                       storage_slice;
    struct iovec vector[ROZOFS_MAX_BLOCK_PER_MSG];
    uint64_t    crc32_errors; 
    uint8_t * device = fidCtx->device;
    
    dbg("%d/%d Read chunk %d : ", st->cid, st->sid, chunk);

    // No specific fault on this FID detected
    *is_fid_faulty = 0;  
    path[0]=0;
    
    /*
    ** When device array is not given, one has to read the header file on disk
    */
    if (device[0] == ROZOFS_UNKNOWN_CHUNK) {
      device_id_is_given = 0;    
    }

    /*
    ** Retrieve the projection size in the message 
    ** and the projection size on disk
    */
    storage_get_projection_size(spare, st->sid, layout, bsize, dist_set,
                                &rozofs_msg_psize, &rozofs_disk_psize); 


retry:

    /*
    ** Let's read the header file from disk
    */
    if (!device_id_is_given) {
      rozofs_stor_bins_file_hdr_t file_hdr;
      STORAGE_READ_HDR_RESULT_E read_hdr_res;  
      
      read_hdr_res = storage_read_header_file(
              st,       // cid/sid context
              fid,      // FID we are looking for
	      spare,    // Whether the storage is spare for this FID
	      &file_hdr,// Returned header file content
              0 );      // do not update header file when not the same recycling value


      /*
      ** Header files are unreadable
      */
      if (read_hdr_res == STORAGE_READ_HDR_ERRORS) {
	*is_fid_faulty = 1; 
	errno = EIO;
	goto out;
      }
      
      /*
      ** Header files does not exist
      */      
      if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) {
        errno = ENOENT;
        goto out;  
      } 
      
      /*
      ** Header files has not the requested recycling value.
      ** The requested file does not exist.
      */      
      if (read_hdr_res == STORAGE_READ_HDR_OTHER_RECYCLING_COUNTER) {
        /*
	** Update the FID context in order to make it fit with the disk content
	** Copy recycling counter value as well as chunk distribution
	*/
	fidCtx->recycle_cpt = rozofs_get_recycle_from_fid(file_hdr.v0.fid);
	memcpy(device,file_hdr.v0.device,ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);
        errno = ENOENT;
        goto out;  
      } 
      
      /*
      ** Update recycle counter in FID context when relevant
      */
      if (fidCtx->recycle_cpt != rozofs_get_recycle_from_fid(file_hdr.v0.fid)) {
        fidCtx->recycle_cpt = rozofs_get_recycle_from_fid(file_hdr.v0.fid); 
      } 

      /* 
      ** The header file has been read
      */
      memcpy(device,file_hdr.v0.device,ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);
      
    } 
    
         
    /*
    ** We are trying to read after the end of file
    */				     
    if (device[chunk] == ROZOFS_EOF_CHUNK) {
      *len_read = 0;
      status = nb_proj * rozofs_msg_psize;
      goto out;
    }

    /*
    ** We are trying to read inside a whole. Return 0 on the requested size
    */      
    if(device[chunk] == ROZOFS_EMPTY_CHUNK) {
      *len_read = nb_proj * rozofs_msg_psize;
      memset(bins,0,* len_read);
      status = *len_read;
      goto out;
    }
    
  
    storage_slice = rozofs_storage_fid_slice(fid);
    storage_build_chunk_full_path(path, st->root, device[chunk], spare, storage_slice, fid,chunk);

    // Open bins file
    fd = open(path, ROZOFS_ST_NO_CREATE_FILE_FLAG, ROZOFS_ST_BINS_FILE_MODE_RO);
    if (fd < 0) {
    
        // Something definitively wrong on device
        if (errno != ENOENT) {
          storio_fid_error(fid, device[chunk], chunk, bid, nb_proj,"open read"); 		
	  storage_error_on_device(st,device[chunk]); 
	  goto out;
	}
	
        // If device id was not given as input, the file path has been deduced from 
	// the header files and so should exist. This is an error !!!
	if (device_id_is_given == 0) {
          storio_fid_error(fid, device[chunk], chunk, bid, nb_proj,"open read"); 			  
	  errno = EIO; // Data file is missing !!!
	  *is_fid_faulty = 1;
	  storage_error_on_device(st,device[chunk]); 
	  goto out;
	}
	
	// The device id was given as input so the file did exist some day,
	// but the file may have been deleted without storio being aware of it.
	// Let's try to find the file again without using the given device id.
	device_id_is_given = 0;
	goto retry ;
    }	

	       
    // Compute the offset and length to write
    
    bins_file_offset = bid * rozofs_disk_psize;
    length_to_read   = nb_proj * rozofs_disk_psize;

    //dbg("read %s bid %d nb %d",path,bid,nb_proj);
    
    /*
    ** Reading the projection directly as they will be sent in message
    */
    if (rozofs_msg_psize == rozofs_disk_psize) {    
      // Read nb_proj * (projection + header)
      nb_read = pread(fd, bins, length_to_read, bins_file_offset);       
    }
    /*
    ** Projections are smaller on disk than in message
    */
    else {
      int          i;
      char *       pMsg;
      
      if (nb_proj > ROZOFS_MAX_BLOCK_PER_MSG) {  
        severe("storage_read more blocks than possible %d vs max %d",
	        nb_proj,ROZOFS_MAX_BLOCK_PER_MSG);
        errno = ESPIPE;			
        goto out;
      }
      pMsg  = (char *) bins;
      for (i=0; i< nb_proj; i++) {
        vector[i].iov_base = pMsg;
        vector[i].iov_len  = rozofs_disk_psize;
	pMsg += rozofs_msg_psize;
      }
      nb_read = preadv(fd, vector, nb_proj, bins_file_offset);      
    } 
    
    // Check error
    if (nb_read == -1) {
        storio_fid_error(fid, device[chunk], chunk, bid, nb_proj,"read"); 			
        severe("pread failed: %s", strerror(errno));
	storage_error_on_device(st,device[chunk]);  
	// A fault probably localized to this FID is detected   
	*is_fid_faulty = 1;   		
        goto out;
    }


    // Check the length read
    if ((nb_read % rozofs_disk_psize) != 0) {
        char fid_str[37];
        rozofs_uuid_unparse(fid, fid_str);
        severe("storage_read failed (FID: %s layout %d bsize %d chunk %d bid %d): read inconsistent length %d not modulo of %d",
	       fid_str,layout,bsize,chunk, (int) bid,(int)nb_read,rozofs_disk_psize);
	nb_read = (nb_read / rozofs_disk_psize) * rozofs_disk_psize;
    }

    int nb_proj_effective;
    nb_proj_effective = nb_read /rozofs_disk_psize ;

    /*
    ** check the crc32c for each projection block
    */
    uint32_t crc32 = fid2crc32((uint32_t *)fid)+bid;
    
    if (rozofs_msg_psize == rozofs_disk_psize) {        
      crc32_errors = storio_check_crc32((char*)bins,
                        		nb_proj_effective,
                			rozofs_disk_psize,
					&st->crc_error,
					crc32);
    }
    else {
      crc32_errors = storio_check_crc32_vect(vector,
                        		     nb_proj_effective,
                			     rozofs_disk_psize,
					     &st->crc_error,
					     crc32);      
    }
    if (crc32_errors!=0) { 
      storio_fid_error(fid, device[chunk], chunk, bid, nb_proj,"read crc32"); 		     
      storage_error_on_device(st,device[chunk]); 
    }	  

    // Update the length read
    *len_read = (nb_read/rozofs_disk_psize)*rozofs_msg_psize;

    *file_size = 0;

    // Read is successful
    status = nb_proj * rozofs_msg_psize;

out:
    if (fd != -1) close(fd);
    return status;
}


int storage_truncate(storage_t * st, storio_device_mapping_t * fidCtx, uint8_t layout, uint32_t bsize, sid_t * dist_set,
        uint8_t spare, fid_t fid, tid_t proj_id,bid_t input_bid,uint8_t version,uint16_t last_seg,uint64_t last_timestamp,
	u_int length_to_write, char * data, int * is_fid_faulty) {
    int status = -1;
    char path[FILENAME_MAX];
    int fd = -1;
    off_t bins_file_offset = 0;
    uint16_t rozofs_msg_psize;
    uint16_t rozofs_disk_psize;
    bid_t bid_truncate;
    size_t nb_write = 0;
    int open_flags;
    int block_per_chunk         = ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(bsize);
    int chunk                   = input_bid/block_per_chunk;
    int result;
    bid_t bid = input_bid - (chunk * block_per_chunk);
    STORAGE_READ_HDR_RESULT_E read_hdr_res;
    int chunk_idx;
    rozofs_stor_bins_file_hdr_t file_hdr;
    int                         rewrite_file_hdr = 0;
    storage_dev_map_distribution_write_ret_e map_result = MAP_FAILURE;
    uint8_t * device;

    if (chunk>=ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) { 
      errno = EFBIG;
      return -1;
    } 
    
    // No specific fault on this FID detected
    *is_fid_faulty = 0;  

    /*
    ** device is in this procedure the device filed of the file_hdr local
    ** working variable. The FID context will be update at the end of the
    ** procedure
    */
    device = file_hdr.v0.device;

    /*
    ** Prepare file header
    */
    memcpy(file_hdr.v0.dist_set_current, dist_set, ROZOFS_SAFE_MAX * sizeof (sid_t));
    file_hdr.v0.layout  = layout;
    file_hdr.v0.bsize   = bsize;
    file_hdr.v0.version = 1;
    file_hdr.v1.cid     = st->cid;
    file_hdr.v1.sid     = st->sid;	
    memcpy(file_hdr.v0.fid, fid, sizeof(fid_t)); 


    /*
    ** Valid FID context. Do not re read disk but use FID ctx information.
    */
    if (fidCtx->device[0] != ROZOFS_UNKNOWN_CHUNK) {
    
      /*
      ** FID context contains valid distribution. Use it.
      */
      memcpy(device,fidCtx->device,ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);   
      
      /*
      ** Process to the recycling when needed
      */
      if (fidCtx->recycle_cpt != rozofs_get_recycle_from_fid(fid)) {
	/*
	** Update FID context
	*/
        fidCtx->recycle_cpt = rozofs_get_recycle_from_fid(fid);
	/*
	** Update disk
	*/	
	int storage_slice = rozofs_storage_fid_slice(fid);
	storage_truncate_recycle(st,device,storage_slice,spare,fid,&file_hdr);
      }   
   
    }
    else {    
      /*
      ** FID context do not contain valid distribution. Read it from disk.
      */    
      read_hdr_res = storage_read_header_file(
              st,       // cid/sid context
              fid,      // FID we are looking for
	      spare,    // Whether the storage is spare for this FID
	      &file_hdr,// Returned header file content
              1 );      // Update header file when not the same recycling value

      /*
      ** File is unreadable
      */
      if (read_hdr_res == STORAGE_READ_HDR_ERRORS) {
	return -1;
      }

      /*
      ** The file has to be created
      */    
      if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) {
	rewrite_file_hdr = 1; // Header files will have to be written back to disk
	/*
	** Initialize chunk distribution
	*/
	memset(device,ROZOFS_EMPTY_CHUNK,ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);
      }
      
      /*
      ** Update FID context, to make it match disk as well as requested FID.
      */
      fidCtx->recycle_cpt = rozofs_get_recycle_from_fid(fid);      
    }  
    
    /*
    ** Let's process to the truncate at the requested size 
    ** using information read from disk
    */  
   
    /*
    ** In case of a file expension through truncate, let's set the previous 
    ** chunks to empty when they where EOF
    */
    for (chunk_idx=0; chunk_idx<chunk; chunk_idx++) {
      if (device[chunk_idx] == ROZOFS_EOF_CHUNK) {
        rewrite_file_hdr = 1;// Header files will have to be re-written to disk
        device[chunk_idx] = ROZOFS_EMPTY_CHUNK;
      }
    }
     
    /*
    ** We may allocate a device for the current truncated chunk
    */ 
    if ((device[chunk] == ROZOFS_EOF_CHUNK)||(device[chunk] == ROZOFS_EMPTY_CHUNK)) {
      rewrite_file_hdr = 1;// Header files will have to be re-written to disk    
      device[chunk] = storio_device_mapping_allocate_device(st);
      open_flags = ROZOFS_ST_BINS_FILE_FLAG; // File should be created
    }
    else {
      open_flags = ROZOFS_ST_NO_CREATE_FILE_FLAG; // File must already exist
    }
    
    // Build the chunk file name
    map_result = storage_dev_map_distribution_write(st, device, chunk, bsize, 
                                        	    fid, layout, dist_set, 
						    spare, path, 0, &file_hdr);
    if (map_result == MAP_FAILURE) {
      goto out;      
    }   

    // Open bins file
    fd = open(path, open_flags, ROZOFS_ST_BINS_FILE_MODE);
    if (fd < 0) {
        storio_fid_error(fid, device[chunk], chunk, bid, last_seg,"open truncate"); 		        
	storage_error_on_device(st,device[chunk]);  				    
        severe("open failed (%s) : %s", path, strerror(errno));
        goto out;
    }


    /*
    ** Retrieve the projection size in the message 
    ** and the projection size on disk
    */
    storage_get_projid_size(spare, proj_id, layout, bsize,
                            &rozofs_msg_psize, &rozofs_disk_psize); 
	       
    // Compute the offset from the truncate
    bid_truncate = bid;
    if (last_seg!= 0) bid_truncate+=1;
    bins_file_offset = bid_truncate * rozofs_disk_psize;
    status = ftruncate(fd, bins_file_offset);
    if (status < 0) goto out;
    
    /*
    ** When the truncate occurs in the middle of a block, it is either
    ** a shortening of the block or a an extension of the file.
    ** When extending the file only the header of the block is written 
    ** to reflect the new size. 
    ** In case of a shortening the whole block to write is given in the
    ** request
    */
    if (last_seg!= 0) {
	
      bins_file_offset = bid * rozofs_disk_psize;

      /*
      ** Rewrite the whole given data block 
      */
      if (length_to_write!= 0)
      {

        length_to_write = rozofs_disk_psize;
	
	/*
	** generate the crc32c for each projection block
	*/
	uint32_t crc32 = fid2crc32((uint32_t *)fid)+bid;
	storio_gen_crc32(data,1,rozofs_disk_psize,crc32);	
	
	nb_write = pwrite(fd, data, length_to_write, bins_file_offset);
	if (nb_write != length_to_write) {
            status = -1;
            storio_fid_error(fid, device[chunk], chunk, bid, last_seg,"write truncate"); 		    
            severe("pwrite failed on last segment: %s", strerror(errno));
	    storage_error_on_device(st,device[chunk]); 
	    // A fault probably localized to this FID is detected   
	    *is_fid_faulty = 1;  	     				    
            goto out;
	}
      
      }
      else {
      
        // Write the block header
        rozofs_stor_bins_hdr_t bins_hdr;  
	bins_hdr.s.timestamp        = last_timestamp;
	bins_hdr.s.effective_length = last_seg;
	bins_hdr.s.projection_id    = proj_id;
	bins_hdr.s.version          = version;

	nb_write = pwrite(fd, &bins_hdr, sizeof(bins_hdr), bins_file_offset);
	if (nb_write != sizeof(bins_hdr)) {
            storio_fid_error(fid, device[chunk], chunk, bid, last_seg,"write hdr truncate"); 	
            severe("pwrite failed on last segment header : %s", strerror(errno));
	    storage_error_on_device(st,device[chunk]); 
	    // A fault probably localized to this FID is detected   
	    *is_fid_faulty = 1;  	     				    
            goto out;
        }   
        
        // Write the block footer
	bins_file_offset += (sizeof(rozofs_stor_bins_hdr_t) 
	        + rozofs_get_psizes(layout,bsize,proj_id) * sizeof (bin_t));
	nb_write = pwrite(fd, &last_timestamp, sizeof(last_timestamp), bins_file_offset);
	if (nb_write != sizeof(last_timestamp)) {
            storio_fid_error(fid, device[chunk], chunk, bid, last_seg,"write foot truncate"); 	
            severe("pwrite failed on last segment footer : %s", strerror(errno));
	    storage_error_on_device(st,device[chunk]);  				    
	    // A fault probably localized to this FID is detected   
	    *is_fid_faulty = 1;  
            goto out;
        }   	  
      }
    } 
    

    /*
    ** Remove the extra chunks
    */
    for (chunk_idx=(chunk+1); chunk_idx<ROZOFS_STORAGE_MAX_CHUNK_PER_FILE; chunk_idx++) {

      if (device[chunk_idx] == ROZOFS_EOF_CHUNK) {
        continue;
      }
      
      if (device[chunk_idx] == ROZOFS_EMPTY_CHUNK) {
        rewrite_file_hdr = 1;      
        device[chunk_idx] = ROZOFS_EOF_CHUNK;
	continue;
      }
      
      storage_rm_data_chunk(st, device[chunk_idx], fid, spare, chunk_idx,1/*errlog*/);
      rewrite_file_hdr = 1;            
      device[chunk_idx] = ROZOFS_EOF_CHUNK;
    } 
    
    /* 
    ** Rewrite file header on disk
    */   
    if (rewrite_file_hdr) {
      dbg("%s","truncate rewrite file header");
      result = storage_write_all_header_files(st, fid, spare, &file_hdr);        
      /*
      ** Failure on every write operation
      */ 
      if (result == 0) goto out;
    }
       
    /*
    ** Update device array in FID cache from header file
    */ 
    memcpy(fidCtx->device,device,ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);
    status = 0;
    
out:

    if (fd != -1) close(fd);
    return status;
}

int storage_rm_chunk(storage_t * st, uint8_t * device, 
                     uint8_t layout, uint8_t bsize, uint8_t spare, 
		     sid_t * dist_set, fid_t fid, 
		     uint8_t chunk, int * is_fid_faulty) {
    rozofs_stor_bins_file_hdr_t file_hdr;

    dbg("%d/%d rm chunk %d : ", st->cid, st->sid, chunk);

    // No specific fault on this FID detected
    *is_fid_faulty = 0;  
    
    /*
    ** When device array is not given, one has to read the header file on disk
    */
    if (device[0] == ROZOFS_UNKNOWN_CHUNK) {
      STORAGE_READ_HDR_RESULT_E read_hdr_res;  
      
      read_hdr_res = storage_read_header_file(
              st,       // cid/sid context
              fid,      // FID we are looking for
	      spare,    // Whether the storage is spare for this FID
	      &file_hdr,// Returned header file content
              0 );      // do not update header file when not the same recycling value

      /*
      ** Header files are unreadable
      */
      if (read_hdr_res == STORAGE_READ_HDR_ERRORS) {
	*is_fid_faulty = 1;
	errno = EIO; 
	return -1;
      }
      
      /*
      ** Header files do not exist
      */      
      if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) {
        return 0;  
      } 

      /* 
      ** The header file has been read
      */
      memcpy(device,file_hdr.v0.device,ROZOFS_STORAGE_MAX_CHUNK_PER_FILE);
    } 
    else {
      file_hdr.v0.layout = layout;
      file_hdr.v0.bsize  = bsize;  ///< Block size as defined in enum ROZOFS_BSIZE_E
      memcpy(file_hdr.v0.fid, fid, sizeof(fid_t));
      memcpy(file_hdr.v0.dist_set_current, dist_set, sizeof(sid_t)*ROZOFS_SAFE_MAX);
    }
    
         
    /*
    ** We are trying to read after the end of file
    */				     
    if (device[chunk] == ROZOFS_EOF_CHUNK) {
      return 0;
    }

    /*
    ** This chunk is a whole
    */      
    if(device[chunk] == ROZOFS_EMPTY_CHUNK) {
      return 0;
    }
    
    /*
    ** Remove data chunk
    */
    storage_rm_data_chunk(st, device[chunk], fid, spare, chunk, 0 /* No errlog*/) ;
    
    // Last chunk
    if ((chunk+1) >= ROZOFS_STORAGE_MAX_CHUNK_PER_FILE) {
      device[chunk] = ROZOFS_EOF_CHUNK;
    }
    // Next chunk is end of file
    else if (device[chunk+1] == ROZOFS_EOF_CHUNK) {  
      device[chunk] = ROZOFS_EOF_CHUNK;
    }
    // Next chunk is not end of file
    else {
      device[chunk] = ROZOFS_EMPTY_CHUNK;
    }
    
    /*
    ** Chunk is now EOF. Are the previous chunks empty ?
    */ 
    while (device[chunk] == ROZOFS_EOF_CHUNK) {
      /*
      ** The file is totaly empty
      */
      if (chunk == 0) {
        storage_dev_map_distribution_remove(st, fid, spare);
	return 0;
      }
      
      chunk--;
      if (device[chunk] == ROZOFS_EMPTY_CHUNK) {
        device[chunk] = ROZOFS_EOF_CHUNK;
      }
    }
    
    /*
    ** Re-write distibution
    */
    memcpy(file_hdr.v0.device,device,sizeof(file_hdr.v0.device));
    storage_write_all_header_files(st, fid, spare, &file_hdr);        
    return 0;
}
int storage_rm_file(storage_t * st, fid_t fid) {
    uint8_t spare = 0;
    STORAGE_READ_HDR_RESULT_E read_hdr_res;
    int chunk;
    rozofs_stor_bins_file_hdr_t file_hdr;


    // For spare and no spare
    for (spare = 0; spare < 2; spare++) {

      /*
      ** When no device id is given as input, let's read the header file 
      */      
      read_hdr_res = storage_read_header_file(
              st,       // cid/sid context
              fid,      // FID we are looking for
	      spare,    // Whether the storage is spare for this FID
	      &file_hdr,// Returned header file content
              0 );      // Do not update header file when not the same recycling value

      /*
      ** File does not exist or is unreadable
      */
      if ((read_hdr_res != STORAGE_READ_HDR_OK)&&(read_hdr_res != STORAGE_READ_HDR_OTHER_RECYCLING_COUNTER)) {
	continue;
      }
      
      for (chunk=0; chunk<ROZOFS_STORAGE_MAX_CHUNK_PER_FILE; chunk++) {
      
        if (file_hdr.v0.device[chunk] == ROZOFS_EOF_CHUNK) {
	  break;
	}
	
	if (file_hdr.v0.device[chunk] == ROZOFS_EMPTY_CHUNK) {
	  continue;
	}

	/*
	** Remove data chunk
	*/
	storage_rm_data_chunk(st, file_hdr.v0.device[chunk], fid, spare, chunk, 0 /* No errlog*/);
      }

      // It's not possible for one storage to store one bins file
      // in directories spare and no spare.
      storage_dev_map_distribution_remove(st, fid, spare);
      return 0;
               
    }
    return 0;
} 
int storage_rm2_file(storage_t * st, fid_t fid, uint8_t spare) {
 STORAGE_READ_HDR_RESULT_E read_hdr_res;
 int chunk;
 rozofs_stor_bins_file_hdr_t file_hdr;


  /*
  ** Let's read the header file 
  */      
  read_hdr_res = storage_read_header_file(
          st,       // cid/sid context
          fid,      // FID we are looking for
	  spare,    // Whether the storage is spare for this FID
	  &file_hdr,// Returned header file content
          0 );      // Update header file when not the same recycling value


  if (read_hdr_res == STORAGE_READ_HDR_ERRORS) {
    /*
    ** Error that prevents the file deletion
    */
    return -1;
  }
  
  if (read_hdr_res == STORAGE_READ_HDR_NOT_FOUND) {
    /*
    ** File already deleted
    */
    return  0;
  }
    
  /*
  ** Delete every chunk
  */
  for (chunk=0; chunk<ROZOFS_STORAGE_MAX_CHUNK_PER_FILE; chunk++) {

    if (file_hdr.v0.device[chunk] == ROZOFS_EOF_CHUNK) {
      break;
    }

    if (file_hdr.v0.device[chunk] == ROZOFS_EMPTY_CHUNK) {
      continue;
    }

    /*
    ** Remove data chunk
    */
    storage_rm_data_chunk(st, file_hdr.v0.device[chunk], fid, spare, chunk, 0 /* No errlog*/);
  }

  storage_dev_map_distribution_remove(st, fid, spare);
  return 0;               
} 

bins_file_rebuild_t ** storage_list_bins_file(storage_t * st, sid_t sid, uint8_t device_id, 
                                              uint8_t spare, uint16_t slice, uint64_t * cookie,
        				      bins_file_rebuild_t ** children, uint8_t * eof,
        				      uint64_t * current_files_nb) {
    int i = 0;
    char path[FILENAME_MAX];
    DIR *dp = NULL;
    struct dirent *ep = NULL;
    bins_file_rebuild_t **iterator = children;
    rozofs_stor_bins_file_hdr_t file_hdr;
    int                         fd;
    int                         nb_read;
    int                         sid_idx;
    int                         safe;

    DEBUG_FUNCTION;
        
    /*
    ** Build the directory path
    */
    storage_build_hdr_path(path, st->root, device_id, spare, slice);
    
     
    // Open directory
    if (!(dp = opendir(path)))
        goto out;

    // Step to the cookie index
    if (*cookie != 0) {
      seekdir(dp, *cookie);
    }

    // Readdir first time
    ep = readdir(dp);


    // The current nb. of bins files in the list
    i = *current_files_nb;

    // Readdir the next entries
    while (ep && i < MAX_REBUILD_ENTRIES) {
    
        if ((strcmp(ep->d_name,".") != 0) && (strcmp(ep->d_name,"..") != 0)) {      

            // Read the file
            storage_build_hdr_path(path, st->root, device_id, spare, slice);
            strcat(path,ep->d_name);

	    fd = open(path, ROZOFS_ST_NO_CREATE_FILE_FLAG, ROZOFS_ST_BINS_FILE_MODE);
	    if (fd < 0) {
	       severe("open(%s) %s", path, strerror(errno));
               // Readdir for next entry
               ep = readdir(dp);	       
	       continue;
            }
            nb_read = pread(fd, &file_hdr, sizeof(file_hdr), 0);
	    close(fd);	    

            // What to do with such an error ?
	    if (nb_read != sizeof(file_hdr)) {
	       severe("nb_read %d vs %d %s", nb_read, (int) sizeof(file_hdr), path);
               // Readdir for next entry
               ep = readdir(dp);     
	       continue;
            }
	    // Check the requested sid is in the distribution
	    safe = rozofs_get_rozofs_safe(file_hdr.v0.layout);
	    for (sid_idx=0; sid_idx<safe; sid_idx++) {
	      if (file_hdr.v0.dist_set_current[sid_idx] == sid) break;
	    }
	    if (sid_idx == safe) {
               // Readdir for next entry
               ep = readdir(dp);	       
	       continue;
            }	    

            // Alloc a new bins_file_rebuild_t
            *iterator = xmalloc(sizeof (bins_file_rebuild_t)); // XXX FREE ?
            // Copy FID
            //rozofs_uuid_parse(ep->d_name, (*iterator)->fid);
	    memcpy((*iterator)->fid,file_hdr.v0.fid,sizeof(fid_t));
            // Copy current dist_set
            memcpy((*iterator)->dist_set_current, file_hdr.v0.dist_set_current,
                    sizeof (sid_t) * ROZOFS_SAFE_MAX);
            // Copy layout
            (*iterator)->layout = file_hdr.v0.layout;
            (*iterator)->bsize = file_hdr.v0.bsize;

            // Go to next entry
            iterator = &(*iterator)->next;

            // Increment the current nb. of bins files in the list
            i++;
        }
	
        // Readdir for next entry
        if (i < MAX_REBUILD_ENTRIES) {
          ep = readdir(dp);
        }  
    }

    // Update current nb. of bins files in the list
    *current_files_nb = i;

    if (ep) {
        // It's not the EOF
        *eof = 0;
	// Save where we are
        *cookie = telldir(dp);
    } else {
        *eof = 1;
    }

    // Close directory
    if (closedir(dp) == -1)
        goto out;

    *iterator = NULL;
out:
    return iterator;
}

int storage_list_bins_files_to_rebuild(storage_t * st, sid_t sid, uint8_t * device_id,
        uint8_t * spare, uint16_t * slice, uint64_t * cookie,
        bins_file_rebuild_t ** children, uint8_t * eof) {

    int status = -1;
    uint8_t spare_it = 0;
    uint64_t current_files_nb = 0;
    bins_file_rebuild_t **iterator = NULL;
    uint8_t device_it = 0;
    uint16_t slice_it = 0;

    DEBUG_FUNCTION;

    // Use iterator
    iterator = children;

    device_it = *device_id;
    spare_it  = *spare;
    slice_it  = *slice;
    
    // Loop on all the devices
    for (; device_it < st->device_number;device_it++,spare_it=0) {

        // For spare and no spare
        for (; spare_it < 2; spare_it++,slice_it=0) {
	
            // For slice
            for (; slice_it < (common_config.storio_slice_number); slice_it++) {

        	// Build path directory for this layout and this spare type
        	char path[FILENAME_MAX];
        	storage_build_hdr_path(path, st->root, device_it, spare_it, slice_it);

        	// Go to this directory
        	if (chdir(path) != 0)
                    continue;

                // List the bins files for this specific directory
                if ((iterator = storage_list_bins_file(st, sid, device_it, spare_it, slice_it, 
		                                       cookie, iterator, eof,
                                                       &current_files_nb)) == NULL) {
                    severe("storage_list_bins_file failed: %s\n",
                            strerror(errno));
                    continue;
                }
		

                // Check if EOF
                if (0 == *eof) {
                    status = 0;
		            *device_id = device_it;
                    *spare = spare_it;
                    *slice = slice_it;
                    goto out;
                } else {
                    *cookie = 0;
                }
            }
	    }    
    }
    *eof = 1;
    status = 0;

out:

    return status;
}

