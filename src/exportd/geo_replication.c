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

#include <fcntl.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <inttypes.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/param.h>
#include <rozofs/common/log.h>
#include <rozofs/core/uma_dbg_api.h>
#include "export.h"
#include "geo_replication.h"
#include "geo_profiler.h"

/**
*  pointers table of the context associated with the eid: MAX is EXPGW_EID_MAX_IDX (see rozofs.h for details)
*/
int geo_rep_srv_ctx_table_init_done = 0;
geo_rep_srv_ctx_t  *geo_rep_srv_ctx_table[EXPORT_GEO_MAX_CTX][EXPGW_EID_MAX_IDX+1];


/*
**_________________________________________________________________________
*      PUBLIC FUNCTIONS
**_________________________________________________________________________
*/


#define SHOW_GEO_PROFILER_PROBE(probe) if (prof->probe[P_COUNT]) \
                    pChar += sprintf(pChar," %-24s | %15"PRIu64" | %15"PRIu64" | %15"PRIu64" | %9"PRIu64" | %18"PRIu64" |\n",\
                    #probe,\
                    prof->probe[GEO_IDX_COUNT],\
                    prof->probe[GEO_IDX_TMO],\
                    prof->probe[GEO_IDX_ERR],\
                    prof->probe[GEO_IDX_COUNT]?prof->probe[GEO_IDX_TIME]/prof->probe[GEO_IDX_COUNT]:0,\
                    prof->probe[GEO_IDX_TIME]);


char * show_geo_profiler_one(char * pChar, uint32_t eid) {
    geo_one_profiler_t * prof;   

    if (eid>EXPGW_EID_MAX_IDX) return pChar;
    
    prof = geo_profiler[eid];
    if (prof == NULL) return pChar;
        

    // Compute uptime for storaged process
    pChar +=  sprintf(pChar, "_______________________ EID = %d _______________________ \n",eid);
    pChar += sprintf(pChar, "   procedure              |     count       |     tmo         |     err         |  time(us) | cumulated time(us) |\n");
    pChar += sprintf(pChar, "--------------------------+-----------------+-----------------+-----------------+-----------+--------------------+\n");
    SHOW_GEO_PROFILER_PROBE(geo_sync_req);
    SHOW_GEO_PROFILER_PROBE(geo_sync_get_next_req);
    SHOW_GEO_PROFILER_PROBE(geo_sync_delete_req);
    SHOW_GEO_PROFILER_PROBE(geo_sync_close_req);

    return pChar;
}


static char * show_geo_profiler_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"geo_profiler reset [ <eid> ] : reset statistics\n");
  pChar += sprintf(pChar,"geo_profiler [ <eid> ]       : display statistics\n");  
  return pChar; 
}
void show_geo_profiler(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    uint32_t eid;
    int ret;

    if (argv[1] == NULL) {
      for (eid=0; eid <= EXPGW_EID_MAX_IDX; eid++) 
        pChar = show_geo_profiler_one(pChar,eid);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
      return;
    }

    if (strcmp(argv[1],"reset")==0) {

      if (argv[2] == NULL) {
	geo_profiler_reset_all();
	uma_dbg_send(tcpRef, bufRef, TRUE, "Done\n");   
	return;	 
      }
      	     
      ret = sscanf(argv[2], "%d", &eid);
      if (ret != 1) {
        show_geo_profiler_help(pChar);	
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;   
      }
      geo_profiler_reset_one(eid);
      uma_dbg_send(tcpRef, bufRef, TRUE, "Done\n");   	  
      return;
    }

    ret = sscanf(argv[1], "%d", &eid);
    if (ret != 1) {
      show_geo_profiler_help(pChar);	
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
      return;   
    }
    pChar = show_geo_profiler_one(pChar,eid);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
    return;
}

/*
**____________________________________________________________________________
*/
/**
*  RECYCLE INDEX FILE SERVICES

*/
/*
**____________________________________________________________________________
*/
/*
**____________________________________________________________________________
*/
/*
** create the recycle file index

   @param ctx_p : pointer to replication context
   
   @retval 0: on success
   @retval -1 on error (see errno for details )
*/
int geo_rep_create_file_index_recycle(geo_rep_srv_ctx_t *ctx_p)
{

   char path[ROZOFS_PATH_MAX];
   int fd = -1;
   
   GEO_REP_RECYCLE_BUILD_PATH(GEO_FILE_IDX);
   fd = open(path, O_RDWR | O_CREAT | O_NOATIME, S_IRWXU);
   if (fd < 0)
   {
     severe(" cannot create index file %s for geo-replication error %s",path,strerror(errno));
     return -1;
   }
   memset(&ctx_p->geo_rep_main_recycle_file,0,sizeof(geo_rep_main_file_t));
   errno = 0;
   if (pwrite(fd,&ctx_p->geo_rep_main_recycle_file,sizeof(geo_rep_main_file_t),0) != sizeof(geo_rep_main_file_t))
   {
     severe(" cannot write index file %s for geo-replication error %s",path,strerror(errno));
   }
  
   if (fd!= -1)
   {
    close(fd);
   }
   return 0;

}

/*
**____________________________________________________________________________
*/
/*
** update the last_index of the index file

   @param ctx_p : pointer to replication context
   
   @retval 0: on success
   @retval -1 on error (see errno for details )
*/
int geo_rep_disk_update_last_index_recycle(geo_rep_srv_ctx_t *ctx_p)
{

   char path[ROZOFS_PATH_MAX];
   int fd = -1;
   
   GEO_REP_RECYCLE_BUILD_PATH(GEO_FILE_IDX);
   fd = open(path, O_RDWR | O_NOATIME, S_IRWXU);
   if (fd < 0)
   {
     severe(" cannot open index file %s for geo-replication error %s",path,strerror(errno));
     return -1;
   }
   errno = 0;
   if (pwrite(fd,&ctx_p->geo_rep_main_recycle_file.last_index,sizeof(uint64_t),8) != 8)
   {
     severe(" cannot write index file %s for geo-replication error %s",path,strerror(errno));
   }
  
   if (fd!= -1)
   {
    close(fd);
   }
   return 0;

}

/*
**____________________________________________________________________________
*/
/*
** update the first_index of the index file

   @param ctx_p : pointer to replication context
   
   @retval 0: on success
   @retval -1 on error (see errno for details )
*/
int geo_rep_disk_update_first_index_recycle(geo_rep_srv_ctx_t *ctx_p)
{

   char path[ROZOFS_PATH_MAX];
   int fd = -1;
   
   GEO_REP_RECYCLE_BUILD_PATH(GEO_FILE_IDX);
   fd = open(path, O_RDWR | O_NOATIME, S_IRWXU);
   if (fd < 0)
   {
     severe(" cannot open index file %s for geo-replication error %s",path,strerror(errno));
     return -1;
   }
   errno = 0;
   if (pwrite(fd,&ctx_p->geo_rep_main_recycle_file.first_index,sizeof(uint64_t),0) != 8)
   {
     severe(" cannot write index file %s for geo-replication error %s",path,strerror(errno));
   }
  
   if (fd!= -1)
   {
    close(fd);
   }
   return 0;

}

/*
**____________________________________________________________________________
*/
/*
** read the index file

   @param ctx_p : pointer to replication context
   
   @retval 0: on success
   @retval -1 on error (see errno for details )
*/
int geo_rep_disk_read_index_file_recycle(geo_rep_srv_ctx_t *ctx_p)
{

   char path[ROZOFS_PATH_MAX];
   int fd = -1;
   int status = -1;
   
   GEO_REP_RECYCLE_BUILD_PATH(GEO_FILE_IDX);
   fd = open(path, O_RDWR | O_NOATIME, S_IRWXU);
   if (fd < 0)
   {
     severe(" cannot open index file %s for geo-replication error %s",path,strerror(errno));
     return status;
   }
   errno = 0;
   if (pread(fd,&ctx_p->geo_rep_main_recycle_file,sizeof(geo_rep_main_file_t),0) != sizeof(geo_rep_main_file_t))
   {
     severe(" cannot read index file %s for geo-replication error %s",path,strerror(errno));
   }
   else
   {
     status = 0;
   }  
   if (fd!= -1)
   {
    close(fd);
   }
   return status;

}
/*
**____________________________________________________________________________
*/
/**
*  attempt to open or create the file index file associated with an export

   @param ctx_p : pointer to replication context
   
   @retval 0: on success
   @retval -1 on error (see errno for details )
*/
int geo_rep_resolve_entry_recycle(geo_rep_srv_ctx_t *ctx_p) 
{
   char path[ROZOFS_PATH_MAX];
   
   GEO_REP_RECYCLE_BUILD_PATH_NONAME;
   if (access(path, F_OK) == -1) 
   {
     if (errno == ENOENT) 
     {
       /*
       ** it is the fisrt time we access to the slice
       **  we need to create the level 1 directory and the 
       ** timestamp file
       */
       if (mkdir(path, S_IRUSR | S_IWUSR | S_IXUSR) != 0)
       {
         severe("mkdir (%s): %s",path,strerror(errno));
         return -1;
       } 
       geo_rep_create_file_index_recycle(ctx_p);               
       return 0;         
     } 
     return -1;
  }
  return 0;
}

/*
**____________________________________________________________________________
*/
/**
*  MAIN INDEX FILE SERVICES

*/
/*
**____________________________________________________________________________
*/

/*
**____________________________________________________________________________
*/
/*
** create the file index

   @param ctx_p : pointer to replication context
   
   @retval 0: on success
   @retval -1 on error (see errno for details )
*/
int geo_rep_create_file_index(geo_rep_srv_ctx_t *ctx_p)
{

   char path[ROZOFS_PATH_MAX];
   int fd = -1;
   
   GEO_REP_BUILD_PATH(GEO_FILE_IDX);
   fd = open(path, O_RDWR | O_CREAT | O_NOATIME, S_IRWXU);
   if (fd < 0)
   {
     severe(" cannot create index file %s for geo-replication error %s",path,strerror(errno));
     return -1;
   }
   memset(&ctx_p->geo_rep_main_file,0,sizeof(geo_rep_main_file_t));
   errno = 0;
   if (pwrite(fd,&ctx_p->geo_rep_main_file,sizeof(geo_rep_main_file_t),0) != sizeof(geo_rep_main_file_t))
   {
     severe(" cannot write index file %s for geo-replication error %s",path,strerror(errno));
   }
  
   if (fd!= -1)
   {
    close(fd);
   }
   return 0;

}

/*
**____________________________________________________________________________
*/
/*
** update the last_index of the index file

   @param ctx_p : pointer to replication context
   
   @retval 0: on success
   @retval -1 on error (see errno for details )
*/
int geo_rep_disk_update_last_index(geo_rep_srv_ctx_t *ctx_p)
{

   char path[ROZOFS_PATH_MAX];
   int fd = -1;
   
   GEO_REP_BUILD_PATH(GEO_FILE_IDX);
   fd = open(path, O_RDWR | O_NOATIME, S_IRWXU);
   if (fd < 0)
   {
     severe(" cannot open index file %s for geo-replication error %s",path,strerror(errno));
     return -1;
   }
   errno = 0;
   if (pwrite(fd,&ctx_p->geo_rep_main_file.last_index,sizeof(uint64_t),8) != 8)
   {
     severe(" cannot write index file %s for geo-replication error %s",path,strerror(errno));
   }
  
   if (fd!= -1)
   {
    close(fd);
   }
   return 0;

}

/*
**____________________________________________________________________________
*/
/*
** update the first_index of the index file

   @param ctx_p : pointer to replication context
   
   @retval 0: on success
   @retval -1 on error (see errno for details )
*/
int geo_rep_disk_update_first_index(geo_rep_srv_ctx_t *ctx_p)
{

   char path[ROZOFS_PATH_MAX];
   int fd = -1;
   
   GEO_REP_BUILD_PATH(GEO_FILE_IDX);
   fd = open(path, O_RDWR | O_NOATIME, S_IRWXU);
   if (fd < 0)
   {
     severe(" cannot open index file %s for geo-replication error %s",path,strerror(errno));
     return -1;
   }
   errno = 0;
   if (pwrite(fd,&ctx_p->geo_rep_main_file.first_index,sizeof(uint64_t),0) != 8)
   {
     severe(" cannot write index file %s for geo-replication error %s",path,strerror(errno));
   }
  
   if (fd!= -1)
   {
    close(fd);
   }
   return 0;

}

/*
**____________________________________________________________________________
*/
/*
** read the index file

   @param ctx_p : pointer to replication context
   
   @retval 0: on success
   @retval -1 on error (see errno for details )
*/
int geo_rep_disk_read_index_file(geo_rep_srv_ctx_t *ctx_p)
{

   char path[ROZOFS_PATH_MAX];
   int fd = -1;
   int status = -1;
   
   GEO_REP_BUILD_PATH(GEO_FILE_IDX);
   fd = open(path, O_RDWR | O_NOATIME, S_IRWXU);
   if (fd < 0)
   {
     severe(" cannot open index file %s for geo-replication error %s",path,strerror(errno));
     return status;
   }
   errno = 0;
   if (pread(fd,&ctx_p->geo_rep_main_file,sizeof(geo_rep_main_file_t),0) != sizeof(geo_rep_main_file_t))
   {
     severe(" cannot read index file %s for geo-replication error %s",path,strerror(errno));
   }
   else
   {
     status = 0;
   }  
   if (fd!= -1)
   {
    close(fd);
   }
   return status;

}
/*
**____________________________________________________________________________
*/
/**
*  attempt to open or create the file index file associated with an export

   @param ctx_p : pointer to replication context
   
   @retval 0: on success
   @retval -1 on error (see errno for details )
*/
int geo_rep_resolve_entry(geo_rep_srv_ctx_t *ctx_p) 
{
   char path[ROZOFS_PATH_MAX];
   
   GEO_REP_BUILD_PATH_NONAME;
   if (access(path, F_OK) == -1) 
   {
     if (errno == ENOENT) 
     {
       /*
       ** it is the fisrt time we access to the slice
       **  we need to create the level 1 directory and the 
       ** timestamp file
       */
       if (mkdir(path, S_IRUSR | S_IWUSR | S_IXUSR) != 0)
       {
         severe("mkdir (%s): %s",path,strerror(errno));
         return -1;
       } 
       geo_rep_create_file_index(ctx_p);               
       return 0;         
     } 
     return -1;
  }
  return 0;
}
/*
**____________________________________________________________________________
*/
static inline unsigned int fid_hash(void *key) {
    uint16_t hash = 0;
    uint16_t *p16;
    p16= key;
    hash = *p16;
//    for (c = key; c != key + 16; c++)
//        hash = *c + (hash << 11) + (hash << 3) - hash;
    return (unsigned int)hash;
}

/*
**____________________________________________________________________________
*/
/**
*  re-init of the hash table of the replicator

   @param ctx_p : pointer to replication context
   
   @retval 0 on success
   @retval -1 on error (see errno for details
*/
int geo_rep_reinit(geo_rep_srv_ctx_t *ctx_p)
{
    if (ctx_p->geo_replication_enable == 0) return 0;
    if ((ctx_p->geo_fid_table_p== NULL) || (ctx_p->geo_hash_table_p== NULL))
    {
      return -1;
    }
    memset(ctx_p->geo_hash_table_p,-1,sizeof(geo_hash_entry_t)*GEO_MAX_HASH_SZ);
    ctx_p->geo_first_idx = 0;
    
    return 0;
}
/*
**____________________________________________________________________________
*/
/**
* update the count of files that have been synced (geo-replication)

  @param ctx_p : pointer to replication context
  @param nb_files : number of files synced
 
 @retval none
*/
void geo_rep_udpate_synced_stats(geo_rep_srv_ctx_t *ctx_p,uint64_t nb_files)
{

   char path[ROZOFS_PATH_MAX];
   int fd = -1;
   size_t size;
   uint64_t cur_nb_file[2];
   
   cur_nb_file[0] = 0;

   GEO_REP_BUILD_PATH_FILE(GEO_FILE_STATS,0);
   if (access(path, F_OK) == -1) 
   {
     ctx_p->stats.access_stat_err++;
     return;          
   }
   fd = open(path, O_RDWR | O_CREAT | O_NOATIME, S_IRWXU);
   if (fd < 0)
   {
     ctx_p->stats.open_stat_err++;
     //severe(" cannot open fid file %s for geo-replication error %s",path,strerror(errno));
     return ;
   } 
   size = pread(fd,cur_nb_file,sizeof(uint64_t),8);
   if (size <0)
   {
     ctx_p->stats.read_stat_err++;  
     return;     
   }
   /*
   ** update the stats
   */
   cur_nb_file[0] += nb_files;   
   size = pwrite(fd,cur_nb_file,sizeof(uint64_t),8);
   if (size <0)
   {
     ctx_p->stats.write_stat_err++;  
   }
   close(fd); 
}
/*
**____________________________________________________________________________
*/
/**
* update the count of file waiting for geo-replication

  @param ctx_p : pointer to replication context
  @param nb_files : number of files to synchronize
 
 @retval none
*/
void geo_rep_udpate_pending_sync_stats(geo_rep_srv_ctx_t *ctx_p,uint64_t nb_files)
{

   char path[ROZOFS_PATH_MAX];
   int fd = -1;
   size_t size;
   uint64_t cur_nb_file[2];
   
   cur_nb_file[0] = 0;
   cur_nb_file[1] = 0;

   GEO_REP_BUILD_PATH_FILE(GEO_FILE_STATS,0);
   if (access(path, F_OK) == -1) 
   {
     if (errno == ENOENT)
     {
       /*
       ** create the file
       */
       fd = open(path, O_RDWR | O_CREAT | O_NOATIME, S_IRWXU);
       if (fd < 0)
       {
	 ctx_p->stats.open_stat_err++;
	 //severe(" cannot open fid file %s for geo-replication error %s",path,strerror(errno));
	 return ;
       } 
       size = pwrite(fd,cur_nb_file,sizeof(uint64_t)*2,0);
       if (size <0)
       {
	 ctx_p->stats.write_stat_err++; 
	 close(fd); 
	 return;     
       } 
       close(fd);      
     }
     else
     {
       ctx_p->stats.access_stat_err++;
       return;          
     }   
   }
   fd = open(path, O_RDWR | O_CREAT | O_NOATIME, S_IRWXU);
   if (fd < 0)
   {
     ctx_p->stats.open_stat_err++;
     //severe(" cannot open fid file %s for geo-replication error %s",path,strerror(errno));
     return ;
   } 
   size = pread(fd,cur_nb_file,sizeof(uint64_t),0);
   if (size <0)
   {
     ctx_p->stats.read_stat_err++;  
     return;     
   }
   /*
   ** update the stats
   */
   cur_nb_file[0] += nb_files;   
   size = pwrite(fd,cur_nb_file,sizeof(uint64_t),0);
   if (size <0)
   {
     ctx_p->stats.write_stat_err++;  
   }
   close(fd); 
}

/*
**____________________________________________________________________________
*/
/**
* Get the geo-replication current file statistics

  @param ctx_p : pointer to replication context
  @param nb_files_pending : array where file count is returned
  @param nb_files_synced : array where file count is returned
 
 @retval 0 succes
 @retval < 0 error (see errno fo details)
*/
int geo_rep_read_sync_stats(geo_rep_srv_ctx_t *ctx_p,uint64_t *nb_files_pending,uint64_t *nb_files_synced)
{

   char path[ROZOFS_PATH_MAX];
   int fd = -1;
   size_t size;
   uint64_t cur_nb_file[2];
   
   *nb_files_pending = 0;
   *nb_files_synced  = 0;

   GEO_REP_BUILD_PATH_FILE(GEO_FILE_STATS,0);
   if (access(path, F_OK) == -1) 
   { 
      ctx_p->stats.access_stat_err++;
      return -1;          
   }
   fd = open(path, O_RDWR | O_CREAT | O_NOATIME, S_IRWXU);
   if (fd < 0)
   {
     ctx_p->stats.open_stat_err++;
     //severe(" cannot open fid file %s for geo-replication error %s",path,strerror(errno));
     return -1;
   } 
   size = pread(fd,cur_nb_file,sizeof(uint64_t)*2,0);
   if (size <0)
   {
     ctx_p->stats.read_stat_err++;  
     return -1;     
   }
   /*
   ** update the stats
   */
   *nb_files_pending = cur_nb_file[0];
   *nb_files_synced  = cur_nb_file[1];
   close(fd);
   return 0; 
}
/*
**____________________________________________________________________________
*/
/**
*   Step to next index file

   @param ctx_p : pointer to replication context
  
  @retval 0: on success
  @retval -1 on error (see errno for details)
*/
int geo_rep_disk_next_index(geo_rep_srv_ctx_t *ctx_p) {
  int  nb_records = 0;
  char path[ROZOFS_PATH_MAX];
  struct stat buf;
   
  /*
  ** Get number of record in the last file
  */
  GEO_REP_BUILD_PATH_FILE(GEO_FILE,ctx_p->geo_rep_main_file.last_index);
  if (stat((const char *)path, &buf) < 0) {
    severe("stat failure for %s error %s",path,strerror(errno));   
    ctx_p->stats.stat_err++; 
  }
  else {
    if (buf.st_size > sizeof(uint64_t)) {
     nb_records = (buf.st_size - sizeof(uint64_t))/sizeof(geo_fid_entry_t);
    } 
  }

  /*
  ** update the statistics related to the number of file to synchronize
  */
  geo_rep_udpate_pending_sync_stats(ctx_p,(uint64_t)nb_records); 
  /*
  ** make the file available for remote side replication
  */
  ctx_p->geo_rep_main_file.last_index++; 
  ctx_p->file_idx_wr_pending = 0;
  geo_rep_disk_update_last_index(ctx_p); 
  return 0;  
}     
/*
**____________________________________________________________________________
*/
/**
*   Flush the geo replication memory chunk on disk

   @param ctx_p : pointer to replication context
   @param forced : when assert the current file is written on disk and global index is incremented by 1
  
  @retval 0: on success
  @retval -1 on error (see errno for details)
*/
int geo_rep_disk_flush(geo_rep_srv_ctx_t *ctx_p,int forced)
{
   char path[ROZOFS_PATH_MAX];
   int fd = -1;
   size_t size=0;
   struct stat buf;
   int    status = -1;
      
   if (ctx_p->geo_replication_enable == 0)
   {
     /*
     ** replication is not active
     */
     return 0;
   }   
   
   /*
   ** Get the lock
   */   
   if (pthread_rwlock_wrlock(&ctx_p->flush_lock) != 0) {
     severe("pthread_rwlock_wrlock %s", strerror(errno));
   }
     
   if (ctx_p->geo_first_idx != 0)
   {
     ctx_p->file_idx_wr_pending = 1;
     for (;;)
     {
       buf.st_size = 0;
       GEO_REP_BUILD_PATH_FILE(GEO_FILE,ctx_p->geo_rep_main_file.last_index);
       /*
       ** check the existence of the file
       */
       if (access(path, F_OK) == -1) 
       {
	 if (errno == ENOENT)
	 {
           /*
	   ** file need to be created
	   */
	   ctx_p->last_time_file_cr8 = time(NULL);
	   break;
	 }
	 ctx_p->stats.access_err++;
	 severe("geo_rep_disk_flush failure for %s error %s",path,strerror(errno));
	 goto out;
       }
       /*
       ** the file already exists,check the length of the file
       */
       if (stat((const char *)path, &buf) < 0)
       {
	 severe("stat failure for %s error %s",path,strerror(errno));   
	 ctx_p->stats.stat_err++;
	 ctx_p->geo_rep_main_file.last_index++; 
	 continue;  
       }
       /*
       ** check if it exceeds the max file length
       */
       if (buf.st_size > ctx_p->max_filesize)
       {
	 ctx_p->geo_rep_main_file.last_index++; 
	 continue; 
       }
       break;         
     }
     /*
     ** update the file index
     */
     geo_rep_disk_update_last_index(ctx_p);
     /*
     ** flush the memory on disk
     */
     size = ctx_p->geo_first_idx*sizeof(geo_fid_entry_t);
     fd = open(path, O_RDWR | O_CREAT | O_NOATIME| O_APPEND, S_IRWXU);
     if (fd < 0)
     {
       ctx_p->stats.open_err++;
       severe(" cannot open fid file %s for geo-replication error %s",path,strerror(errno));
       goto out;
     }
     while(1)
     {
       /*
       ** check if the file header need to be created
       */
       if (buf.st_size == 0)
       {
          uint64_t cr8time = time(NULL);
	 if (write(fd,&cr8time,sizeof(cr8time)) != sizeof(cr8time))
	 {
	   severe(" cannot write header of index file %s for geo-replication error %s",path,strerror(errno));
	   ctx_p->stats.write_err++;
	   break;
	 }  	

       }
       if (write(fd,ctx_p->geo_fid_table_p,size) != ctx_p->geo_first_idx*sizeof(geo_fid_entry_t))
       {
	 severe(" cannot write index file %s for geo-replication error %s",path,strerror(errno));
	 ctx_p->stats.write_err++;
       }
       break; 
     }     
     if (fd != -1) close(fd);
     ctx_p->stats.flush_count++;
   }
   /*
   ** check if we should move to the next index
   */
   uint64_t cur_time = time(NULL);
   if ((ctx_p->file_idx_wr_pending != 0) && ((ctx_p->max_filesize < (size+buf.st_size))|| 
       ((ctx_p->last_time_file_cr8+ctx_p->delay_next_file) < cur_time) ||
       (forced != 0)))
   {
     geo_rep_disk_next_index(ctx_p);
   }
  /*
  ** reinit of the hash table
  */
  geo_rep_reinit(ctx_p);
  status = 0;
    
out:

  /*
  ** Release the lock
  */
  if (pthread_rwlock_unlock(&ctx_p->flush_lock) != 0) {
    severe("pthread_rwlock_unlock %s", strerror(errno));
  } 
  
  return status;  
}
/*
**____________________________________________________________________________
*/
/**
*  insert a fid in the current replication array

   @param ctx_p : pointer to replication context
   @param : fid : fid of the file to update
   @param: offset_start : first byte
   @param: offset_last : last byte
   @param layout : layout of the file
   @param cid: cluster id
   @param sids: list of the storage identifier
   
   @retval 0 on success
   @retval -1 on error (see errno for details
*/
int geo_rep_insert_fid(geo_rep_srv_ctx_t *ctx_p,
                       fid_t fid,uint64_t off_start,uint64_t off_end,
		       uint8_t layout,cid_t cid,sid_t *sids_p)
{
    uint32_t hash;
    geo_hash_entry_t *hash_entry;
    geo_fid_entry_t *p;
    int found = 0;
    int i;
    int empty_idx;
    int entry_idx;
    
    if (ctx_p == NULL) return 0;
    if (ctx_p->geo_replication_enable == 0) return 0;
    /*
    ** get the hash associated with the fid
    */
    hash = fid_hash(fid);
    hash_entry = &ctx_p->geo_hash_table_p[(hash)%GEO_MAX_HASH_SZ];
    /*
    ** search for an exact match
    */
    found = 0;
    empty_idx = -1;
    for (i=0; i < GEO_MAX_COLL_ENTRY; i++)
    {
       if (hash_entry->entry[i] == 0xffff) 
       {
         if (empty_idx == -1)
	 {
	   empty_idx = i; 
	 }
         continue;
       }
       p = &ctx_p->geo_fid_table_p[hash_entry->entry[i]];
       if (memcmp(fid,p->fid,sizeof(fid_t)) == 0)
       {
         
         /*
	 ** entry is found
	 */
	 if ((0==off_start) && (0==off_end))
	 {
	   p->off_start = 0;
	   p->off_end   = 0;
           ctx_p->stats.delete_count++;
	 }
	 else
	 {
	   if (p->off_start > off_start) p->off_start = off_start;
	   if (p->off_end < off_end) p->off_end = off_end;
	 }
         ctx_p->stats.update_count++;
	 found = 1;
	 break;       
       }    
    }
    if (found==0)
    {
      /*
      ** not found, need to allocate a new index
      */
      if (ctx_p->geo_first_idx == 0)
      {
	ctx_p->last_time_flush = time(NULL);    
      }
      entry_idx = ctx_p->geo_first_idx++;

      if (empty_idx != -1)
      {
	hash_entry->entry[empty_idx] = entry_idx;
      }
      else
      {
       ctx_p->stats.coll_count++;
	hash_entry->entry[hash%GEO_MAX_COLL_ENTRY] = entry_idx;    
      }
      /*
      ** fill the entry
      */
      ctx_p->stats.insert_count++;
      p = &ctx_p->geo_fid_table_p[entry_idx];    
      memcpy(p->fid,fid,sizeof(fid_t));
      p->off_start = off_start;
      p->off_end = off_end;
      p->layout = layout;
      p->cid = cid;
      memcpy(p->sids,sids_p,sizeof(sid_t)*ROZOFS_SAFE_MAX);
    }
    /*
    ** check if the file must be flushed on disk
    */
    uint64_t cur_time = time(NULL);
#if 0
    severe("cur %llu dead-line %llu",(long long unsigned int)cur_time,
                                     (long long unsigned int)ctx_p->last_time_flush+ctx_p->delay);
#endif
    if ((ctx_p->geo_first_idx >= (GEO_MAX_ENTRIES-1))|| (ctx_p->last_time_flush+ctx_p->delay) < cur_time )
    {
      geo_rep_disk_flush(ctx_p,0);
    }
    return 0; 
}
/*
**____________________________________________________________________________
*/
/**
*  Restart from scratch the FID synchronization
   Every FID must be synchronized toward the other site

   @param ctx_p : pointer to replication context
   
   @retval 0 on success
   @retval -1 on error (see errno for details)
*/
int geo_rep_restart_from_scratch(geo_rep_srv_ctx_t *ctx_p) {
  char     cmd[256];
  char     path[256];
  int      idx;
  int      status=-1;
  
  /*
  ** Call geoRepList tool to build lists of FIDs to synchronize
  */
  GEO_REP_BUILD_PATH_NONAME;
  sprintf (cmd, "rozo_geoRepList -e %d -p %s -c %s",
           ctx_p->eid, path, export_get_config_file_path());   
  if (system(cmd)!=0) {};

  /*
  ** Get the lock
  */   
  if (pthread_rwlock_wrlock(&ctx_p->flush_lock) != 0) {
    severe("pthread_rwlock_wrlock %s", strerror(errno));
    return -1;
  }

  /*
  ** Check whether a file is pending
  */
  if (ctx_p->file_idx_wr_pending) {
    /*
    ** Finish this file and go to next index
    */
    geo_rep_disk_next_index(ctx_p);
  }  
  	   
  /*
  ** Loop on the result files and add them to geo replication list
  */
  idx = 1;
  while (1) {
  
    GEO_REP_BUILD_PATH_NONAME;
    sprintf(cmd,"%s/geoList.%d",path,idx);
    
    /*
    ** Check whether this result file exists
    */
    if (access(cmd,R_OK) != 0) {
      /*
      ** No more file
      */
      status = 0;
      break;
    }

    /*
    ** Build destination name
    */
    GEO_REP_BUILD_PATH_FILE(GEO_FILE,ctx_p->geo_rep_main_file.last_index);    

    /*
    ** Rename the file
    */
    if (rename(cmd,path)<0) {
      severe("rename(%s,%s) %s", cmd, path, strerror(errno));
      unlink(cmd);
      break;
    }

    /*
    ** Finish this file and go to next file
    */
    geo_rep_disk_next_index(ctx_p);
    
    /*
    ** Next result file
    */
    idx++;
  }
  
  /*
  ** Release the lock
  */
  if (pthread_rwlock_unlock(&ctx_p->flush_lock) != 0) {
    severe("pthread_rwlock_unlock %s", strerror(errno));
  }  
  
  return status;
}
/*
**____________________________________________________________________________
*/
/**
*  Thread that will build the list of FID to re synchronize

   @param ctx_p : pointer to replication context
   
   @retval 0 on success
   @retval -1 on error (see errno for details)
*/
static void *geo_rep_restart_from_scratch_thread(void *param) {
  char                name[32];
  geo_rep_srv_ctx_t * ctx_p = (geo_rep_srv_ctx_t *) param;
  int                 status=-1;

  sprintf(name,"geoScratch.eid%d.site%d", ctx_p->eid, ctx_p->site_id);
  uma_dbg_thread_add_self(name);
  
  info("%s started",name);
  
  pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
  status = geo_rep_restart_from_scratch(ctx_p);
  pthread_exit(&status);
}  
/*
**____________________________________________________________________________
*/
/**
*  Launch a thread that will build th whole list of FID to resynchronize
*  a whole export

   @param from_site : the source site of the synchronization
   @param eid       : the export identifier
   
   @retval 0 on success
   @retval -1 on error 
*/
int geo_rep_restart_from_scratch_launch_thread(int from_site, int eid) {
  pthread_t          thid;  
  geo_rep_srv_ctx_t *ctx_p=NULL;
  
  /*
  ** Retrieve and check the geo replication context
  */
  if ((from_site!= 0)&&(from_site!= 1)) {
    severe("Unexpected site number %d", from_site);
    return -1;
  }  

  if (eid>EXPGW_EID_MAX_IDX) {
    severe("Unexpected eid number %d", eid);
    return -1;
  }  

  ctx_p = geo_rep_srv_ctx_table[from_site][eid];

  if (ctx_p == NULL){
    severe("No georeplication context for eid number %d and site %d", eid, from_site);
    return -1;
  }  
  
  if (ctx_p->geo_replication_enable == 0) {
    severe("georeplication disabled for eid number %d and site %d", eid, from_site);
    return -1;
  }
          
  /*
  ** Start a thread for this task
  */    
  if (pthread_create(&thid, NULL, geo_rep_restart_from_scratch_thread, ctx_p) != 0) {
     severe("geo_rep_restart_from_scratch_launch_thread %s", strerror(errno));
     return -1;
  }
  return 0;
}     
/*
**____________________________________________________________________________
*/
/**
*  Rozodiag CLI to restart georeplication from scratch
   
   @retval 0 on success
   @retval -1 on error 
*/
void geo_rep_restart_from_scratch_dbg(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();
  int   site=-1;
  int   eid;
  

  /* 
  ** Parse command
  */
  if (argv[1] == NULL) {
    sprintf(pChar,"Missing site number\n");
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
    return;
  }
  if (sscanf(argv[1],"%d", &site) != 1) {
    sprintf(pChar,"Unexpected site number %s\n",argv[1]);  
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   	  
    return;
  }  
  if ((site!=0)&&(site!=1)){
    sprintf(pChar,"Bad number %d\n",site);    
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());  
    return;
  }
  
  /*
  ** Try each eid
  */
  for (eid=0; eid<EXPGW_EID_MAX_IDX; eid++) {
    if (geo_rep_srv_ctx_table[site][eid] != NULL) {
      if (geo_rep_restart_from_scratch_launch_thread(site,eid)==0) {
        pChar += sprintf(pChar,"eid %d started\n",eid);
      }
      else {
        pChar += sprintf(pChar,"Failed to start eid %d\n",eid);       	
      }
    }
  }
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());    
}
/*
**____________________________________________________________________________
*/
/**
* release of a geo replication context

   @param ctx_p : pointer to the replication context
   
   @retval none
*/
void geo_rep_ctx_release(geo_rep_srv_ctx_t *ctx_p)
{
  if (ctx_p == NULL) return;
  
  if (ctx_p->geo_fid_table_p != NULL)
  {
     free(ctx_p->geo_fid_table_p);
  }
  if (ctx_p->geo_hash_table_p != NULL)
  {
     free(ctx_p->geo_hash_table_p);
  }
  free(ctx_p);

}
/*
**____________________________________________________________________________
*/
/**
* geo-relication polling of one exportd

  @param ctx_p: pointer to the geo-replication context
  
  @retval: none
*/
void geo_replication_poll_one_exportd(geo_rep_srv_ctx_t *ctx_p)
{
  uint64_t cur_time = time(NULL);
  
  /*
  ** check if a flush is requested
  */
  if ((ctx_p->geo_first_idx >= (GEO_MAX_ENTRIES-1))|| (ctx_p->last_time_flush+ctx_p->delay) < cur_time )
  {
    geo_rep_disk_flush(ctx_p,0);
  }   
}


/*
**____________________________________________________________________________
*/
/**
*  init of the hash table of the replicator

   @param : root_path: root_path of the export
   @param : eid: exportd identifier
   @param : site_id: site identifier
   
   @retval <> NULL pointer to the  replication context
   @retval NULL, error (see errno for details
*/
void *geo_rep_init(int eid,int site_id,char *root_path)
{
    int ret;
    geo_rep_srv_ctx_t *ctx_p= NULL;
    
    if (geo_rep_srv_ctx_table_init_done == 0)
    {
       /*
       ** clear the pointers table
       */
       memset(&geo_rep_srv_ctx_table[0][0],0,sizeof(void*)*(EXPORT_GEO_MAX_CTX*EXPGW_EID_MAX_IDX+1));
       geo_rep_srv_ctx_table_init_done = 1;
    }      
    /*
    ** check the case of the reload: the context might be already allocated:
    */
    if (site_id > EXPORT_GEO_MAX_CTX)
    {
       severe("site is out of range %d max is %d",site_id,(int)EXPORT_GEO_MAX_CTX);
       return NULL;    
    }
    if(eid > EXPGW_EID_MAX_IDX)
    {
       severe("eid is out of range %d max is %d",eid,(int)EXPGW_EID_MAX_IDX);
       return NULL;
    
    }    
    if (geo_rep_srv_ctx_table[site_id][eid] != NULL)
    {
       return geo_rep_srv_ctx_table[site_id][eid];
    }
    /*
    ** allocate a context for the export replication
    */
    ctx_p = malloc(sizeof(geo_rep_srv_ctx_t));
    if (ctx_p == NULL)
    {
      /*
      ** out of memory
      */
      return NULL;
    }
    memset(ctx_p,0,sizeof(geo_rep_srv_ctx_t));
    /*
    ** init of the various fields
    */
    ctx_p->eid             = eid;
    ctx_p->site_id         = site_id;
#warning GEO_REP_FREQ_SECS set to 1
    ctx_p->delay           = GEO_REP_FREQ_SECS;
    ctx_p->delay_next_file = GEO_REP_NEXT_FILE_FREQ_SECS;
    ctx_p->max_filesize    = GEO_MAX_FILE_SIZE;
     
    ctx_p->geo_fid_table_p = malloc(sizeof(geo_fid_entry_t)*GEO_MAX_ENTRIES);
    if (ctx_p->geo_fid_table_p== NULL)
    {
       geo_rep_ctx_release(ctx_p);
       return NULL;
    }
    memset(ctx_p->geo_fid_table_p,0,sizeof(geo_fid_entry_t)*GEO_MAX_ENTRIES);
    
    ctx_p->geo_hash_table_p = malloc(sizeof(geo_hash_entry_t)*GEO_MAX_HASH_SZ);
    if (ctx_p->geo_hash_table_p== NULL)
    {
       geo_rep_ctx_release(ctx_p);
       return NULL;
    }
    memset(ctx_p->geo_hash_table_p,-1,sizeof(geo_hash_entry_t)*GEO_MAX_HASH_SZ);
    ctx_p->geo_first_idx = 0;
    ctx_p->file_idx_wr_pending = 0;
    geo_rep_clear_stats(ctx_p);
    /*
    ** copy the root path of the export
    */
    strcpy(ctx_p->geo_rep_export_root_path,root_path);
    ret = geo_rep_resolve_entry(ctx_p);
    if (ret < 0)
    {
      return NULL;
    }
    geo_rep_disk_read_index_file(ctx_p);

    ret = geo_rep_resolve_entry_recycle(ctx_p);
    if (ret < 0)
    {
      return NULL;
    }
    geo_rep_disk_read_index_file_recycle(ctx_p);

    
    ctx_p->geo_replication_enable = 1;
    /*
    ** store the context in the pointer table at the site and eid index
    */
    geo_rep_srv_ctx_table[site_id][eid] = ctx_p;
    return ctx_p;
}
