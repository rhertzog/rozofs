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
#ifndef GEO_REPLICATION_H
#define GEO_REPLICATION_H
#include <rozofs/rozofs.h>
#include <stdint.h>
#include <time.h>
#include <rozofs/common/geo_replica_str.h>
 
#define GEO_MAX_HASH_SZ (1024*64)

#define GEO_MAX_ENTRIES (1024*1)

#define GEO_DIRECTORY "georep_dir"
#define GEO_DIR_RECYCLE "georep_dir_recycle"
#define GEO_FILE_IDX "indexes"
#define GEO_FILE "sync"
#define GEO_FILE_STATS "sync_stats"

#define GEO_REP_BUILD_PATH(name)  \
    sprintf(path, "%s/%s_%d/%s", ctx_p->geo_rep_export_root_path,GEO_DIRECTORY, ctx_p->site_id,name);
#define GEO_REP_RECYCLE_BUILD_PATH(name)  \
    sprintf(path, "%s/%s_%d/%s", ctx_p->geo_rep_export_root_path,GEO_DIR_RECYCLE, ctx_p->site_id,name);
#define GEO_REP_BUILD_PATH_NONAME  \
    sprintf(path, "%s/%s_%d", ctx_p->geo_rep_export_root_path,GEO_DIRECTORY, ctx_p->site_id);
#define GEO_REP_RECYCLE_BUILD_PATH_NONAME  \
    sprintf(path, "%s/%s_%d", ctx_p->geo_rep_export_root_path,GEO_DIR_RECYCLE, ctx_p->site_id);

#define GEO_REP_BUILD_PATH_FILE(name,idx)  \
    sprintf(path, "%s/%s_%d/%s_%llu", ctx_p->geo_rep_export_root_path,GEO_DIRECTORY,\
                   ctx_p->site_id,name,(long long unsigned int)idx);

#define GEO_REP_FILE_HDR_SZ sizeof(uint64_t)  /**< geo-replication file header size: contains the resultof time() */

#define GEO_MAX_COLL_BIT 2
#define GEO_MAX_COLL_ENTRY (1<<GEO_MAX_COLL_BIT) 
 typedef struct _geo_hash_entry_t
 {
   uint16_t  entry[GEO_MAX_COLL_ENTRY];
 } geo_hash_entry_t;

typedef struct _geo_rep_stats
{
   uint64_t  insert_count;
   uint64_t  update_count;
   uint64_t  delete_count;
   uint64_t  coll_count;
   uint64_t  flush_count;
   uint64_t  stat_err;   /**< error on stat counter */
   uint64_t  open_err;   /**< error on open counter */
   uint64_t  write_err;   /**< error on write counter */
   uint64_t  access_err; /**< error on access counter */
   uint64_t  open_stat_err;   /**< error on open counter */
   uint64_t  write_stat_err;   /**< error on write counter */
   uint64_t  read_stat_err;   /**< error on read counter */
   uint64_t  access_stat_err; /**< error on access counter */
} geo_rep_stats_t;

typedef struct _geo_rep_main_file_t
{
   uint64_t first_index;  /**< index of the first file  */
   uint64_t last_index;  /**< index of the last file   */
} geo_rep_main_file_t;

#define GEO_REP_RECYCLE_FREQ 2
#define GEO_REP_FREQ_SECS 1 
#define GEO_REP_NEXT_FILE_FREQ_SECS (GEO_REP_FREQ_SECS*4) 
#define GEO_MAX_FILE_SIZE (GEO_MAX_ENTRIES*sizeof(geo_fid_entry_t))

typedef struct _geo_rep_srv_ctx_t
{
   int  eid;           /**< export identifier         */
   int  site_id;       /**< site identifier           */
   int  recycle_counter;       /**< current counter for recycle polling          */
   int geo_replication_enable;  /**< geo replication status */
   geo_fid_entry_t *geo_fid_table_p;  /**< ptr to the memory array used for storing the fid to synchronize */
   geo_hash_entry_t *geo_hash_table_p; /**< hash table associated with the previous table                 */
   geo_rep_main_file_t geo_rep_main_file; /**< indexes of the first and last files containing fid to sync. */
   geo_rep_main_file_t geo_rep_main_recycle_file; /**< indexes of the first and last files containing fid to recycle */
   uint64_t geo_rep_main_recycle_first_idx;
   geo_rep_stats_t stats;                 /**< statistics                                                  */
   int geo_first_idx;                    /**< next index to allocated in the fid memory table              */
   uint64_t delay;                       /**< delay in seconds before flushing on disk                     */
   uint64_t delay_next_file;             /**< delay in seconds before incrementing the file number         */
   uint64_t last_time_flush;             /**< last time for which a new file has been written              */
   uint64_t last_time_file_cr8;          /**< last time for which a new file has been written              */
   uint64_t max_filesize;                /**< max file size before moving to the next file                 */
   int      file_idx_wr_pending;         /**< assert to 1 on the first flush and clear on file index write */
   
   char geo_rep_export_root_path[ROZOFS_PATH_MAX];  /**< root pathname of the export                       */
   int  synchro_rate;
   uint64_t last_time_watch;         /**< timestamp associated with last_time_sync_count                   */
   uint64_t last_time_sync_count;    /**< last count of file synced                                        */
   pthread_rwlock_t  flush_lock;     /**< lock used for handling multiple thread flushing on disk          */
} geo_rep_srv_ctx_t;


void show_geo_profiler(char * argv[], uint32_t tcpRef, void *bufRef);

/*
**____________________________________________________________________________
*/
/**
*  geo replication stats flush

  @param none
  
  @retval none
*/
static inline void geo_rep_clear_stats(geo_rep_srv_ctx_t *ctx_p)
{
  memset(&ctx_p->stats,0,sizeof(geo_rep_stats_t));
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
void *geo_rep_init(int eid,int site_id,char *root_path);
/*
**____________________________________________________________________________
*/
/**
*  re-init of the hash table of the replicator

   @param ctx_p : pointer to replication context
   
   @retval 0 on success
   @retval -1 on error (see errno for details
*/
int geo_rep_reinit(geo_rep_srv_ctx_t *ctx_p);
/*
**____________________________________________________________________________
*/
/**
* release of a geo replication context

   @param ctx_p : pointer to the replication context
   
   @retval none
*/
void geo_rep_ctx_release(geo_rep_srv_ctx_t *ctx_p);
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
		       uint8_t layout,cid_t cid,sid_t *sids_p);
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
int geo_rep_disk_flush(geo_rep_srv_ctx_t *ctx_p,int forced);

/*
**____________________________________________________________________________
*/
void show_geo_replication(char * argv[], uint32_t tcpRef, void *bufRef);

/*
**____________________________________________________________________________
*/
/**
*  add a context in the debug table

   @param p : ocntext to add (the key is the value of the eid
   
   @retval 0 on success
   @retval -1 on error
*/
int geo_rep_dbg_add(geo_rep_srv_ctx_t *p);
/*
**____________________________________________________________________________
*/
/*
** read the index file

   @param ctx_p : pointer to replication context
   
   @retval 0: on success
   @retval -1 on error (see errno for details )
*/
int geo_rep_disk_read_index_file(geo_rep_srv_ctx_t *ctx_p);

/*
**____________________________________________________________________________
*/
/*
** update the last_index of the index file

   @param ctx_p : pointer to replication context
   
   @retval 0: on success
   @retval -1 on error (see errno for details )
*/
int geo_rep_disk_update_last_index(geo_rep_srv_ctx_t *ctx_p);

/*
**____________________________________________________________________________
*/
/*
** update the first_index of the index file

   @param ctx_p : pointer to replication context
   
   @retval 0: on success
   @retval -1 on error (see errno for details )
*/
int geo_rep_disk_update_first_index(geo_rep_srv_ctx_t *ctx_p);

/*
**____________________________________________________________________________
*/
/**
* geo-relication polling of one exportd

  @param ctx_p: pointer to the geo-replication context
  
  @retval: none
*/
void geo_replication_poll_one_exportd(geo_rep_srv_ctx_t *ctx_p);


/*
**____________________________________________________________________________
*/
/*
** create the recycle file index

   @param ctx_p : pointer to replication context
   
   @retval 0: on success
   @retval -1 on error (see errno for details )
*/
int geo_rep_create_file_index_recycle(geo_rep_srv_ctx_t *ctx_p);

/*
**____________________________________________________________________________
*/
/*
** update the last_index of the index file

   @param ctx_p : pointer to replication context
   
   @retval 0: on success
   @retval -1 on error (see errno for details )
*/
int geo_rep_disk_update_last_index_recycle(geo_rep_srv_ctx_t *ctx_p);

/*
**____________________________________________________________________________
*/
/*
** update the first_index of the index file

   @param ctx_p : pointer to replication context
   
   @retval 0: on success
   @retval -1 on error (see errno for details )
*/
int geo_rep_disk_update_first_index_recycle(geo_rep_srv_ctx_t *ctx_p);

/*
**____________________________________________________________________________
*/
/*
** read the index file

   @param ctx_p : pointer to replication context
   
   @retval 0: on success
   @retval -1 on error (see errno for details )
*/
int geo_rep_disk_read_index_file_recycle(geo_rep_srv_ctx_t *ctx_p);

/*
**____________________________________________________________________________
*/
/**
*  attempt to open or create the file index file associated with an export

   @param ctx_p : pointer to replication context
   
   @retval 0: on success
   @retval -1 on error (see errno for details )
*/
int geo_rep_resolve_entry_recycle(geo_rep_srv_ctx_t *ctx_p) ;

/*
**____________________________________________________________________________
*/
/**
* update the count of files that have been synced (geo-replication)

  @param ctx_p : pointer to replication context
  @param nb_files : number of files synced
 
 @retval none
*/
void geo_rep_udpate_synced_stats(geo_rep_srv_ctx_t *ctx_p,uint64_t nb_files);
/*
**____________________________________________________________________________
*/
/**
* update the count of file waiting for geo-replication

  @param ctx_p : pointer to replication context
  @param nb_files : number of files to synchronize
 
 @retval none
*/
void geo_rep_udpate_pending_sync_stats(geo_rep_srv_ctx_t *ctx_p,uint64_t nb_files);
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
int geo_rep_read_sync_stats(geo_rep_srv_ctx_t *ctx_p,uint64_t *nb_files_pending,uint64_t *nb_files_synced);
/*
**____________________________________________________________________________
*/
/**
*  Rozodiag CLI to restart georeplication from scratch
*/
void geo_rep_restart_from_scratch_dbg(char * argv[], uint32_t tcpRef, void *bufRef);
#endif
