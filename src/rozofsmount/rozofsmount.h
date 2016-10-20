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

#ifndef ROZOFSMOUNT_H
#define ROZOFSMOUNT_H

#define FUSE_USE_VERSION 26
#include <fuse/fuse_lowlevel.h>

#include <rozofs/common/htable.h>
#include <rozofs/rpc/rozofs_rpc_util.h>
#include <rozofs/core/rozofs_tx_common.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/rozofs_throughput.h>

#include "file.h"

#define ROZOFSMOUNT_MAX_EXPORT_TX 32
#define ROZOFSMOUNT_MAX_STORCLI_TX  32
#define ROZOFSMOUNT_MAX_TX (ROZOFSMOUNT_MAX_EXPORT_TX+ROZOFSMOUNT_MAX_STORCLI_TX)

#define hash_xor8(n)    (((n) ^ ((n)>>8) ^ ((n)>>16) ^ ((n)>>24)) & 0xff)
#define ROOT_INODE 1

extern exportclt_t exportclt;

extern list_t inode_entries;
extern htable_t htable_inode;
extern htable_t htable_fid;
extern uint64_t rozofs_ientries_count;

extern double direntry_cache_timeo ;
extern double entry_cache_timeo ;
extern double attr_cache_timeo ;
extern int rozofs_cache_mode;
extern int rozofs_mode;
extern int rozofs_rotation_read_modulo;
extern int rozofs_bugwatch;
extern uint16_t rozofsmount_diag_port;

typedef struct rozofsmnt_conf {
    char *host;
    char *export;
    char *passwd;
    unsigned buf_size;
    unsigned min_read_size;
    unsigned nbstorcli;    
    unsigned max_retry;
    unsigned dbg_port;  /**< lnkdebug base port: rozofsmount=dbg_port, storcli(1)=dbg_port+1, ....  */
    unsigned instance;  /**< rozofsmount instance: needed when more than 1 rozofsmount run the same server and exports the same filesystem */
    unsigned export_timeout;
    unsigned storcli_timeout;
    unsigned storage_timeout;
    unsigned fs_mode; /**< rozofs mode: 0-> file system/ 1-> block mode */
    unsigned cache_mode;  /**< 0: no option, 1: direct_read, 2: keep_cache */
    unsigned attr_timeout;
    unsigned attr_timeout_ms;
    unsigned entry_timeout;
    unsigned entry_timeout_ms;
    unsigned symlink_timeout;
    unsigned shaper;
    unsigned rotate;
    unsigned posix_file_lock;    
    unsigned bsd_file_lock;  
    unsigned no_file_lock;  
    unsigned max_write_pending ; /**< Maximum number pending write */
    unsigned quota; /* ignored */    
    unsigned noXattr;
    int site;
    int conf_site_file;
    unsigned running_site;
    unsigned mojThreadWrite;
    unsigned mojThreadRead;    
    unsigned mojThreadThreshold;  
    unsigned no0trunc;     
    /*
    ** when set this options tells that this client is the only writter of
    ** the file it writes, and so can avoid some internal re-read when writes
    ** are not aligned on block bondary
    */
    unsigned onlyWriter;  
    /*
    ** Whether to favor local storage on read to save network bandwidth
    ** in case of poor network connection
    */
    unsigned localPreference;    
    unsigned noReadFaultTolerant;         
} rozofsmnt_conf_t;
rozofsmnt_conf_t conf;

typedef struct dirbuf {
    char *p;
    size_t size;
    uint8_t eof;
    uint64_t cookie;
} dirbuf_t;

/** entry kept locally to map fuse_inode_t with rozofs fid_t */
typedef struct ientry {
    fuse_ino_t inode; ///< value of the inode allocated by rozofs
    fid_t fid; ///< unique file identifier associated with the file or directory
    fid_t pfid; ///< unique file identifier associated with the parent
     /**< assert to 1 when there is a setattr on mtime  , clear on write */
    int64_t file_extend_size; /**< The pending size extenstion */
    /* some bit fields... */
    uint64_t  mtime_locked:1;  
    uint64_t  file_extend_pending:1; /**< assert to one when file is extended by not yet confirm on exportd */
    uint64_t  file_extend_running:1; /**< assert to one when file is extended by not yet confirm on exportd */
    dirbuf_t db; ///< buffer used for directory listing
    unsigned long nlookup; ///< number of lookup done on this entry (used for forget)
    mattr_t attrs;   /**< attributes caching for fs_mode = block mode   */
    list_t list;
    /** This is the address of the latest file_t structure on which there is some data
     ** pending in the buffer that have not been flushed to disk. Only one file_t at a time
     ** can be in this case for all the open that have occurred on this file. Writing into
     ** a file_t buffer automaticaly triggers the flush to disk of the previous pending write.
     */ 
    file_t    * write_pending;
    /**
     ** This counter is used for a reader to know whether the data in its buffer can be
     ** used safely or if they must be thrown away and a re-read from the disk is required
     ** because some write has occurred since the last read.
     */
    uint64_t    read_consistency;
    uint64_t    timestamp;
    uint64_t    timestamp_wr_block;
    char      * symlink_target;
    uint64_t    symlink_ts;
    int         pending_getattr_cnt;   /**< pending get attr count  */
} ientry_t;



/*
** About exportd id quota
*/
extern uint64_t eid_free_quota;
extern int rozofs_xattr_disable; /**< assert to one to disable xattr for the exported file system */
/*
** write alignment statistics
*/
extern uint64_t    rozofs_aligned_write_start[2];
extern uint64_t    rozofs_aligned_write_end[2];

extern int rozofs_site_number;
/**______________________________________________________________________________
*/
/**
*  get the current site number of the rozofsmount client

*/
static inline int rozofs_get_site_number()
{
  return rozofs_site_number;
}

/**______________________________________________________________________________
*/
/**
*  Set export id free block count when a quota is set
*  @param free_quota   Count of free blocks before reaching the hard quota
*
*/
static inline void eid_set_free_quota(uint64_t free_quota) {
  eid_free_quota = free_quota;
}
/**______________________________________________________________________________
*/
/**
*  Check export id hard quota
*
*  @param oldSize   Old size of the file
*  @param newSize   New size of the file
*
* @retval 0 not enough space left
* @retval 1 there is the requested space
*
*/
static inline int eid_check_free_quota(uint32_t bsize, uint64_t oldSize, uint64_t newSize) {
  uint64_t oldBlocks;
  uint64_t newBlocks;
  uint32_t bbytes = ROZOFS_BSIZE_BYTES(bsize);

  if (eid_free_quota == -1) return 1; // No quota so go on
  
  // File truncate is always good
  if (newSize < oldSize) return 1;

  // Compute current number of blocks of the file
  oldBlocks = oldSize / bbytes;
  if (oldSize % bbytes) oldBlocks++;

  // Compute futur number of blocks of the file
  newBlocks = newSize / bbytes;
  if (newSize % bbytes) newBlocks++;  
  
  if ((newBlocks-oldBlocks) > eid_free_quota) {
    errno = ENOSPC;
    return 0;
  }  
  return 1;
}


static inline uint32_t fuse_ino_hash_fnv_with_len( void *key1) {

    unsigned char *d = (unsigned char *) key1;
    int i = 0;
    int h;

     h = 2166136261U;
    /*
     ** hash on name
     */
    d = key1;
    for (i = 0; i <sizeof (fuse_ino_t) ; d++, i++) {
        h = (h * 16777619)^ *d;
    }
    return (uint32_t) h;
}

static inline uint32_t fuse_ino_hash(void *n) {
    return fuse_ino_hash_fnv_with_len(n);
}


extern uint64_t hash_inode_collisions_count;
extern uint64_t hash_inode_max_collisions;
extern uint64_t hash_inode_cur_collisions;

static inline int fuse_ino_cmp(void *v1, void *v2) {
      int ret;
      ret =  memcmp(v1, v2, sizeof (fuse_ino_t));
      if (ret != 0) {
          hash_inode_collisions_count++;
	  hash_inode_cur_collisions++;
	  return ret;
      }
      if (hash_inode_max_collisions < hash_inode_cur_collisions) hash_inode_max_collisions = hash_inode_cur_collisions;
      return ret;
//    return (*(fuse_ino_t *) v1 - *(fuse_ino_t *) v2);

}

static inline int fid_cmp(void *key1, void *key2) {
    return memcmp(key1, key2, sizeof (fid_t));
}

static inline unsigned int fid_hash(void *key) {
    uint32_t hash = 0;
    uint8_t *c;
    for (c = key; c != key + 16; c++)
        hash = *c + (hash << 6) + (hash << 16) - hash;
    return hash;
}

static inline void ientries_release() {
    list_t *p, *q;
    return;
 
    htable_release(&htable_inode);
    htable_release(&htable_fid);

    list_for_each_forward_safe(p, q, &inode_entries) {
        ientry_t *entry = list_entry(p, ientry_t, list);
        list_remove(p);
        free(entry);
    }
}

static inline void put_ientry(ientry_t * ie) {
    DEBUG("put inode: %llx\n",(unsigned long long int)ie->inode);
    rozofs_ientries_count++;
    htable_put(&htable_inode, &ie->inode, ie);
//    htable_put(&htable_fid, ie->fid, ie);
    list_push_front(&inode_entries, &ie->list);
}

static inline void del_ientry(ientry_t * ie) {
    DEBUG("del inode: %llx\n",(unsigned long long int) ie->inode);
    rozofs_ientries_count--;
    htable_del(&htable_inode, &ie->inode);
//    htable_del(&htable_fid, ie->fid);
    list_remove(&ie->list);
    if (ie->db.p != NULL) {
      free(ie->db.p);
      ie->db.p = NULL;
    }
    if (ie->symlink_target) {
      free(ie->symlink_target);
      ie->symlink_target = NULL;
    }
    free(ie);    
}

static inline ientry_t *get_ientry_by_inode(fuse_ino_t ino) {
    rozofs_inode_t fake_id;

    fake_id.fid[1]=ino;
    if (ROZOFS_DIR_FID == fake_id.s.key) 
    {
      fake_id.s.key = ROZOFS_DIR;
    }
    hash_inode_cur_collisions = 0;
    return htable_get(&htable_inode, &fake_id.fid[1]);
}

static inline ientry_t *get_ientry_by_fid(fid_t fid) {
    rozofs_inode_t *fake_id = (rozofs_inode_t *) fid;
    return get_ientry_by_inode(fake_id->fid[1]);
}

static inline ientry_t *alloc_ientry(fid_t fid) {
	ientry_t *ie;
	rozofs_inode_t *inode_p ;
	
	inode_p = (rozofs_inode_t*) fid;

	ie = xmalloc(sizeof(ientry_t));
	memcpy(ie->fid, fid, sizeof(fid_t));
        memset(ie->pfid, 0, sizeof(fid_t));
	ie->inode = inode_p->fid[1]; //fid_hash(fid);
	list_init(&ie->list);
	ie->db.size = 0;
	ie->db.eof = 0;
	ie->db.cookie = 0;
	ie->db.p = NULL;
	ie->nlookup = 0;
        ie->write_pending = NULL; 
        ie->read_consistency = 1;
	ie->file_extend_size = 0;
	ie->file_extend_pending = 0;
	ie->file_extend_running = 0;
	ie->mtime_locked        = 0;
	ie->timestamp_wr_block = 0;
	ie->symlink_target = NULL;
        ie->symlink_ts     = 0;
	ie->pending_getattr_cnt= 0;
	put_ientry(ie);

	return ie;
}
static inline ientry_t *recycle_ientry(ientry_t * ie, fid_t fid) {

	memcpy(ie->fid, fid, sizeof(fid_t));
	ie->db.size = 0;
	ie->db.eof = 0;
	ie->db.cookie = 0;
	if (ie->db.p != NULL) {
	  free(ie->db.p);
	  ie->db.p = NULL;
	}	
	// ie->nlookup = 0;
        ie->write_pending = NULL; 
        ie->read_consistency = 1;
	ie->file_extend_size = 0;	
	ie->file_extend_pending = 0;
	ie->file_extend_running = 0;
	ie->timestamp_wr_block = 0;
	if (ie->symlink_target) {
	  free(ie->symlink_target);
	  ie->symlink_target = NULL;
	}	
        ie->symlink_ts     = 0;
	return ie;
}

static inline void ientry_update_parent(ientry_t * ie, fid_t pfid) {

	memcpy(ie->pfid, pfid, sizeof(fid_t));
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

int rozofs_asynchronous_flush(struct fuse_file_info *fi) ;
/**
*  Flush all write pending on a given ientry 

 @param ie : pointer to the ientry in the cache
 
 @retval 1  on success
 @retval 0  in case of any flushing error
 */

static inline int flush_write_ientry(ientry_t * ie) {
    file_t              * f;
    struct fuse_file_info fi;
    int                   ret;
    
    /*
    ** Check whether any write is pending in some buffer open on this file by any application
    */
    if ((f = ie->write_pending) != NULL) {

       ie->write_pending = NULL;
       
       /*
       ** Double check this file descriptor points to this ie
       */
       if (f->ie != ie) {
         char fid_string[64];
	 uuid_unparse(ie->fid, fid_string);
         severe("Bad write pending ino %llu FID %s", (long long unsigned int)ie->inode, fid_string);
	 return 1;
       }
     
       fi.fh = (unsigned long) f;
       ret = rozofs_asynchronous_flush(&fi);
       if (ret == 0) return 0;

       f->buf_write_wait = 0;
       f->write_from     = 0;
       f->write_pos      = 0;
    }
    return 1;
}

static inline struct stat *mattr_to_stat(mattr_t * attr, struct stat *st, uint32_t bsize) {
    memset(st, 0, sizeof (struct stat));
    st->st_mode = attr->mode;
    st->st_nlink = attr->nlink;
    st->st_size = attr->size;
    st->st_ctime = attr->ctime;
    st->st_atime = attr->atime;
    st->st_mtime = attr->mtime;
    st->st_blksize = ROZOFS_BSIZE_BYTES(bsize);
    st->st_blocks = ((attr->size + 512 - 1) / 512);
    st->st_dev = 0;
    st->st_uid = attr->uid;
    st->st_gid = attr->gid;
    return st;
}

static inline mattr_t *stat_to_mattr(struct stat *st, mattr_t * attr,
		int to_set) {
	if (to_set & FUSE_SET_ATTR_MODE)
		attr->mode = st->st_mode;
	if (to_set & FUSE_SET_ATTR_SIZE)
		attr->size = st->st_size;
	if (to_set & FUSE_SET_ATTR_ATIME)
		attr->atime = st->st_atime;
	if (to_set & FUSE_SET_ATTR_MTIME)
		attr->mtime = st->st_mtime;
	if (to_set & FUSE_SET_ATTR_UID)
		attr->uid = st->st_uid;
	if (to_set & FUSE_SET_ATTR_GID)
		attr->gid = st->st_gid;
	return attr;
}

/*Export commands prototypes*/
void rozofs_ll_getattr_nb(fuse_req_t req, fuse_ino_t ino,
		struct fuse_file_info *fi);

void rozofs_ll_setattr_nb(fuse_req_t req, fuse_ino_t ino, struct stat *stbuf,
		int to_set, struct fuse_file_info *fi);

void rozofs_ll_lookup_nb(fuse_req_t req, fuse_ino_t parent, const char *name);

void rozofs_ll_mkdir_nb(fuse_req_t req, fuse_ino_t parent, const char *name,
		mode_t mode);

void rozofs_ll_mknod_nb(fuse_req_t req, fuse_ino_t parent, const char *name,
		mode_t mode, dev_t rdev);

void rozofs_ll_open_nb(fuse_req_t req, fuse_ino_t ino,
		struct fuse_file_info *fi);

void rozofs_ll_symlink_nb(fuse_req_t req, const char *link, fuse_ino_t parent,
		const char *name);

void rozofs_ll_readlink_nb(fuse_req_t req, fuse_ino_t ino);

void rozofs_ll_link_nb(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
		const char *newname);

void rozofs_ll_unlink_nb(fuse_req_t req, fuse_ino_t parent, const char *name);

void rozofs_ll_rmdir_nb(fuse_req_t req, fuse_ino_t parent, const char *name);

void rozofs_ll_rename_nb(fuse_req_t req, fuse_ino_t parent, const char *name,
		fuse_ino_t newparent, const char *newname);

void rozofs_ll_statfs_nb(fuse_req_t req, fuse_ino_t ino);

void rozofs_ll_create_nb(fuse_req_t req, fuse_ino_t parent, const char *name,
		mode_t mode, struct fuse_file_info *fi);

void rozofs_ll_setxattr_nb(fuse_req_t req, fuse_ino_t ino, const char *name,
		const char *value, size_t size, int flags);

void rozofs_ll_getxattr_nb(fuse_req_t req, fuse_ino_t ino, const char *name,
		size_t size);

void rozofs_ll_removexattr_nb(fuse_req_t req, fuse_ino_t ino, const char *name);

void rozofs_ll_listxattr_nb(fuse_req_t req, fuse_ino_t ino, size_t size);

void rozofs_ll_readdir_nb(fuse_req_t req, fuse_ino_t ino, size_t size,
		off_t off, struct fuse_file_info *fi);

void rozofs_ll_read_nb(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
		struct fuse_file_info *fi);

void rozofs_ll_write_nb(fuse_req_t req, fuse_ino_t ino, const char *buf,
		size_t size, off_t off, struct fuse_file_info *fi);

void rozofs_ll_flush_nb(fuse_req_t req, fuse_ino_t ino,
		struct fuse_file_info *fi);

void rozofs_ll_release_nb(fuse_req_t req, fuse_ino_t ino,
		struct fuse_file_info *fi);

void rozofs_ll_getlk_nb(fuse_req_t req, fuse_ino_t ino,
		struct fuse_file_info *fi, struct flock *lock);

void rozofs_ll_setlk_nb(fuse_req_t req, fuse_ino_t ino,
		struct fuse_file_info *fi, struct flock *lock, int sleep);

void rozofs_ll_flock_nb(fuse_req_t req, fuse_ino_t ino,
		struct fuse_file_info *fi, int op);

void init_write_flush_stat(int max_write_pending);
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
int buf_flush(void *fuse_ctx_p,file_t *p);

int export_write_block_asynchrone(void *fuse_ctx_p, file_t *file_p,
		sys_recv_pf_t recv_cbk);

/*
**__________________________________________________________________
*/
/**
 API to clear the buffer after a flush
 If some data is pending in the buffer the clear is not done

 @param *p : pointer to the file structure where read buffer information can be retrieved

 @retval -1 some data to write is pending
 @retval 0 if the read buffer is not empty
 */
int clear_read_data(file_t *p);

/*
**__________________________________________________________________
*/
/**
  the goal of that API is to update the metadata attributes in
  the ientry.
  
  @param ientry_t *ie
  @param mattr_t  attr

*/
static inline void rozofs_ientry_update(ientry_t *ie,mattr_t  *attr_p)
{

    /**
    *  update the timestamp in the ientry context
    */
    ie->timestamp = rozofs_get_ticker_us();
    /*
    ** check if there is a pending extension of the size
    */
    if ((ie->file_extend_pending == 0)&&(ie->file_extend_running == 0))
    {
       /*
       ** nothing pending so full copy
       */
       memcpy(&ie->attrs,attr_p, sizeof (mattr_t));   
       return;
   }
   /*
   ** preserve the size of the ientry
   */
   uint64_t file_size = ie->attrs.size;
   memcpy(&ie->attrs,attr_p, sizeof (mattr_t));   
   ie->attrs.size = file_size;
}
/*
**__________________________________________________________________
*/
/**
  rozofsmount applicative supervision callback to check the connection 
  toward the exportd thanks to an EP_NULL question/answer
  
  @param sock_p socket context

*/
void rozofs_export_lbg_cnx_polling(af_unix_ctx_generic_t  *sock_p);
/**
*  reset all the locks of a given client
*  This is an internal request that do not trigger any response toward fuse


 @param eid           :eid this client is mounted on
 @param client_hash   :reference of the client
 
 @retval none
*/
void rozofs_ll_clear_client_file_lock(int eid, uint64_t client_hash);
/**
* Read/write bandwidth counters
*/
#define ROZOFS_READ_THR_E 0
#define ROZOFS_WRITE_THR_E 1
extern rozofs_thr_cnts_t *rozofs_thr_counter[];

#endif
