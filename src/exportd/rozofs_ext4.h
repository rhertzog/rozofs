/*
 *  ext4.h
 *
 * Copyright (C) 1992, 1993, 1994, 1995
 * Remy Card (card@masi.ibp.fr)
 * Laboratoire MASI - Institut Blaise Pascal
 * Universite Pierre et Marie Curie (Paris VI)
 *
 *  from
 *
 *  linux/include/linux/minix_fs.h
 *
 *  Copyright (C) 1991, 1992  Linus Torvalds
 */

#ifndef _EXT4_H
#define _EXT4_H

#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <linux/types.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/mattr.h>
#include "exp_cache.h"


//typedef handle_t;

#define GFP_NOFS 0

struct dentry{
  lv2_entry_t *d_inode;
  export_tracking_table_t *trk_tb_p;

};

/*
 * Historically, a buffer_head was used to map a single block
 * within a page, and of course as the unit of I/O through the
 * filesystem and block layers.  Nowadays the basic I/O unit
 * is the bio, and buffer_heads are used for extracting block
 * mappings (via a get_block_t call), for tracking state within
 * a page (via a page_mapping) and for wrapping bio submission
 * for backward compatibility reasons (e.g. submit_bh).
 */
struct buffer_head {
/*
** RozoFS field
*/
        void *lv2_entry;     /**< pointer to the level 2 entry  */
        int b_count;         /** <users using this buffer_head */
	size_t b_size;       /**< size of mapping */
	char *b_data;	     /**< pointer to data within the page */

	void *b_bdev;        /**< unused */
	uint64_t b_blocknr;    /**<used for storing an extended block reference */
};
/*
 * Describe an inode's exact location on disk and in memory
 */
struct ext4_iloc
{
	struct buffer_head *bh;
	unsigned long offset;
};


static inline struct ext4_inode *ext4_raw_inode(struct ext4_iloc *iloc)
{
	return (struct ext4_inode *) (iloc->bh->b_data + iloc->offset);
}

#define EXT4_FEATURE_COMPAT_EXT_ATTR		0x0008

/*
 * Inode dynamic state flags
 */
enum {
	EXT4_STATE_JDATA,		/* journaled data exists */
	EXT4_STATE_NEW,			/* inode is newly created */
	EXT4_STATE_XATTR,		/* has in-inode xattrs */
	EXT4_STATE_NO_EXPAND,		/* No space for expansion */
	EXT4_STATE_DA_ALLOC_CLOSE,	/* Alloc DA blks on close */
	EXT4_STATE_EXT_MIGRATE,		/* Inode is migrating */
	EXT4_STATE_DIO_UNWRITTEN,	/* need convert on dio done*/
	EXT4_STATE_NEWENTRY,		/* File just added to dir */
	EXT4_STATE_DELALLOC_RESERVED,	/* blks already reserved for delalloc */
};


/*
 * "quick string" -- eases parameter passing, but more importantly
 * saves "metadata" about the string (ie length and the hash).
 *
 * hash comes first so it snuggles against d_parent in the
 * dentry.
 */
struct qstr {
	unsigned int hash;
	unsigned int len;
	const unsigned char *name;
};

/**
*  RozoFS section
*/

/**
*  read the extended attribute from disk

   @param inode : tracking context associated with the export
   @param inode_key  : inode value of the extended attribute
*/
struct buffer_head *sb_bread(lv2_entry_t *inode,uint64_t inode_key);


#define kmalloc(size,unused) malloc(size)
#define kzalloc(size,unused) calloc(1,size)
#define kfree(data) free(data)


#define EXT4_ERROR_INODE(inode,text,val) printf(text,val)

static inline void lock_buffer(struct buffer_head *bh) {}
static inline void unlock_buffer(struct buffer_head *bh) {}

static inline void get_bh(struct buffer_head *bh)
{
        bh->b_count++;
}

static inline int ext4_test_inode_state(lv2_entry_t *inode, int bit)	
{									
	return (inode->attributes.s.i_state & (1<<bit));	
}

static inline int ext4_clear_inode_state(lv2_entry_t *inode, int bit)	
{									
	return (inode->attributes.s.i_state &= (~(1<<bit)));	
}

static inline int ext4_set_inode_state(lv2_entry_t *inode, int bit)	
{									
	return (inode->attributes.s.i_state |= (1<<bit));	
}
extern export_tracking_table_t *xattr_trk_p;

static inline export_tracking_table_t *xattr_get_tracking_context()
{
   return xattr_trk_p;
}
static inline void xattr_set_tracking_context(struct dentry *entry)
{
    xattr_trk_p = entry->trk_tb_p;
}

/*
**__________________________________________________________________
*/
/**
*  set the inode has dirty in order to re-write it on disk
   @param create : assert to 1 for allocated a block
   @param inode: pointer to the in memory inode
   @param bh: pointer to the data relative to the inode
*/

int ext4_handle_dirty_metadata(int create,lv2_entry_t *inode,struct buffer_head *bh);

/*
**__________________________________________________________________
*/
/**
*   get a memory block for storing extended attributes
*/

struct buffer_head *sb_getblk(lv2_entry_t *inode);

/*
**__________________________________________________________________
*/
/**
*  release buffer

   @param bh : buffer handle: take care since the pointer might be null
*/
void brelse(struct buffer_head *bh);

/*
**__________________________________________________________________
*/
/**
*  get the pointer to the inode;
*/
int ext4_get_inode_loc(lv2_entry_t *inode,struct ext4_iloc *iloc);

/*
**__________________________________________________________________
*/
/**
*  set the inode has dirty in order to re-write it on disk

   @param inode: pointer to the in memory inode
   @param bh: pointer to the data relative to the inode
*/
int ext4_mark_iloc_dirty(void *unused,lv2_entry_t *inode,struct ext4_iloc *iloc_p);
#endif	/* _EXT4_H */
