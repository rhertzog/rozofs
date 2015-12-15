/*
 * linux/fs/ext4/xattr.c
 *
 * Copyright (C) 2001-2003 Andreas Gruenbacher, <agruen@suse.de>
 *
 * Fix by Harrison Xing <harrison@mountainviewdata.com>.
 * Ext4 code with a lot of help from Eric Jarman <ejarman@acm.org>.
 * Extended attributes for symlinks and special files added per
 *  suggestion of Luka Renko <luka.renko@hermes.si>.
 * xattr consolidation Copyright (c) 2004 James Morris <jmorris@redhat.com>,
 *  Red Hat Inc.
 * ea-in-inode support by Alex Tomas <alex@clusterfs.com> aka bzzz
 *  and Andreas Gruenbacher <agruen@suse.de>.
 */

/*
 * Extended attributes are stored directly in inodes (on file systems with
 * inodes bigger than 128 bytes) and on additional disk blocks. The i_file_acl
 * field contains the block number if an inode uses an additional block. All
 * attributes must fit in the inode and one additional block. Blocks that
 * contain the identical set of attributes may be shared among several inodes.
 * Identical blocks are detected by keeping a cache of blocks that have
 * recently been accessed.
 *
 * The attributes in inodes and on blocks have a different header; the entries
 * are stored in the same format:
 *
 *   +------------------+
 *   | header           |
 *   | entry 1          | |
 *   | entry 2          | | growing downwards
 *   | entry 3          | v
 *   | four null bytes  |
 *   | . . .            |
 *   | value 1          | ^
 *   | value 3          | | growing upwards
 *   | value 2          | |
 *   +------------------+
 *
 * The header is followed by multiple entry descriptors. In disk blocks, the
 * entry descriptors are kept sorted. In inodes, they are unsorted. The
 * attribute values are aligned to the end of the block in no specific order.
 *
 * Locking strategy
 * ----------------
 * inode->attributes.s.i_file_acl is protected by inode->attributes.s.xattr_sem.
 * EA blocks are only changed if they are exclusive to an inode, so
 * holding xattr_sem also means that nothing but the EA block's reference
 * count can change. Multiple writers to the same block are synchronized
 * by the buffer lock.
 */

#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/mattr.h>
#include "rozofs_ext4.h"
#include "xattr.h"
#include <rozofs/common/acl.h>
#include "exp_cache.h"



#define BHDR(bh) ((struct ext4_xattr_header *)((bh)->b_data))
#define ENTRY(ptr) ((struct ext4_xattr_entry *)(ptr))
#define BFIRST(bh) ENTRY(BHDR(bh)+1)
#define IS_LAST_ENTRY(entry) (*(__u32 *)(entry) == 0)

#ifdef EXT4_XATTR_DEBUG
# define ea_idebug(inode, f...) do { \
		printk(KERN_DEBUG "inode %s:%lu: ", \
			inode->i_sb->s_id, inode->i_ino); \
		printk(f); \
		printk("\n"); \
	} while (0)
# define ea_bdebug(bh, f...) do { \
		char b[BDEVNAME_SIZE]; \
		printk(KERN_DEBUG "block %s:%lu: ", \
			bdevname(bh->b_bdev, b), \
			(unsigned long) bh->b_blocknr); \
		printk(f); \
		printk("\n"); \
	} while (0)
#else
# define ea_idebug(f...)
# define ea_bdebug(f...)
#endif



static int ext4_xattr_list(struct dentry *dentry, char *buffer,
			   size_t buffer_size);

static struct mb_cache *ext4_xattr_cache;

const struct xattr_handler *ext4_xattr_handler_map[] = {
	[EXT4_XATTR_INDEX_USER]		     = &ext4_xattr_user_handler,
	[EXT4_XATTR_INDEX_POSIX_ACL_ACCESS]  = &ext4_xattr_acl_access_handler,
	[EXT4_XATTR_INDEX_POSIX_ACL_DEFAULT] = &ext4_xattr_acl_default_handler,
	[EXT4_XATTR_INDEX_TRUSTED]	     = &ext4_xattr_trusted_handler,
	[EXT4_XATTR_INDEX_SECURITY]	     = &ext4_xattr_security_handler,
	[EXT4_XATTR_INDEX_SYSTEM]	     = &ext4_xattr_system_handler,

};

const struct xattr_handler *ext4_xattr_handlers[] = {
	&ext4_xattr_user_handler,
	&ext4_xattr_trusted_handler,
	&ext4_xattr_acl_access_handler,
	&ext4_xattr_acl_default_handler,
	&ext4_xattr_security_handler,
	&ext4_xattr_system_handler,
	NULL
};

#define ARRAY_SIZE(a) \
((sizeof(a) / sizeof(*(a))) /                     \
  (size_t)(!(sizeof(a) % sizeof(*(a)))))

static inline const struct xattr_handler *
ext4_xattr_handler(int name_index)
{
	const struct xattr_handler *handler = NULL;

	if (name_index > 0 && name_index < ARRAY_SIZE(ext4_xattr_handler_map))
		handler = ext4_xattr_handler_map[name_index];
	return handler;
}

/*
 * Inode operation listxattr()
 *
 * dentry->d_inode->i_mutex: don't care
 */
ssize_t
ext4_listxattr(struct dentry *dentry, char *buffer, size_t size)
{
	return ext4_xattr_list(dentry, buffer, size);
}



/*
**_____________________________________________________________________________
*/
/**
*  the purpose of the procedure is to check that the list of name is consistent

   @param entry : pointer to the first xattr entry
   @param end : pointer to the end of the inode (either root inode or xattr inode
   
   @retval 0 on success
   @retval < 0 on error
*/
static int
ext4_xattr_check_names(struct ext4_xattr_entry *entry, void *end)
{
	while (!IS_LAST_ENTRY(entry)) {
		struct ext4_xattr_entry *next = EXT4_XATTR_NEXT(entry);
		if ((void *)next >= end)
		{
	           severe("ext4_xattr_check_names bad size error:EIO");
	           return -EIO;
		}
		entry = next;
	}
	return 0;
}
/*
**_____________________________________________________________________________
*/
static inline int
ext4_xattr_check_block(struct buffer_head *bh)
{
	int error;

	if (BHDR(bh)->h_magic != cpu_to_le32(EXT4_XATTR_MAGIC) ||
	    BHDR(bh)->h_blocks != cpu_to_le32(1))
	{
	   severe("ext4_xattr_check_block inconsistent header error:EIO");
	   return -EIO;
	}
	error = ext4_xattr_check_names(BFIRST(bh), bh->b_data + bh->b_size);
	return error;
}

static inline int
ext4_xattr_check_entry(struct ext4_xattr_entry *entry, size_t size)
{
	size_t value_size = le32_to_cpu(entry->e_value_size);

	if (entry->e_value_block != 0 || value_size > size ||
	    le16_to_cpu(entry->e_value_offs) + value_size > size)
	{
	   severe("ext4_xattr_check_entry bad format-> error:EIO");
	   return -EIO;
        }
	return 0;
}
/*
**_____________________________________________________________________________
*/
/**
*  find the name attribut

  @param pentry : pointer to the entry where the name is found (in/out)
  @param name_index : type of extended attribute
  @param name : name of the extended attribute within the type
  @param size
  @param sorted : 
  
  @retval 0 : found an pentry pointer to the extended attribute array in memory
  @retval < 0 : not found
*/
static int
ext4_xattr_find_entry(struct ext4_xattr_entry **pentry, int name_index,
		      const char *name, size_t size, int sorted)
{
	struct ext4_xattr_entry *entry;
	size_t name_len;
	int cmp = 1;

	if (name == NULL)
		return -EINVAL;
	name_len = strlen(name);
	entry = *pentry;
	for (; !IS_LAST_ENTRY(entry); entry = EXT4_XATTR_NEXT(entry)) {
		cmp = name_index - entry->e_name_index;
		if (!cmp)
			cmp = name_len - entry->e_name_len;
		if (!cmp)
			cmp = memcmp(name, entry->e_name, name_len);
		if (cmp <= 0 && (sorted || cmp == 0))
			break;
	}
	*pentry = entry;
	if (!cmp && ext4_xattr_check_entry(entry, size))
	{
	   severe("ext4_xattr_find_entry error:EIO");
	   return -EIO;
	}
	return cmp ? -ENODATA : 0;
}

/*
**_____________________________________________________________________________
*/
/**
*  Get the block that contains an extended attribute

   @param inode 
   @param name_index : type of the extended attribute
   @param name : name of the extended attribute
   @param buffer : buffer for storing extended attriute value
   @param buffer_size : size of the buffer
   
   @retval >=0 size on success
   @retval < 0 on error
*/
static int
ext4_xattr_block_get(lv2_entry_t *inode, int name_index, const char *name,
		     void *buffer, size_t buffer_size)
{
	struct buffer_head *bh = NULL;
	struct ext4_xattr_entry *entry;
	size_t size;
	int error;

	ea_idebug(inode, "name=%d.%s, buffer=%p, buffer_size=%ld",
		  name_index, name, buffer, (long)buffer_size);

	error = -ENODATA;
	/*
	** check if the extended inode exist
	*/
	if (!inode->attributes.s.i_file_acl)
		goto cleanup;
	ea_idebug(inode, "reading block %u", inode->attributes.s.i_file_acl);
	/*
	** attempt top read the extended inode block: it might be either
	** already in memory or we have to read it on disk
	*/
	bh = sb_bread(inode, inode->attributes.s.i_file_acl);
	if (!bh)
		goto cleanup;
	ea_bdebug(bh, "b_count=%d, refcount=%d",
		atomic_read(&(bh->b_count)), le32_to_cpu(BHDR(bh)->h_refcount));
	/*
	** check block consistency
	*/
	if (ext4_xattr_check_block(bh)) {
bad_block:
		EXT4_ERROR_INODE(inode, "bad block %llu",
				 (long long unsigned int)inode->attributes.s.i_file_acl);
	        severe("bad block error:EIO");
		error = -EIO;
		goto cleanup;
	}

	/*
	** search for the entry in the buffer
	*/
	entry = BFIRST(bh);
	error = ext4_xattr_find_entry(&entry, name_index, name, bh->b_size, 1);
	if (error == -EIO)
	{
	    goto bad_block;
	}
	if (error)
		goto cleanup;
	size = le32_to_cpu(entry->e_value_size);
	if (buffer) {
	        /*
		** application has request the value of the attribute: when size
		** only is request the buffer is NULL
		*/
		error = -ERANGE;
		if (size > buffer_size)
			goto cleanup;
		memcpy(buffer, bh->b_data + le16_to_cpu(entry->e_value_offs),
		       size);
	}
	error = size;

cleanup:
	brelse(bh);
	return error;
}

/*
**_____________________________________________________________________________
*/
/**
*  Get  an extended attribute from the root inode

   @param inode 
   @param name_index : type of the extended attribute
   @param name : name of the extended attribute
   @param buffer : buffer for storing extended attriute value
   @param buffer_size : size of the buffer
   
   @retval >=0 size on success
   @retval < 0 on error
*/

static int
ext4_xattr_ibody_get(lv2_entry_t *inode, int name_index, const char *name,
		     void *buffer, size_t buffer_size)
{
	struct ext4_xattr_ibody_header *header;
	struct ext4_xattr_entry *entry;
	struct ext4_inode *raw_inode;
	struct ext4_iloc iloc;
	size_t size;
	void *end;
	int error;

	if (!ext4_test_inode_state(inode, EXT4_STATE_XATTR))
		return -ENODATA;
	error = ext4_get_inode_loc(inode, &iloc);
	if (error)
		return error;
	raw_inode = ext4_raw_inode(&iloc);
	header = IHDR(inode, raw_inode);
	entry = IFIRST(header);
	end = (void *)raw_inode + ROZOFS_INODE_SZ;
	/*
	** check block consistency
	*/
	error = ext4_xattr_check_names(entry, end);
	if (error)
		goto cleanup;
        /*
	** attempt to get the extended attribut
	*/
	error = ext4_xattr_find_entry(&entry, name_index, name,
				      end - (void *)entry, 0);
	if (error)
		goto cleanup;
	size = le32_to_cpu(entry->e_value_size);
	if (buffer) {
	        /*
		** since buffer is not NULL? value is requested
		*/
		error = -ERANGE;
		if (size > buffer_size)
			goto cleanup;
		memcpy(buffer, (void *)IFIRST(header) +
		       le16_to_cpu(entry->e_value_offs), size);
	}
	error = size;

cleanup:
	brelse(iloc.bh);
	return error;
}
/*
**_____________________________________________________________________________
*/
/*
 * ext4_xattr_get()
 *
 * Copy an extended attribute into the buffer
 * provided, or compute the buffer size required.
 * Buffer is NULL to compute the size of the buffer required.
 *
 * Returns a negative error number on failure, or the number of bytes
 * used / required on success.
 */
int
ext4_xattr_get(lv2_entry_t *inode, int name_index, const char *name,
	       void *buffer, size_t buffer_size)
{
	int error;
        /*
	** get the extended attribut from the root inode
	*/
	error = ext4_xattr_ibody_get(inode, name_index, name, buffer,
				     buffer_size);
	if (error == -ENODATA)
	        /*
		** not found on root inode, attempt on extended inode
		*/
		error = ext4_xattr_block_get(inode, name_index, name, buffer,
					     buffer_size);
        errno = 0-error;
	return error;
}
/*
**_____________________________________________________________________________
*/

static int
ext4_xattr_list_entries(struct dentry *dentry, struct ext4_xattr_entry *entry,
			char *buffer, size_t buffer_size)
{
	size_t rest = buffer_size;

	for (; !IS_LAST_ENTRY(entry); entry = EXT4_XATTR_NEXT(entry)) {
		const struct xattr_handler *handler =
			ext4_xattr_handler(entry->e_name_index);

		if (handler) {
			size_t size = handler->list(dentry, buffer, rest,
						    entry->e_name,
						    entry->e_name_len,
						    handler->flags);
			if (buffer) {
				if (size > rest)
					return -ERANGE;
				buffer += size;
			}
			rest -= size;
		}
	}
	return buffer_size - rest;
}
/*
**_____________________________________________________________________________
*/
/**
*   Get the list of the extended attribute from the extended inode
*/
static int
ext4_xattr_block_list(struct dentry *dentry, char *buffer, size_t buffer_size)
{
	lv2_entry_t *inode = dentry->d_inode;
	struct buffer_head *bh = NULL;
	int error;

	ea_idebug(inode, "buffer=%p, buffer_size=%ld",
		  buffer, (long)buffer_size);

	error = 0;
	if (!inode->attributes.s.i_file_acl)
		goto cleanup;
	ea_idebug(inode, "reading block %u", inode->attributes.s.i_file_acl);
	bh = sb_bread(inode, inode->attributes.s.i_file_acl);
	error = -EIO;
	if (!bh)
	{
	  //severe("FDL error:EIO");
	  goto cleanup;
	}
	ea_bdebug(bh, "b_count=%d, refcount=%d",
		atomic_read(&(bh->b_count)), le32_to_cpu(BHDR(bh)->h_refcount));
	if (ext4_xattr_check_block(bh)) {
		EXT4_ERROR_INODE(inode, "bad block %llu",
				 (long long unsigned int)inode->attributes.s.i_file_acl);
         	//severe("FDL error:EIO");
		error = -EIO;
		goto cleanup;
	}
	error = ext4_xattr_list_entries(dentry, BFIRST(bh), buffer, buffer_size);

cleanup:
	brelse(bh);

	return error;
}
/*
**_____________________________________________________________________________
*/
/**
*  Get the list of the extended attributes from the root inode

*/
static int
ext4_xattr_ibody_list(struct dentry *dentry, char *buffer, size_t buffer_size)
{
	lv2_entry_t *inode = dentry->d_inode;
	struct ext4_xattr_ibody_header *header;
	struct ext4_inode *raw_inode;
	struct ext4_iloc iloc;
	void *end;
	int error;

	if (!ext4_test_inode_state(inode, EXT4_STATE_XATTR))
		return 0;
	error = ext4_get_inode_loc(inode, &iloc);
	if (error)
		return error;
	raw_inode = ext4_raw_inode(&iloc);
	header = IHDR(inode, raw_inode);
	end = (void *)raw_inode + ROZOFS_INODE_SZ;
	error = ext4_xattr_check_names(IFIRST(header), end);
	if (error)
		goto cleanup;
	error = ext4_xattr_list_entries(dentry, IFIRST(header),
					buffer, buffer_size);

cleanup:
	brelse(iloc.bh);
	return error;
}
/*
**_____________________________________________________________________________
*/
/*
 * ext4_xattr_list()
 *
 * Copy a list of attribute names into the buffer
 * provided, or compute the buffer size required.
 * Buffer is NULL to compute the size of the buffer required.
 *
 * Returns a negative error number on failure, or the number of bytes
 * used / required on success.
 */
static int
ext4_xattr_list(struct dentry *dentry, char *buffer, size_t buffer_size)
{
	int ret, ret2;

	ret = ret2 = ext4_xattr_ibody_list(dentry, buffer, buffer_size);
	if (ret < 0)
		goto errout;
	if (buffer) {
		buffer += ret;
		buffer_size -= ret;
	}
	ret = ext4_xattr_block_list(dentry, buffer, buffer_size);
	if (ret < 0)
		goto errout;
	ret += ret2;
errout:
	return ret;
}


/*
 * Release the xattr block BH: If the reference count is > 1, decrement
 * it; otherwise free the block.
 */
static void
ext4_xattr_release_block(void  *handle, lv2_entry_t *inode,
			 struct buffer_head *bh)
{
   export_tracking_table_t *trk_tb_p;
   rozofs_inode_t fake_inode; 
   fid_t fid;     
   

   trk_tb_p = xattr_get_tracking_context();  
   /*
   ** get the reference of the extendsed block to release
   */
   fake_inode.fid[1] = bh->b_blocknr;
   memcpy(fid,&fake_inode.fid[0],sizeof(fid_t));
   exp_attr_delete(trk_tb_p,fid);
 
#if 0
	int error = 0;

	lock_buffer(bh);
	if (BHDR(bh)->h_refcount == cpu_to_le32(1)) {
		ea_bdebug(bh, "refcount now=0; freeing");
		get_bh(bh);
		ext4_free_blocks(handle, inode, bh, 0, 1,
				 EXT4_FREE_BLOCKS_METADATA |
				 EXT4_FREE_BLOCKS_FORGET);
		unlock_buffer(bh);
	} else {
		le32_add_cpu(&BHDR(bh)->h_refcount, -1);
		unlock_buffer(bh);
		error = ext4_handle_dirty_metadata(handle, inode, bh);
		if (IS_SYNC(inode))
			ext4_handle_sync(handle);
		dquot_free_block(inode, EXT4_C2B(EXT4_SB(inode->i_sb), 1));
		ea_bdebug(bh, "refcount now=%d; releasing",
			  le32_to_cpu(BHDR(bh)->h_refcount));
	}
out:
	ext4_std_error(inode->i_sb, error);
	return;
#endif
}

/*
 * Find the available free space for EAs. This also returns the total number of
 * bytes used by EA entries.
 */
 size_t ext4_xattr_free_space(struct ext4_xattr_entry *last,
				    size_t *min_offs, void *base, int *total)
{
	for (; !IS_LAST_ENTRY(last); last = EXT4_XATTR_NEXT(last)) {
		*total += EXT4_XATTR_LEN(last->e_name_len);
		if (!last->e_value_block && last->e_value_size) {
			size_t offs = le16_to_cpu(last->e_value_offs);
			if (offs < *min_offs)
				*min_offs = offs;
		}
	}
	return (*min_offs - ((void *)last - base) - sizeof(__u32));
}

struct ext4_xattr_info {
	int name_index;
	const char *name;
	const void *value;
	size_t value_len;
};

struct ext4_xattr_search {
	struct ext4_xattr_entry *first;
	void *base;
	void *end;
	struct ext4_xattr_entry *here;
	int not_found;
};
/*
**_____________________________________________________________________________
*/
/**
*  insert a extended attribute in memory either in the root inode or extended inode

   note : to remove an entry that was there just set i.value = NULL.
   
   @param i: information related to the extended attribute to insert
   @param s: place where extended attribute with be inserted
   
   @retval 0 on success
   @retval < 0 on error
*/
static int
ext4_xattr_set_entry(struct ext4_xattr_info *i, struct ext4_xattr_search *s)
{
	struct ext4_xattr_entry *last;
	size_t free, min_offs = s->end - s->base, name_len = strlen(i->name);

	/* Compute min_offs and last. */
	last = s->first;
	for (; !IS_LAST_ENTRY(last); last = EXT4_XATTR_NEXT(last)) {
		if (!last->e_value_block && last->e_value_size) {
			size_t offs = le16_to_cpu(last->e_value_offs);
			if (offs < min_offs)
				min_offs = offs;
		}
	}
	free = min_offs - ((void *)last - s->base) - sizeof(__u32);
	if (!s->not_found) {
		if (!s->here->e_value_block && s->here->e_value_size) {
			size_t size = le32_to_cpu(s->here->e_value_size);
			free += EXT4_XATTR_SIZE(size);
		}
		free += EXT4_XATTR_LEN(name_len);
	}
	if (i->value) {
		if (free < EXT4_XATTR_SIZE(i->value_len) ||
		    free < EXT4_XATTR_LEN(name_len) +
			   EXT4_XATTR_SIZE(i->value_len))
			return -ENOSPC;
	}

	if (i->value && s->not_found) {
		/* Insert the new name. */
		size_t size = EXT4_XATTR_LEN(name_len);
		size_t rest = (void *)last - (void *)s->here + sizeof(__u32);
		memmove((void *)s->here + size, s->here, rest);
		memset(s->here, 0, size);
		s->here->e_name_index = i->name_index;
		s->here->e_name_len = name_len;
		memcpy(s->here->e_name, i->name, name_len);
	} else {
		if (!s->here->e_value_block && s->here->e_value_size) {
			void *first_val = s->base + min_offs;
			size_t offs = le16_to_cpu(s->here->e_value_offs);
			void *val = s->base + offs;
			size_t size = EXT4_XATTR_SIZE(
				le32_to_cpu(s->here->e_value_size));

			if (i->value && size == EXT4_XATTR_SIZE(i->value_len)) {
				/* The old and the new value have the same
				   size. Just replace. */
				s->here->e_value_size =
					cpu_to_le32(i->value_len);
				memset(val + size - EXT4_XATTR_PAD, 0,
				       EXT4_XATTR_PAD); /* Clear pad bytes. */
				memcpy(val, i->value, i->value_len);
				return 0;
			}

			/* Remove the old value. */
			memmove(first_val + size, first_val, val - first_val);
			memset(first_val, 0, size);
			s->here->e_value_size = 0;
			s->here->e_value_offs = 0;
			min_offs += size;

			/* Adjust all value offsets. */
			last = s->first;
			while (!IS_LAST_ENTRY(last)) {
				size_t o = le16_to_cpu(last->e_value_offs);
				if (!last->e_value_block &&
				    last->e_value_size && o < offs)
					last->e_value_offs =
						cpu_to_le16(o + size);
				last = EXT4_XATTR_NEXT(last);
			}
		}
		if (!i->value) {
			/* Remove the old name. */
			size_t size = EXT4_XATTR_LEN(name_len);
			last = ENTRY((void *)last - size);
			memmove(s->here, (void *)s->here + size,
				(void *)last - (void *)s->here + sizeof(__u32));
			memset(last, 0, size);
		}
	}

	if (i->value) {
		/* Insert the new value. */
		s->here->e_value_size = cpu_to_le32(i->value_len);
		if (i->value_len) {
			size_t size = EXT4_XATTR_SIZE(i->value_len);
			void *val = s->base + min_offs - size;
			s->here->e_value_offs = cpu_to_le16(min_offs - size);
			memset(val + size - EXT4_XATTR_PAD, 0,
			       EXT4_XATTR_PAD); /* Clear the pad bytes. */
			memcpy(val, i->value, i->value_len);
		}
	}
	return 0;
}

/*
**_____________________________________________________________________________
*/
/**
*  search for an attribute within the extended inode

   @param inode : pointer to the root inode
   @param i : pointer to the element to search
   @param bs
   
   @retval 0 found
   @retval < 0 not found
*/
struct ext4_xattr_block_find {
	struct ext4_xattr_search s;
	struct buffer_head *bh;
};

static int
ext4_xattr_block_find(lv2_entry_t *inode, struct ext4_xattr_info *i,
		      struct ext4_xattr_block_find *bs)
{
	int error;

	ea_idebug(inode, "name=%d.%s, value=%p, value_len=%ld",
		  i->name_index, i->name, i->value, (long)i->value_len);

	if (inode->attributes.s.i_file_acl) {
		/* The inode already has an extended attribute block. */
		/**
		*  get the inode that contains the extended attributes:
		*  it might be possible that the inode has already been loaded
		** in memory. In that case in sb_bread we need to check the memory
		** pointer. If the memory pointer is null, we should read it from disk
		*/
		bs->bh = sb_bread(inode, inode->attributes.s.i_file_acl);
		error = -EIO;
		if (!bs->bh)
		{
     	          //severe("FDL error:EIO");
		  goto cleanup;
		}
		ea_bdebug(bs->bh, "b_count=%d, refcount=%d",
			atomic_read(&(bs->bh->b_count)),
			le32_to_cpu(BHDR(bs->bh)->h_refcount));
		if (ext4_xattr_check_block(bs->bh)) {
			EXT4_ERROR_INODE(inode, "bad block %llu",
					 (long long unsigned int)inode->attributes.s.i_file_acl);
    	               // severe("FDL error:EIO");
			error = -EIO;
			goto cleanup;
		}
		/* Find the named attribute. */
		bs->s.base = BHDR(bs->bh);
		bs->s.first = BFIRST(bs->bh);
		bs->s.end = bs->bh->b_data + bs->bh->b_size;
		bs->s.here = bs->s.first;
		error = ext4_xattr_find_entry(&bs->s.here, i->name_index,
					      i->name, bs->bh->b_size, 1);
		if (error && error != -ENODATA)
			goto cleanup;
		bs->s.not_found = error;
	}
	error = 0;

cleanup:
	return error;
}
/*
**_____________________________________________________________________________
*/
/**
*  set an extended attribute in the extended inode

   @param handle
   @param inode : pointer to the root inode
   @param i: descriptor of the extended attribute
   @param bs: place where extended attribute can fit (might be null for a new extended attribute)
   
   @retval 0 on success
   @retval < 0 error
*/
static int
ext4_xattr_block_set(void  *handle, lv2_entry_t *inode,
		     struct ext4_xattr_info *i,
		     struct ext4_xattr_block_find *bs)
{
	struct buffer_head *new_bh = NULL;
	struct ext4_xattr_search *s = &bs->s;
	int error = 0;

#define header(x) ((struct ext4_xattr_header *)(x))
        /*
	** control against the max size of the extended attribute inode
	*/
	if (i->value && i->value_len > (ROZOFS_XATTR_BLOCK_SZ-sizeof(uint64_t)))
		return -ENOSPC;
	/*
	** check if the block exists in memory
	*/
	if (s->base) {
	       /*
	       ** the block exists so update it with the entry and then
	       ** push it to disk
	       */
		lock_buffer(bs->bh);
	       ea_bdebug(bs->bh, "modifying in-place");
	       /*
	       ** push the extended attribut in the current entry
	       */
	       error = ext4_xattr_set_entry(i, s);
	       unlock_buffer(bs->bh);
	       if (error == -EIO)
	       {
	          //severe("FDL error:EIO");
		  goto bad_block;
	       }
	       if (!error)
		       error = ext4_handle_dirty_metadata(0,inode,bs->bh);
	       if (error)
		       goto cleanup;
	       goto inserted;
	} 
	/* 
	** The extended block does not exist:
	** Allocate a buffer where we construct the new block. 
	** note in that case bs->bh is NULL since nothing has been read from disk
	*/
	s->base = kzalloc(ROZOFS_XATTR_BLOCK_SZ, GFP_NOFS);
	/* assert(header == s->base) */
	error = -ENOMEM;
	if (s->base == NULL)
		goto cleanup;
	header(s->base)->h_magic = cpu_to_le32(EXT4_XATTR_MAGIC);
	header(s->base)->h_blocks = cpu_to_le32(1);
	header(s->base)->h_refcount = cpu_to_le32(1);
	s->first = ENTRY(header(s->base)+1);
	s->here = ENTRY(header(s->base)+1);
	s->end = s->base + (ROZOFS_XATTR_BLOCK_SZ-sizeof(uint64_t));

	error = ext4_xattr_set_entry(i, s);
	if (error == -EIO)
	{
	   //severe("FDL error:EIO");
	   goto bad_block;
	}
	if (error)
		goto cleanup;

inserted:
	if (!IS_LAST_ENTRY(s->first)) {
                if (bs->bh && s->base == bs->bh->b_data) {
			/* We were modifying this block in-place. */
			ea_bdebug(bs->bh, "keeping this block");
			new_bh = bs->bh;
			get_bh(new_bh);
		} else {
			/* 
			** We need to allocate a new block 
			*/
			new_bh = sb_getblk(inode);
			if (!new_bh) {
			        /*
				** out of memory
				*/
				error = -ENOMEM;
				goto cleanup;
			}
			lock_buffer(new_bh);
			memcpy(new_bh->b_data, s->base, new_bh->b_size);
			unlock_buffer(new_bh);
			/*
			** allocate a block write the extended block and the metadata on disk
			*/
			error = ext4_handle_dirty_metadata(1,inode, new_bh);
			if (error)
				goto cleanup;
		}
	}

	/* Update the inode. */
	inode->attributes.s.i_file_acl = new_bh ? new_bh->b_blocknr : 0;

	/* Drop the previous xattr block. */
	if (bs->bh && bs->bh != new_bh)
		ext4_xattr_release_block(handle, inode, bs->bh);
	error = 0;

cleanup:
	brelse(new_bh);
	if (!(bs->bh && s->base == bs->bh->b_data))
		kfree(s->base);

	return error;


bad_block:
	EXT4_ERROR_INODE(inode, "bad block %llu",
			 (long long unsigned int)inode->attributes.s.i_file_acl);
	goto cleanup;

#undef header
}

/**
*  iloc contains a pointer to a bh structure and an offset
*/

struct ext4_xattr_ibody_find {
	struct ext4_xattr_search s;
	struct ext4_iloc iloc; 
};

/*
**_____________________________________________________________________________
*/
/**
*  get information related to an extended attributes

   @param inode : pointer to the root inode
   @param i: information about the extended attribute to search (name_index,name)/(value,value_len)
   @param is
   
   @retval 0 : found
   @retval  < 0 not found
*/
static int
ext4_xattr_ibody_find(lv2_entry_t *inode, struct ext4_xattr_info *i,
		      struct ext4_xattr_ibody_find *is)
{
	struct ext4_xattr_ibody_header *header;
	struct ext4_inode *raw_inode;
	int error;
        /**
	*  check of the inode has some extended attributes
	*/
	if (inode->attributes.s.i_extra_isize == 0)
	{
	   /*
	   ** no extended attributes in the major inode
	   */
           return 0;
	}
	/*
	** get the pointer to the xattr array
	*/
	raw_inode = ext4_raw_inode(&is->iloc);
	header = IHDR(inode, raw_inode);
	is->s.base = is->s.first = IFIRST(header);
	is->s.here = is->s.first;
	/*
	** the end is set at the end of the block: 512 is the size used by RozoFS
	*/
	is->s.end = (void *)raw_inode + ROZOFS_INODE_SZ;
	if (ext4_test_inode_state(inode, EXT4_STATE_XATTR)) {
		error = ext4_xattr_check_names(IFIRST(header), is->s.end);
		if (error)
			return error;
		/* Find the named attribute. */
		error = ext4_xattr_find_entry(&is->s.here, i->name_index,
					      i->name, is->s.end -
					      (void *)is->s.base, 0);
		if (error && error != -ENODATA)
			return error;
		is->s.not_found = error;
	}
	return 0;
}
/*
**_____________________________________________________________________________
*/
/**
*   set the body of an extended attribut in the inode

    @param handle
    @param inode : pointer to the root inode
    @param i: extended attribute to insert
    @param is: place where extended attribute will be inserted
    
    @retval 0 on sucess
*/
static int
ext4_xattr_ibody_set(void  *handle, lv2_entry_t *inode,
		     struct ext4_xattr_info *i,
		     struct ext4_xattr_ibody_find *is)
{
	struct ext4_xattr_ibody_header *header;
	struct ext4_xattr_search *s = &is->s;
	int error;
        /*
	** check if it remains some space in the root inode
	*/
	if (inode->attributes.s.i_extra_isize == 0)
		return -ENOSPC;
	error = ext4_xattr_set_entry(i, s);
	if (error)
		return error;
	header = IHDR(inode, ext4_raw_inode(&is->iloc));
	if (!IS_LAST_ENTRY(s->first)) {
		header->h_magic = cpu_to_le32(EXT4_XATTR_MAGIC);
		ext4_set_inode_state(inode, EXT4_STATE_XATTR);
	} else {
		header->h_magic = cpu_to_le32(0);
		ext4_clear_inode_state(inode, EXT4_STATE_XATTR);
	}
	return 0;
}
/*
**_____________________________________________________________________________
*/
/*
 * ext4_xattr_set_handle()
 *
 * Create, replace or remove an extended attribute for this inode.  Value
 * is NULL to remove an existing extended attribute, and non-NULL to
 * either replace an existing extended attribute, or create a new extended
 * attribute. The flags XATTR_REPLACE and XATTR_CREATE
 * specify that an extended attribute must exist and must not exist
 * previous to the call, respectively.
 *
 * Returns 0, or a negative error number on failure.
 */
int
ext4_xattr_set_handle(void  *handle, lv2_entry_t *inode, int name_index,
		      const char *name, const void *value, size_t value_len,
		      int flags)
{
	struct ext4_xattr_info i = {
		.name_index = name_index,
		.name = name,
		.value = value,
		.value_len = value_len,

	};
	struct ext4_xattr_ibody_find is = {
		.s = { .not_found = -ENODATA, },
	};
	struct ext4_xattr_block_find bs = {
		.s = { .not_found = -ENODATA, },
	};
	unsigned long no_expand;
	int error;

	if (!name)
		return -EINVAL;
	if (strlen(name) > 255)
		return -ERANGE;

	no_expand = ext4_test_inode_state(inode, EXT4_STATE_NO_EXPAND);
	ext4_set_inode_state(inode, EXT4_STATE_NO_EXPAND);
        /**
	* searching for the extended attribute within the root inode
	*/
	error = ext4_get_inode_loc(inode, &is.iloc);
	if (error)
		goto cleanup;

	error = ext4_xattr_ibody_find(inode, &i, &is);
	if (error)
		goto cleanup;
	if (is.s.not_found)
	        /*
		** the extended attribute has not been found, search within the extended inode
		*/
		error = ext4_xattr_block_find(inode, &i, &bs);
	if (error)
		goto cleanup;
	if (is.s.not_found && bs.s.not_found) {
                /*
		** neither in root inode nor extended inode
		*/
      		error = -ENODATA;
		if (flags & XATTR_REPLACE)
			goto cleanup;
		error = 0;
		if (!value)
			goto cleanup;
	} else {
		error = -EEXIST;
		if (flags & XATTR_CREATE)
			goto cleanup;
	}
	if (!value) {
	        /*
		** insert an extended attribut with no value associated with it
		*/
		if (!is.s.not_found)
		        /*
			** insert in the root inode
			*/
			error = ext4_xattr_ibody_set(handle, inode, &i, &is);
		else if (!bs.s.not_found)
		        /*
			** insert in the extended inode
			*/
			error = ext4_xattr_block_set(handle, inode, &i, &bs);
	} else {
	        /*
		** attempt to insert the extended attribute in the root inode
		*/
		error = ext4_xattr_ibody_set(handle, inode, &i, &is);
		if (!error && !bs.s.not_found) {    
		        /*
			** the extended attribute was in the extended inode, so remove it
			*/
			i.value = NULL;
			error = ext4_xattr_block_set(handle, inode, &i, &bs);
		} else if (error == -ENOSPC) {
			if (inode->attributes.s.i_file_acl && !bs.s.base) {
				error = ext4_xattr_block_find(inode, &i, &bs);
				if (error)
					goto cleanup;
			}
			error = ext4_xattr_block_set(handle, inode, &i, &bs);
			if (error)
				goto cleanup;
			if (!is.s.not_found) {
				i.value = NULL;
				error = ext4_xattr_ibody_set(handle, inode, &i,
							     &is);
			}
		}
	}
	if (!error) {
		if (!value)
			ext4_clear_inode_state(inode, EXT4_STATE_NO_EXPAND);
		error = ext4_mark_iloc_dirty(handle, inode, &is.iloc);
		/*
		 * The bh is consumed by ext4_mark_iloc_dirty, even with
		 * error != 0.
		 */
		is.iloc.bh = NULL;
	}

cleanup:
	brelse(is.iloc.bh);
	brelse(bs.bh);
	if (no_expand == 0)
		ext4_clear_inode_state(inode, EXT4_STATE_NO_EXPAND);
	return error;
}
/*
**_____________________________________________________________________________
*/
/*
 * ext4_xattr_set()
 *
 * Like ext4_xattr_set_handle, but start from an inode. This extended
 * attribute modification is a filesystem transaction by itself.
 *
 * Returns 0, or a negative error number on failure.
 */
int
ext4_xattr_set(lv2_entry_t *inode, int name_index, const char *name,
	       const void *value, size_t value_len, int flags)
{
	int error;
	void *handle=NULL;


       error = ext4_xattr_set_handle(handle, inode, name_index, name,
				     value, value_len, flags);
        errno = 0-error;
	return error;
}

/*
 * ext4_xattr_delete_inode()
 *
 * Free extended attribute resources associated with this inode. This
 * is called immediately before an inode is freed. We have exclusive
 * access to the inode.
 */
void
ext4_xattr_delete_inode(void  *handle, lv2_entry_t *inode)
{
	struct buffer_head *bh = NULL;

	if (!inode->attributes.s.i_file_acl)
		goto cleanup;
	bh = sb_bread(inode, inode->attributes.s.i_file_acl);
	if (!bh) {
		EXT4_ERROR_INODE(inode, "block %llu read error",
				 (long long unsigned int)inode->attributes.s.i_file_acl);
		goto cleanup;
	}
	if (BHDR(bh)->h_magic != cpu_to_le32(EXT4_XATTR_MAGIC) ||
	    BHDR(bh)->h_blocks != cpu_to_le32(1)) {
		EXT4_ERROR_INODE(inode, "bad block %llu",
				 (long long unsigned int)inode->attributes.s.i_file_acl);
		goto cleanup;
	}
	ext4_xattr_release_block(handle, inode, bh);
	inode->attributes.s.i_file_acl = 0;

cleanup:
	brelse(bh);
}




int
ext4_init_xattr(void)
{

	return 0;
}

void
ext4_exit_xattr(void)
{

	ext4_xattr_cache = NULL;
}
