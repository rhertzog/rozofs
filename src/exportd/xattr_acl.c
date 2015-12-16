/*
 * linux/fs/ext4/acl.c
 *
 * Copyright (C) 2001-2003 Andreas Gruenbacher, <agruen@suse.de>
 */



#include <string.h>
#include <linux/fs.h>
#include <errno.h>
//#include "export.h"
//#include "ext4_jbd2.h"
#include "rozofs_ext4.h"
#include "xattr.h"
#include <rozofs/common/acl.h>


struct posix_acl *rozofs_acl_p = NULL;

/*
 * Free an ACL handle.
 */
static inline void
posix_acl_release(struct posix_acl *acl)
{
//		free(acl);
}

/*
 * Init a fresh posix_acl
 */
void
posix_acl_init(struct posix_acl *acl, int count)
{
	acl->a_count = count;
}

/*
 * Allocate a new ACL with the specified number of entries.
 */
struct posix_acl *
posix_acl_alloc(int count)
{
//	const size_t size = sizeof(struct posix_acl) +
//	                    count * sizeof(struct posix_acl_entry);
       /*
       ** Check if the buffer for acl management has been allocated
       */
       if (rozofs_acl_p == NULL)
       {
         rozofs_acl_p  = malloc(4096);
       }
       if (rozofs_acl_p)
		posix_acl_init(rozofs_acl_p, count);
	return rozofs_acl_p;
}

/*
 * Convert from extended attribute to in-memory representation.
 */
struct posix_acl *
posix_acl_from_xattr(const void *value, size_t size)
{
	posix_acl_xattr_header *header = (posix_acl_xattr_header *)value;
	posix_acl_xattr_entry *entry = (posix_acl_xattr_entry *)(header+1), *end;
	int count;
	struct posix_acl *acl;
	struct posix_acl_entry *acl_e;

	if (!value)
		return NULL;
	if (size < sizeof(posix_acl_xattr_header))
		 return NULL; //ERR_PTR(-EINVAL);
	if (header->a_version != cpu_to_le32(POSIX_ACL_XATTR_VERSION))
		return NULL; //ERR_PTR(-EOPNOTSUPP);

	count = posix_acl_xattr_count(size);
	if (count < 0)
		return NULL; //ERR_PTR(-EINVAL);
	if (count == 0)
		return NULL;
	
	acl = posix_acl_alloc(count);
	if (!acl)
		return NULL; //ERR_PTR(-ENOMEM);
	acl_e = acl->a_entries;
	
	for (end = entry + count; entry != end; acl_e++, entry++) {
		acl_e->e_tag  = le16_to_cpu(entry->e_tag);
		acl_e->e_perm = le16_to_cpu(entry->e_perm);

		switch(acl_e->e_tag) {
			case ACL_USER_OBJ:
			case ACL_GROUP_OBJ:
			case ACL_MASK:
			case ACL_OTHER:
				acl_e->e_id = ACL_UNDEFINED_ID;
				break;

			case ACL_USER:
			case ACL_GROUP:
				acl_e->e_id = le32_to_cpu(entry->e_id);
				break;

			default:
				goto fail;
		}
	}
	return acl;

fail:
	posix_acl_release(acl);
	return NULL; //ERR_PTR(-EINVAL);
}

/*
 * Returns 0 if the acl can be exactly represented in the traditional
 * file mode permission bits, or else 1. Returns -E... on error.
 */
#define S_IRWXUGO	(S_IRWXU|S_IRWXG|S_IRWXO)
int
posix_acl_equiv_mode(const struct posix_acl *acl, mode_t *mode_p)
{
	const struct posix_acl_entry *pa, *pe;
	mode_t mode = 0;
	int not_equiv = 0;

	FOREACH_ACL_ENTRY(pa, acl, pe) {
		switch (pa->e_tag) {
			case ACL_USER_OBJ:
				mode |= (pa->e_perm & S_IRWXO) << 6;
				break;
			case ACL_GROUP_OBJ:
				mode |= (pa->e_perm & S_IRWXO) << 3;
				break;
			case ACL_OTHER:
				mode |= pa->e_perm & S_IRWXO;
				break;
			case ACL_MASK:
				mode = (mode & ~S_IRWXG) |
				       ((pa->e_perm & S_IRWXO) << 3);
				not_equiv = 1;
				break;
			case ACL_USER:
			case ACL_GROUP:
				not_equiv = 1;
				break;
			default:
				return -1;
		}
	}
        if (mode_p)
                *mode_p = (*mode_p & ~S_IRWXUGO) | mode;
        return not_equiv;
}

/**
**________________________________________________________________
*/
/**
*  Check if the extended attributes are related to acl access

   @param name: name of the extended attribute
   @param value: value of the extended attribute
   @param size: size of the extended attributes
   
   @retval -1 error
   @retval 0 extended attribute is acl_access with equivalent mode
   @retval 1 extended attribute is acl_access with no equivalent mode
   
*/
int rozofs_acl_access_check(const char *name, const char *value, size_t size,mode_t *mode_p)
{
    struct posix_acl *acl_p;
    int ret;
    
    if (strcmp(name,POSIX_ACL_XATTR_ACCESS)!=0)
    {
      return -1;    
    }
    /*
    ** Parse the acl 
    */
    acl_p = posix_acl_from_xattr(value, size);
    if (acl_p != NULL)
    {
      ret = posix_acl_equiv_mode(acl_p,mode_p);
      posix_acl_release(acl_p);
    }
    return ret;
}

/*
 * Extended attribute handlers
 */
static size_t
ext4_xattr_list_acl_access(struct dentry *dentry, char *list, size_t list_len,
			   const char *name, size_t name_len, int type)
{
	const size_t size = sizeof(POSIX_ACL_XATTR_ACCESS);

//	if (!test_opt(dentry->d_sb, POSIX_ACL))
//		return 0;
	if (list && size <= list_len)
		memcpy(list, POSIX_ACL_XATTR_ACCESS, size);
	return size;
}

static size_t
ext4_xattr_list_acl_default(struct dentry *dentry, char *list, size_t list_len,
			    const char *name, size_t name_len, int type)
{
	const size_t size = sizeof(POSIX_ACL_XATTR_DEFAULT);

//	if (!test_opt(dentry->d_sb, POSIX_ACL))
//		return 0;
	if (list && size <= list_len)
		memcpy(list, POSIX_ACL_XATTR_DEFAULT, size);
	return size;
}

static int
ext4_xattr_get_acl(struct dentry *dentry, const char *name, void *buffer,
		   size_t size, int type)
{
	int name_index;

	if (strcmp(name, "") != 0)
		return -EINVAL;
//	if (!test_opt(dentry->d_sb, POSIX_ACL))
//		return -EOPNOTSUPP;

	switch (type) {
	case ACL_TYPE_ACCESS:
		name_index = EXT4_XATTR_INDEX_POSIX_ACL_ACCESS;
		break;
	case ACL_TYPE_DEFAULT:
		name_index = EXT4_XATTR_INDEX_POSIX_ACL_DEFAULT;
		break;
	default:
		return -EINVAL;
	}
	return ext4_xattr_get(dentry->d_inode, name_index,
			      name, buffer, size);

}

static int
ext4_xattr_set_acl(struct dentry *dentry, const char *name, const void *value,
		   size_t size, int flags, int type)
{
	int name_index;

	if (strcmp(name, "") != 0)
		return -EINVAL;

	switch (type) {
	case ACL_TYPE_ACCESS:
		name_index = EXT4_XATTR_INDEX_POSIX_ACL_ACCESS;
		break;
	case ACL_TYPE_DEFAULT:
		name_index = EXT4_XATTR_INDEX_POSIX_ACL_DEFAULT;
		break;
	default:
		return -EINVAL;
	}
		
	return ext4_xattr_set(dentry->d_inode, name_index,
		      name, value, size, flags);

}

const struct xattr_handler ext4_xattr_acl_access_handler = {
	.prefix	= POSIX_ACL_XATTR_ACCESS,
	.flags	= ACL_TYPE_ACCESS,
	.list	= ext4_xattr_list_acl_access,
	.get	= ext4_xattr_get_acl,
	.set	= ext4_xattr_set_acl,
};

const struct xattr_handler ext4_xattr_acl_default_handler = {
	.prefix	= POSIX_ACL_XATTR_DEFAULT,
	.flags	= ACL_TYPE_DEFAULT,
	.list	= ext4_xattr_list_acl_default,
	.get	= ext4_xattr_get_acl,
	.set	= ext4_xattr_set_acl,
};
