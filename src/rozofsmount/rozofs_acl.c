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
#include <string.h>
#include <linux/fs.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <linux/types.h>
#include <errno.h>
#include <fcntl.h>
#include <rozofs/common/log.h>
#include <rozofs/common/acl.h>

#define _XOPEN_SOURCE 700

struct posix_acl *rozofs_acl_p = NULL;
/**
**________________________________________________________________
*/
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
/**
**________________________________________________________________
*/
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
/**
**________________________________________________________________
*/
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
		 return NULL; 
	if (header->a_version != cpu_to_le32(POSIX_ACL_XATTR_VERSION))
		return NULL; 

	count = posix_acl_xattr_count(size);
	if (count < 0)
		return NULL; 
	if (count == 0)
		return NULL;
	
	acl = posix_acl_alloc(count);
	if (!acl)
		return NULL; 
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
	return NULL; 
}
/**
**________________________________________________________________
*/
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
//      if (ret == 0) info("FDL mode %x",mode);
      posix_acl_release(acl_p);
    }
    return ret;
}
