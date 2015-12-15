/*
  File: fs/ext4/acl.h

  (C) 2001 Andreas Gruenbacher, <a.gruenbacher@computer.org>
*/
#ifndef ACL_H
#define ACL_H
#include "posix_acl_attr.h"


#define cpu_to_le32(val) val
#define le32_to_cpu(val) val
#define le16_to_cpu(val) val
#define cpu_to_le16(val) val
#define le32_add_cpu(val1,val2) (val1+val2)


#define ACL_UNDEFINED_ID	(-1)

/* a_type field in acl_user_posix_entry_t */
#define ACL_TYPE_ACCESS		(0x8000)
#define ACL_TYPE_DEFAULT	(0x4000)

/* e_tag entry in struct posix_acl_entry */
#define ACL_USER_OBJ		(0x01)
#define ACL_USER		(0x02)
#define ACL_GROUP_OBJ		(0x04)
#define ACL_GROUP		(0x08)
#define ACL_MASK		(0x10)
#define ACL_OTHER		(0x20)

/* permissions in the e_perm field */
#define ACL_READ		(0x04)
#define ACL_WRITE		(0x02)
#define ACL_EXECUTE		(0x01)
//#define ACL_ADD		(0x08)
//#define ACL_DELETE		(0x10)

struct posix_acl_entry {
	short			e_tag;
	unsigned short		e_perm;
	unsigned int		e_id;
};

struct posix_acl {
//	union {
//		atomic_t		a_refcount;
//		struct rcu_head		a_rcu;
//	};
	unsigned int		a_count;
	struct posix_acl_entry	a_entries[0];
};

#define FOREACH_ACL_ENTRY(pa, acl, pe) \
	for(pa=(acl)->a_entries, pe=pa+(acl)->a_count; pa<pe; pa++)



#define EXT4_ACL_VERSION	0x0001

typedef struct {
	__le16		e_tag;
	__le16		e_perm;
	__le32		e_id;
} ext4_acl_entry;

typedef struct {
	__le16		e_tag;
	__le16		e_perm;
} ext4_acl_entry_short;

typedef struct {
	__le32		a_version;
} ext4_acl_header;

static inline size_t ext4_acl_size(int count)
{
	if (count <= 4) {
		return sizeof(ext4_acl_header) +
		       count * sizeof(ext4_acl_entry_short);
	} else {
		return sizeof(ext4_acl_header) +
		       4 * sizeof(ext4_acl_entry_short) +
		       (count - 4) * sizeof(ext4_acl_entry);
	}
}

static inline int ext4_acl_count(size_t size)
{
	ssize_t s;
	size -= sizeof(ext4_acl_header);
	s = size - 4 * sizeof(ext4_acl_entry_short);
	if (s < 0) {
		if (size % sizeof(ext4_acl_entry_short))
			return -1;
		return size / sizeof(ext4_acl_entry_short);
	} else {
		if (s % sizeof(ext4_acl_entry))
			return -1;
		return s / sizeof(ext4_acl_entry) + 4;
	}
}




#endif 

