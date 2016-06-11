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

#ifndef _MATTR_H
#define _MATTR_H

#include <rozofs/rozofs.h>


/*
** Constant for file resizing
*/
#define ROZOFS_RESIZEA 0x20524553495A4541LL
#define ROZOFS_RESIZEM 0x20524553495A454DLL


/** API meta attributes functions.
 */

/** all we need to know about a managed file.
 *
 * attributes fid, cid, sids are rozofs data storage information.
 * others are a used the same way struct stat(2)
 */
 #define ROZOFS_HAS_XATTRIBUTES 0x80000000
 
static inline void rozofs_set_xattr_flag(uint32_t *mode) 
{*mode &= (~ROZOFS_HAS_XATTRIBUTES);} 

static inline void  rozofs_clear_xattr_flag(uint32_t *mode) {*mode |= (ROZOFS_HAS_XATTRIBUTES);}

static inline int rozofs_has_xattr(uint32_t mode)
 {
    return (mode&ROZOFS_HAS_XATTRIBUTES)?0:1; 
 }
 
 typedef struct mattr {
    fid_t fid;                      /**< unique file id */
    sid_t sids[ROZOFS_SAFE_MAX];    /**< sid of storage nodes target (regular file only)*/
    cid_t cid;                      /**< cluster id 0 for non regular files */
    uint32_t mode;                  /**< see stat(2) */
    uint32_t uid;                   /**< see stat(2) */
    uint32_t gid;                   /**< see stat(2) */
    uint16_t nlink;                 /**< see stat(2) */
    uint64_t ctime;                 /**< see stat(2) */
    uint64_t atime;                 /**< see stat(2) */
    uint64_t mtime;                 /**< see stat(2) */
    uint64_t size;                  /**< see stat(2) */
    uint32_t children;              /**< number of children (excluding . and ..) */
} mattr_t;


#if 0
/**
*  extended attributes structure
*/
#define ROZOFS_OBJ_NAME_MAX 96
#define ROZOFS_OBJ_MAX_SUFFIX 8
typedef union
{
   char inode_buf[512];
   struct {
     mattr_t attrs;  /**< standard attributes       */
     fid_t   pfid;   /**< parent fid                */
     uint32_t i_extra_isize;  /**< array reserved for extended attributes */
     uint32_t i_state;     /**< inode state               */
     uint64_t i_file_acl;  /**< extended inode */
     uint64_t i_link_name;  /**< symlink block */
     mdirent_fid_name_info_t fname;  /**< reference of the name within the dentry file */
     char     suffix[ROZOFS_OBJ_MAX_SUFFIX];
//     char    name[ROZOFS_OBJ_NAME_MAX]; 
   } s;
} ext_mattr_t;

#define ROZOFS_I_EXTRA_ISIZE (sizeof(mattr_t)+sizeof(fid_t)+ \
                              2*sizeof(uint32_t)+sizeof(uint64_t)+\
			      ROZOFS_OBJ_NAME_MAX)
#else
#define ROZOFS_OBJ_NAME_MAX 60
#define ROZOFS_OBJ_MAX_SUFFIX 16
/**
*  structure used for tracking the location of the fid and name of the object
*/
typedef struct _mdirent_fid_name_info_t
{
    uint16_t coll:1;   /**< asserted to 1 if coll_idx is significant  */
    uint16_t root_idx:15;   /**< index of the root file  */
    uint16_t coll_idx;   /**< index of the collision file */
    uint16_t chunk_idx:12; 
    uint16_t nb_chunk :4;
} mdirent_fid_name_info_t;

#define ROZOFS_FNAME_TYPE_DIRECT 0
#define ROZOFS_FNAME_TYPE_INDIRECT 1
typedef struct _inode_fname_t
{
   uint16_t name_type:1;
   uint16_t len:15;
   uint16_t hash_suffix;
   union
   {
     char name[ROZOFS_OBJ_NAME_MAX]; /**< direct case   */
     struct
     {
       mdirent_fid_name_info_t name_dentry;
       char suffix[ROZOFS_OBJ_MAX_SUFFIX];     
     } s;
   };
 } rozofs_inode_fname_t;


typedef union
{
   char inode_buf[512];
   struct inode_internal_t {
     mattr_t attrs;      /**< standard attributes       */
     uint64_t cr8time;   /**< creation time          */
     fid_t   pfid;   /**< parent fid                */
     uint32_t hash1;   /**< parent/name hash1  */
     uint32_t hash2;   /**< parent/name hash2  */
     uint32_t i_extra_isize;  /**< array reserved for extended attributes */
     uint32_t i_state;     /**< inode state               */
     uint64_t i_file_acl;  /**< extended inode */
     uint64_t i_link_name;  /**< symlink block */
     uint64_t hpc_reserved;  /**< reserved for hpc */
     rozofs_inode_fname_t fname;  /**< reference of the name within the dentry file */
   } s;
} ext_mattr_t;

#define ROZOFS_I_EXTRA_ISIZE (sizeof(struct inode_internal_t))

#define ROZOFS_I_EXTRA_ISIZE_BIS (sizeof(ext_mattr_t) -sizeof(struct inode_internal_t))
#endif

/** initialize mattr_t
 *
 * fid is not initialized
 * cid is set to UINT16_MAX (serve to detect unset value)
 * sids is filled with 0
 *
 * @param mattr: the mattr to initialize.
 */
void mattr_initialize(mattr_t *mattr);

/** initialize mattr_t
 *
 * fid is not initialized
 * cid is set to UINT16_MAX (serve to detect unset value)
 * sids is filled with 0
 *
 * @param mattr: the mattr to release.
 */
void mattr_release(mattr_t *mattr);
/*
**__________________________________________________________________
*/
/**
* store the file name in the inode
  The way the name is stored depends on the size of
  the filename: when the name is less than 62 bytes
  it is directly stored in the inode
  
  @param inode_fname_p: pointer to the array used for storing object name
  @param name: name of the object
  @param dentry_fname_info_p :pointer to the array corresponding to the fname in dentry
*/
void exp_store_fname_in_inode(rozofs_inode_fname_t *inode_fname_p,
                              char *name,
			      mdirent_fid_name_info_t *dentry_fname_info_p);

/*
**__________________________________________________________________
*/
/**
* store the directory name in the inode
  The way the name is stored depends on the size of
  the filename: when the name is less than 62 bytes
  it is directly stored in the inode
  
  @param inode_fname_p: pointer to the array used for storing object name
  @param name: name of the object
  @param dentry_fname_info_p :pointer to the array corresponding to the fname in dentry
*/
void exp_store_dname_in_inode(rozofs_inode_fname_t *inode_fname_p,
                              char *name,
			      mdirent_fid_name_info_t *dentry_fname_info_p);
#endif
