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
 #include <stdint.h>
 #include <unistd.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/rpc/export_profiler.h>
#include <src/exportd/export.h>

 uint32_t export_profiler_eid;
export_one_profiler_t  * export_profiler[EXPGW_EID_MAX_IDX+1] = { 0 };
extern export_t *fake_export_p;
int dirent_current_eid;


/*
** return the pointer to the exportd structure
**
**   For the case of the test we use a fake exportd*
*/

export_t *exports_lookup_export(eid_t eid) {


    return fake_export_p;
}

/**
* internal structure used for bitmap root_idx: only for directory
*/
#define DIRENT_MAX_IDX_FOR_EXPORT 4096
#define DIRENT_MASK_FOR_EXPORT (4096-1)
#define DIRENT_FILE_BYTE_BITMAP_SZ (DIRENT_MAX_IDX_FOR_EXPORT/8)
typedef struct _dirent_dir_root_idx_bitmap_t
{
   int dirty; /**< assert to one if the bitmap must be re-written on disk */
   char bitmap[DIRENT_FILE_BYTE_BITMAP_SZ];
} dirent_dir_root_idx_bitmap_t;



/** build a full path based on export root and fid of the lv2 file
 *
 * lv2 is the second level of files or directories in storage of metadata
 * they are acceded thru mreg or mdir API according to their type.
 *
 * @param root_path: root path of the exportd
 * @param fid: the fid we are looking for
 * @param path: the path to fill in
 */

static inline int export_lv2_resolve_path_internal(char *root_path, fid_t fid, char *path) {
    uint32_t slice;
    uint32_t subslice;
    char str[37];

    /*
     ** extract the slice and subsclie from the fid
     */
    mstor_get_slice_and_subslice(fid, &slice, &subslice);
    /*
     ** convert the fid in ascii
     */
    uuid_unparse(fid, str);
    sprintf(path, "%s/%d/%s", root_path, slice, str);
    return 0;

    return -1;
}

/** build a full path based on export root and fid of the lv2 file
 *
 * lv2 is the second level of files or directories in storage of metadata
 * they are acceded thru mreg or mdir API according to their type.
 *
 * @param export: the export we are searching on
 * @param fid: the fid we are looking for
 * @param path: the path to fill in
 */

int export_lv2_resolve_path(export_t *export, fid_t fid, char *path) {
    int ret;

    START_PROFILING(export_lv2_resolve_path);

    ret = export_lv2_resolve_path_internal(export->root, fid, path);

    STOP_PROFILING(export_lv2_resolve_path);
    return ret;
}



/*
 **__________________________________________________________________
 */
/**
* service to check if the bitmap for root_idx must be loaded

  @param lvl2 : level 2 entry
  @param fid : file id of the directory
  @param e:   pointer to the export structure
  
  @retval 0 on success
  @retval < 0 on error
*/

int export_dir_load_root_idx_bitmap(export_t *e,fid_t fid,lv2_entry_t *lvl2)
{
   int fd = -1;
   char node_path[PATH_MAX];
   char lv3_path[PATH_MAX];
   dirent_dir_root_idx_bitmap_t *bitmap_p;

   if (lvl2->dirent_root_idx_p != NULL)
   {
     /*
     ** already loaded
     */
     return 0;   
   }
   /*
   ** allocate the memory
   */
   if (lvl2->dirent_root_idx_p == NULL)
   {
     lvl2->dirent_root_idx_p = malloc(sizeof(dirent_dir_root_idx_bitmap_t));
     if (lvl2->dirent_root_idx_p == NULL) goto error;
   }
   bitmap_p = (dirent_dir_root_idx_bitmap_t*)lvl2->dirent_root_idx_p;
   /*
   ** read the bitmap from disk
   */    
   if (export_lv2_resolve_path(e, fid, node_path) != 0) goto error;
   sprintf(lv3_path, "%s/%s", node_path, MDIR_ATTRS_FNAME);   
   if ((fd = open(lv3_path, O_RDONLY | O_NOATIME, S_IRWXU)) < 0) 
   {
     goto error;
   }
   ssize_t len = pread(fd,bitmap_p->bitmap,DIRENT_FILE_BYTE_BITMAP_SZ,0);
   if (len != DIRENT_FILE_BYTE_BITMAP_SZ) goto error;
   /*
   ** clear the dirty bit
   */
   bitmap_p->dirty = 0;
   /*
   ** close the file
   */
   close(fd);
   return 0;
   
error:
   if (fd != -1) close(fd);
   if (lvl2->dirent_root_idx_p != NULL)
   {
      free(lvl2->dirent_root_idx_p);
      lvl2->dirent_root_idx_p = NULL;  
   }
   return -1;
}

/*
**__________________________________________________________________
*/
/**
*   update the root_idx bitmap in memory

   @param ctx_p: pointer to the level2 cache entry
   @param root_idx : root index to update
   @param set : assert to 1 when the root_idx is new/ 0 for removing
   

*/
void export_dir_update_root_idx_bitmap(void *ctx_p,int root_idx,int set)
{
    uint16_t byte_idx;
    int bit_idx ;
    dirent_dir_root_idx_bitmap_t *bitmap_p;
    
    if (ctx_p == NULL) return;
    
    bitmap_p = (dirent_dir_root_idx_bitmap_t*)ctx_p;
    
    if (root_idx >DIRENT_MAX_IDX_FOR_EXPORT) return;
    
    byte_idx = root_idx/8;
    bit_idx =  root_idx%8;
    if (set)
    {
       if (bitmap_p->bitmap[byte_idx] & (1<<bit_idx)) return;
       bitmap_p->bitmap[byte_idx] |= 1<<bit_idx;    
    }
    else
    {
       bitmap_p->bitmap[byte_idx] &=~(1<<bit_idx);        
    }
    bitmap_p->dirty = 1;
}
/*
**__________________________________________________________________
*/
/**
*   check the presence of a root_idx  in the bitmap 

   @param ctx_p: pointer to the level2 cache entry
   @param root_idx : root index to update

  @retval 1 asserted
  @retval 0 not set   

*/
int export_dir_check_root_idx_bitmap_bit(void *ctx_p,int root_idx)
{
    uint16_t byte_idx;
    int bit_idx ;
    dirent_dir_root_idx_bitmap_t *bitmap_p;
    
    if (ctx_p == NULL) return 1;
    
    bitmap_p = (dirent_dir_root_idx_bitmap_t*)ctx_p;
    if (root_idx >DIRENT_MAX_IDX_FOR_EXPORT) return 1;
    
    byte_idx = root_idx/8;
    bit_idx =  root_idx%8;

    if (bitmap_p->bitmap[byte_idx] & (1<<bit_idx)) 
    {
      return 1;
    }
    return 0;
}
/*
**__________________________________________________________________
*/
/**
* service to flush on disk the root_idx bitmap if it is dirty

  @param bitmap_p : pointer to the root_idx bitmap
  @param fid : file id of the directory
  @param e:   pointer to the export structure
  
  @retval 0 on success
  @retval < 0 on error
*/

int export_dir_flush_root_idx_bitmap(export_t *e,fid_t fid,dirent_dir_root_idx_bitmap_t *bitmap_p)
{
   int fd = -1;
   char node_path[PATH_MAX];
   char lv3_path[PATH_MAX];

   if (bitmap_p == NULL)
   {
     /*
     ** nothing to flush
     */
     return 0;   
   }
   if (bitmap_p->dirty == 0) return 0;
   /*
   ** bitmap has changed :write the bitmap on disk
   */    
   if (export_lv2_resolve_path(e, fid, node_path) != 0) goto error;
   
   sprintf(lv3_path, "%s/%s", node_path, MDIR_ATTRS_FNAME);   
   if ((fd = open(lv3_path, O_WRONLY | O_CREAT | O_NOATIME, S_IRWXU)) < 0) {
        goto error;
   }
   ssize_t len = pwrite(fd,bitmap_p->bitmap,DIRENT_FILE_BYTE_BITMAP_SZ,0);
   if (len != DIRENT_FILE_BYTE_BITMAP_SZ) goto error;
   /*
   ** clear the dirty bit
   */
   bitmap_p->dirty = 0;
   /*
   ** close the file
   */
   close(fd);
   return 0;
   
error:
   if (fd != -1) close(fd);
   return -1;
}


/**
*  open the parent directory

   @param e : pointer to the export structure
   @param parent : fid of the parent directory
   
   @retval > 0 : fd of the directory
   @retval < 0 error
*/
int export_open_parent_directory(export_t *e,fid_t parent)
{
    int fd = -1;
    
    dirent_current_eid = e->eid;
    char node_path[PATH_MAX];
    if (export_lv2_resolve_path(e, parent, node_path) != 0)
        goto out;
   if ((fd = open(node_path, O_RDONLY | O_NOATIME, S_IRWXU)) < 0) {
        goto out;
    }
out:
   return fd;
}

