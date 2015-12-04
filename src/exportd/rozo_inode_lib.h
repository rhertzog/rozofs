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
 
#ifndef _ROZO_INODE_LIB_H
#define _ROZO_INODE_LIB_H


#ifdef __cplusplus
extern "C" {
#endif

 
#include <sys/param.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <uuid/uuid.h>
#include <errno.h>
#include <string.h>



struct perf {
	struct timeval tv;
};
/**
**______________________________________________________________
*/  
static inline int perf_start(struct perf *p)
{
	return gettimeofday(&(p->tv), 0);
}
/**
**______________________________________________________________
*/  
static inline int perf_stop(struct perf *p)
{
	return gettimeofday(&(p->tv), 0);
}
/**
**______________________________________________________________
*/  
static inline void perf_print(struct perf stop, struct perf start,unsigned  long long dsize)
{

	long long secs = stop.tv.tv_sec - start.tv.tv_sec;
	long long usecs = secs * 1000000 + stop.tv.tv_usec - start.tv.tv_usec;

	printf("runtime = %10lld usecs", usecs);
	if (dsize != 0) {
#if 0 // not bug in printf for 32-bit
		printf(", bandwidth %lld MB in %.4f sec = %.2f MB/s\n", dsize/(1024*1024), 
			((double) usecs)/1000000, ((double) dsize) / (double)usecs);
#else
		printf(", nb files %lld  ", dsize);
		printf("in %.4f sec ",(double)usecs/1000000);
		printf("= %.2f files/s\n", (double)(dsize*1000000/usecs));
#endif
	}
	else
		printf("\n");
}


/**
* inode check callback
*/
typedef int (*check_inode_dirent_pf_t)(void *inode); 

typedef struct _scan_export_intf_t
{
   char *root_path; /*< pointer to the root path where metadata are stored */
   fid_t fid_start; /**< fid of the directory from which the scan must start */
   char *dir_pathname; /**< pathname where directory result must be written   */
   char *reg_pathname; /**< pathname where regular result must be written   */
   check_inode_dirent_pf_t dir_cbk; /**< check callback associated with the directories */
   check_inode_dirent_pf_t reg_cbk; /**< check callback associated with the regular files */
   /*
   ** statistics
   */
   uint64_t directory_count;
   uint64_t file_count;
   /*
   ** status
   */
   int status;
   int line;
   int error;

} scan_export_intf_t;
/*
**______________________________________________________________________________
*/
/**
*  Public scan an exportd with a criteria

   @param intf_p: pointer to the request
   
*/
int export_scan_from_dir(scan_export_intf_t *intf_p);

/**
* inode check callback
*/
typedef int (*check_inode_pf_t)(void *export,void *inode,void *p); 

/*
**_______________________________________________________________________________
*/
/**
*  scan of the inodes of a given type:
   
   @param export: pointer to the export context
   @param type: type of the inode to search for
   @param read : assert to one if inode attributes must be read
   @param callback_fct : optional callback function, NULL if none
   @param param : pointer to an opaque parameter or NULL
   @param callback_trk_fct : optional callback function associated with the tracking file, NULL if none
   @param param_trk : pointer to an opaque parameter or NULL
   
   @retval
*/
int rz_scan_all_inodes(void *export,int type,int read,check_inode_pf_t callback_fct,void *param,
                       check_inode_pf_t callback_trk_fct,void *param_trk);
/*
**_______________________________________________________________________________
*/
/**
   rozo inode lib initialization

    @param  : pointer to the root path of the exportd under analysis
    
    @retval <> NULL export reference to call for any operation
    @retval == NULL error
*/
void *rz_inode_lib_init(char *root_path);

/*
**__________________________________________________
*/
/**
*  API to get the pathname of the objet: @rozofs_uuid@<FID_parent>/<child_name>

   @param export : pointer to the export structure
   @param inode_attr_p : pointer to the inode attribute
   @param buf: output buffer
   
   @retval buf: pointer to the beginning of the outbuffer
*/
char *rozo_get_parent_child_path(void *exportd,void *inode_attr_p,char *buf);
/*
**__________________________________________________
*/
/**
   Get the rozoFS attributes in the struct stat form
   
   @param inode_attr_p: pointer to the rozoFS attributes
   @param st : pointer to the struct stat array
   
   @retval 0 on success
   @retval < 0 on error
*
*/
int rozofs_fstat(void *inode_attr_p,struct stat *st);
/*
**__________________________________________________
*/
/**
*  Get the file distribution (cid and sids)

   @param inode_p : pointer to the inode
   @param p: file distribution
   
   @retval none
*/
typedef struct _rozofs_file_distribution_t
{
   int cid;
   uint8_t sids[ROZOFS_SAFE_MAX];
} rozofs_file_distribution_t;

void rozofs_get_file_distribution(void *inode_p,rozofs_file_distribution_t *p);

/*
**_______________________________________________________________________________
*/
/**
*  Get the name of the object

   @param fname: pointer to the description of the object name
   @param bufout : output buffer;
   @param pfid :parent fid
   
   @retval pointer to the output buffer
*/
char *get_fname(void *e,char *bufout,void *fname,fid_t pfid);

/*
**_______________________________________________________________________________
*/
/**
   @param mode: set to 0 to clear the verbose mode
*/
void rz_set_verbose_mode(int mode);
#ifdef __cplusplus
}
#endif

#endif
