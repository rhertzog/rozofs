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
#define _XOPEN_SOURCE 500

#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h>
#include <uuid/uuid.h>
#include <sys/vfs.h> 
#include <pthread.h> 
#include <sys/wait.h>
#include <dirent.h>
#include <sys/mount.h>
 
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include <rozofs/common/list.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/profile.h>
#include <rozofs/common/mattr.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/ruc_timer_api.h>
#include <rozofs/core/rozofs_share_memory.h>

#include "storio_device_mapping.h"
#include "storaged.h"

extern char * pHostArray[];

struct blkio_info {
	unsigned int rd_ios;	/* Read I/O operations */
	unsigned int rd_merges;	/* Reads merged */
	unsigned long long rd_sectors; /* Sectors read */
	unsigned int rd_ticks;	/* Time in queue + service for read */
	unsigned int wr_ios;	/* Write I/O operations */
	unsigned int wr_merges;	/* Writes merged */
	unsigned long long wr_sectors; /* Sectors written */
	unsigned int wr_ticks;	/* Time in queue + service for write */
	unsigned int ticks;	/* Time of requests in queue */
	unsigned int aveq;	/* Average queue length */
};

uint32_t STORIO_DEVICE_PERIOD=3;
uint32_t storio_next_automount_attempt=0;



/*_____________________________________
** Parameter of the relocate thread
*/
typedef struct _storio_relocate_ctx_t {
  storage_t     * st;
  int             dev;
} storio_relocate_ctx_t;

/*
**____________________________________________________
**  Start a thread for relocation a device
**  
**  @param st   The storage context
**  @param dev  The device number to rebuild
**   
**  @retval 0 when the thread is succesfully started
*/
extern char storaged_config_file[];
extern char * storaged_hostname;
void * storio_device_relocate_thread(void *arg) {
  storio_relocate_ctx_t      *pRelocate = arg;
  storage_device_ctx_t    *   pDev = &pRelocate->st->device_ctx[pRelocate->dev];
  char            cmd[256];
  int             status;
  pid_t           pid;
  int             ret;
  
  pid = fork();  
  if (pid == 0) {
    char * pChar = cmd;
 
    pChar += rozofs_string_append(pChar,"storage_rebuild -fg --quiet -c ");
    pChar += rozofs_string_append(pChar,storaged_config_file);
    pChar += rozofs_string_append(pChar," -R -l 4 -r ");
    pChar += rozofs_string_append(pChar,pRelocate->st->export_hosts);
    pChar += rozofs_string_append(pChar," -p ");
    pChar += rozofs_u32_append(pChar,common_config.device_self_healing_process);
    pChar += rozofs_string_append(pChar," --sid ");
    pChar += rozofs_u32_append(pChar,pRelocate->st->cid);
    *pChar++ ='/';
    pChar += rozofs_u32_append(pChar,pRelocate->st->sid);
    pChar += rozofs_string_append(pChar," --device ");
    pChar += rozofs_u32_append(pChar,pRelocate->dev);
    pChar += rozofs_string_append(pChar," -o selfhealing_cid");
    pChar += rozofs_u32_append(pChar,pRelocate->st->cid);
    pChar += rozofs_string_append(pChar,"_sid");
    pChar += rozofs_u32_append(pChar,pRelocate->st->sid);
    pChar += rozofs_string_append(pChar,"_dev");
    pChar += rozofs_u32_append(pChar,pRelocate->dev);
    
    if (pHostArray[0] != NULL) {
      pChar += rozofs_string_append(pChar," -H ");
      pChar += rozofs_string_append(pChar,pHostArray[0]);
      int idx=1;
      while (pHostArray[idx] != NULL) {
        *pChar++ = '/';
        pChar += rozofs_string_append(pChar,pHostArray[idx++]);
      }	
    }
            
    errno = 0;	
    status = system(cmd);
    exit(status);
  }
   
  /* Check for rebuild sub processes status */    
  ret = waitpid(pid,&status,0);
  
  if ((ret == pid)&&(WEXITSTATUS(status)==0)) {
    /* Relocate is successfull. Let's put the device Out of service */
    pDev->status = storage_device_status_oos;
  }
  else {
    /* Relocate has failed. Let's retry later */
    pDev->failure /= 2;
    pDev->status   = storage_device_status_failed;
  }
  
  free(pRelocate);
  pthread_exit(NULL); 
}
/*
**____________________________________________________
**  Start a thread for relocation a device
**  
**  @param st  The storage context
**  @param dev The device number to rebuild
**   
**  @retval 0 when the thread is succesfully started
*/
int storio_device_relocate(storage_t * st, int dev) {
  pthread_attr_t             attr;
  int                        err;
  pthread_t                  thrdId;
  storio_relocate_ctx_t    * pRelocate;

  if (st->export_hosts == NULL) {
    severe("storio_device_relocate cid %d sid %d no export hosts",st->cid, st->sid);
    return -1;
  }
  
  pRelocate = malloc(sizeof(storio_relocate_ctx_t));
  if (pRelocate == NULL) {
    severe("storio_device_relocate malloc(%d) %s",(int)sizeof(storio_relocate_ctx_t), strerror(errno));
    return -1;
  }  
  pRelocate->st  = st;
  pRelocate->dev = dev;

  err = pthread_attr_init(&attr);
  if (err != 0) {
    severe("storio_device_relocate pthread_attr_init() %s",strerror(errno));
    free(pRelocate);
    return -1;
  }  

  err = pthread_create(&thrdId,&attr,storio_device_relocate_thread,pRelocate);
  if (err != 0) {
    severe("storio_device_relocate pthread_create() %s", strerror(errno));
    free(pRelocate);
    return -1;
  }    
  
  return 0;
}
/*
**____________________________________________________
** Read linux disk stat file
** 
*/
#define DISK_STAT_BUFFER_SIZE (1024*64)
char disk_stat_buffer[DISK_STAT_BUFFER_SIZE];
void storage_read_disk_stats(void) {
  FILE *                   iofd;
  
  disk_stat_buffer[0] = 0;
  iofd = fopen("/proc/diskstats", "r");
  if (iofd) {
    if(fread(disk_stat_buffer,DISK_STAT_BUFFER_SIZE, 1,iofd)){}
  }  
  fclose(iofd);
}
/*
**____________________________________________________
** Get the device activity on the last period
**
** @param pDev    The pointer to the device context
**
** @retval 0 on success, -1 when this device has not been found 
*/
int storage_get_device_usage(cid_t cid, sid_t sid, uint8_t dev, storage_device_ctx_t *pDev) {
  char            * p = disk_stat_buffer;
  int               major,minor;
  char              devName[128];
  struct blkio_info blkio;

  while (1) {
      
    devName[0] = 0;
	       
    sscanf(p, "%4d %4d %s %u %u %llu %u %u %u %llu %u %*u %u %u",
	   &major, &minor, (char *)&devName,
	   &blkio.rd_ios, &blkio.rd_merges,
	   &blkio.rd_sectors, &blkio.rd_ticks, 
	   &blkio.wr_ios, &blkio.wr_merges,
	   &blkio.wr_sectors, &blkio.wr_ticks,
	   &blkio.ticks, &blkio.aveq);
	   
    if ((pDev->major == major) && (pDev->minor == minor)) {

      memcpy(pDev->devName,devName,7);
      pDev->devName[7] = 0;

      /* Number of read during the last period */
      pDev->rdDelta     = blkio.rd_ios - pDev->rdCount;
      /* Save total read count for next run */
      pDev->rdCount     = blkio.rd_ios;
      /* Duration of read during the last period */
      uint32_t rdTicks  = blkio.rd_ticks - pDev->rdTicks;
      /* Save total read duration for next run */      
      pDev->rdTicks     = blkio.rd_ticks;
      /* Average read duration in usec */
      pDev->rdAvgUs     = pDev->rdDelta ? rdTicks*1000/pDev->rdDelta : 0;
      
      /* Number of write during the last period */
      pDev->wrDelta     = blkio.wr_ios - pDev->wrCount;
      /* Save total write count for next run */
      pDev->wrCount     = blkio.wr_ios;
      /* Duration of write during the last period */      
      uint32_t wrTicks  = blkio.wr_ticks - pDev->wrTicks;
      /* Save total write duration for next run */            
      pDev->wrTicks     = blkio.wr_ticks;
      /* Average write duration in usec */      
      pDev->wrAvgUs     = pDev->wrDelta ? wrTicks*1000/pDev->wrDelta : 0;

      /*
      ** % of disk usage during the last period
      */    
      if (pDev->ticks == 0) {
        pDev->usage = 0;
      }
      else {
        pDev->usage = (blkio.ticks-pDev->ticks) / (STORIO_DEVICE_PERIOD*10);
      }	
      
      /*
      ** Log warning when configured for
      */
      if ((common_config.disk_read_threshold != 0) 
      &&  (pDev->rdAvgUs >= common_config.disk_read_threshold)) {      
        warning("%s cid %d sid %d dev %d READ AVG %d us (%dsec: %d wr + %d rd = %d%c)",
	      devName, cid, sid, dev, pDev->rdAvgUs, 
	      STORIO_DEVICE_PERIOD,pDev->wrDelta, pDev->rdDelta, pDev->usage,'%');
      }

      if ((common_config.disk_write_threshold != 0) 
      &&  (pDev->wrAvgUs >= common_config.disk_write_threshold)) {      
        warning("%s cid %d sid %d dev %d WRITE AVG %d us (%dsec: %d wr + %d rd = %d%c)",
	      devName, cid, sid, dev, pDev->wrAvgUs, 
	      STORIO_DEVICE_PERIOD,pDev->wrDelta, pDev->rdDelta, pDev->usage,'%');
      }
                  
      if ((common_config.disk_usage_threshold != 0) 
      &&  (pDev->usage >= common_config.disk_usage_threshold)) {
        warning("%s cid %d sid %d dev %d USAGE (%dsec: %d wr + %d rd = %d%c)",
	      devName, cid, sid, dev,
	      STORIO_DEVICE_PERIOD,pDev->wrDelta, pDev->rdDelta, pDev->usage,'%');      
      }
      /* Save current ticks for next run */
      pDev->ticks = blkio.ticks;
      return 0;
    }
    
    /* Next line */
    while((*p!=0)&&(*p!='\n')) p++;
    if (*p==0) return -1;
    p++;
  }  
  return -1;
}  
/*
**____________________________________________________
** Get the major and minor of a storage device
** 
** @param st      The storage context
** @param dev     The device number within this storage
**
** @retval 0 on success, -1 on error
*/
static inline int storio_device_get_major_and_minor(storage_t * st, int dev) {
  char          path[FILENAME_MAX];
  char        * pChar = path;
  struct stat   buf;
  
  pChar += rozofs_string_append(pChar, st->root);
  *pChar++ ='/';
  pChar += rozofs_u32_append(pChar, dev); 

  if (stat(path, &buf)==0) {
    st->device_ctx[dev].major = major(buf.st_dev);
    st->device_ctx[dev].minor = minor(buf.st_dev);  
    return 0;  
  }
  return -1;
}
	
/**____________________________________________________
** Check whether the mounted device is actually dedicated
** to the expected device
** 
** @param st      The storage context
** @param dev     The expectd device number
**
** @retval 0 on success, -1 on error
*/
int storio_check_expcted_mounted_device(storage_t   * st, int dev) {
  char            path[FILENAME_MAX];
  DIR           * dp = NULL;
  struct dirent   ep;
  struct dirent * pep;
  int             status = 0;   
  int             cid,sid,device; 
  int             ret;  
  char          * pChar = path;

  /*
  ** Build the device path
  */
  pChar += rozofs_string_append(pChar, st->root);
  *pChar++ ='/';
  pChar += rozofs_u32_append(pChar, dev);     

  // Open directory
  if (!(dp = opendir(path))) {
    severe("opendir(%s) %s",path,strerror(errno));
    goto out;
  }

  // Readdir to find out the file identifying the device
  while (readdir_r(dp,&ep,&pep) == 0) {
    
    // end of directory
    if (pep == NULL) goto out;
    
    // Check whether this is the expected file
    if (strncmp(pep->d_name,"storage_c",strlen("storage_c")) != 0) continue;
    
    ret = sscanf(pep->d_name,"storage_c%d_s%d_%d",&cid, &sid, &device);
    if (ret != 3) continue;
    
    if ((cid != st->cid) || (sid != st->sid) || (device != dev)) {   
      status = -1;
    }
    else {
      status  = 0;
    }
    goto out;  
  }

out:

  // Close directory
  if (dp) closedir(dp);

  if ((status == -1)&&(common_config.device_automount)) {
    /* 
    ** let's unmount the device that is not at the correct place
    */
    if (umount2(path,MNT_FORCE)==-1) {}    
  } 
    
  return status;  
}  
/*
**____________________________________________________
** Check whether the access to the device is still granted
** and get the number of free blocks
**
** @param st     The storage context
** @param dev    The device number
** @param free   The free space in bytes on this device
** @param size   The size of the device
** @param bs     The block size of the FS of the device
** @param diagnostic A diagnostic of the problem on the device
** @param relocate_allowed Wheteher relocation could take place
**
** @retval -1 if device is failed, 0 when device is OK
*/
static inline int storio_device_monitor_get_free_space(storage_t   * st, 
						       int           dev,   
						       uint64_t    * free, 
						       uint64_t    * size, 
						       uint64_t    * bs, 
						       storage_device_diagnostic_e * diagnostic, 
						       int         * relocate_allowed) {
  struct statfs sfs;
  char          path[FILENAME_MAX];
  char        * pChar = path;
  uint64_t      threashold;

  *relocate_allowed = 0;
    
  pChar += rozofs_string_append(pChar, st->root);
  *pChar++ ='/';
  pChar += rozofs_u32_append(pChar, dev); 

  /*
  ** Check that the device is writable
  */
  if (access(path,W_OK) != 0) {
    if (errno == EACCES) {
      *diagnostic = DEV_DIAG_READONLY_FS;
    }
    else {
      *diagnostic = DEV_DIAG_FAILED_FS;
    }    
    *relocate_allowed = 1;
    return -1;
  }

  /*
  ** Get statistics
  */
  if (statfs(path, &sfs) != 0) {
    *diagnostic = DEV_DIAG_FAILED_FS;    
    *relocate_allowed = 1;
    return -1;
  }  

  /*
  ** Check we can see an X file. 
  ** This would mean that the device is not mounted
  */
  rozofs_string_append(pChar, "/X");
  if (access(path,F_OK) == 0) {
    *diagnostic = DEV_DIAG_UNMOUNTED;
    *relocate_allowed = 1;
    return -1;
  }  

  /*
  ** Check device idenfying file is the one expected
  */
  if (storio_check_expcted_mounted_device(st,dev) == -1) {
    *diagnostic = DEV_DIAG_INVERTED_DISK;
    return -1;    
  } 
  
  /*
  ** Check whether RozoFS storage structure is created under
  ** this device
  */
  rozofs_storage_device_subdir_create(st->root, dev);
  
  *size = sfs.f_blocks;
  *bs   = sfs.f_bsize;
  
  // Less than 100 inodes !!!
  if (sfs.f_ffree < 100) {
    *diagnostic = DEV_DIAG_INODE_DEPLETION;
    *free  = 0;    
    return 0;
  }   
  
  *free = sfs.f_bfree;

  /*
  ** Under a given limit, say there is no space left
  */
  threashold = *size;
  threashold /= 1024;
  if (threashold > 1024) threashold =  1024;
  if (*free < threashold) {  
    *free  = 0;   
    *diagnostic = DEV_DIAG_BLOCK_DEPLETION;
    return 0;
  }         
  *diagnostic = DEV_DIAG_OK;
  return 0;
}
/*
**____________________________________________________
 * API to be called periodically to monitor errors on a period
 *
 * @param st: the storage to be initialized.
 *
 * @return a bitmask of the device having encountered an error
 */
static inline uint64_t storio_device_monitor_error(storage_t * st) {
  int dev;
  uint64_t bitmask = 0;  
  int old_active = st->device_errors.active;
  int new_active = 1 - old_active;
  
  
  for (dev = 0; dev < STORAGE_MAX_DEVICE_NB; dev++) {   
    st->device_errors.errors[new_active][dev] = 0;    
  }  
  st->device_errors.active = new_active;
 
  
  for (dev = 0; dev < STORAGE_MAX_DEVICE_NB; dev++) {    
    st->device_errors.total[dev] = st->device_errors.total[dev] + st->device_errors.errors[old_active][dev];
    bitmask |= (1ULL<<dev);
  }  
  return bitmask;
}
/*
**____________________________________________________
**  Device monitoring 
** 
**  @param allow_disk_spin_down  whether disk spin down 
**                               should be considered
*/
void storio_device_monitor(uint32_t allow_disk_spin_down) {
  int           dev;
  int           passive;
  storage_t   * st;
  storage_device_ctx_t *pDev;
  int           max_failures;
  int           rebuilding;
  storage_share_t *share;
  uint64_t      bfree=0;
  uint64_t      bmax=0;
  uint64_t      bsz=0;   
  uint64_t      sameStatus=0;
  int           activity;
  int           relocate_allowed=0;
  uint32_t      now = time(NULL);
       
  /*
  ** Loop on every storage managed by this storio
  */ 
  st = NULL;
  while ((st = storaged_next(st)) != NULL) {
  
    /*
    ** Resolve the share memory address
    */
    share = storage_get_share(st); 
    if (share->monitoring_period != STORIO_DEVICE_PERIOD) {
      share->monitoring_period = STORIO_DEVICE_PERIOD;
    }  

    if (st->selfHealing == -1) {
      /* No self healing configured */
      max_failures = -1;
      rebuilding   = 1; /* Prevents going on rebuilding */
    }
    else {
      
     if (st->selfHealing < 0) {
	/* To speed up tests. Not for normal usage */
	max_failures = 1;
      }
      else {    
        /*
        ** Compute the maximium number of failures before relocation
        */
        max_failures = (st->selfHealing * 60)/STORIO_DEVICE_PERIOD;
      }
    
      /*
      ** Check whether some device is already in relocating status
      */
      rebuilding = 0;       
      for (dev = 0; dev < st->device_number; dev++) {
	pDev = &st->device_ctx[dev];
	if (pDev->status == storage_device_status_relocating) {
	  rebuilding = 1; /* No more than 1 rebuild at a time */
	  break;
	}
      }             
    }  
    
    /*
    ** Collect errors on devices
    */
    storio_device_monitor_error(st);

    /*
    ** Update the table of free block count on device to help
    ** for distribution of new files on devices 
    */
    passive = 1 - st->device_free.active; 


    /*
    ** Read system disk stat file
    */
    storage_read_disk_stats();  
    
    /*
    ** Check whether some devices need to be mounted or unmounted now
    */
    while (common_config.device_automount) {
      int count=0; // Number of mounted devices
      
      if (storio_next_automount_attempt > now) break;
      
      /*
      ** Loop on devices to find one that requires to be mounted
      */
      for (dev = 0; dev < st->device_number; dev++) {
	if ((st->device_ctx[dev].diagnostic == DEV_DIAG_UNMOUNTED)
	||  (st->device_ctx[dev].diagnostic == DEV_DIAG_INVERTED_DISK)) {
	  char                     * p;
	  char                       rozofs_storio_path[PATH_MAX];

          /*
	  ** Attempt to find the missing devices and to mount them
	  */
	  p = rozofs_storio_path;
	  p += rozofs_string_append(p, st->root);
	  p += rozofs_string_append(p, "/mnt_test");  
          storage_automount_devices(rozofs_storio_path,&count);
	  
	  /*
	  ** Mount has not been successfull. Let's retry later but not immediatly
	  */
	  if (count==0) {

	    /* Next automount attempt in 3 minutes */
	    storio_next_automount_attempt = now + 180; 

	    if (st->selfHealing > 0) {
	      /* Self healing is configured. Better retry before auto rebuild */
	      storio_next_automount_attempt = now + 59;
	    }
	    break;
	  } 
	  
	  /*
	  ** Some device has been mounted. Create sub directory structure when needed.
	  */
          storage_subdirectories_create(st);	     
          break;
	}
      }
      break;
    }
        
    for (dev = 0; dev < st->device_number; dev++) {

      pDev = &st->device_ctx[dev];
      bfree = 0;
      bmax  = 0;
      sameStatus = 0;

      /*
      ** Get major and minor of the device if not yet done
      */
      if (pDev->major == 0) {
	if (storio_device_get_major_and_minor(st,dev)==0) {
	  if (share) {
	    share->dev[dev].major = pDev->major;
	    share->dev[dev].minor = pDev->minor;
	  }
	}
      }
      
      /*
      ** Read device usage from Linux disk statistics
      */
      storage_get_device_usage(st->cid,st->sid,dev,pDev);

      pDev->monitor_run++;
      if ((pDev->wrDelta==0) && (pDev->rdDelta==0)) {
        activity = 0;
        pDev->monitor_no_activity++;
      }
      else {
        activity = 1;
	pDev->last_activity_time = now;
      }		

     
      /*
      ** Check whether re-init is required
      */
      if (pDev->action==STORAGE_DEVICE_REINIT) {
        /*
	** Wait for the end of the relocation to re initialize the device
	*/
        if (pDev->status != storage_device_status_relocating) {
	  pDev->action = STORAGE_DEVICE_NO_ACTION;
	  pDev->status = storage_device_status_init;
	}
      }

      /*
      ** Check wether errors must be reset
      */
      if (pDev->action==STORAGE_DEVICE_RESET_ERRORS) {
	pDev->action = STORAGE_DEVICE_NO_ACTION;
        memset(&st->device_errors, 0, sizeof(storage_device_errors_t));
        storio_clear_faulty_fid();      
      }


      switch(pDev->status) {

        /* 
	** (re-)Initialization 
	*/
        case storage_device_status_init:
	  /*
	  ** Clear every thing
	  */
          memset(&st->device_errors, 0, sizeof(storage_device_errors_t));
          storio_clear_faulty_fid();      
	  pDev->failure = 0;
	  pDev->status = storage_device_status_is;   
	  // continue on next case 


	/*
	** Device In Service. No fault up to now
	*/  
        case storage_device_status_is:	
	  /*
	  ** When some errors have occurred the device goes to degraded
	  ** which is equivallent to IS but with some errors
	  */
	  if (st->device_errors.total[dev] != 0 ) {
	    pDev->status = storage_device_status_degraded;
	  }

	  /*
	  ** When disk spin down is allowed, do not try to access the disks
	  ** to update the status if no access has occurred on the disk.
	  */
	  if (allow_disk_spin_down) {
	    if (activity==0) {
              sameStatus = 1;
	      break;
            }
	  } 	  
	  
	  if (storio_device_monitor_get_free_space(st, dev, &bfree, &bmax, &bsz, &pDev->diagnostic,&relocate_allowed) != 0) {
	    /*
	    ** The device is failing !
	    */
	    pDev->status = storage_device_status_failed;
	  }
	  break;

	case storage_device_status_degraded:

	  /*
	  ** When some errors have occurred the device goes to degraded
	  ** which is equivallent to IS but with some errors
	  */
	  if (st->device_errors.total[dev] == 0 ) {
	    pDev->status = storage_device_status_is;
	  }
	  
	  /*
	  ** When disk spin down is allowed, do not try to access the disks
	  ** to update the status if no access has occurred on the disk.
	  */
	  if (allow_disk_spin_down) {
	    if (activity==0) {
              sameStatus = 1;
	      break;
            }
	  } 	
	   
	  /*
	  ** Check whether the access to the device is still granted
	  ** and get the number of free blocks
	  */
	  if (storio_device_monitor_get_free_space(st, dev, &bfree, &bmax, &bsz, &pDev->diagnostic, &relocate_allowed) != 0) {
	    /*
	    ** The device is failing !
	    */
	    pDev->status = storage_device_status_failed;
	  }
	  break;

	/*
	** Device failed. Check whether the status is confirmed
	*/  
	case storage_device_status_failed:

	  /*
	  ** Check whether the access to the device is still granted
	  ** and get the number of free blocks
	  */
	  if (storio_device_monitor_get_free_space(st, dev, &bfree, &bmax, &bsz, &pDev->diagnostic,&relocate_allowed) != 0) {
	    /*
	    ** Still failed
	    */	
	    pDev->failure++;
	    /*
	    ** When self healing is configured and no other device
	    ** is relocating on this storage and failure threashold
	    ** has been crossed, relocation should take place
	    */
	    if ((rebuilding==0) && (relocate_allowed) && (pDev->failure >= max_failures)) {
	      /*
	      ** Let's start self healing
	      */
	      pDev->status = storage_device_status_relocating;
	      if (storio_device_relocate(st,dev) == 0) {
	        rebuilding = 1; /* On rebuild at a time */
	      }	
	      else {
	        pDev->status = storage_device_status_failed;	        
	      }
	    }
	    break;
	  }

	  /*
	  ** The device is repaired
	  */
	  pDev-> status = storage_device_status_is;
	  pDev->failure = 0;	  
	  break;


	case storage_device_status_relocating:  
	  break;

	case storage_device_status_oos:
	  break;

	default:
	  break;    
      }	

      /*
      ** The device is unchanged and no access has been done to check it.
      ** The status is the same as previously
      */
      if (sameStatus) {
        int active = 1 - passive;
	st->device_free.blocks[passive][dev] = st->device_free.blocks[active][dev]; 
      }
      else {
        /*
        ** The device has been accessed and checked
        */
        st->device_free.blocks[passive][dev] = bfree;  
      }
      
      if (share) {
        memcpy(share->dev[dev].devName,pDev->devName,8);
	share->dev[dev].status     = pDev->status;
	share->dev[dev].diagnostic = pDev->diagnostic;
	share->dev[dev].free       = bfree * bsz;
	share->dev[dev].size       = bmax  * bsz;
	share->dev[dev].usage      = pDev->usage;
	share->dev[dev].rdNb       = pDev->rdDelta;
	share->dev[dev].rdUs       = pDev->rdAvgUs;
	share->dev[dev].wrNb       = pDev->wrDelta;
	share->dev[dev].wrUs       = pDev->wrAvgUs;
	share->dev[dev].lastActivityDelay = pDev->last_activity_time;
      }
    } 

    /*
    ** Switch active and passive records
    */
    st->device_free.active = passive; 

  }              
}
/*
**____________________________________________________
**  Device monotoring thread
** 
**  @param param: Not significant
*/
void * storio_device_monitor_thread(void * param) {

  uma_dbg_thread_add_self("Device monitor");

  /*
  ** Never ending loop
  */ 
  while ( 1 ) {
    sleep(STORIO_DEVICE_PERIOD);
    storio_device_monitor(common_config.allow_disk_spin_down);         
  }  
}

/*
**____________________________________________________
*/
/*
  start a periodic timer to update the available volume on devices
*/
int storio_device_mapping_monitor_thread_start() {
  pthread_attr_t             attr;
  int                        err;
  pthread_t                  thrdId;

  /*
  ** Set the polling periodicity in seconds
  */
  if (common_config.file_distribution_rule != rozofs_file_distribution_size_balancing) {
    STORIO_DEVICE_PERIOD = 10;
  }
  
  /*
  ** 1rst call to monitoring function, and access to the disk 
  ** to get the disk free space 
  */
  storio_device_monitor(FALSE);;

  /*
  ** Start monitoring thread
  */
  err = pthread_attr_init(&attr);
  if (err != 0) {
    severe("storio_device_mapping_monitor_thread_start pthread_attr_init() %s",strerror(errno));
    return -1;
  }  

  err = pthread_create(&thrdId,&attr,storio_device_monitor_thread,NULL);
  if (err != 0) {
    severe("storio_device_mapping_monitor_thread_start pthread_create() %s", strerror(errno));
    return -1;
  }    
  
  return 0;
}

