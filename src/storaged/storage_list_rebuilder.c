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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <pthread.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <sys/resource.h>
#include <unistd.h>
#include <libintl.h>
#include <sys/poll.h>
#include <rpc/rpc.h>
#include <rpc/pmap_clnt.h>
#include <libconfig.h>
#include <getopt.h>
#include <inttypes.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/vfs.h>
#include <malloc.h>

#include <rozofs/rozofs_srv.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/list.h>
#include <rozofs/common/common_config.h>
#include <rozofs/rpc/mproto.h>
#include <rozofs/rpc/sproto.h>
#include <rozofs/rpc/spproto.h>
#include <rozofs/core/rozofs_core_files.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rozofs_timer_conf.h>
#include <rozofs/core/rozofs_fid_string.h>

#include "config.h"
#include "sconfig.h"
#include "storage.h"
#include "sconfig.h"
#include "rbs.h"
#include "rbs_eclient.h"
#include "rbs_sclient.h"

int cid = -1;
int sid = -1;
int rebuildRef = -1;
int instance = -1;

char        rebuild_directory_path[FILENAME_MAX]; 
char        fid_list[FILENAME_MAX]; 
char        statFilename[FILENAME_MAX]; 
 
static rbs_storage_config_t storage_config;

sconfig_t   sconfig;
uint8_t prj_id_present[ROZOFS_SAFE_MAX];
int         quiet=0;

storage_t storaged_storages[STORAGES_MAX_BY_STORAGE_NODE] = { { 0 } };
uint16_t storaged_nrstorages = 0;

char                command[1];
char              * fid2rebuild_string=NULL;
uint8_t storio_nb_threads = 0;
uint8_t storaged_nb_ports = 0;
uint8_t storaged_nb_io_processes = 0;
/* nb. of retries for get bins on storages */
static uint32_t retries = 10;

// Rebuild storage variables

int sigusr_received=0;
rpcclt_t   rpcclt_export;
  
/*__________________________________________________________________________
*/
void rbs_cath_sigusr(int sig){
  sigusr_received = 1;
  info("pause requested");
  signal(SIGUSR1, rbs_cath_sigusr);  
}

static int storaged_initialize() {
    int status = -1;
    list_t *p = NULL;
    DEBUG_FUNCTION;

    /* Initialize rozofs constants (redundancy) */
    rozofs_layout_initialize();

    storaged_nrstorages = 0;

    storaged_nb_io_processes = 1;
    
    storio_nb_threads = common_config.nb_disk_thread;

    storaged_nb_ports = sconfig.io_addr_nb;

    /* For each storage on configuration file */
    list_for_each_forward(p, &sconfig.storages) {
        storage_config_t *sc = list_entry(p, storage_config_t, list);
        /* Initialize the storage */
        if (storage_initialize(storaged_storages + storaged_nrstorages++,
                sc->cid, sc->sid, sc->root,
		sc->device.total,
		sc->device.mapper,
		sc->device.redundancy,
		-1,NULL) != 0) {
            severe("can't initialize storage (cid:%d : sid:%d) with path %s",
                    sc->cid, sc->sid, sc->root);
            goto out;
        }
    }

    status = 0;
out:
    return status;
}



/*
**____________________________________________________
*/
/*
  Allocate a device for a file
  
   @param st: storage context
*/
uint32_t storio_device_mapping_allocate_device(storage_t * st) {
  struct statfs sfs;
  int           dev;
  uint64_t      max=0;
  int           choosen_dev=0;
  char          path[FILENAME_MAX];  
  
  for (dev = 0; dev < st->device_number; dev++) {
    char * pChar = path;
    pChar += rozofs_string_append(pChar,st->root);
    *pChar++ = '/';
    pChar += rozofs_u32_append(pChar,dev);
    *pChar++ = '/';  
    *pChar = 0;  
               
    if (statfs(path, &sfs) != -1) {
      if (sfs.f_bfree > max) {
        max         = sfs.f_bfree;
	choosen_dev = dev;
      }
    }
  }  
  return choosen_dev;
}

static void storaged_release() {
    DEBUG_FUNCTION;
    int i;
    list_t *p, *q;

    for (i = 0; i < storaged_nrstorages; i++) {
        storage_release(&storaged_storages[i]);
    }
    storaged_nrstorages = 0;

    // Free config

    list_for_each_forward_safe(p, q, &sconfig.storages) {

        storage_config_t *s = list_entry(p, storage_config_t, list);
        free(s);
    }
}

typedef struct rbs_error_t {
  uint64_t spare_start;
  uint64_t spare_start_again;
  uint64_t spare_read;
  uint64_t spare_read_enoent;
  uint64_t spare_read_no_enough;
  uint64_t spare_write_empty;
  uint64_t spare_write_proj;
  uint64_t spare_write_broken;  

  uint64_t nom_start;
  uint64_t nom_start_again;
  uint64_t nom_read;
  uint64_t nom_read_enoent;
  uint64_t nom_transform;
  uint64_t nom_write;  
  uint64_t nom_write_broken;  

} RBS_ERROR_T;

static RBS_ERROR_T rbs_error = {0};

#define RBS_DISPLAY_ERROR(x) {if (rbs_error.x) REBUILD_MSG("%20s = %llu", #x, (long long unsigned int) rbs_error.x);}

void display_rbs_errors() {
  RBS_DISPLAY_ERROR(spare_start);
  RBS_DISPLAY_ERROR(spare_start_again);
  RBS_DISPLAY_ERROR(spare_read);
  RBS_DISPLAY_ERROR(spare_read_enoent);
  RBS_DISPLAY_ERROR(spare_read_no_enough);
  RBS_DISPLAY_ERROR(spare_write_empty);
  RBS_DISPLAY_ERROR(spare_write_proj);
  RBS_DISPLAY_ERROR(spare_write_broken);
  RBS_DISPLAY_ERROR(nom_start);
  RBS_DISPLAY_ERROR(nom_start_again);
  RBS_DISPLAY_ERROR(nom_read);
  RBS_DISPLAY_ERROR(nom_read_enoent);
  RBS_DISPLAY_ERROR(nom_transform);
  RBS_DISPLAY_ERROR(nom_write); 
  RBS_DISPLAY_ERROR(nom_write_broken); 
}

typedef enum _rbs_exe_code_e {
  RBS_EXE_SUCCESS,
  RBS_EXE_FAILED,
  RBS_EXE_ENOENT,
  RBS_EXE_BROKEN
} RBS_EXE_CODE_E;

RBS_EXE_CODE_E rbs_restore_one_spare_entry(storage_t       * st, 
                                           uint8_t           layout,
                                	   int               local_idx, 
			        	   int               relocate,
			        	   uint32_t        * block_start,
			        	   uint32_t          block_end, 
			     		   rb_entry_t      * re, 
					   uint8_t           spare_idx,
					   uint64_t        * size_written,
					   uint64_t        * size_read,
					   uint8_t         * error) {
    RBS_EXE_CODE_E status = RBS_EXE_FAILED;
    int i = 0;
    int ret = -1;
    rbs_storcli_ctx_t working_ctx;
    int block_idx = 0;
    uint8_t rbs_prj_idx_table[ROZOFS_SAFE_MAX];
    int     count;
    rbs_inverse_block_t * pBlock;
    int prj_count;
    uint16_t prj_ctx_idx;
    uint16_t projection_id;
    char   *  pforward = NULL;
    rozofs_stor_bins_hdr_t * rozofs_bins_hdr_p;
    rozofs_stor_bins_footer_t * rozofs_bins_foot_p;
//    rozofs_stor_bins_hdr_t * bins_hdr_local_p;    
    int    remove_file=0;
    uint32_t   rebuild_ref=0;
            
    // Get rozofs layout parameters
    uint32_t bsize             = re->bsize;
    uint32_t bbytes            = ROZOFS_BSIZE_BYTES(bsize);
    uint8_t  rozofs_safe       = rozofs_get_rozofs_safe(layout);
    uint8_t  rozofs_forward    = rozofs_get_rozofs_forward(layout);
    uint8_t  rozofs_inverse    = rozofs_get_rozofs_inverse(layout);
    uint16_t disk_block_size   = rozofs_get_max_psize_on_disk(layout,bsize);
    uint32_t requested_blocks  = ROZOFS_BLOCKS_IN_BUFFER(layout);
    uint32_t nb_blocks_read_distant = requested_blocks;
    uint32_t block_per_chunk = ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(re->bsize);
    uint32_t chunk_stop;
    uint32_t chunk;
    uint16_t rozofs_msg_psize  = rozofs_get_max_psize_in_msg(layout,bsize);

    // Clear the working context
    memset(&working_ctx, 0, sizeof (working_ctx));

    /*
    ** Compute starting and stopping chunks
    */    
    chunk = *block_start / block_per_chunk;
    if (block_end==0xFFFFFFFF) chunk_stop = 0xFFFFFFFF; // whole file
    else                       chunk_stop = chunk;      // one chunk
          
    while (chunk<=chunk_stop) {

      *block_start = chunk * block_per_chunk;
      block_end    = *block_start + block_per_chunk - 1;
      
      remove_file = 1; /* Should possibly remove the chunk at the end */

      rebuild_ref = sclient_rebuild_start_rbs(re->storages[local_idx], st->cid, st->sid, re->fid,
                                              relocate?SP_NEW_DEVICE:SP_SAME_DEVICE, chunk, 1 /* spare */, 
					      *block_start, block_end);
      if (rebuild_ref == 0) {
	remove_file = 0;
	if (errno == EAGAIN) {
	  rbs_error.spare_start_again++;
	  *error = rozofs_rbs_error_file_to_much_running_rebuild;
	}  
	else {
	  rbs_error.spare_start++;
	  *error = rozofs_rbs_error_rebuild_start_failed;
        }	  
	goto out;
      }     

      // While we can read in the bins file
      while(*block_start <= block_end) {


          // Clear the working context
	  if (working_ctx.data_read_p) {
            free(working_ctx.data_read_p);
            working_ctx.data_read_p = NULL;	
	  }	
          // Free bins read in previous round
	  for (i = 0; i < rozofs_safe; i++) {
              if (working_ctx.prj_ctx[i].bins) {
        	  free(working_ctx.prj_ctx[i].bins);
		  working_ctx.prj_ctx[i].bins = NULL;
	      }	   
	      working_ctx.prj_ctx[i].prj_state = PRJ_READ_IDLE;
	  }


	  if ((block_end-*block_start+1) < requested_blocks){
	    requested_blocks = (block_end-*block_start+1);
	  }

          // Read every available bins
	  ret = rbs_read_all_available_proj(re->storages, spare_idx, layout, bsize, st->cid,
                                	    re->dist_set_current, re->fid, *block_start,
                                	    requested_blocks, &nb_blocks_read_distant,
                                	    &working_ctx,
					    size_read);

	  // Reading at least inverse projection has failed				  
          if (ret != 0) {
              remove_file = 0;// Better keep the file	
              errno = EIO;
 	      rbs_error.spare_read++;
	      *error = rozofs_rbs_error_read_error;		      
              goto out;
          }

	  if (nb_blocks_read_distant == 0) break; // End of chunk
	  
          if (nb_blocks_read_distant == -1) {
	     /*
	     ** File has been deleted
	     */
	     errno = ENOENT;
	     rbs_error.spare_read_enoent++;
	     status = RBS_EXE_ENOENT;
	     goto out;
          }	  	

	  // Loop on the received blocks
          pBlock = &working_ctx.block_ctx_table[0];	
          for (block_idx = 0; block_idx < nb_blocks_read_distant; block_idx++,pBlock++) {

              count = rbs_count_timestamp_tb(working_ctx.prj_ctx, spare_idx, layout, bsize, block_idx,
                                             rbs_prj_idx_table, 
			  		     &pBlock->timestamp,
                                             &pBlock->effective_length);

	      // Less than rozofs_inverse projection. Can not regenerate anything 
	      // from what has been read				   	
	      if (count < 0) {
                  remove_file = 0;// Better keep the file	    
        	  errno = EIO;
		  rbs_error.spare_read_no_enough++;
	          *error = rozofs_rbs_error_not_enough_projection_read;	  
        	  goto out;	      
	      }

	      // All projections have been read. Nothing to regenerate for this block
	      if (count >= rozofs_forward) continue;


              // Case of the empty block
              if (pBlock->timestamp == 0) {

		 prj_ctx_idx = rbs_prj_idx_table[0];
		 
	         if (pforward == NULL) pforward = memalign(32,rozofs_msg_psize);
                 memset(pforward,0,rozofs_msg_psize);
		 
        	 // Store the projection localy	
        	 ret = sclient_write_rbs(re->storages[spare_idx], st->cid, st->sid,
	                        	 layout, bsize, 1 /* Spare */,
					 re->dist_set_current, re->fid,
					 *block_start+block_idx, 1,
					 (const bin_t *)pforward,
					 rebuild_ref);
        	 remove_file = 0;	// This file must exist   
        	 if (ret < 0) {
		   if (errno == EAGAIN) {
		     rbs_error.spare_write_broken++;
		     status = RBS_EXE_BROKEN;
		     *error = rozofs_rbs_error_rebuild_broken;
		   }
		   else {
		     rbs_error.spare_write_empty++;
		     *error = rozofs_rbs_error_write_failed;		     
		   }
                   goto out;
        	 }	
	         *size_written += disk_block_size;
		 continue;
	      } 

	      // Need to regenerate a projection and need 1rst to regenerate initial data.	

	      // Allocate memory for initial data
              if (working_ctx.data_read_p == NULL) {
		working_ctx.data_read_p = memalign(32,bbytes);
	      }		


              memset(prj_id_present,0,sizeof(prj_id_present));

              for (prj_count = 0; prj_count < count; prj_count++) {

        	  // Get the pointer to the beginning of the projection and extract
        	  // the projection ID
        	  prj_ctx_idx = rbs_prj_idx_table[prj_count];

        	  rozofs_stor_bins_hdr_t *rozofs_bins_hdr_p =
                	  (rozofs_stor_bins_hdr_t*) (working_ctx.prj_ctx[prj_ctx_idx].bins
                	  + (rozofs_msg_psize/sizeof(bin_t) * block_idx));

        	  // Extract the projection_id from the header and fill the table
        	  // of projections for the block block_idx for each projection
        	  projection_id = rozofs_bins_hdr_p->s.projection_id;

		  prj_id_present[projection_id] = 1;

		  if (prj_count < rozofs_inverse) {
        	      rbs_projections[prj_count].angle.p = rozofs_get_angles_p(layout,projection_id);
        	      rbs_projections[prj_count].angle.q = rozofs_get_angles_q(layout,projection_id);
        	      rbs_projections[prj_count].size    = rozofs_get_psizes(layout,bsize,projection_id);
        	      rbs_projections[prj_count].bins    = (bin_t*) (rozofs_bins_hdr_p + 1);
		  }   
              }

              // Inverse data for the block (first_block_idx + block_idx)
              transform128_inverse_copy((pxl_t *) working_ctx.data_read_p,
                		rozofs_inverse,
                		bbytes / rozofs_inverse / sizeof (pxl_t),
                		rozofs_inverse, rbs_projections,
				rozofs_get_max_psize(layout,bsize)*sizeof(bin_t));

	      // Find out which projection id to regenerate
              for (projection_id = 0; projection_id < rozofs_safe; projection_id++) {
	          if (prj_id_present[projection_id] == 0) break;
	      }

	      // Allocate memory for regenerated projection
	      if (pforward == NULL) {
	        pforward = memalign(32,rozofs_msg_psize);
	      }	
	      rozofs_bins_hdr_p = (rozofs_stor_bins_hdr_t *) pforward;
	      rozofs_bins_foot_p = (rozofs_stor_bins_footer_t*)((bin_t*)(rozofs_bins_hdr_p+1)+rozofs_get_psizes(layout,bsize,projection_id));	

	      // Describe projection to rebuild 
              rbs_projections[projection_id].angle.p = rozofs_get_angles_p(layout,projection_id);
              rbs_projections[projection_id].angle.q = rozofs_get_angles_q(layout,projection_id);
              rbs_projections[projection_id].size    = rozofs_get_128bits_psizes(layout,bsize,projection_id);
              rbs_projections[projection_id].bins    = (bin_t*) (rozofs_bins_hdr_p + 1);

              // Generate projections to rebuild
              transform128_forward_one_proj((bin_t *)working_ctx.data_read_p, 
	                        	 rozofs_inverse, 
	                        	 bbytes / rozofs_inverse / sizeof (pxl_t),
					 projection_id, 
					 rbs_projections);

	      // Fill projection header			       
	      rozofs_bins_hdr_p->s.projection_id     = projection_id;			       
	      rozofs_bins_hdr_p->s.effective_length  = pBlock->effective_length;
	      rozofs_bins_hdr_p->s.timestamp         = pBlock->timestamp;			       
              rozofs_bins_hdr_p->s.version           = 0;
              rozofs_bins_hdr_p->s.filler            = 0;

              rozofs_bins_foot_p->timestamp          = pBlock->timestamp;	

              // Store the projection localy	
              ret = sclient_write_rbs(re->storages[spare_idx], st->cid, st->sid,
	                              layout, bsize, 1 /* Spare */,
				      re->dist_set_current, re->fid,
				      *block_start+block_idx, 1,
				      (const bin_t *)rozofs_bins_hdr_p,
				      rebuild_ref);
              remove_file = 0;// This file must exist		   
              if (ret < 0) {
		if (errno == EAGAIN) {
		  rbs_error.spare_write_broken++;
		  status = RBS_EXE_BROKEN;
		  *error = rozofs_rbs_error_rebuild_broken;		  
		}
		else {
		  rbs_error.spare_write_proj++;
        	  severe("sclient_write_rbs failed %s", strerror(errno));
		  *error = rozofs_rbs_error_write_failed;		  		  		  
		}
                goto out;	      
              }	
	      *size_written += disk_block_size;	             
          }

	  *block_start += nb_blocks_read_distant;

      }	
      
      if (remove_file) {
	ret = sclient_remove_chunk_rbs(re->storages[local_idx], st->cid, st->sid, layout, 
	                               1/*spare*/, re->bsize,
	                               re->dist_set_current, re->fid, chunk, rebuild_ref);
      }

      if (rebuild_ref != 0) {
	sclient_rebuild_stop_rbs(re->storages[local_idx], st->cid, st->sid, re->fid, 
	                         rebuild_ref, SP_SUCCESS);
				 
	rebuild_ref = 0;
      } 
      
      // end of file
      if (*block_start == chunk * block_per_chunk) {
        break;
      }
      
      // Next chunk
      chunk++;        
    }
      
    status = RBS_EXE_SUCCESS;
    			      
out:

    *block_start = chunk * block_per_chunk;    
    
    if (rebuild_ref != 0) {
      sclient_rebuild_stop_rbs(re->storages[local_idx], st->cid, st->sid, re->fid, 
                               rebuild_ref, SP_FAILURE);
    }    
    
    // Clear the working context
    if (working_ctx.data_read_p) {
      free(working_ctx.data_read_p);
      working_ctx.data_read_p = NULL;	
    }	    
    for (i = 0; i < rozofs_safe; i++) {
        if (working_ctx.prj_ctx[i].bins) {
            free(working_ctx.prj_ctx[i].bins);
	    working_ctx.prj_ctx[i].bins = NULL;
	}	   
    }    

    if (pforward) {
      free(pforward);
      pforward = NULL;
    }

    return status;
}

#define RESTORE_ONE_RB_ENTRY_FREE_ALL \
	for (i = 0; i < rozofs_safe; i++) {\
            if (working_ctx.prj_ctx[i].bins) {\
        	free(working_ctx.prj_ctx[i].bins);\
		working_ctx.prj_ctx[i].bins = NULL;\
	    }\
	}\
	if (working_ctx.data_read_p) {\
          free(working_ctx.data_read_p);\
          working_ctx.data_read_p = NULL;\
	}\
	if (saved_bins) {\
	  free(saved_bins);\
	  saved_bins = NULL;\
	}\
        memset(&working_ctx, 0, sizeof (working_ctx));
	

RBS_EXE_CODE_E rbs_restore_one_rb_entry(storage_t       * st, 
                                        uint8_t           layout,
                        		int               local_idx, 
					int               relocate,
					uint32_t        * block_start,
					uint32_t          block_end, 
					rb_entry_t      * re, 
					uint8_t           proj_id_to_rebuild,
					uint64_t        * size_written,
					uint64_t        * size_read,
					uint8_t         * error) {
    RBS_EXE_CODE_E    status = RBS_EXE_FAILED;
    int               ret    = -1;
    rbs_storcli_ctx_t working_ctx;
    uint32_t          rebuild_ref;
  
    // Get rozofs layout parameters
    uint32_t bsize                  = re->bsize;
    uint16_t rozofs_disk_psize      = rozofs_get_psizes(layout,bsize,proj_id_to_rebuild);
    uint8_t  rozofs_safe            = rozofs_get_rozofs_safe(layout);
    uint16_t rozofs_max_psize       = rozofs_get_max_psize_in_msg(layout,bsize);
    uint32_t requested_blocks       = ROZOFS_BLOCKS_IN_BUFFER(layout);
    uint32_t nb_blocks_read_distant = requested_blocks;
    bin_t * saved_bins = NULL;
    int     i;   
    uint32_t block_per_chunk = ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(re->bsize);
    uint32_t chunk           = *block_start / block_per_chunk;;
       
    // Clear the working context
    memset(&working_ctx, 0, sizeof (working_ctx));

    rebuild_ref = sclient_rebuild_start_rbs(re->storages[local_idx], st->cid, st->sid, re->fid, 
                                            relocate?SP_NEW_DEVICE:SP_SAME_DEVICE, chunk, 0 /* spare */,  
					    *block_start, block_end);
    if (rebuild_ref == 0) {
      if (errno == EAGAIN) {
        rbs_error.nom_start_again++;
	*error = rozofs_rbs_error_file_to_much_running_rebuild;
      }
      else {
        rbs_error.nom_start++;
	*error = rozofs_rbs_error_rebuild_start_failed;	
      }
      goto out;
    }
        
    // While we can read in the bins file
    while(*block_start <= block_end) {
         
        // Clear the working context
	RESTORE_ONE_RB_ENTRY_FREE_ALL;
	
	if ((block_end!=0xFFFFFFFF) && ((block_end-*block_start+1) < requested_blocks)) {
	  requested_blocks = (block_end-*block_start+1);
	}

        // Try to read blocks on others storages
        ret = rbs_read_blocks(re->storages, local_idx, layout, bsize, st->cid,
                	      re->dist_set_current, re->fid, *block_start,
                	      requested_blocks, &nb_blocks_read_distant, retries,
                	      &working_ctx,
			      size_read);

        if (ret != 0) {
	  rbs_error.nom_read++;
	  *error = rozofs_rbs_error_read_error;	
	  goto out;
	}
        if (nb_blocks_read_distant == 0) break; // End of file
        if (nb_blocks_read_distant == -1) { // File deleted
	   status = RBS_EXE_ENOENT;
	   rbs_error.nom_read_enoent++;
	   goto out;
        }
	
        /*
	** Check whether the projection to rebuild has been read
	*/
	if ((working_ctx.prj_ctx[proj_id_to_rebuild].bins != NULL)
	&&  (working_ctx.prj_ctx[proj_id_to_rebuild].nbBlocks != 0)) {
	  saved_bins = working_ctx.prj_ctx[proj_id_to_rebuild].bins;
	  working_ctx.prj_ctx[proj_id_to_rebuild].bins = NULL;
	} 
	
	if (working_ctx.prj_ctx[proj_id_to_rebuild].bins == NULL) {
          // Allocate memory to store projection
          working_ctx.prj_ctx[proj_id_to_rebuild].bins = memalign(32,rozofs_max_psize * requested_blocks);
        }		  

        memset(working_ctx.prj_ctx[proj_id_to_rebuild].bins, 0,rozofs_max_psize * requested_blocks);

        // Generate projections to rebuild
        ret = rbs_transform_forward_one_proj(working_ctx.prj_ctx,
                			     working_ctx.block_ctx_table,
                			     layout,bsize,0,
                			     nb_blocks_read_distant,
                			     proj_id_to_rebuild,
                			     working_ctx.data_read_p);
        if (ret != 0) {
            severe("rbs_transform_forward_one_proj failed: %s",strerror(errno));
	    rbs_error.nom_transform++;
	    *error = rozofs_rbs_error_transform_error;
            goto out;
        }
	
	/*
	** Compare to read bins
	*/
#if 0		
	if ((saved_bins) 
	&&  (memcmp(working_ctx.prj_ctx[proj_id_to_rebuild].bins,
	             saved_bins,
		     rozofs_disk_psize*nb_blocks_read_distant*sizeof(bin_t)) == 0)
	   ) {
            /* No need to rewrites these blocks */
	    *block_start += nb_blocks_read_distant;
	    continue;
	}
#endif
	
        // Store the projection localy	
        ret = sclient_write_rbs(re->storages[local_idx], st->cid, st->sid,
	                        layout, bsize, 0 /* Not spare */,
				re->dist_set_current, re->fid,
				*block_start, nb_blocks_read_distant,
				working_ctx.prj_ctx[proj_id_to_rebuild].bins,
				rebuild_ref);
	if (ret < 0) {
	  if (errno == EAGAIN) {
	    rbs_error.nom_write_broken++;
	    status = RBS_EXE_BROKEN;
	    *error = rozofs_rbs_error_rebuild_broken;
	  }
	  else {
	    rbs_error.nom_write++;
	    *error = rozofs_rbs_error_write_failed;	    
            severe("sclient_write_rbs failed: %s", strerror(errno));
	  }
          goto out;
        }
	*size_written += (nb_blocks_read_distant * (rozofs_disk_psize+3) * 8);
				
	*block_start += nb_blocks_read_distant;	             
    }
    status = RBS_EXE_SUCCESS;
out:
    if (rebuild_ref != 0) {
      sclient_rebuild_stop_rbs(re->storages[local_idx], st->cid, st->sid, re->fid, rebuild_ref, 
                               (status==0)?SP_SUCCESS:SP_FAILURE);
    }    
    RESTORE_ONE_RB_ENTRY_FREE_ALL;   
    return status;
}
void storaged_rebuild_compact_list(char * fid_list, int fd, int entry_size) {
  uint64_t   write_offset;
  uint64_t   read_offset;
  rozofs_rebuild_entry_file_t   file_entry;
  fid_t      null_fid={0};
        
  write_offset = 0;
  read_offset  = 0;
  
  while (pread(fd,&file_entry,entry_size,read_offset) == entry_size) {
      
    read_offset += entry_size;  
       
    if (file_entry.todo == 0) continue;
    if (memcmp(null_fid,file_entry.fid,sizeof(fid_t))==0) {
      continue;
    }

    if (pwrite(fd, &file_entry, entry_size, write_offset)!=entry_size) {
      severe("pwrite size %lu offset %llu %s",(unsigned long int)entry_size, 
             (unsigned long long int)write_offset, strerror(errno));
    }     
    write_offset += entry_size;
  }
    
    
  if (ftruncate(fd, write_offset) < 0) {
    severe("ftruncate(%s,%llu) %s",fid_list,(long long unsigned int)write_offset,strerror(errno));
  }    
}
/*
** Check the existence of a FID from the export
** Just in case it is being deleted
*/
int check_fid_deleted_from_export(fid_t fid) {
  uint32_t   bsize;
  uint8_t    layout; 
  ep_mattr_t attr;
  int        ret;

  // Resolve this FID thanks to the exportd
  ret = rbs_get_fid_attr(&rpcclt_export, storage_config.export_hostname, fid, &attr, &bsize, &layout);
 
  if ((ret != 0)&&(errno == ENOENT)) return 1;
  return 0;
}
/*
*/
int storaged_rebuild_list(char * fid_list, char * statFilename) {
  int        fdlist = -1;
  int        fdstat = -1;  
  int        nbJobs=0;
  int        nbSuccess=0;
  list_t     cluster_entries;
  uint64_t   offset;
  ROZOFS_RBS_COUNTERS_T         statistics;
  rozofs_rebuild_entry_file_t   file_entry;
  rozofs_rebuild_entry_file_t   file_entry_saved;
  int        ret;
  uint8_t    rozofs_safe,rozofs_forward,rozofs_inverse; 
  uint8_t    prj;
  int        spare;
  char     * pExport_hostname = NULL;
  int        local_index=-1;    
  rb_entry_t re;
  fid_t      null_fid={0};
  int        failed,available;  
  uint64_t   size_written = 0;
  uint64_t   size_read    = 0;
  uint64_t   entry_idx;

        
  fdlist = open(fid_list,O_RDWR);
  if (fdlist < 0) {
    if (errno == ENOENT) {
      REBUILD_MSG("  <-> %s no file to rebuild",fid_list);    
	  return 0;
    }
    
	severe("Can not open file %s %s",fid_list,strerror(errno));
    goto error;
  } 

        
  fdstat = open(statFilename,O_RDWR | O_CREAT, 0755);
  if (fdstat < 0) {
      severe("Can not open file %s %s",statFilename,strerror(errno));
      goto error;
  }
  memset(&statistics,0,sizeof(statistics));
  ret = pread(fdstat,&statistics,sizeof(statistics),0);  
  if ((ret != 0) && (ret != sizeof(statistics))) {
      severe("Can not read statistics in file %s %s",statFilename,strerror(errno));
      goto error;
  }  


        
  int entry_size = rbs_entry_size_from_layout(storage_config.layout);
  //info("layout %d entry size %d", storage_config.layout, entry_size);

  // Initialize the list of storage config
  if (sconfig_initialize(&sconfig) != 0) {
      severe("Can't initialize storaged config: %s.\n",strerror(errno));
      goto error;
  }
  
  // Read the configuration file
  if (sconfig_read(&sconfig, storage_config.config_file,0) != 0) {
      severe("Failed to parse storage configuration file %s : %s.\n",storage_config.config_file,strerror(errno));
      goto error;
  }

  // Initialization of the storage configuration
  if (storaged_initialize() != 0) {
      severe("can't initialize storaged: %s.", strerror(errno));
      goto error;
  }


  // Initialize the list of cluster(s)
  list_init(&cluster_entries);
  
  memset(&rpcclt_export,0,sizeof(rpcclt_export));

  // Try to get the list of storages for this cluster ID
  pExport_hostname = rbs_get_cluster_list(&rpcclt_export, 
                           storage_config.export_hostname, 
			   storage_config.site,
			   storage_config.storage.cid, 
			   &cluster_entries);			   
  if (pExport_hostname == NULL) {			   
      severe("Can't get list of others cluster members from export server (%s) for storage to rebuild (cid:%u; sid:%u): %s\n",
              storage_config.export_hostname, 
	      storage_config.storage.cid, 
	      storage_config.storage.sid, 
	      strerror(errno));
      goto error;
  }
    
  // Get connections for this given cluster  
  rbs_init_cluster_cnts(&cluster_entries, storage_config.storage.cid, storage_config.storage.sid,&failed,&available);
  
  /*
  ** Check that enough servers are available
  */
  rozofs_inverse = rozofs_get_rozofs_inverse(storage_config.layout);  
  rozofs_safe    = rozofs_get_rozofs_safe(storage_config.layout);
  rozofs_forward = rozofs_get_rozofs_forward(storage_config.layout);

  if (available<rozofs_inverse) {
    /*
    ** Not possible to rebuild any thing
    */
    REBUILD_MSG("only %d failed storages !!!",available);
    goto error;
  }
  if (failed > (rozofs_forward-rozofs_inverse)) {
    /*
    ** Possibly some file may not be rebuilt !!! 
    */
    REBUILD_MSG("   -> %s rebuild start (%d storaged failed!!!)",fid_list, failed);
  }
  else {
    /*
    ** Every file should be rebuilt 
    */
    REBUILD_MSG("   -> %s rebuild start",fid_list);
  }
  
  nbJobs    = 0;
  nbSuccess = 0;
  offset    = 0;

  entry_idx       = 0;
  
  while (pread(fdlist,&file_entry,entry_size,offset) == entry_size) {
  
    if (sigusr_received) break;
  
    entry_idx++;
    offset += entry_size; 
       
    if (file_entry.todo == 0) continue;
    if (memcmp(null_fid,file_entry.fid,sizeof(fid_t))==0) {
      severe("Null entry");
      continue;
    }
    // Padd end of distribution with 0. Just in case...
    memset(&file_entry.dist_set_current[rozofs_safe],0,ROZOFS_SAFE_MAX-rozofs_safe); 
    memcpy(&file_entry_saved,&file_entry,entry_size);

    nbJobs++;
        
#if 0
  {
    char fid_string[128];
    int i;
    rozofs_uuid_unparse(file_entry.fid,fid_string);  
    printf("rebuilding FID %s layout %d bsize %d from %llu to %llu dist %d",
          fid_string,storage_config.layout, file_entry.bsize,
         (long long unsigned int) file_entry.block_start, 
	 (long long unsigned int) file_entry.block_end,
	 file_entry.dist_set_current[0]);
   for (i=1;i<rozofs_safe;i++) printf("-%d", file_entry.dist_set_current[i]);
   printf("\n");
  }  
#endif
    
    memcpy(re.fid,file_entry.fid, sizeof(re.fid));
    memcpy(re.dist_set_current,file_entry.dist_set_current, sizeof(re.dist_set_current));
    re.bsize = file_entry.bsize;
    re.layout = storage_config.layout;

    // Get storage connections for this entry
    local_index = rbs_get_rb_entry_cnts(&re, 
                              &cluster_entries, 
                              storage_config.storage.cid, 
			      storage_config.storage.sid,
                              rozofs_inverse);
    if (local_index == -1) {
      if      (errno==EINVAL) file_entry.error = rozofs_rbs_error_no_such_cluster;
      else if (errno==EPROTO) file_entry.error = rozofs_rbs_error_not_enough_storages_up;
      else                    file_entry.error = rozofs_rbs_error_unknown;
      severe( "rbs_get_rb_entry_cnts failed cid/sid %d/%d %s", 
	          storage_config.storage.cid,
			  storage_config.storage.sid,
			  strerror(errno));
      continue; // Try with the next
    }
    
    // Compute the proj_id to rebuild
    // Check if the storage to rebuild is
    // a spare for this entry
    for (prj = 0; prj < rozofs_safe; prj++) {
        if (re.dist_set_current[prj] == storage_config.storage.sid)  break;
    }  
    if (prj >= rozofs_forward) spare = 1;
    else                       spare = 0;


    int retry = 0; 
    while (retry < 1) { /* reloop 3 times immediatly on broken rebuild */
      
      retry++; 
     
      size_written = 0;
      size_read    = 0;
      
      // Restore this entry
      uint32_t block_start = file_entry.block_start;
      if (spare == 1) {
	ret = rbs_restore_one_spare_entry(&storage_config.storage, storage_config.layout, local_index, file_entry.relocate,
                                          &block_start, file_entry.block_end, 
					  &re, prj,
					  &size_written,
					  &size_read,
					  &file_entry.error);     
      }
      else {
	ret = rbs_restore_one_rb_entry(&storage_config.storage, storage_config.layout, local_index, file_entry.relocate,
                                       &block_start, file_entry.block_end, 
				       &re, prj, 
				       &size_written,
				       &size_read,
				       &file_entry.error);
      } 
	  
      /*
      ** In case the rebuild failed, check with the export whether 
      ** the FID still exists
      */
      if (ret == RBS_EXE_FAILED) {
        if (check_fid_deleted_from_export(file_entry.fid)) ret = RBS_EXE_ENOENT;
      }
      /* 
      ** Rebuild is successfull
      */
      switch(ret) {
      
	case RBS_EXE_SUCCESS:
	case RBS_EXE_ENOENT:	
	
	  if (ret == RBS_EXE_ENOENT) {
	    file_entry.error = rozofs_rbs_error_file_deleted;
	    statistics.deleted++;
	  }  
	  else {                      
	    file_entry.error = rozofs_rbs_error_none;
	  }
	  nbSuccess++;
	  // Update counters in header file 
	  statistics.done_files++;
	  if (spare == 1) {
        statistics.written_spare += size_written;
	    statistics.read_spare    += size_read;
	  }
	  statistics.written += size_written;
	  statistics.read    += size_read;       

	  if (pwrite(fdstat, &statistics, sizeof(statistics), 0)!= sizeof(statistics)) {
         severe("pwrite %s %s",statFilename,strerror(errno));
	  }

	  if ((nbSuccess % (16*1024)) == 0) {
            REBUILD_MSG("  ~ %s %d/%d",fid_list,nbSuccess,nbJobs);
	  } 
	  /*
	  ** This file has been rebuilt so remove it from the job list
	  */
	  file_entry.todo = 0;
          break;


	/*
	** Rebuild is failed, nevetherless some pieces of the file may have
	** been successfully rebuilt and needs not to be rebuilt again on a
	** next trial
	*/
	default:
	  severe("Unexpected return code %d.",ret);	  
	case RBS_EXE_FAILED:
	case RBS_EXE_BROKEN:
	  /*
	  ** In case of file relocation, the new data chunk file has been removed 
	  ** and the previous data chunk file location has been restored
	  ** when the rebuild has failed. So we are back to the starting point of
	  ** the rebuilt and there has been no improvment...
	  ** When no relocation was requested, the block_start has increased up to 
	  ** where the rebuild has failed. These part before block_start is rebuilt 
	  ** and needs not to be redone although the glocal rebuild has failed. 
	  */
	  if (!file_entry.relocate) {
            file_entry.block_start = block_start;
	  }
	  break;  
      }

      /*
      ** Update input job file if any change
      */
      if (memcmp(&file_entry_saved,&file_entry, entry_size) != 0) {
	if (pwrite(fdlist, &file_entry, entry_size, offset-entry_size)!=entry_size) {
	  severe("pwrite size %lu offset %llu %s",(unsigned long int)entry_size, 
        	 (unsigned long long int) offset-entry_size, strerror(errno));
	}
      }
      
      if (ret != RBS_EXE_BROKEN) break;
      /* reloop 3 times immediatly on broken rebuild */
    } 
    
    /* Next file to rebuild */     
  }
  
  if (nbSuccess == nbJobs) {
    REBUILD_MSG("  <-  %s rebuild success of %d files",fid_list,nbSuccess);    
    close(fdlist);
	unlink(fid_list);
    close(fdstat);	
    return 0;
  }
  
  /*
  ** Truncate the file after the last failed entry
  */
  if (nbSuccess!=0) {
    storaged_rebuild_compact_list(fid_list, fdlist, entry_size);
  }
   
error: 
  if (sigusr_received) {
    REBUILD_MSG("  !!! %s rebuild paused %d/%d",fid_list,nbJobs-nbSuccess,nbJobs);    
  }
  else {
    REBUILD_MSG("  !!! %s rebuild failed %d/%d",fid_list,nbJobs-nbSuccess,nbJobs);
  }	
  display_rbs_errors();
  if (fdlist != -1) close(fdlist);   
  if (fdstat != -1) close(fdstat);   
  return 1;
}

static void on_stop() {
    DEBUG_FUNCTION;   

    rozofs_layout_release();
    storaged_release();
    closelog();
}

char * utility_name=NULL;

void usage() {

    printf("RozoFS storage rebuilder - %s\n", VERSION);
    printf("Usage: %s [OPTIONS]\n\n",utility_name);
    printf("   -h, --help\t\t\tprint this message.\n");
	printf("   -c, --cid\tcid to rebuild.\n");
	printf("   -s, --sid\tsid to rebuild.\n");
	printf("   -r, --ref\trebuild reference.\n");
	printf("   -i, --instance\trebuild instance number.\n");    
    printf("   -q, --quiet \tDo not print.\n");    
}

int main(int argc, char *argv[]) {
    int    c;
    char * dir;  
	 
    /*
    ** Change local directory to "/"
    */
    if (chdir("/")!= 0) {}
    rozofs_layout_initialize();
        
    static struct option long_options[] = {
        { "help", no_argument, 0, 'h'},
        { "cid", required_argument, 0, 'c'},	
        { "sid", required_argument, 0, 's'},	
        { "ref", required_argument, 0, 'r'},	
        { "quiet", no_argument, 0, 'q'},
        { "instance", required_argument, 0, 'i'},	
        { 0, 0, 0, 0}
    };

    // Get utility name
    utility_name = basename(argv[0]);   

    // Init of the timer configuration
    rozofs_tmr_init_configuration();
   
    while (1) {

        int option_index = 0;
        c = getopt_long(argc, argv, "hH:c:s:r:i:q:", long_options, &option_index);

        if (c == -1)
            break;

        switch (c) {

            case 'h':
                usage();
                exit(EXIT_SUCCESS);
                break;

            case 'c':
			  if (sscanf(optarg,"%d",&cid)!=1) {
			    usage();
                exit(EXIT_FAILURE);
			  }
			  break;

            case 's':
			  if (sscanf(optarg,"%d",&sid)!=1) {
			    usage();
                exit(EXIT_FAILURE);
			  }
			  break;
			  			  
            case 'r':
			  if (sscanf(optarg,"%d",&rebuildRef)!=1) {
			    usage();
                exit(EXIT_FAILURE);
			  }
			  break;			  
			  break;
			  			  
            case 'i':
			  if (sscanf(optarg,"%d",&instance)!=1) {
			    usage();
                exit(EXIT_FAILURE);
			  }
			  break;			  

	        case 'q':
	          quiet = 1;
		      break;
		
            case '?':
                usage();
                exit(EXIT_SUCCESS);
                break;
            default:
                usage();
                exit(EXIT_FAILURE);
                break;
        }
    }
    uma_dbg_record_syslog_name("RBS_LIST");
    
    
    /*
    ** Check parameter consistency
    */
    if (cid == -1){
        fprintf(stderr, "storage_rebuilder failed. Missing --cid option.\n");
        exit(EXIT_FAILURE);
    }  
    if (sid == -1){
        fprintf(stderr, "storage_rebuilder failed. Missing --sid option.\n");
        exit(EXIT_FAILURE);
    }  
    if (rebuildRef == -1){
        fprintf(stderr, "storage_rebuilder failed. Missing --rebuildRef option.\n");
        exit(EXIT_FAILURE);
    }  
    if (instance == -1){
        fprintf(stderr, "storage_rebuilder failed. Missing --instance option.\n");
        exit(EXIT_FAILURE);
    }              
    /*
    ** read common config file
    */
    common_config_read(NULL);    
 
    /*
	** Read storage configuration file
	*/
    dir = get_rebuild_sid_directory_name(rebuildRef,cid,sid);
    if (rbs_read_storage_config_file(dir, &storage_config) == NULL) {
	  severe("No storage conf file in %s",dir);
      fprintf(stderr, "No storage conf file in %s",dir);
      exit(EXIT_FAILURE);	    
	}		
	
    sprintf(fid_list, "%s/job%d",dir,instance);
    sprintf(statFilename, "%s/stat%d",dir,instance);

    /*
    ** SIGUSR handler
    */
    signal(SIGUSR1, rbs_cath_sigusr);  

	
    // Start rebuild storage   
    if (storaged_rebuild_list(fid_list,statFilename) != 0) goto error;    
    on_stop();
    exit(EXIT_SUCCESS);
    
error:
    exit(EXIT_FAILURE);
}
