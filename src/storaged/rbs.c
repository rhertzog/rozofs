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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <limits.h>

#include <rozofs/common/list.h>
#include <rozofs/common/htable.h>
#include <rozofs/rozofs.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/rpc/rpcclt.h>
#include <rozofs/rpc/mclient.h>
#include <rozofs/common/profile.h>
#include <rozofs/rpc/spproto.h>
#include <rozofs/core/rozofs_host2ip.h>

#include "storage.h"
#include "rbs_sclient.h"
#include "rbs_eclient.h"
#include "rbs.h"

DECLARE_PROFILING(spp_profiler_t);

/* Size of htable for bins files to rebuild  */
#define REBUILD_HSIZE 16384
/* Print debug trace */
#define DEBUG_RBS 0
/* Time in seconds between two passes of reconstruction */
#define RBS_TIME_BETWEEN_2_PASSES 30

/* Local storage to rebuild */
static storage_t storage_to_rebuild;
/* List of cluster(s) */
static list_t cluster_entries;
/* List of bins files to rebuild */
static list_t rebuild_entries;
/* Htable for bins files to rebuild */
static htable_t htable_fid;
/* RPC client for exports server */
static rpcclt_t rpcclt_export;
/* nb. of retries for get bins on storages */
static uint32_t retries = 10;
uint8_t prj_id_present[ROZOFS_SAFE_MAX];

static inline int fid_cmp(void *key1, void *key2) {
    return memcmp(key1, key2, sizeof (fid_t));
}

static unsigned int fid_hash(void *key) {
    uint32_t hash = 0;
    uint8_t *c;
    for (c = key; c != key + 16; c++)
        hash = *c + (hash << 6) + (hash << 16) - hash;
    return hash;
}

static inline void put_rb_entry(rb_entry_t * re) {
    htable_put(&htable_fid, re->fid, re);
    list_push_front(&rebuild_entries, &re->list);
}

static inline void del_rb_entry(rb_entry_t * re) {
    htable_del(&htable_fid, re->fid);
    list_remove(&re->list);
}

static inline rb_entry_t *get_rb_entry_by_fid(fid_t fid) {
    return htable_get(&htable_fid, fid);
}

static int ckeck_mtime(int fd, struct timespec st_mtim) {
    struct stat st;

    if (fstat(fd, &st) != 0)
        return -1;

    if (st_mtim.tv_sec == st.st_mtim.tv_sec &&
            st_mtim.tv_nsec == st.st_mtim.tv_nsec)
        return 0;

    return -1;
}

static int rbs_restore_one_spare_entry(storage_t * st, rb_entry_t * re, char * path, uint8_t spare_idx) {
    int status = -1;
    int i = 0;
    int fd = -1;
    int ret = -1;
    struct stat loc_file_stat;
    uint32_t loc_file_init_blocks_nb = 0;
    uint8_t version = 0;
    uint32_t nb_blocks_read_distant = ROZOFS_BLOCKS_MAX;
    bid_t first_block_idx = 0;
    uint64_t file_size = 0;
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
    rozofs_stor_bins_hdr_t * bins_hdr_local_p;    
    bin_t * loc_read_bins_p = NULL;
    size_t local_len_read;
    int    remove_file;

        
    // Get rozofs layout parameters
    uint8_t  layout            = re->layout;
    uint8_t  rozofs_safe       = rozofs_get_rozofs_safe(layout);
    uint8_t  rozofs_forward    = rozofs_get_rozofs_forward(layout);
    uint8_t  rozofs_inverse    = rozofs_get_rozofs_inverse(layout);
    uint16_t disk_block_size   = (rozofs_get_max_psize(layout)*sizeof (bin_t)) + sizeof (rozofs_stor_bins_hdr_t);
    uint16_t disk_block_bins_size   = disk_block_size/sizeof(bin_t);

    // Clear the working context
    memset(&working_ctx, 0, sizeof (rbs_storcli_ctx_t));

    // Check that this file already exists on the storage to rebuild
    if (stat(path, &loc_file_stat) == 0) {
    
        // Compute the nb. of blocks	
	loc_file_init_blocks_nb = loc_file_stat.st_size - ROZOFS_ST_BINS_FILE_HDR_SIZE;
        loc_file_init_blocks_nb /= disk_block_size;
		
	// Allocate memory to read the local file	
        loc_read_bins_p = xmalloc(ROZOFS_BLOCKS_MAX*disk_block_size);
	
	remove_file = 1; // This file probably needs to be removed
    }
    else {
        remove_file = 0;	
    }

    // While we can read in the bins file
    while (nb_blocks_read_distant == ROZOFS_BLOCKS_MAX) {
         
        // Free bins read in previous round
	for (i = 0; i < rozofs_safe; i++) {
            if (working_ctx.prj_ctx[i].bins) {
        	free(working_ctx.prj_ctx[i].bins);
		working_ctx.prj_ctx[i].bins      = NULL;
	    }	   
	    working_ctx.prj_ctx[i].prj_state = PRJ_READ_IDLE;
	}
    
        // Read every available bins
	ret = rbs_read_all_available_proj(re->storages, spare_idx, layout, st->cid,
                                	  re->dist_set_current, re->fid, first_block_idx,
                                	  ROZOFS_BLOCKS_MAX, &nb_blocks_read_distant,
                                	  &working_ctx);
					  
	// Reading at least inverse projection has failed				  
        if (ret != 0) {
            remove_file	= 0;// Better keep the file	
            errno = EIO;	
            goto out;
        }
	
	if (nb_blocks_read_distant == 0) break; // End of file	
	 
	// If local file exist and has some interesting blocks
	local_len_read = 0;
	if (loc_file_init_blocks_nb > first_block_idx) {
	
            // Read local bins
            ret = storage_read(st, layout, re->dist_set_current, 1/*spare*/, re->fid,
                    first_block_idx, nb_blocks_read_distant, loc_read_bins_p,
                    &local_len_read, &file_size);

            if (ret != 0) {
	        local_len_read = 0;
                severe("storage_read failed: %s", strerror(errno));
            }
            else {
                // Compute the nb. of local blocks read
                local_len_read = local_len_read / disk_block_size;
	    }		      	
	}
	
	// Loop on the received blocks
        pBlock = &working_ctx.block_ctx_table[0];	
        for (block_idx = 0; block_idx < nb_blocks_read_distant; block_idx++,pBlock++) {

            count = rbs_count_timestamp_tb(working_ctx.prj_ctx, layout, block_idx,
                                           rbs_prj_idx_table, 
			  		   &pBlock->timestamp,
                                           &pBlock->effective_length);
					   
	    // Less than rozofs_inverse projection. Can not regenerate anything 
	    // from what has been read				   	
	    if (count < 0) {
                remove_file = 0;// Better keep the file	    
        	errno = EIO;	
        	goto out;	      
	    }
	    
	    // All projections have been read. Nothing to regenerate for this block
	    if (count >= rozofs_forward) continue;

	       
            // Enough projection read to regenerate data, but not as much as required.   


            // Is this block already written on local disk
            if (block_idx < local_len_read) {

                bins_hdr_local_p = (rozofs_stor_bins_hdr_t *) 
			(loc_read_bins_p + (disk_block_bins_size * block_idx));

               // Compare timestamp of local and distant block
               if (bins_hdr_local_p->s.timestamp == pBlock->timestamp) {
                   remove_file = 0;// This file must exist
                   continue; // Check next block
               }
	    }   
	    

            // Case of the empty block
            if (pBlock->timestamp == 0) {
	     
	       prj_ctx_idx = rbs_prj_idx_table[0];
	           
               // Store the projections on local bins file	
               ret = storage_write(st, layout, re->dist_set_current, 1/*spare*/,
                		   re->fid, first_block_idx+block_idx, 1, version,
                		   &file_size, working_ctx.prj_ctx[prj_ctx_idx].bins);
               remove_file = 0;	// This file must exist   
               if (ret <= 0) {
                   severe("storage_write failed %s: %s", path, strerror(errno));
                   goto out;
               }	       
	       continue;
	    }  		
	    
	    // Need to regenerate a projection and need 1rst to regenerate initial data.	
	    
	    // Allocate memory for initial data
            if (working_ctx.data_read_p == NULL) {
	      working_ctx.data_read_p = xmalloc(ROZOFS_BSIZE);
	    }		


            memset(prj_id_present,0,sizeof(prj_id_present));
	    
            for (prj_count = 0; prj_count < count; prj_count++) {

        	// Get the pointer to the beginning of the projection and extract
        	// the projection ID
        	prj_ctx_idx = rbs_prj_idx_table[prj_count];

        	rozofs_stor_bins_hdr_t *rozofs_bins_hdr_p =
                	(rozofs_stor_bins_hdr_t*) (working_ctx.prj_ctx[prj_ctx_idx].bins
                	+ (disk_block_bins_size * block_idx));

        	// Extract the projection_id from the header and fill the table
        	// of projections for the block block_idx for each projection
        	projection_id = rozofs_bins_hdr_p->s.projection_id;
		
		prj_id_present[projection_id] = 1;
		
		if (prj_count < rozofs_inverse) {
        	    rbs_projections[prj_count].angle.p = rozofs_get_angles_p(layout,projection_id);
        	    rbs_projections[prj_count].angle.q = rozofs_get_angles_q(layout,projection_id);
        	    rbs_projections[prj_count].size    = rozofs_get_psizes(layout,projection_id);
        	    rbs_projections[prj_count].bins    = (bin_t*) (rozofs_bins_hdr_p + 1);
		}   
            }

            // Inverse data for the block (first_block_idx + block_idx)
            transform_inverse((pxl_t *) working_ctx.data_read_p,
                	      rozofs_inverse,
                	      ROZOFS_BSIZE / rozofs_inverse / sizeof (pxl_t),
                	      rozofs_inverse, rbs_projections);
	    
	    // Find out which projection id to regenerate
            for (projection_id = 0; projection_id < rozofs_safe; projection_id++) {
	        if (prj_id_present[projection_id] == 0) break;
	    }
	    
	    // Allocate memory for regenerated projection
	    if (pforward == NULL) pforward = xmalloc(disk_block_size);
	    rozofs_bins_hdr_p = (rozofs_stor_bins_hdr_t *) pforward;
	    
	    // Describe projection to rebuild 
            rbs_projections[projection_id].angle.p = rozofs_get_angles_p(layout,projection_id);
            rbs_projections[projection_id].angle.q = rozofs_get_angles_q(layout,projection_id);
            rbs_projections[projection_id].size    = rozofs_get_psizes(layout,projection_id);
            rbs_projections[projection_id].bins    = (bin_t*) (rozofs_bins_hdr_p + 1);

            // Generate projections to rebuild
            transform_forward_one_proj((const bin_t *)working_ctx.data_read_p, 
	                               rozofs_inverse, 
	                               ROZOFS_BSIZE / rozofs_inverse / sizeof (pxl_t),
				       projection_id, 
				       rbs_projections);
				
	    // Fill projection header			       
	    rozofs_bins_hdr_p->s.projection_id     = projection_id;			       
	    rozofs_bins_hdr_p->s.effective_length  = pBlock->effective_length;
	    rozofs_bins_hdr_p->s.timestamp         = pBlock->timestamp;			       
            rozofs_bins_hdr_p->s.version           = 0;
            rozofs_bins_hdr_p->s.filler            = 0;  
	    
            // Store the projections on local bins file	
            ret = storage_write(st, layout, re->dist_set_current, 1/*spare*/,
                		re->fid, first_block_idx+block_idx, 1, version,
                		&file_size, (const bin_t *)rozofs_bins_hdr_p);
            remove_file = 0;// This file must exist		   
            if (ret <= 0) {
        	severe("storage_write failed %s: %s", path, strerror(errno));
        	goto out;
            }	       
        }
	
	first_block_idx += nb_blocks_read_distant;
	    				  
    }	
    
    
    // Check if the initial local bins file size is bigger
    // than others bins files
    if (loc_file_init_blocks_nb > first_block_idx) {

        off_t length = ROZOFS_ST_BINS_FILE_HDR_SIZE + first_block_idx * disk_block_size;
        ret = truncate(path, length);
        if (ret != 0) {
            severe("truncate(%s) failed: %s", path, strerror(errno));
            goto out;
        }
    }
        
    status = 0;
    			      
out:
    // This spare file used to exist but is not needed any more
    if (remove_file) {
        storage_rm_file(st, layout, re->dist_set_current,re->fid);
    }  
    
    for (i = 0; i < rozofs_safe; i++) {
        if (working_ctx.prj_ctx[i].bins) {
            free(working_ctx.prj_ctx[i].bins);
	    working_ctx.prj_ctx[i].bins = NULL;
	}	   
    }    
    if (fd != -1) {
      close(fd);
      fd = -1;
    }  
    if (working_ctx.data_read_p) {
      free(working_ctx.data_read_p);
      working_ctx.data_read_p = NULL;
    }  
    if (pforward) {
      free(pforward);
      pforward = NULL;
    }
    if (loc_read_bins_p) {
      free(loc_read_bins_p);
      loc_read_bins_p = NULL;
    }	
    return status;
}
static int rbs_restore_one_rb_entry(storage_t * st, rb_entry_t * re) {
    int status = -1;
    int i = 0;
    char path[FILENAME_MAX];
    int fd = -1;
    uint8_t spare = 0;
    int ret = -1;
    size_t nb_write = 0;
    uint8_t loc_file_exist = 1;
    struct stat loc_file_stat;
    uint32_t loc_file_init_blocks_nb = 0;
    bin_t * loc_read_bins_p = NULL;
    uint8_t version = 0;
    tid_t proj_id_to_rebuild = 0;
    uint32_t nb_blocks_read_distant = ROZOFS_BLOCKS_MAX;
    bid_t first_block_idx = 0;
    uint64_t file_size = 0;
    rbs_storcli_ctx_t working_ctx;

    // Get rozofs layout parameters
    uint8_t layout = re->layout;
    uint8_t rozofs_safe = rozofs_get_rozofs_safe(layout);
    uint8_t rozofs_forward = rozofs_get_rozofs_forward(layout);
    uint16_t rozofs_max_psize = rozofs_get_max_psize(layout);

    // Clear the working context
    memset(&working_ctx, 0, sizeof (rbs_storcli_ctx_t));

    // Compute the proj_id to rebuild
    // Check if the storage to rebuild is
    // a spare for this entry
    for (i = 0; i < rozofs_safe; i++) {
        if (re->dist_set_current[i] == st->sid) {
            proj_id_to_rebuild = i;
            if (i >= rozofs_forward) {
                spare = 1;
            }
        }
    }

    // Build the full path of directory that contains the bins file
    storage_map_distribution(st, re->layout, re->dist_set_current, spare, path);

    // Check that this directory already exists, otherwise it will be create
    if (access(path, F_OK) == -1) {
        if (errno == ENOENT) {
            // If the directory doesn't exist, create it
            if (mkdir(path, ROZOFS_ST_DIR_MODE) != 0) {
                severe("mkdir failed (%s) : %s", path, strerror(errno));
                goto out;
            }
        } else {
            goto out;
        }
    }

    // Build the path of bins file
    storage_map_projection(re->fid, path);
    
    // Process specifically the case of the spare storage
    if (spare == 1) {
      status = rbs_restore_one_spare_entry(st, re, path, proj_id_to_rebuild);
      goto out;
    }    

    // Check that this file already exists
    // on the storage to rebuild
    if (access(path, F_OK) == -1) 
        loc_file_exist = 0;
     
    // If the local file exist
    // we must to check the nb. of blocks for this file
    if (loc_file_exist) {
        // Stat file
        if (stat(path, &loc_file_stat) != 0)
            goto out;
	if (!S_ISREG(loc_file_stat.st_mode)) {
	    severe("%s is %x", path, loc_file_stat.st_mode);
	    goto out;  	   	
	}
	if (loc_file_stat.st_size < ROZOFS_ST_BINS_FILE_HDR_SIZE) {
	    severe("%s has size %d", path, (int)loc_file_stat.st_size);
	    loc_file_exist = 0;  	   
	}
	else {
            // Compute the nb. of blocks
            loc_file_init_blocks_nb = (loc_file_stat.st_size -
                    ROZOFS_ST_BINS_FILE_HDR_SIZE) /
                    ((rozofs_max_psize * sizeof (bin_t))
                    + sizeof (rozofs_stor_bins_hdr_t));
	}  
    }

    // While we can read in the bins file
    while (nb_blocks_read_distant == ROZOFS_BLOCKS_MAX) {

        // Clear the working context
        memset(&working_ctx, 0, sizeof (rbs_storcli_ctx_t));

        // Try to read blocks on others storages
        ret = rbs_read_blocks(re->storages, layout, st->cid,
                re->dist_set_current, re->fid, first_block_idx,
                ROZOFS_BLOCKS_MAX, &nb_blocks_read_distant, retries,
                &working_ctx);

        if (ret != 0) {
            severe("rbs_read_blocks failed for block %"PRIu64": %s",
                    first_block_idx, strerror(errno));
            goto out;
        }

        if (nb_blocks_read_distant == 0)
            continue; // End of file


        if (first_block_idx == 0) {
            // Open local bins file for the first write
            fd = open(path, ROZOFS_ST_BINS_FILE_FLAG, ROZOFS_ST_BINS_FILE_MODE);
            if (fd < 0) {
                severe("open failed (%s) : %s", path, strerror(errno));
                goto out;
            }
        }

        // If we write the bins file for the first time,
        // we must write the header
        if (!loc_file_exist && first_block_idx == 0) {
            // Prepare file header
            rozofs_stor_bins_file_hdr_t file_hdr;
            memcpy(file_hdr.dist_set_current, re->dist_set_current,
                    ROZOFS_SAFE_MAX * sizeof (sid_t));
            memset(file_hdr.dist_set_next, 0, ROZOFS_SAFE_MAX * sizeof (sid_t));
            file_hdr.layout = layout;
            file_hdr.version = version;

            // Write the header for this bins file
            nb_write = pwrite(fd, &file_hdr, sizeof (file_hdr), 0);
            if (nb_write != sizeof (file_hdr)) {
                severe("pwrite failed: %s", strerror(errno));
                goto out;
            }
        }

        if (stat(path, &loc_file_stat) != 0)
            goto out;

        // If projections to rebuild are not present on local file
        // - generate the projections to rebuild
        // - store projections on local bins file
        if (!loc_file_exist || loc_file_init_blocks_nb <= first_block_idx) {

            // Allocate memory for store projection
            working_ctx.prj_ctx[proj_id_to_rebuild].bins =
                    xmalloc((rozofs_max_psize * sizeof (bin_t) +
                    sizeof (rozofs_stor_bins_hdr_t)) * nb_blocks_read_distant);

            memset(working_ctx.prj_ctx[proj_id_to_rebuild].bins, 0,
                    (rozofs_max_psize * sizeof (bin_t) +
                    sizeof (rozofs_stor_bins_hdr_t)) *
                    nb_blocks_read_distant);

            // Generate projections to rebuild
            ret = rbs_transform_forward_one_proj(
                    working_ctx.prj_ctx,
                    working_ctx.block_ctx_table,
                    layout,
                    0,
                    nb_blocks_read_distant,
                    proj_id_to_rebuild,
                    working_ctx.data_read_p);
            if (ret != 0) {
                severe("rbs_transform_forward_one_proj failed: %s",
                        strerror(errno));
                goto out;
            }

            // Check mtime of local file
            ret = ckeck_mtime(fd, loc_file_stat.st_mtim);
            if (ret != 0) {
                severe("rbs_restore_one_rb_entry failed:"
                        " concurrent access detected");
                goto out;
            }

            // Store the projections on local bins file
            ret = storage_write(st, layout, re->dist_set_current, spare,
                    re->fid, first_block_idx, nb_blocks_read_distant, version,
                    &file_size, working_ctx.prj_ctx[proj_id_to_rebuild].bins);

            if (ret <= 0) {
                severe("storage_write failed: %s", strerror(errno));
                goto out;
            }

            // Update local file stat
            if (fstat(fd, &loc_file_stat) != 0)
                goto out;

            // Free projections generated
            free(working_ctx.prj_ctx[proj_id_to_rebuild].bins);
            working_ctx.prj_ctx[proj_id_to_rebuild].bins = NULL;

            // Free data read
            free(working_ctx.data_read_p);
            working_ctx.data_read_p = NULL;

            // Update the next first block to read
            first_block_idx += nb_blocks_read_distant;

            // Go to the next blocks
            continue;
        }

        loc_read_bins_p = NULL;
        size_t local_len_read = 0;
        uint32_t local_blocks_nb_read = 0;

        // If the bins file exist and the size is sufficient:
        if (loc_file_exist) {

            // Allocate memory for store local bins read
            loc_read_bins_p = xmalloc(nb_blocks_read_distant *
                    ((rozofs_max_psize * sizeof (bin_t))
                    + sizeof (rozofs_stor_bins_hdr_t)));

            // Read local bins
            ret = storage_read(st, layout, re->dist_set_current, spare, re->fid,
                    first_block_idx, nb_blocks_read_distant, loc_read_bins_p,
                    &local_len_read, &file_size);

            if (ret != 0) {
                severe("storage_read failed: %s", strerror(errno));
                goto out;
            }

            // Compute the nb. of local blocks read
            local_blocks_nb_read = local_len_read /
                    ((rozofs_max_psize * sizeof (bin_t))
                    + sizeof (rozofs_stor_bins_hdr_t));

            // For each block read on distant storages
            for (i = 0; i < nb_blocks_read_distant; i++) {

                // If the block exist on local bins file
                if (i < local_blocks_nb_read) {

                    // Get pointer on current bins header
                    bin_t * current_loc_bins_p = loc_read_bins_p +
                            ((rozofs_max_psize +
                            (sizeof (rozofs_stor_bins_hdr_t) / sizeof (bin_t)))
                            * i);

                    rozofs_stor_bins_hdr_t * bins_hdr_local_p =
                            (rozofs_stor_bins_hdr_t *) current_loc_bins_p;

                    // Compare timestamp of local and distant block
                    if (bins_hdr_local_p->s.timestamp ==
                            working_ctx.block_ctx_table[i].timestamp) {
                        // The timestamp is the same on local
                        // Not need to generate a projection
                        if (DEBUG_RBS == 1) {
                            severe("SAME TS FOR BLOCK: %"PRIu64"",
                                    (first_block_idx + i));
                        }
                        continue; // Check next block
                    }
                }

                // The timestamp is not the same

                // Allocate memory for store projection
                working_ctx.prj_ctx[proj_id_to_rebuild].bins =
                        xmalloc((rozofs_max_psize * sizeof (bin_t) +
                        sizeof (rozofs_stor_bins_hdr_t)) *
                        nb_blocks_read_distant);

                memset(working_ctx.prj_ctx[proj_id_to_rebuild].bins, 0,
                        (rozofs_max_psize * sizeof (bin_t) +
                        sizeof (rozofs_stor_bins_hdr_t)) *
                        nb_blocks_read_distant);

                // Generate the nb_blocks_read projections
                ret = rbs_transform_forward_one_proj(
                        working_ctx.prj_ctx,
                        working_ctx.block_ctx_table,
                        layout,
                        i,
                        1,
                        proj_id_to_rebuild,
                        working_ctx.data_read_p);

                if (ret != 0) {
                    severe("rbs_transform_forward_one_proj failed: %s",
                            strerror(errno));
                    goto out;
                }

                // Check mtime of local file
                ret = ckeck_mtime(fd, loc_file_stat.st_mtim);
                if (ret != 0) {
                    severe("rbs_restore_one_rb_entry failed:"
                            " concurrent access detected");
                    goto out;
                }

                // warning("WRITE BLOCK: %lu", (first_block_idx + i));

                bin_t * bins_to_write =
                        working_ctx.prj_ctx[proj_id_to_rebuild].bins +
                        ((rozofs_max_psize + (sizeof (rozofs_stor_bins_hdr_t)
                        / sizeof (bin_t))) * i);

                // Store the projections on local bins file
                ret = storage_write(st, layout, re->dist_set_current,
                        spare, re->fid, first_block_idx + i, 1, version,
                        &file_size,
                        bins_to_write);

                if (ret <= 0) {
                    severe("storage_write failed: %s", strerror(errno));
                    goto out;
                }

                // Update local file stat
                if (fstat(fd, &loc_file_stat) != 0) {
                    goto out;
                }

                // Free projections generated
                free(working_ctx.prj_ctx[proj_id_to_rebuild].bins);
                working_ctx.prj_ctx[proj_id_to_rebuild].bins = NULL;
            }
            // Free local bins read
            free(loc_read_bins_p);
            loc_read_bins_p = NULL;
        }

        // Update the first block to read
        first_block_idx += nb_blocks_read_distant;

        free(working_ctx.data_read_p);
        working_ctx.data_read_p = NULL;
    }

    // OK, clear pointers
    loc_read_bins_p = NULL;
    working_ctx.data_read_p = NULL;
    memset(&working_ctx, 0, sizeof (rbs_storcli_ctx_t));

    // Check if the initial local bins file size is bigger
    // than others bins files
    if (loc_file_exist && loc_file_init_blocks_nb > first_block_idx) {

        off_t length = ROZOFS_ST_BINS_FILE_HDR_SIZE + first_block_idx
                * (rozofs_max_psize * sizeof (bin_t) +
                sizeof (rozofs_stor_bins_hdr_t));

        // Check mtime of local file
        ret = ckeck_mtime(fd, loc_file_stat.st_mtim);
        if (ret != 0) {
            severe("rbs_restore_one_rb_entry failed:"
                    " concurrent access detected");
            goto out;
        }
        ret = ftruncate(fd, length);
        if (ret != 0) {
            severe("ftruncate failed: %s", strerror(errno));
            goto out;
        }
    }

    status = 0;
out:
    if (fd != -1)
        close(fd);
    if (working_ctx.prj_ctx[proj_id_to_rebuild].bins != NULL)
        free(working_ctx.prj_ctx[proj_id_to_rebuild].bins);
    if (working_ctx.data_read_p != NULL)
        free(working_ctx.data_read_p);
    if (loc_read_bins_p != NULL)
        free(loc_read_bins_p);
    return status;
}

/** Initialize a storage to rebuild
 *
 * @param cid: unique id of cluster that owns this storage.
 * @param sid: the unique id for the storage to rebuild.
 * @param storage_root: the absolute path where rebuild bins file(s) 
 * will be store.
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int rbs_initialize(cid_t cid, sid_t sid, const char *storage_root) {
    int status = -1;
    DEBUG_FUNCTION;

    // Initialize the storage to rebuild
    if (storage_initialize(&storage_to_rebuild, cid, sid, storage_root) != 0)
        goto out;

    // Initialize the list of cluster(s)
    list_init(&cluster_entries);

    // Initialize list and htables for bins files to rebuild
    list_init(&rebuild_entries);
    htable_initialize(&htable_fid, REBUILD_HSIZE, fid_hash, fid_cmp);

    status = 0;
out:
    return status;
}

/** Initialize connections (via mproto and sproto) to one storage
 *
 * @param rb_stor: storage to connect.
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int rbs_stor_cnt_initialize(rb_stor_t * rb_stor) {
    int status = -1;
    int i = 0;
    mp_io_address_t io_address[STORAGE_NODE_PORTS_MAX];
    DEBUG_FUNCTION;

    // Copy hostname for this storage
    strncpy(rb_stor->mclient.host, rb_stor->host, ROZOFS_HOSTNAME_MAX);
    memset(io_address, 0, STORAGE_NODE_PORTS_MAX * sizeof (mp_io_address_t));
    rb_stor->sclients_nb = 0;

    struct timeval timeo;
    timeo.tv_sec = RBS_TIMEOUT_MPROTO_REQUESTS;
    timeo.tv_usec = 0;

    // Initialize connection with this storage (by mproto)
    if (mclient_initialize(&rb_stor->mclient, timeo) != 0) {
        severe("failed to join storage (host: %s), %s.",
                rb_stor->host, strerror(errno));
        goto out;
    } else {
        // Send request to get TCP ports for this storage
        if (mclient_ports(&rb_stor->mclient, io_address) != 0) {
            severe("Warning: failed to get ports for storage (host: %s)."
                    , rb_stor->host);
            goto out;
        }
    }

    // Initialize each TCP ports connection with this storage (by sproto)
    for (i = 0; i < STORAGE_NODE_PORTS_MAX; i++) {
        if (io_address[i].port != 0) {

            struct timeval timeo;
            timeo.tv_sec = RBS_TIMEOUT_SPROTO_REQUESTS;
            timeo.tv_usec = 0;

            uint32_t ip = io_address[i].ipv4;

            if (ip == INADDR_ANY) {
                // Copy storage hostname and IP
                strcpy(rb_stor->sclients[i].host, rb_stor->host);
                rozofs_host2ip(rb_stor->host, &ip);
            } else {
                sprintf(rb_stor->sclients[i].host, "%u.%u.%u.%u", ip >> 24,
                        (ip >> 16) & 0xFF, (ip >> 8) & 0xFF, ip & 0xFF);
            }

            rb_stor->sclients[i].ipv4 = ip;
            rb_stor->sclients[i].port = io_address[i].port;
            rb_stor->sclients[i].status = 0;
            rb_stor->sclients[i].rpcclt.sock = -1;

            if (sclient_initialize(&rb_stor->sclients[i], timeo) != 0) {
                severe("failed to join storage (host: %s, port: %u), %s.",
                        rb_stor->host, rb_stor->sclients[i].port,
                        strerror(errno));
                goto out;
            }
            rb_stor->sclients_nb++;
        }
    }

    status = 0;
out:
    return status;
}

/** Retrieves the list of bins files to rebuild from a storage
 *
 * @param rb_stor: storage contacted.
 * @param cid: unique id of cluster that owns this storage.
 * @param sid: the unique id for the storage to rebuild.
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int rbs_get_rb_entry_list_one_storage(rb_stor_t *rb_stor, cid_t cid,
        sid_t sid, int failed) {
    int status = -1;
    uint8_t layout = 0;
    uint8_t spare = 0;
    uint64_t cookie = 0;
    uint8_t eof = 0;
    sid_t dist_set[ROZOFS_SAFE_MAX];
    bins_file_rebuild_t * children = NULL;
    bins_file_rebuild_t * iterator = NULL;
    bins_file_rebuild_t * free_it = NULL;

    DEBUG_FUNCTION;

    memset(dist_set, 0, sizeof (sid_t) * ROZOFS_SAFE_MAX);

    // While the end of the list is not reached
    while (eof == 0) {

        // Send a request to storage to get the list of bins file(s)
        if (rbs_get_rb_entry_list(&rb_stor->mclient, cid, rb_stor->sid, sid,
                &spare, &layout, dist_set, &cookie, &children, &eof) != 0) {
            severe("rbs_get_rb_entry_list failed: %s\n", strerror(errno));
            goto out;
        }

        iterator = children;

        // For each entry 
        while (iterator != NULL) {

	    /*
	    ** Check that not too much storages are failed for this layout
	    */
	    if (failed) {
	      switch(iterator->layout) {

		case LAYOUT_2_3_4:
	          if (failed>1) {
		    severe("%d failed storages on LAYOUT_2_3_4",failed);
		    return -1;
		  }
		  break;
		case LAYOUT_4_6_8:
	          if (failed>2) {
		    severe("%d failed storages on LAYOUT_4_6_8",failed);
		    return -1;
		  }
		  break;	
		case LAYOUT_8_12_16:
	          if (failed>2) {
		    severe("%d failed storages on LAYOUT_8_12_16",failed);
		    return -1;
		  }
		  break;	
		default:	         	 		 	 
		  severe("Unexpected layout %d",iterator->layout);
		  return -1;
	      }
	      failed = 0;
	    }
	   
            rb_entry_t *nre = 0;

            // Verify if this entry is already present in list 
            if (!(nre = get_rb_entry_by_fid(iterator->fid))) {

                nre = xmalloc(sizeof (rb_entry_t));
                memcpy(nre->fid, iterator->fid, sizeof (fid_t));
                nre->layout = iterator->layout;
                memcpy(nre->dist_set_current, iterator->dist_set_current,
                        sizeof (sid_t) * ROZOFS_SAFE_MAX);
                list_init(&nre->list);
                put_rb_entry(nre);
            }

            free_it = iterator;
            iterator = iterator->next;
            free(free_it);
        }
    }

    status = 0;
out:

    return status;
}

/** Check if the storage is present on cluster list
 *
 * @param cluster_entries: list of cluster(s).
 * @param cid: unique id of cluster that owns this storage.
 * @param sid: the unique id for the storage to rebuild.
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int rbs_check_cluster_list(list_t * cluster_entries, cid_t cid, sid_t sid) {
    list_t *p, *q;

    list_for_each_forward(p, cluster_entries) {

        rb_cluster_t *clu = list_entry(p, rb_cluster_t, list);

        if (clu->cid == cid) {

            list_for_each_forward(q, &clu->storages) {

                rb_stor_t *stor = list_entry(q, rb_stor_t, list);

                // Check if the sid to rebuild exist in the list
                if (stor->sid == sid)
                    return 0;
            }
        }
    }
    errno = EINVAL;
    return -1;
}

/** Init connections for storage members of a given cluster but not for the 
 *  storage with sid=sid
 *
 * @param cluster_entries: list of cluster(s).
 * @param cid: unique id of cluster that owns this storage.
 * @param sid: the unique id for the storage to rebuild.
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
static int rbs_init_cluster_cnts(list_t * cluster_entries, cid_t cid,
        sid_t sid, int * failed, int * available) {
    list_t *p, *q;
    
    *failed = 0;
    *available = 0;

    list_for_each_forward(p, cluster_entries) {

        rb_cluster_t *clu = list_entry(p, rb_cluster_t, list);

        if (clu->cid == cid) {

            list_for_each_forward(q, &clu->storages) {

                rb_stor_t *rb_stor = list_entry(q, rb_stor_t, list);

                if (rb_stor->sid == sid) {
		    (*failed)++;
                    continue;
                }
		
                // Get connections for this storage
                if (rbs_stor_cnt_initialize(rb_stor) != 0) {
		    (*failed)++;
                    continue;
                }
		(*available)++;
            }
        }
    }
    return 0;
}

/** Release storages connections of cluster(s)
 *
 * @param cluster_entries: list of cluster(s).
 */
static void rbs_release_cluster_cnts(list_t * cluster_entries) {
    list_t *p, *q, *r, *s;
    int i = 0;

    list_for_each_forward_safe(p, q, cluster_entries) {

        rb_cluster_t *clu = list_entry(p, rb_cluster_t, list);

        list_for_each_forward_safe(r, s, &clu->storages) {

            rb_stor_t *rb_stor = list_entry(r, rb_stor_t, list);

            // Remove cnts fot this storage
            mclient_release(&rb_stor->mclient);

            for (i = 0; i < rb_stor->sclients_nb; i++)
                sclient_release(&rb_stor->sclients[i]);
        }
    }
}

/** Release the list of cluster(s)
 *
 * @param cluster_entries: list of cluster(s).
 */
static void rbs_release_cluster_list(list_t * cluster_entries) {
    list_t *p, *q, *r, *s;
    int i = 0;

    list_for_each_forward_safe(p, q, cluster_entries) {

        rb_cluster_t *clu = list_entry(p, rb_cluster_t, list);

        list_for_each_forward_safe(r, s, &clu->storages) {

            rb_stor_t *rb_stor = list_entry(r, rb_stor_t, list);

            // Remove and free storage
            mclient_release(&rb_stor->mclient);

            for (i = 0; i < rb_stor->sclients_nb; i++)
                sclient_release(&rb_stor->sclients[i]);

            list_remove(&rb_stor->list);
            free(rb_stor);

        }

        // Remove and free cluster
        list_remove(&clu->list);
        free(clu);
    }
}

/** Retrieves the list of bins files to rebuild for a given storage
 *
 * @param cluster_entries: list of cluster(s).
 * @param cid: unique id of cluster that owns this storage.
 * @param sid: the unique id for the storage to rebuild.
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
static int rbs_get_rb_entry_list_one_cluster(list_t * cluster_entries,
        cid_t cid, sid_t sid, int failed) {
    list_t *p, *q;
    int status = -1;

    list_for_each_forward(p, cluster_entries) {

        rb_cluster_t *clu = list_entry(p, rb_cluster_t, list);

        if (clu->cid == cid) {

            list_for_each_forward(q, &clu->storages) {

                rb_stor_t *rb_stor = list_entry(q, rb_stor_t, list);

                if (rb_stor->sid == sid)
                    continue;

		if (rb_stor->mclient.rpcclt.client == NULL)
		    continue;  
		    
                // Get the list of bins files to rebuild for this storage
                if (rbs_get_rb_entry_list_one_storage(rb_stor, cid, sid, failed) != 0) {
                    goto out;
                }
            }
        }
    }

    status = 0;
out:
    return status;
}

/** Release the list of entries to rebuild
 *
 * @param rebuild_entries: list of entries to rebuild.
 */
static void rbs_release_rb_entry_list(list_t * rebuild_entries) {
    list_t *p, *q;

    list_for_each_forward_safe(p, q, rebuild_entries) {
        rb_entry_t *re = list_entry(p, rb_entry_t, list);
        // Free this entry
        del_rb_entry(re);
        free(re->storages);
        free(re);
    }
}

int rbs_sanity_check(const char *export_host_list, cid_t cid, sid_t sid,
        const char *root) {

    int status = -1;
    char * pExport_host = 0;

    DEBUG_FUNCTION;

    // Try to initialize the storage to rebuild
    if (rbs_initialize(cid, sid, root) != 0) {
        // Probably a path problem
        fprintf(stderr, "Can't initialize rebuild storage (cid:%u; sid:%u;"
                " path:%s): %s\n", cid, sid, root, strerror(errno));
        goto out;
    }

    // Try to get the list of storages for this cluster ID
    pExport_host = rbs_get_cluster_list(&rpcclt_export, export_host_list, cid,
            &cluster_entries);
    if (pExport_host == NULL) {
        fprintf(stderr, "Can't get list of others cluster members from export"
                " server (%s) for storage to rebuild (cid:%u; sid:%u): %s\n",
                export_host_list, cid, sid, strerror(errno));
        goto out;
    }

    // Check the list of cluster
    if (rbs_check_cluster_list(&cluster_entries, cid, sid) != 0) {
        fprintf(stderr, "The storage to rebuild with sid=%u is not present in"
                " cluster with cid=%u\n", sid, cid);
        goto out;
    }

    status = 0;

out:
    // Free cluster(s) list
    rbs_release_cluster_list(&cluster_entries);

    // Free the htable
    htable_release(&htable_fid);

    return status;
}

int rbs_rebuild_storage(const char *export_host_list, cid_t cid, sid_t sid,
        const char *root, uint8_t stor_idx) {
    list_t *p, *q;
    uint64_t current_nb_rb_files = 0;
    int status = -1;
    char * pExport_host = 0;
    int failed,available;

    DEBUG_FUNCTION;

    // Indicate rebuild status
    SET_PROBE_VALUE(rb_status[stor_idx], 1);

    // Initialize the storage to rebuild
    if (rbs_initialize(cid, sid, root) != 0) {
        severe("can't init. storage to rebuild (cid:%u;sid:%u;path:%s)",
                cid, sid, root);
        goto out;
    }

    // Get the list of storages for this cluster ID
    pExport_host = rbs_get_cluster_list(&rpcclt_export, export_host_list, cid,
            &cluster_entries);
    if (pExport_host == NULL) {	
        severe("rbs_get_cluster_list failed (cid: %u) : %s", cid, strerror(errno));
        goto out;
    }

    // Check the list of cluster
    if (rbs_check_cluster_list(&cluster_entries, cid, sid) != 0)
        goto out;

    // Indicate rebuild status
    SET_PROBE_VALUE(rb_status[stor_idx], 2);

    // Get connections for this given cluster
    rbs_init_cluster_cnts(&cluster_entries, cid, sid, &failed, &available);

    // Indicate rebuild status
    SET_PROBE_VALUE(rb_status[stor_idx], 3);

    // Get the list of bins files to rebuild for this storage
    if (rbs_get_rb_entry_list_one_cluster(&cluster_entries, cid, sid, failed) != 0)
        goto out;

    // Set for monitoring, the nb. of files to rebuild
    SET_PROBE_VALUE(rb_files_total[stor_idx], list_size(&rebuild_entries));

    // Indicate rebuild status
    SET_PROBE_VALUE(rb_status[stor_idx], 4);

    // Rebuild each bins file

    while (list_size(&rebuild_entries) != 0) {

        list_for_each_forward_safe(p, q, &rebuild_entries) {

            rb_entry_t *re = list_entry(p, rb_entry_t, list);

            if (DEBUG_RBS == 1) {
                // Just for test: print UUID for this bins file
                char fid_str[37];
                uuid_unparse(re->fid, fid_str);
                severe("rb. entry: (FID: %s; layout: %u)", fid_str, re->layout);
            }

            // Compute the rozofs constants for this layout
            uint8_t rozofs_inverse = rozofs_get_rozofs_inverse(re->layout);

            // Get storage connections for this entry
            if (rbs_get_rb_entry_cnts(re, &cluster_entries, cid, sid,
                    rozofs_inverse) != 0) {
                severe("rbs_get_rb_entry_cnts failed: %s", strerror(errno));
                continue; // Try with the next
            }

            // Restore this entry
            if (rbs_restore_one_rb_entry(&storage_to_rebuild, re) != 0) {
                severe("rbs_restore_one_rb_entry failed: %s", strerror(errno));
                continue; // Try with the next
            }

            // Free this entry
            del_rb_entry(re);
            free(re->storages);
            free(re);

            // Update for monitoring, the current nb. of files rebuild
            current_nb_rb_files++;
            SET_PROBE_VALUE(rb_files_current[stor_idx], current_nb_rb_files);
        }

        // End of list

        // The list is not empty
        if (list_size(&rebuild_entries) != 0) {

            severe("All bins files are not rebuilt for storage (cid:%u;sid:%u)",
                    cid, sid);

            // Release connections to storages
            rbs_release_cluster_cnts(&cluster_entries);

            // Sleep between 2 passes
            sleep(RBS_TIME_BETWEEN_2_PASSES);

            // Try to re-init connections to storages
            rbs_init_cluster_cnts(&cluster_entries, cid, sid, &failed, &available);
        }
    }

    // Indicate rebuild status
    SET_PROBE_VALUE(rb_status[stor_idx], 5);

    status = 0;

out:
    // Free list of rebuild entries
    rbs_release_rb_entry_list(&rebuild_entries);

    // Free cluster(s) list
    rbs_release_cluster_list(&cluster_entries);

    // Free the htable
    htable_release(&htable_fid);

    return status;
}
