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

#ifndef _COMMON_CONFIG_H
#define _COMMON_CONFIG_H

#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <libconfig.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/types.h>

/*
** Default parameter values
*/
#define rozofs_cc_def_trashed_file_per_run     7
#define rozofs_cc_min_trashed_file_per_run     1
#define rozofs_cc_max_trashed_file_per_run     50000

#define rozofs_cc_def_nb_core_file             1
#define rozofs_cc_min_nb_core_file             0
#define rozofs_cc_max_nb_core_file             8

#define rozofs_cc_def_nb_disk_thread           4
#define rozofs_cc_min_nb_disk_thread           2
#define rozofs_cc_max_nb_disk_thread           16

#define rozofs_cc_def_storio_multiple_mode     TRUE

#define rozofs_cc_def_crc32c_check             TRUE

#define rozofs_cc_def_crc32c_generate          TRUE

#define rozofs_cc_def_crc32c_hw_forced         FALSE

#define rozofs_cc_def_storio_slice_number      1024
#define rozofs_cc_min_storio_slice_number      8
#define rozofs_cc_max_storio_slice_number      (32*1024)

#define rozofs_cc_def_allow_disk_spin_down     FALSE

#define rozofs_cc_def_core_file_directory      "/var/run/rozofs_core"

#define rozofs_cc_def_export_dscp      46  /* EF   */
#define rozofs_cc_min_export_dscp      0  /* EF   */
#define rozofs_cc_max_export_dscp      46  /* EF   */
#define rozofs_cc_def_storio_dscp      34  /* AF41 */
#define rozofs_cc_min_storio_dscp      0  /* AF41 */
#define rozofs_cc_max_storio_dscp      34  /* AF41 */


#define rozofs_cc_def_numa_aware               FALSE

#define rozofs_cc_def_fid_recycle             FALSE
#define rozofs_cc_def_wr_ack_on_inverse       FALSE
#define rozofs_cc_def_export_attr_thread      TRUE
#define rozofs_cc_def_rozofsmount_fuse_reply_thread      TRUE
#define rozofs_cc_def_export_versioning      FALSE

typedef enum _rozofs_file_distribution_rule_e {
  rozofs_file_distribution_size_balancing,
  rozofs_file_distribution_round_robin,
  
  rozofs_file_distribution_max
} rozofs_file_distribution_rule_e;
#define rozofs_cc_def_file_distribution_rule    rozofs_file_distribution_size_balancing
#define rozofs_cc_min_file_distribution_rule    0
#define rozofs_cc_max_file_distribution_rule    (rozofs_file_distribution_max-1)

#define rozofs_cc_def_disk_usage_threshold      0
#define rozofs_cc_min_disk_usage_threshold      0   // No threashold
#define rozofs_cc_max_disk_usage_threshold      100

#define rozofs_cc_def_disk_read_threshold       0
#define rozofs_cc_min_disk_read_threshold       0  // No threashold
#define rozofs_cc_max_disk_read_threshold       1000000

#define rozofs_cc_def_disk_write_threshold      0
#define rozofs_cc_min_disk_write_threshold      0  // No threashold
#define rozofs_cc_max_disk_write_threshold      1000000


#define rozofs_cc_def_trash_high_threshold      1000
#define rozofs_cc_min_trash_high_threshold      0  // No threashold
#define rozofs_cc_max_trash_high_threshold      1000000

#define rozofs_cc_def_recycle_truncate_blocks     0 // by default truncate file to 0 when recycling it
#define rozofs_cc_min_recycle_truncate_blocks     0  
#define rozofs_cc_max_recycle_truncate_blocks     0x7FFFFFFF


#define rozofs_cc_def_storio_buf_cnt      128
#define rozofs_cc_min_storio_buf_cnt      64  // No threashold
#define rozofs_cc_max_storio_buf_cnt      1024


#define rozofs_cc_def_export_buf_cnt      128
#define rozofs_cc_min_export_buf_cnt      32  // No threashold
#define rozofs_cc_max_export_buf_cnt      1024

/*
** Common configuration parameters
*/
typedef struct _common_config_t { 
  uint32_t    nb_core_file;
  uint32_t    nb_disk_thread; 
  uint32_t    storio_multiple_mode; 
  uint32_t    crc32c_check;
  uint32_t    crc32c_generate;
  uint32_t    crc32c_hw_forced;  
  uint32_t    trashed_file_per_run;
  uint32_t    storio_slice_number;
  uint32_t    allow_disk_spin_down;
  char      * core_file_directory;
  uint32_t    numa_aware;     
  uint32_t    file_distribution_rule;
  uint32_t    disk_usage_threshold;
  uint32_t    disk_read_threshold;  
  uint32_t    disk_write_threshold;  
  uint32_t    export_dscp;  
  uint32_t    storio_dscp;  
  uint32_t    fid_recycle;  
  uint32_t    trash_high_threshold;
  uint32_t    wr_ack_on_inverse;
  /* 
  ** Number of block to which the file should be truncated 
  ** on storage at the time it is recycled
  */ 
  uint32_t    recycle_truncate_blocks;
  uint32_t    storio_buf_cnt;
  uint32_t    export_buf_cnt;
  uint32_t    export_attr_thread;
  uint32_t    rozofsmount_fuse_reply_thread;
  uint32_t    export_versioning;

} common_config_t;
  
extern common_config_t common_config;
  
/*
** Read the configuration file and initialize the configuration 
** global variable.
**
** @param file : the name of the configuration file
**               when NULL the default configuration is read
**
** retval The address of the glocal vriable containing the 
** common configuration
*/
void common_config_read(char * file);

#endif
