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

#include "config.h"
#include "common_config.h"
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>


static char config_file_name[256] = {0};
static int  config_file_is_read=0;
common_config_t common_config;

#define COMMON_CONFIG_SHOW_BOOL(val)  {\
  pChar += rozofs_string_padded_append(pChar, 24, rozofs_left_alignment, #val);\
  if (common_config.val) pChar += rozofs_string_append(pChar, ": True  ");\
  else                   pChar += rozofs_string_append(pChar, ": False ");\
  if (common_config.val == rozofs_cc_def_##val) {\
    pChar += rozofs_string_append(pChar, " \t(default)");\
  }\
  pChar += rozofs_eol(pChar);\
}

#define COMMON_CONFIG_SHOW_STRING(val)  {\
  pChar += rozofs_string_padded_append(pChar, 24, rozofs_left_alignment, #val);\
  *pChar++ = ':';\
  *pChar++ = ' ';\
  *pChar++ = '\"';\
  if (common_config.val!=NULL) pChar += rozofs_string_append(pChar, common_config.val);\
  *pChar++ = '\"';\
  if ((char*)common_config.val == (char*)rozofs_cc_def_##val) {\
    pChar += rozofs_string_append(pChar, " \t(default)");\
  }\
  pChar += rozofs_eol(pChar);\
}
    
#define COMMON_CONFIG_SHOW_INT(val)  {\
  pChar += rozofs_string_padded_append(pChar, 24, rozofs_left_alignment, #val);\
  pChar += rozofs_string_append(pChar, ": ");\
  pChar += rozofs_i32_append(pChar, common_config.val);\
  if (common_config.val == rozofs_cc_def_##val) {\
    pChar += rozofs_string_append(pChar, " \t(default)");\
  }\
  pChar += rozofs_eol(pChar);\
}  
void show_common_config(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();
  
  if (argv[1] != NULL) {
    if (strcmp(argv[1],"reload")==0) {  
      common_config_read(NULL);
    }
  }
      
  if (config_file_is_read==0) {
    pChar += rozofs_string_append(pChar,"Can not read configuration file ");    
  }
  pChar += rozofs_string_append(pChar,config_file_name);
  pChar += rozofs_eol(pChar);

  COMMON_CONFIG_SHOW_INT(nb_core_file);
  COMMON_CONFIG_SHOW_INT(nb_disk_thread); 
  COMMON_CONFIG_SHOW_BOOL(storio_multiple_mode); 
  COMMON_CONFIG_SHOW_BOOL(crc32c_check);
  COMMON_CONFIG_SHOW_BOOL(crc32c_generate);
  COMMON_CONFIG_SHOW_BOOL(crc32c_hw_forced);
  COMMON_CONFIG_SHOW_INT(trashed_file_per_run); 
  COMMON_CONFIG_SHOW_INT(storio_slice_number);    
  COMMON_CONFIG_SHOW_BOOL(allow_disk_spin_down);
  COMMON_CONFIG_SHOW_STRING(core_file_directory);
  COMMON_CONFIG_SHOW_BOOL(numa_aware);
  COMMON_CONFIG_SHOW_INT(file_distribution_rule);
  COMMON_CONFIG_SHOW_INT(disk_usage_threshold);
  COMMON_CONFIG_SHOW_INT(disk_read_threshold);  
  COMMON_CONFIG_SHOW_INT(disk_write_threshold);   
  COMMON_CONFIG_SHOW_INT(export_dscp);   
  COMMON_CONFIG_SHOW_INT(storio_dscp); 
  COMMON_CONFIG_SHOW_INT(trash_high_threshold); 
    
  COMMON_CONFIG_SHOW_BOOL(fid_recycle);
  COMMON_CONFIG_SHOW_BOOL(wr_ack_on_inverse);
  COMMON_CONFIG_SHOW_INT(recycle_truncate_blocks);
  COMMON_CONFIG_SHOW_INT(storio_buf_cnt);
  COMMON_CONFIG_SHOW_INT(export_buf_cnt);
  COMMON_CONFIG_SHOW_BOOL(export_attr_thread);
  COMMON_CONFIG_SHOW_BOOL(rozofsmount_fuse_reply_thread);
  COMMON_CONFIG_SHOW_BOOL(export_versioning);
  COMMON_CONFIG_SHOW_INT(alloc_estimated_mb);
  
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
  return;          
}









int  boolval;  
#define COMMON_CONFIG_READ_BOOL(val)  {\
  common_config.val = rozofs_cc_def_##val;\
  if (config_lookup_bool(&cfg, #val, &boolval)) { \
    common_config.val = boolval;\
  }\
}  


#if (((LIBCONFIG_VER_MAJOR == 1) && (LIBCONFIG_VER_MINOR >= 4)) \
             || (LIBCONFIG_VER_MAJOR > 1))
int               intval;
#else
long int          intval;
#endif

#define COMMON_CONFIG_READ_INT(val)  {\
  common_config.val = rozofs_cc_def_##val;\
  if (config_lookup_int(&cfg, #val, &intval)) { \
    if (intval<rozofs_cc_min_##val) {\
      common_config.val = rozofs_cc_min_##val;\
    }\
    else if (intval>rozofs_cc_max_##val) { \
      common_config.val = rozofs_cc_max_##val;\
    }\
    else {\
      common_config.val = intval;\
    }\
  }\
} 

const char * charval;
#define COMMON_CONFIG_READ_STRING(val)  {\
  common_config.val = rozofs_cc_def_##val;\
  if (config_lookup_string(&cfg, #val, &charval)) {\
    common_config.val = strdup(charval);\
  }\
} 

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
void common_config_read(char * fname) {
  config_t          cfg; 
#if (((LIBCONFIG_VER_MAJOR == 1) && (LIBCONFIG_VER_MINOR >= 4)) \
             || (LIBCONFIG_VER_MAJOR > 1))
  int               intval;
#else
  long int          intval;
#endif      

  if (config_file_is_read == 0) {
    uma_dbg_addTopic("cconf",show_common_config);

    /*
    ** Build the file name
    */
    if (fname == NULL) {
      strcpy(config_file_name,ROZOFS_DEFAULT_CONFIG);
    }
    else {
      strcpy(config_file_name,fname);    
    } 
  }
  
  
  /*
  ** Initialize lib config working structure
  */
  config_init(&cfg);

  config_file_is_read = 1;
  if (config_read_file(&cfg, config_file_name) == CONFIG_FALSE) {
    if (errno == ENOENT) {
      info("can't read %s: %s (line %d).", config_file_name, config_error_text(&cfg),
            config_error_line(&cfg));
    }
    else {
      severe("can't read %s: %s (line %d).", config_file_name, config_error_text(&cfg),
            config_error_line(&cfg));
    }	    
    config_file_is_read = 0;	    
  }
  
  /*
  ** Number of slices for the storio
  */  
  COMMON_CONFIG_READ_INT(storio_slice_number);

  /*
  ** Look up for number of core files
  */  
  COMMON_CONFIG_READ_INT(nb_core_file);

  /*
  ** Look up for number of disk threads
  */  
  COMMON_CONFIG_READ_INT(nb_disk_thread);
  
  /*
  ** Is storio in multiple or single mode
  */   
  COMMON_CONFIG_READ_BOOL(storio_multiple_mode);


  /*
  ** CRC32
  */
  COMMON_CONFIG_READ_BOOL(crc32c_check);
  COMMON_CONFIG_READ_BOOL(crc32c_generate);
  COMMON_CONFIG_READ_BOOL(crc32c_hw_forced);


  /* Number of file deleted in a run of rm bins thread */
  COMMON_CONFIG_READ_INT(trashed_file_per_run);    

  /*
  ** Whether we must let the disk spin down and so stop
  ** to access them when no modification has been requested
  */   
  COMMON_CONFIG_READ_BOOL(allow_disk_spin_down);

  /*
  ** What directory to use for core files
  */   
  COMMON_CONFIG_READ_STRING(core_file_directory);

  /*
  ** Shall we take into account the NUMA architecture
  */
  COMMON_CONFIG_READ_BOOL(numa_aware);

  /*
  ** Read allocation rule 
  */
  COMMON_CONFIG_READ_INT(file_distribution_rule);  

  /*
  ** Read warning thresholds for disk usage
  */
  COMMON_CONFIG_READ_INT(disk_usage_threshold);
  COMMON_CONFIG_READ_INT(disk_read_threshold);  
  COMMON_CONFIG_READ_INT(disk_write_threshold);  
  /*
  ** read the dscp of the export and storio
  */
  COMMON_CONFIG_READ_INT(export_dscp);  
  COMMON_CONFIG_READ_INT(storio_dscp);      

  /*
  ** Shall we take into account the fid recycle
  */
  COMMON_CONFIG_READ_BOOL(fid_recycle);
  /*
  ** read the trash high threshold when fid recycling starts
  */
  COMMON_CONFIG_READ_INT(trash_high_threshold); 
  /*
  ** get the boolean that indicates if storcli must respond upon write inverse versus forward projections
  */
  COMMON_CONFIG_READ_BOOL(wr_ack_on_inverse);
  /*
  ** Size to which the file should be truncated on storage
  ** at the time it is recycled
  */
  COMMON_CONFIG_READ_INT(recycle_truncate_blocks);  
  COMMON_CONFIG_READ_INT(storio_buf_cnt);  
  COMMON_CONFIG_READ_INT(export_buf_cnt);  
  /*
  ** activation of export writebehind attributes thread
  */
  COMMON_CONFIG_READ_BOOL(export_attr_thread);
  /*
  ** activation of rozofsmount reply fuse threads 
  */
  COMMON_CONFIG_READ_BOOL(rozofsmount_fuse_reply_thread);
  /*
  ** support of directory/file versioning for export  
  */
  COMMON_CONFIG_READ_BOOL(export_versioning);
  
  COMMON_CONFIG_READ_INT(alloc_estimated_mb);
  /*
  ** Free lib config working structure
  */
  config_destroy(&cfg);
}
