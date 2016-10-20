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
#include <dirent.h> 
#include <sys/wait.h>
#include <signal.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/list.h>
#include <rozofs/common/rozofs_site.h>
#include <rozofs/common/common_config.h>
#include <rozofs/rpc/mproto.h>
#include <rozofs/rpc/sproto.h>
#include <rozofs/rpc/spproto.h>
#include <rozofs/rpc/eproto.h>
#include <rozofs/core/rozofs_core_files.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rozofs_timer_conf.h>
#include <rozofs/core/rozofs_string.h>
#include <stdarg.h>
 
#include "config.h"
#include "sconfig.h"
#include "storage.h"
#include "storaged.h"
#include "sconfig.h"
#include "rbs.h"
#include "storaged_nblock_init.h"
#include "rbs_sclient.h"
#include "rbs_eclient.h"

#define STORAGE_REBUILD_PID_FILE "storage_rebuild"

sconfig_t   storaged_config;
char      * pExport_host = NULL;


storage_t storaged_storages[STORAGES_MAX_BY_STORAGE_NODE] = { { 0 } };
uint16_t  storaged_nrstorages = 0;

uint64_t        rb_fid_table_count=0;
uint32_t        previous_delay=0;

char            command[1024];


static rbs_storage_config_t storage_config;
static storage_t * storage_to_rebuild = &storage_config.storage;

/* RPC client for exports server */
static rpcclt_t rpcclt_export;

/* List of cluster(s) */
static list_t cluster_entries;
uint32_t    run_loop=0;
uint8_t rbs_start_process = 0;
  
int     nb_rbs_entry=0;
typedef struct rbs_monitor_s {
  uint8_t  cid;
  uint8_t  sid;
  uint8_t  layout;
  int      list_building_sec;
  uint64_t nb_files;
  uint64_t done_files;
  uint64_t deleted;  
  uint64_t written_spare;
  uint64_t written;
  uint64_t read_spare;
  uint64_t read;  
  char     status[64];
} RBS_MONITOR_S;
RBS_MONITOR_S rbs_monitor[128];



typedef struct _rbs_devices_t {
    uint32_t                     total; 
    uint32_t                     mapper;
    uint32_t                     redundancy;
} rbs_devices_t;

typedef enum RBS_STATUS_e {
  RBS_STATUS_BUILD_JOB_LIST,
  RBS_STATUS_PROCESSING_LIST,
  RBS_STATUS_FAILED,
  RBS_STATUS_ERROR,
  RBS_STATUS_SUCCESS 
} RBS_STATUS_E;

/** Structure used to store configuration for each storage to rebuild */
typedef struct rbs_stor_config {
    char export_hostname[ROZOFS_HOSTNAME_MAX]; ///< export hostname or IP.
    cid_t cid; //< unique id of cluster that owns this storage.
    RBS_STATUS_E status;
    sid_t sid; ///< unique id of this storage for one cluster.
    rbs_devices_t  device;    
    uint8_t stor_idx; ///< storage index used for display statistics.
    char root[PATH_MAX]; ///< absolute path.
} rbs_stor_config_t;

rbs_stor_config_t rbs_stor_configs[STORAGES_MAX_BY_STORAGE_NODE] ;

char header_msg[1024*8]={0};
int  header_msg_size=0;

int rbs_index=0;
  
uint8_t storio_nb_threads = 0;
uint8_t storaged_nb_ports = 0;
uint8_t storaged_nb_io_processes = 0;

int current_file_index = 0;
char                rbs_monitor_file_path[ROZOFS_PATH_MAX]={0};
int quiet=0;
int sigusr_received=0;

#define usage(fmt, ...) {printf(fmt, ##__VA_ARGS__);usage_display();}

/*________________________________________________
*
* Parameter structure
*/
typedef enum rbs_rebuild_type_e  {
  rbs_rebuild_type_fid,
  rbs_rebuild_type_device,
  rbs_rebuild_type_storage
} RBS_REBUILD_TYPE_E;

typedef struct _rbs_parameter_t {
  char     storaged_config_file[PATH_MAX];
  char     rbs_export_hostname[ROZOFS_HOSTNAME_MAX];
  int      rbs_device_number;
  RBS_REBUILD_TYPE_E type;
  char   * storaged_hostname;
  int      cid;
  int      sid;
  fid_t    fid2rebuild;
  int      parallel;
  int      storaged_geosite;
  int      relocate;
  int      max_reloop;
  char   * output;
  int      clear;
  int      resume;
  int      pause;
  int      abort;
  int      list;
  int      rebuildRef;
  int      background;
  char   * simu;
} rbs_parameter_t;

rbs_parameter_t parameter;
/*
**____________________________________________________
** Create or re-create the monitoring file for this rebuild process
*/
#define HEADER "# This file was generated by storage_rebuild(8).\n# All changes to this file will be lost.\n# "

time_t loc_time;
char   initial_date[80];

void static inline rbs_status_file_name(char * given_name) {
  struct tm date;

  char * pChar = rbs_monitor_file_path;
  if (*pChar != 0) return;
    
  pChar += rozofs_string_append(pChar,DAEMON_PID_DIRECTORY);
  pChar += rozofs_string_append(pChar,"storage_rebuild/");
  if (access(rbs_monitor_file_path,W_OK) == -1) {
    mkdir(rbs_monitor_file_path,S_IRWXU | S_IROTH);
  }	

  loc_time=time(NULL);
  localtime_r(&loc_time,&date); 

  if (given_name != NULL) {
    pChar += rozofs_string_append(pChar, given_name);  
  }
  else {
    pChar += rozofs_u32_padded_append(pChar, 4, rozofs_right_alignment, date.tm_year+1900); 
    *pChar++ =':'; 
    pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, date.tm_mon+1);  
    *pChar++ =':';     
    pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, date.tm_mday);  
    *pChar++ ='_';     
    pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, date.tm_hour);  
    *pChar++ =':';     
    pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, date.tm_min);  
    *pChar++ =':';     
    pChar += rozofs_u32_padded_append(pChar, 2, rozofs_zero, date.tm_sec);  
    *pChar++ ='_';     
    pChar += rozofs_u32_append(pChar, parameter.rebuildRef);  

  }		   
  ctime_r(&loc_time,initial_date);  
}
/*________________________________________________
*
* Initialize parameter structure with default values
* @param   cnf Structure to initialize
*/
void rbs_conf_init(rbs_parameter_t * par) {
  fid_t fid_null={0};

  strcpy(par->storaged_config_file,STORAGED_DEFAULT_CONFIG);
  par->rbs_export_hostname[0] = 0;
  par->rbs_device_number    = -1;
  par->type                 = rbs_rebuild_type_storage;
  par->storaged_hostname    = NULL;
  par->cid                  = -1;
  par->sid                  = -1;
  memset(par->fid2rebuild,0,sizeof(fid_t));
  par->parallel             = common_config.device_self_healing_process;
  par->relocate             = 0;
  par->max_reloop           = DEFAULT_REBUILD_RELOOP;
  par->output               = NULL;
  par->clear                = 0;
  par->rebuildRef           = 0;
  par->resume               = 0;
  par->pause                = 0;  
  par->list                 = 0;
  par->background           = 1;
  par->simu                 = NULL;
  memcpy(par->fid2rebuild,fid_null,sizeof(fid_t));
  
  par->storaged_geosite = rozofs_get_local_site();
  if (par->storaged_geosite == -1) {
    par->storaged_geosite = 0;
  }
}
/*________________________________________________
*
* Display this utility usage
*/
void usage_display() {


    printf("\nStorage node rebuild - RozoFS %s\n", VERSION);
    printf("Usage: storage_rebuild [OPTIONS]\n\n");
    printf("   -h, --help                \tPrint this message.\n");
    printf("   -r, --rebuild <names>     \tlist of \'/\' separated host where exportd is running (mandatory)\n");
    printf("   -d, --device <device>     \tDevice number to rebuild.\n");
    printf("                             \tAll devices are rebuilt when omitted.\n");
    printf("   -s, --sid <cid/sid>       \tCluster and storage identifier to rebuild.\n");
    printf("                             \tAll <cid/sid> are rebuilt when omitted.\n");
    printf("   -f, --fid <FID>           \tSpecify one FID to rebuild. -s must also be set.\n");
    printf("   -p, --parallel=<val>      \tNumber of rebuild processes in parallel per cid/sid\n");
    printf("                             \t(default is %d, maximum is %d)\n",
           common_config.device_self_healing_process,MAXIMUM_PARALLEL_REBUILD_PER_SID);   
    printf("   -g, --geosite             \tTo force site number in case of geo-replication\n");
    printf("   -R, --relocate            \tTo rebuild a device by relocating files\n");
    printf("   -l, --loop                \tNumber of reloop in case of error (default %d)\n",DEFAULT_REBUILD_RELOOP);
    printf("   -q, --quiet               \tDo not display messages\n");
    printf("   -C, --clear               \tClear the status of the device after it has been set OOS\n");
    printf("   -o, --output=<file>       \tTo give the name of the rebuild status file (under %s/storage_rebuild)\n",DAEMON_PID_DIRECTORY);
    printf("   -id <id>                  \tIdentifier of a non completed rebuild.\n");
    printf("   -abort                    \tAbort a rebuild\n");
    printf("   -pause                    \tPause a rebuild\n");
    printf("   -resume                   \tResume a rebuild\n");
    printf("   -list                     \tDisplay the list of FID to rebuild\n");
    printf("   -fg                       \tTo force foreground execution\n");
    printf("   -bg                       \tTo force background execution\n");
    printf(" mainly for tests:\n");
    printf("   -H, --host=storaged-host  \tSpecify the hostname to rebuild\n");
    printf("   -c, --config=config-file  \tSpecify config file to use\n");
    printf("                             \t(default: %s).\n",STORAGED_DEFAULT_CONFIG);

    printf("Rebuilding a whole storage node as fast as possible:\n");
    printf("storage_rebuild -r 192.168.0.201/192.168.0.202 -p %d\n\n",MAXIMUM_PARALLEL_REBUILD_PER_SID);
    printf("Rebuilding every devices of sid 2 of cluster 1:\n");
    printf("storage_rebuild -r 192.168.0.201/192.168.0.202 -s 1/2\n\n");
    printf("Rebuilding only device 3 of sid 2 of cluster 1:\n");
    printf("storage_rebuild -r 192.168.0.201/192.168.0.202 -s 1/2 -d 3\n\n");
    printf("Rebuilding by relocating device 3 of sid 2 of cluster 1 on other devices:\n");
    printf("storage_rebuild -r 192.168.0.201/192.168.0.202 -s 1/2 -d 3 --relocate\n\n");
    printf("Puting a device back in service when it is replaced after\n");
    printf("an automatic relocation (self healing)\n");
    printf("storage_rebuild -r 192.168.0.201/192.168.0.202 -s 1/2 -d 3 --clear\n\n");    
    printf("Pause a running rebuild in order to resume it later\n");
    printf("storage_rebuild -id <id> -pause\n\n"); 
    printf("Resume a paused or failed rebuild\n");
    printf("storage_rebuild -id <id> -resume\n\n"); 
    printf("Check the list of remaining FID to rebuild\n");
    printf("storage_rebuild -id <id> -list\n\n");     
    printf("Abort definitively a running rebuild\n");
    printf("storage_rebuild -id <id> -abort\n\n");                   
    exit(EXIT_SUCCESS);
}

/*________________________________________________
*
* Command parsing
*/
#define GET_PARAM(opt) {\
  idx++;\
  if (idx >= argc) usage("argument without value \"%s\".\n",#opt);\
  optarg = argv[idx];\
}
#define NEXT_ARG {\
  idx++;\
  if (idx >= argc) break;\
  optarg = argv[idx];\
}  
#define IS_ARG(x) (strcmp(optarg, #x)==0)  

#define GET_INT_PARAM(opt,val) {\
  GET_PARAM(opt)\
  ret = sscanf(optarg,"%u",&val);\
  if (ret != 1) {\
   REBUILD_FAILED("\"%s\" option has not int value \"%s\"\n",#opt,optarg);\
   exit(EXIT_FAILURE);\
  }\
}      
 
void parse_command(int argc, char *argv[], rbs_parameter_t * par) {
  int    ret;
  int    idx;
  char * optarg;

  if (argc < 2) usage("Only %d arguments.\n",argc); 
  
  idx = 0;
  while (idx < argc) {
  
    NEXT_ARG;
    //printf("optarg = %s\n",optarg);

    if (IS_ARG(-h) || IS_ARG(--help) || IS_ARG(?)) {
      usage("%s\n"," ");
      exit(EXIT_SUCCESS);
    }
    
    if (IS_ARG(-c) || IS_ARG(--config)) {
      GET_PARAM(--config)
      if (!realpath(optarg, par->storaged_config_file)) {
	REBUILD_FAILED("No such configuration file %s.",optarg);
	exit(EXIT_FAILURE);
      }
      continue;
    }  

    if IS_ARG(-id) {
      GET_INT_PARAM(-id,par->rebuildRef);
      continue;
    }  	    	

    if IS_ARG(-resume) {
      par->resume = 1;
      continue;
    } 
    
    if IS_ARG(-pause) {
      par->pause = 1;
      continue;
    }
    
    if IS_ARG(-abort) {
      par->abort = 1;
      continue;
    }
             
    if IS_ARG(-list) {
      par->list = 1;     
      continue;
    } 
    
    if (IS_ARG(-R) || IS_ARG(--relocate)) {
      par->relocate = 1;     
      continue;
    } 

    if (IS_ARG(-C) || IS_ARG(--clear)) {
      par->clear = 1;     
      continue;
    }  


    if (IS_ARG(-q) || IS_ARG(--quiet)) {
      quiet = 1;     
      continue;
    }  

    if IS_ARG(-bg) { 
      par->background = 1;     
      continue;
    }  

    if IS_ARG(-fg) { 
      par->background = 0;     
      continue;
    }  
        
    if (IS_ARG(-r) || IS_ARG(--rebuild)) { 
      GET_PARAM(--rebuild)
      if (strncpy(par->rbs_export_hostname, optarg, ROZOFS_HOSTNAME_MAX) == NULL) {
        REBUILD_FAILED("Bad host name %s.", optarg);
        exit(EXIT_FAILURE);
      }
      rbs_start_process = 1;
      continue;
    }  
	  
    if (IS_ARG(-s) || IS_ARG(--sid)) { 
      GET_PARAM(--sid)
      ret = sscanf(optarg,"%d/%d", &par->cid, &par->sid);
      if (ret != 2) {
	    REBUILD_FAILED("-s option requires also cid/sid.\n");
        exit(EXIT_FAILURE);
      }	
      continue;
    }  
    
    if (IS_ARG(-f) || IS_ARG(--fid)) { 
      GET_PARAM(--fid)
      ret = rozofs_uuid_parse(optarg,par->fid2rebuild);
      if (ret != 0) {
	    REBUILD_FAILED("Bad FID format %s.", optarg);
        exit(EXIT_FAILURE);
      }
      par->type = rbs_rebuild_type_fid; 
      par->rbs_device_number = -2; // To tell one FID to rebuild 
      continue;
    } 
	  
    if (IS_ARG(-o) || IS_ARG(--output)) { 
      GET_PARAM(--output)
      rbs_status_file_name(optarg);	  
      continue;
    } 	
	  
	// --simu exportd configuration file for vbox
    if (IS_ARG(--simu)) { 
      GET_PARAM(--simu)
	  par->simu = optarg;	  	  
      continue;
    }
	
    if (IS_ARG(-l) || IS_ARG(--loop)) { 
      GET_INT_PARAM(-l,par->max_reloop)
      continue;
    }
	  									
    if (IS_ARG(-d) || IS_ARG(--device)) { 
      GET_INT_PARAM(-d,par->rbs_device_number)
      par->type = rbs_rebuild_type_device; 
      continue;
    }
	  			
    if (IS_ARG(-g) || IS_ARG(--geosite)) { 
      GET_INT_PARAM(-g,par->storaged_geosite)
      if ((par->storaged_geosite!=0)&&(par->storaged_geosite!=1)) { 
        REBUILD_FAILED("Site number must be within [0:1] instead of %s.", optarg);
        exit(EXIT_FAILURE);
      }
      continue;
    }

    if (IS_ARG(-p) || IS_ARG(--parallel)) { 
      GET_INT_PARAM(-p,par->parallel)
      if (par->parallel > MAXIMUM_PARALLEL_REBUILD_PER_SID) {
        REBUILD_MSG("--parallel value is too big %d. Assume maximum parallel value of %d\n", 
		       par->parallel, MAXIMUM_PARALLEL_REBUILD_PER_SID);
	par->parallel = MAXIMUM_PARALLEL_REBUILD_PER_SID;
      }
      continue;
    }
				
    if (IS_ARG(-H) || IS_ARG(--host)) { 
      GET_PARAM(--host)
      par->storaged_hostname = optarg;
      continue;
    }

    usage("Unexpected argument \"%s\".\n",optarg);
  }


  /*
  ** On rebuild resume the rebuild identifier must be provided
  */
  if ((par->resume)||(par->list)||(par->pause)||(par->abort)) {
    if (par->rebuildRef==0) {
      REBUILD_FAILED("-resume -pause -abort and -list options require a rebuild identifier.");
      exit(EXIT_FAILURE);         
    }
  }
  else {
    if (rbs_start_process == 0) {
        REBUILD_FAILED("Missing mandatory option --rebuild");    
        exit(EXIT_FAILURE);      
    }     
    /*
    ** When neither resume nor ailed is given, the rebuild ref is the process pid
    */    
    par->rebuildRef = getpid();
  }

  /*
  ** When FID is given, eid and cid/sid is mandatory
  */ 
  if (par->type == rbs_rebuild_type_fid) {
    if ((par->cid==-1)&&(par->sid==-1)) {
      REBUILD_FAILED("--fid option requires --sid option too.");
      exit(EXIT_FAILURE);      
    }
	par->parallel = 1;
  }
  /*
  ** When relocate is set, cid/sid and device are mandatory 
  */
  if (par->relocate) {
    if ((par->cid==-1)&&(par->sid==-1)) {
      REBUILD_FAILED("--relocate option requires --sid option too.");
      exit(EXIT_FAILURE);      
    }
    if (par->type != rbs_rebuild_type_device) {
      REBUILD_FAILED("--relocate option requires --device option too.");
      exit(EXIT_FAILURE);      
    }
  }
  /*
  ** Clear errors and reinitialize disk
  */
  if (par->clear) {

    /*
    ** When clear is set cid/sid must too
    */    
    if ((par->cid==-1)&&(par->sid==-1)) {
      REBUILD_FAILED("--clear option requires --sid option too.");
      exit(EXIT_FAILURE);      
    }
  }
  
}
/*________________________________________________
*
* Search for -a or --resume in command
* and return the rebuild reference
*/
static rbs_parameter_t localPar;  
int is_command_resume(int argc, char *argv[]) {
  
  rbs_conf_init(&localPar);
  parse_command(argc,argv,&localPar);
  if (localPar.resume == 0) return 0;
  
  if (strcmp(localPar.storaged_config_file,STORAGED_DEFAULT_CONFIG)!=0) {
    REBUILD_FAILED("-resume and --config options are incompatibles.");
    exit(EXIT_FAILURE);      
  }
  if (localPar.rbs_export_hostname[0] != 0) {
    REBUILD_FAILED("-resume and --rebuild options are incompatibles.");
    exit(EXIT_FAILURE);      
  }
  if ((localPar.cid!=-1)||(localPar.sid!=-1)) {
    REBUILD_FAILED("-resume and --sid options are incompatibles.");
    exit(EXIT_FAILURE);      
  } 
  if (localPar.type == rbs_rebuild_type_device) {
    REBUILD_FAILED("-resume and --device options are incompatibles.");
    exit(EXIT_FAILURE);      
  } 
  if (localPar.type == rbs_rebuild_type_fid) {
    REBUILD_FAILED("-resume and --fid options are incompatibles.");
    exit(EXIT_FAILURE);      
  } 
  if (localPar.cid!=-1) {
    REBUILD_FAILED("-resume and --fid options are incompatibles.");
    exit(EXIT_FAILURE);      
  }        
  if (localPar.relocate) {
    REBUILD_FAILED("-resume and --relocate options are incompatibles.");
    exit(EXIT_FAILURE);      
  } 
  return localPar.rebuildRef;
}
/*____________________________________________________
   Rebuild monitoring
*/

#define RBS_MAX_MONITOR_PATH 128
typedef struct rbs_monitor_file_list_s {    
  char          name[RBS_MAX_MONITOR_PATH];  
  uint64_t      mtime;
} RBS_MONITOR_FILE_LIST_S;

#define RBS_MONITOR_MAX_FILES   32
static RBS_MONITOR_FILE_LIST_S rbs_monitor_file_list[RBS_MONITOR_MAX_FILES];
/*
**____________________________________________________
** Purge excedent files
*/
void rbs_monitor_purge(void) {
  struct dirent * dirItem;
  struct stat     statBuf;
  DIR           * dir;
  uint32_t        nb,idx;
  uint32_t        older;
  char file_path[FILENAME_MAX];
  char * pChar;

  pChar = file_path;
  pChar += rozofs_string_append(pChar,DAEMON_PID_DIRECTORY);
  pChar += rozofs_string_append(pChar,"/storage_rebuild/");  
  
  /* Open core file directory */ 
  dir=opendir(file_path);
  if (dir==NULL) return;

  nb = 0;

  while ((dirItem=readdir(dir))!= NULL) {
    
    /* Skip . and .. */ 
    if (dirItem->d_name[0] == '.') continue;

    rozofs_string_append(pChar,dirItem->d_name); 
    
    if (strlen(file_path) >= RBS_MAX_MONITOR_PATH) {
      /* Too big : can not store it, so delete it */
      unlink(file_path);
      continue;
    }

    /* Get file date */ 
    if (stat(file_path,&statBuf) < 0) {   
      severe("rbs_monitor_purge : stat(%s) %s",file_path,strerror(errno));
      unlink(file_path);
      continue;	           
    }
      
    /* Maximum number of file not yet reached. Just register this one */
    if (nb < RBS_MONITOR_MAX_FILES) {
      rbs_monitor_file_list[nb].mtime = statBuf.st_mtime;
      strcpy(rbs_monitor_file_list[nb].name,file_path);      
      nb ++;
      continue;
    }

    /* Maximum number of file is reached. Remove the older */     

    /* Find older in already registered list */ 
    older = 0;
    for (idx=1; idx < RBS_MONITOR_MAX_FILES; idx ++) {
      if (rbs_monitor_file_list[idx].mtime < rbs_monitor_file_list[older].mtime) older = idx;
    }

    /* 
    ** If older in list is older than the last one read, 
    ** the last one read replaces the older in the array and the older is removed
    */
    if (rbs_monitor_file_list[older].mtime < (uint32_t)statBuf.st_mtime) {
      unlink(rbs_monitor_file_list[older].name);	
      rbs_monitor_file_list[older].mtime = statBuf.st_mtime;
      strcpy(rbs_monitor_file_list[older].name, file_path);
      continue;
    }
    /*
    ** Else the last read is removed 
    */
    unlink(file_path);
  }
  closedir(dir);  
}


static char rebuild_status[64];
static char myFormatedString[1024];
int rbs_monitor_file_update(void) {
    int status = -1;
    int fd = -1;
    int i;
    uint64_t nb_files=0;
    uint64_t done_files=0;
    uint64_t deleted=0;
    uint64_t written=0;
    uint64_t written_spare=0;
    uint64_t read_spare=0;
    uint64_t read=0;
    char * pt;
          
    if ((fd = open(rbs_monitor_file_path, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IROTH)) < 0) {
        severe("can't open %s", rbs_monitor_file_path);
        goto out;
    }

    uint32_t sec = time(NULL) - loc_time + previous_delay;
    if (sec == 0) sec = 1;


    if (header_msg_size == 0) {
      char * pt = header_msg;
      pt += rozofs_string_append(pt, HEADER);
      pt += rozofs_string_append(pt, VERSION);
      *pt++ = ' ';
      pt += rozofs_string_append(pt, ROZO_GIT_REF);
      pt += rozofs_string_append(pt,"\nid        : ");
      pt += rozofs_u32_append(pt,parameter.rebuildRef);      
      pt += rozofs_string_append(pt,"\nstarting  : ");
      pt += rozofs_string_append(pt,initial_date);
      pt += rozofs_string_append(pt,"command   : ");
      pt += rozofs_string_append(pt,command);
      pt += rozofs_string_append(pt,"\nparallel  : ");
      pt += rozofs_u32_append(pt,parameter.parallel);
      pt += rozofs_eol(pt); 
      header_msg_size = pt - header_msg;
    }
    dprintf(fd,"%s",header_msg);
    
    pt = myFormatedString;
    pt += rozofs_string_append(pt, "status    : ");
    pt += rozofs_string_append(pt, rebuild_status);    
    pt += rozofs_string_append(pt, "\nloop      : ");
    pt += rozofs_u32_append(pt, run_loop);

    pt += rozofs_string_append(pt, "\ndelay     : ");
    
    uint32_t seconds= sec % 60; 
    uint32_t min=sec/60;
    uint32_t hour=min/60; 
    min = min % 60;
    pt += rozofs_u32_append(pt, hour);
    pt += rozofs_string_append(pt, ":");  
    pt += rozofs_u32_padded_append(pt, 2, rozofs_zero, min);
    pt += rozofs_string_append(pt, ":");
    pt += rozofs_u32_padded_append(pt, 2, rozofs_zero, seconds);  
     
    dprintf(fd,"%s\n",myFormatedString);
    
    for (i=0; i<nb_rbs_entry; i++) {
    
      pt = myFormatedString;
 
      pt += rozofs_string_append(pt, "\n* cid/sid : ");
      pt += rozofs_u32_append(pt, rbs_monitor[i].cid);
      *pt++ = '/';    
      pt += rozofs_u32_append(pt, rbs_monitor[i].sid);
      pt += rozofs_string_append(pt, "\n            - status   : ");
      pt += rozofs_string_append(pt, rbs_monitor[i].status);           
      if (rbs_monitor[i].layout == 0xFF) {
        pt += rozofs_string_append(pt, "\n            - nb files : ");
        pt += rozofs_u32_append(pt, rbs_monitor[i].done_files);      
        dprintf(fd,"%s\n",myFormatedString);       
        continue;
      }

      pt += rozofs_string_append(pt, "\n            - layout   : ");
      pt += rozofs_u32_append(pt, rbs_monitor[i].layout);
      pt += rozofs_string_append(pt, "\n            - listing  : ");
      pt += rozofs_u32_append(pt, rbs_monitor[i].list_building_sec);
      pt += rozofs_string_append(pt, " sec\n            - nb files : ");
      pt += rozofs_u32_append(pt, rbs_monitor[i].done_files);
      *pt++ = '/';    
      pt += rozofs_u32_append(pt, rbs_monitor[i].nb_files);       
      nb_files   += rbs_monitor[i].nb_files;
      done_files += rbs_monitor[i].done_files;

      if (rbs_monitor[i].nb_files) {
        pt += rozofs_string_append(pt, " (");
        pt += rozofs_u32_append(pt, rbs_monitor[i].done_files*100/rbs_monitor[i].nb_files);       
        pt += rozofs_string_append(pt, "%)");
      }
      
      pt += rozofs_string_append(pt, "\n            - deleted  : ");  
      pt += rozofs_u32_append(pt, rbs_monitor[i].deleted);
      deleted += rbs_monitor[i].deleted;


      pt += rozofs_string_append(pt, "\n            - written  : ");
      pt += rozofs_bytes_padded_append(pt, 7, (long long unsigned int)rbs_monitor[i].written);
      written += rbs_monitor[i].written;

      pt += rozofs_string_append(pt, "\n                         . nominal : ");
      pt += rozofs_bytes_padded_append(pt, 7, (long long unsigned int)rbs_monitor[i].written-rbs_monitor[i].written_spare);
      pt += rozofs_string_append(pt, "\n                         . spare   : ");
      pt += rozofs_bytes_padded_append(pt, 7, (long long unsigned int)rbs_monitor[i].written_spare);
      written_spare += rbs_monitor[i].written_spare;
      
      pt += rozofs_string_append(pt, "\n            - read     : ");
      pt += rozofs_bytes_padded_append(pt, 7, (long long unsigned int)rbs_monitor[i].read);
      read += rbs_monitor[i].read;
      
      pt += rozofs_string_append(pt, "\n                         . nominal : ");
      pt += rozofs_bytes_padded_append(pt, 7,(long long unsigned int)rbs_monitor[i].read-rbs_monitor[i].read_spare);
      read_spare += rbs_monitor[i].read_spare; 
      pt += rozofs_string_append(pt, "\n                         . spare   : ");
      pt += rozofs_bytes_padded_append(pt, 7,(long long unsigned int)rbs_monitor[i].read_spare);  
      dprintf(fd,"%s\n",myFormatedString); 
    }


    pt = myFormatedString;
    pt += rozofs_string_append(pt, "\n* total   :");
    pt += rozofs_string_append(pt, "\n            - nb files : ");
    pt += rozofs_u32_append(pt, done_files);
    *pt++ = '/';    
    pt += rozofs_u64_append(pt, nb_files); 
    pt += rozofs_string_append(pt, "\n            - deleted  : ");
    pt += rozofs_u64_append(pt, deleted);

#if 0          
    if (nb_files) {
      pt += rozofs_string_append(pt, " (");
      pt += rozofs_u32_append(pt, done_files*100/nb_files);       
      pt += rozofs_string_append(pt, "%)");
    }
#endif

    pt += rozofs_string_append(pt, "\n            - written  : ");
    pt += rozofs_bytes_padded_append(pt, 7,written);
    pt += rozofs_string_append(pt, " (");
    pt += rozofs_bytes_padded_append(pt, 7,written/sec);   
    pt += rozofs_string_append(pt, "/s)");
         
    pt += rozofs_string_append(pt, "\n                         . nominal : ");
    pt += rozofs_bytes_padded_append(pt, 7,written-written_spare);
    pt += rozofs_string_append(pt, "\n                         . spare   : ");
    pt += rozofs_bytes_padded_append(pt, 7,written_spare);

    pt += rozofs_string_append(pt, "\n            - read     : ");
    pt += rozofs_bytes_padded_append(pt, 7, read);
    pt += rozofs_string_append(pt, " (");    
    pt += rozofs_bytes_padded_append(pt, 7, read/sec);
    pt += rozofs_string_append(pt, "/s)");

    pt += rozofs_string_append(pt, "\n                         . nominal : ");
    pt += rozofs_bytes_padded_append(pt, 7,read-read_spare);
    pt += rozofs_string_append(pt, "\n                         . spare   : ");
    pt += rozofs_bytes_padded_append(pt, 7,read_spare);
    dprintf(fd,"%s\n",myFormatedString); 
     

    status = 0;
out:
    if (fd > 0) close(fd);

    return status;
}
void rbs_monitor_string_append( char *str) {
  int fd = -1;
  
  if ((fd = open(rbs_monitor_file_path, O_WRONLY | O_APPEND, S_IRWXU | S_IROTH)) < 0) {
    severe("can't open %s", rbs_monitor_file_path);
    goto out;
  }

  /* Format the string */
  dprintf(fd, "%s", str);

out:
  if (fd > 0) close(fd);
}
void rbs_monitor_update(char * new, int cid, int sid) {
  char * pt = rebuild_status;
  
  pt += rozofs_string_append(pt, new);

  if (cid != 0) {
    pt += rozofs_string_append(pt," (");
    pt += rozofs_u32_append(pt, cid); 
    *pt++ = '/';   
    pt += rozofs_u32_append(pt, sid);   
    *pt++ = ')'; 
    *pt = 0;
    
    rozofs_string_append(rbs_monitor[rbs_index].status, new);       
  }
 
  rbs_monitor_file_update();
}
int rbs_monitor_display() {
  char cmdString[256];
  char * pChar = cmdString;
  
  if (quiet) return 0;

  if (rbs_monitor_file_path[0] == 0) return 0;

  pChar += rozofs_string_append(pChar,"cat ");
  pChar += rozofs_string_append(pChar,rbs_monitor_file_path);
  return system(cmdString);
}

/*
**____________________________________________________

  Save delay
*/
void save_consummed_delay(void) {
  char fname[1024];
  int  fd = -1;
    
  sprintf(fname,"%s/%s",get_rebuild_directory_name(parameter.rebuildRef),"delay");
  
  uint32_t delay = time(NULL) - loc_time + previous_delay;
  /*
  ** Save command 
  */
  if ((fd = open(fname, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IROTH)) < 0) {
    severe("can't open %s %s", fname,strerror(errno));
    return;
  }
  
  if (write(fd,&delay,sizeof(delay))<=0) {
    severe("write(%s) %s",fname,strerror(errno));
  }
  close(fd);
  //info("PID %s written",fname);
}
/*
**____________________________________________________

  Save delay
*/
uint32_t read_previous_delay() {
  char      fname[1024];
  int       fd = -1;
    
  sprintf(fname,"%s/%s",get_rebuild_directory_name(parameter.rebuildRef),"delay");
  
  /*
  ** Save command 
  */
  if ((fd = open(fname, O_RDONLY , S_IRWXU | S_IROTH)) < 0) {
    severe("can't open %s %s", fname,strerror(errno));
    return 0; 
  }
  
  if (read(fd,&previous_delay,sizeof(previous_delay))<=0) {
    severe("write(%s) %s",fname,strerror(errno));
    close(fd);
	return -1;
  }
  
  close(fd);
  return 0;
}
/*
**____________________________________________________

  Save command in command file
*/
void save_pid() {
  char fname[1024];
  int  fd = -1;
  pid_t pid = getpid();
    
  sprintf(fname,"%s/%s",get_rebuild_directory_name(parameter.rebuildRef),"pid");
  
  /*
  ** Save command 
  */
  if ((fd = open(fname, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IROTH)) < 0) {
    severe("can't open %s %s", fname,strerror(errno));
    return;
  }
  
  if (write(fd,&pid,sizeof(pid))<=0) {
    severe("write(%s) %s",fname,strerror(errno));
  }
  close(fd);
  //info("PID %s written",fname);
}
/*
**____________________________________________________

  Save command in command file
*/
void forget_pid() {
  char fname[1024];
    
  sprintf(fname,"%s/%s",get_rebuild_directory_name(parameter.rebuildRef),"pid");
  unlink(fname);
}
/*
**____________________________________________________

  Save command in command file
*/
pid_t read_pid() {
  char fname[1024];
  int  fd = -1;
  pid_t pid= 0;
    
  sprintf(fname,"%s/%s",get_rebuild_directory_name(parameter.rebuildRef),"pid");
  
  /*
  ** Save command 
  */
  if ((fd = open(fname, O_RDONLY, S_IRWXU | S_IROTH)) < 0) {
    if (errno != ENOENT) severe("can't open %s %s", fname,strerror(errno));
  }
  else {   
    if (read(fd,&pid,sizeof(pid))<=0) {
      severe("read(%s) %s",fname,strerror(errno));
    } 
    close(fd);
  }  
  return pid;
}
/*
**____________________________________________________

  Crash callback
*/
static void on_crash(int sig) {
    // Remove pid file
    forget_pid();
    // Kill all sub-processes
    rozofs_session_leader_killer(1000000);    
    closelog();
}    
/*
**--------------------FID hash table
*/

/*
** FID hash table to prevent registering 2 times
**   the same FID for rebuilding
*/
#define FID_TABLE_HASH_SIZE  (16*1024)

#define FID_MAX_ENTRY      31
typedef struct _rb_fid_entries_t {
    int                        count;
    int                        padding;
    struct _rb_fid_entries_t * next;   
    fid_t                      fid[FID_MAX_ENTRY];
    uint8_t                    chunk[FID_MAX_ENTRY];
} rb_fid_entries_t;

rb_fid_entries_t ** rb_fid_table=NULL;


/*
**
*/
static inline unsigned int fid_hash(void *key) {
    uint32_t hash = 0;
    uint8_t *c;
    for (c = key; c != key + 16; c++)
        hash = *c + (hash << 6) + (hash << 16) - hash;
    return hash % FID_TABLE_HASH_SIZE;
}
static inline void rb_hash_table_initialize() {
  int size;
  
  size = sizeof(void *)*FID_TABLE_HASH_SIZE;
  rb_fid_table = malloc(size);
  memset(rb_fid_table,0,size);
  rb_fid_table_count = 0;
}

int rb_hash_table_search(fid_t fid) {
  int      i;
  unsigned int idx = fid_hash(fid);
  rb_fid_entries_t * p;
  fid_t            * pF;
  
  p = rb_fid_table[idx];
  
  while (p != NULL) {
    pF = &p->fid[0];
    for (i=0; i < p->count; i++,pF++) {
      if (memcmp(fid, pF, sizeof (fid_t)) == 0) return 1;
    }
    p = p->next;
  }
  return 0;
}
int rb_hash_table_search_chunk(fid_t fid,int chunk) {
  int      i;
  unsigned int idx = fid_hash(fid);
  rb_fid_entries_t * p;
  fid_t            * pF;
  uint8_t          * pC;
  
  p = rb_fid_table[idx];
  
  while (p != NULL) {
    pF = &p->fid[0];
    pC = &p->chunk[0];
    
    for (i=0; i < p->count; i++,pF++,pC++) {
      if (*pC != chunk) continue;
      if (memcmp(fid, pF, sizeof (fid_t)) == 0) return 1;
    }
    p = p->next;
  }
  return 0;
}
rb_fid_entries_t * rb_hash_table_new(idx) {
  rb_fid_entries_t * p;
    
  p = (rb_fid_entries_t*) malloc(sizeof(rb_fid_entries_t));
  p->count = 0;
  p->next = rb_fid_table[idx];
  rb_fid_table[idx] = p;
  
  return p;
}
rb_fid_entries_t * rb_hash_table_get(idx) {
  rb_fid_entries_t * p;
    
  p = rb_fid_table[idx];
  if (p == NULL)                 p = rb_hash_table_new(idx);
  if (p->count == FID_MAX_ENTRY) p = rb_hash_table_new(idx);  
  return p;
}
void rb_hash_table_insert(fid_t fid) {
  unsigned int idx = fid_hash(fid);
  rb_fid_entries_t * p;
  
  p = rb_hash_table_get(idx);
  memcpy(p->fid[p->count],fid,sizeof(fid_t));
  p->count++;
  rb_fid_table_count++;
}
void rb_hash_table_insert_chunk(fid_t fid, int chunk) {
  unsigned int idx = fid_hash(fid);
  rb_fid_entries_t * p;
  
  p = rb_hash_table_get(idx);
  memcpy(p->fid[p->count],fid,sizeof(fid_t));
  p->chunk[p->count] = chunk;
  p->count++;
  rb_fid_table_count++;
}
void rb_hash_table_delete() {
  int idx;
  rb_fid_entries_t * p, * pNext;
  
  if (rb_fid_table == NULL) return;
  
  for (idx = 0; idx < FID_TABLE_HASH_SIZE; idx++) {
    
    p = rb_fid_table[idx];
    while (p != NULL) {
      pNext = p->next;
      free(p);
      p = pNext;
    }
  }
  
  free(rb_fid_table);
  rb_fid_table = NULL;
}

/** Write the storage to rebuild information in the heaer
 *  of the rebuild file one the layout is known
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
static int rbs_write_st2rebuild(uint8_t layout) {

  /*
  ** Update layout in st2rebuild
  */
  storage_config.layout = layout;
  rbs_monitor[rbs_index].layout = layout;
           
  return rbs_write_storage_config_file(parameter.rebuildRef, &storage_config);
} 
  

/** Retrieves the list of bins files to rebuild from a storage
 *
 * @param rb_stor: storage contacted.
 * @param cid: unique id of cluster that owns this storage.
 * @param sid: the unique id for the storage to rebuild.
 *
 * @return: the number of file to rebuild.  -1 otherwise (errno is set)
 */
uint64_t rbs_get_rb_entry_list_one_storage(rb_stor_t *rb_stor, cid_t cid, sid_t sid, int *cfgfd, int failed, int * entry_size) {
    uint16_t slice = 0;
    uint8_t spare = 0;
    uint8_t device = 0;
    uint64_t cookie = 0;
    uint8_t eof = 0;
    sid_t dist_set[ROZOFS_SAFE_MAX];
    bins_file_rebuild_t * children = NULL;
    bins_file_rebuild_t * iterator = NULL;
    bins_file_rebuild_t * free_it = NULL;
    int            ret;
    rozofs_rebuild_entry_file_t file_entry;
	uint64_t count=0;
    
    DEBUG_FUNCTION;

  
    memset(dist_set, 0, sizeof (sid_t) * ROZOFS_SAFE_MAX);

    // While the end of the list is not reached
    while (eof == 0) {

        // Send a request to storage to get the list of bins file(s)
        if (rbs_get_rb_entry_list(&rb_stor->mclient, cid, rb_stor->sid, sid,
                &device, &spare, &slice, &cookie, &children, &eof) != 0) {
            severe("rbs_get_rb_entry_list failed: %s\n", strerror(errno));
            return -1;;
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

           // Verify if this entry is already present in list
	    if (rb_hash_table_search(iterator->fid) == 0) { 

	        /*
		** Layout not yet filled in st2rebuild. It is time
		** to update it
		*/
	    if (*entry_size == 0) {
		  ret = rbs_write_st2rebuild(iterator->layout);
		  if (ret != 0) return -1;
		  *entry_size = rbs_entry_size_from_layout(iterator->layout);
                }
			    
                rb_hash_table_insert(iterator->fid);
			
		memcpy(file_entry.fid,iterator->fid, sizeof (fid_t));
		file_entry.bsize       = iterator->bsize;
        	file_entry.todo        = 1;    
	        file_entry.relocate    = 0;		    
		file_entry.block_start = 0;  
		file_entry.block_end   = -1; 
                file_entry.error       = rozofs_rbs_error_none;
        	memcpy(file_entry.dist_set_current, iterator->dist_set_current, sizeof (sid_t) * ROZOFS_SAFE_MAX);	    

#if 0
  {
    char fid_string[128];
    int i;
    rozofs_uuid_unparse(file_entry.fid,fid_string);  
    printf("record FID %s bsize %d from %llu to %llu dist %d",
          fid_string,file_entry.bsize,
         (long long unsigned int) file_entry.block_start, 
	 (long long unsigned int) file_entry.block_end,
	 file_entry.dist_set_current[0]);
   for (i=1;i<4;i++) printf("-%d", file_entry.dist_set_current[i]);
   printf("\n");
  }  
#endif
        	ret = write(cfgfd[current_file_index],&file_entry,*entry_size); 
		if (ret != *entry_size) {
		  severe("can not write file cid%d sid%d %d %s",cid,sid,current_file_index,strerror(errno));
		}	    
		current_file_index++;
		count++;
		if (current_file_index >= parameter.parallel) current_file_index = 0; 		
		
            }

            free_it = iterator;
            iterator = iterator->next;
            free(free_it);
        }
      }

    return count;
}








int rbs_initialize(cid_t cid, sid_t sid, const char *storage_root, 
                   uint32_t dev, uint32_t dev_mapper, uint32_t dev_red) {
    int status = -1;
    DEBUG_FUNCTION;

    // Initialize the storage to rebuild 
    if (storage_initialize(storage_to_rebuild, cid, sid, storage_root,
		dev,
		dev_mapper,
		dev_red,
		-1,NULL) != 0)
        goto out;

    // Initialize the list of cluster(s)
    list_init(&cluster_entries);

    status = 0;
out:
    return status;
}

static int storaged_initialize() {
    int status = -1;
    list_t *p = NULL;
    DEBUG_FUNCTION;

   storaged_nrstorages = 0;

    storaged_nb_io_processes = 1;
    
    storio_nb_threads = common_config.nb_disk_thread;

    storaged_nb_ports = storaged_config.io_addr_nb;

    /* For each storage on configuration file */
    list_for_each_forward(p, &storaged_config.storages) {
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
int rbs_sanity_check(cid_t cid, sid_t sid, const char *root, uint32_t dev, uint32_t dev_mapper, uint32_t dev_red) {

    int status = -1;

    DEBUG_FUNCTION;

    // Try to initialize the storage to rebuild
    if (rbs_initialize(cid, sid, root, dev, dev_mapper, dev_red) != 0) {
        // Probably a path problem
        REBUILD_FAILED("Can't initialize rebuild storage (cid:%u; sid:%u;"
                " path:%s): %s\n", cid, sid, root, strerror(errno));
        goto out;
    }
    
    // Try to get the list of storages for this cluster ID
    pExport_host = rbs_get_cluster_list(&rpcclt_export, parameter.rbs_export_hostname, 
                                        parameter.storaged_geosite, cid, &cluster_entries);
    if (pExport_host == NULL) {	    
        REBUILD_FAILED("Can't get list of others cluster members from export"
                " server (%s) for storage to rebuild (cid:%u; sid:%u): %s\n",
                parameter.rbs_export_hostname, cid, sid, strerror(errno));
        goto out;
    }

    // Check the list of cluster
    if (rbs_check_cluster_list(&cluster_entries, cid, sid) != 0) {
        REBUILD_FAILED("No such storage (sid=%u) in cluster with cid=%u\n", sid, cid);
        goto out;
    }

    status = 0;

out:
    // Free cluster(s) list
    rbs_release_cluster_list(&cluster_entries);

    return status;
}
/** Check each storage to rebuild
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int rbs_write_count_file(int cid, int sid, uint64_t count) {
  char            filename[FILENAME_MAX];
  char          * pChar;
  char          * dir;
  int             fd;

  dir = get_rebuild_sid_directory_name(parameter.rebuildRef,cid,sid);

  /*
  ** Write the count file
  */
  pChar = filename;
  pChar += rozofs_string_append(pChar,dir);
  pChar += rozofs_string_append(pChar,"/count");
  fd = open(filename, O_WRONLY | O_APPEND | O_CREAT | O_TRUNC,0755);
  if (fd<0) {
    severe("open(%s) %s\n",filename,strerror(errno));
	return -1;
  }	
  if (write(fd,&count, sizeof(uint64_t)) != sizeof(uint64_t)) {
    severe("write(%d,%s) %s\n",fd,filename,strerror(errno));
	close(fd);
	return -1;			  
  }
  close(fd);
  info("cid/sid %d/%d : %llu files",cid,sid,(long long unsigned int)count);
  return 0;  
}
/** Check each storage to rebuild
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
static int rbs_check() {
    list_t *p = NULL;
    int status = -1;
    DEBUG_FUNCTION;

    // For each storage present on configuration file

    list_for_each_forward(p, &storaged_config.storages) {
        storage_config_t *sc = list_entry(p, storage_config_t, list);

        // Sanity check for rebuild this storage
        if (rbs_sanity_check( 
	        sc->cid, sc->sid, sc->root,
		sc->device.total,sc->device.mapper,sc->device.redundancy) != 0)
            goto out;
    }
    status = 0;
out:
    return status;
}

int rbs_storio_reinit(cid_t cid, sid_t sid, uint8_t dev, uint8_t reinit) {
    int status = -1;
    int ret;
    rb_stor_t stor;

    ret = rbs_get_storage(&rpcclt_export, parameter.rbs_export_hostname, parameter.storaged_geosite, cid, sid, &stor) ;
    if (ret == 0) {
      status = sclient_clear_error_rbs(&stor.sclients[0], cid, sid, dev, reinit);
    } 
    return status;
}
/** 
** Build storage list from export
**
** python script rozo_make_rebuild_lists.py will request export to build the
** list of FID supported by a list of ci:sid
** At the end of the script directory /tmp/rbs.<rebuildRef> should contain
** the job lists.
 */
int rbs_build_job_list_from_export() {
  int       idx;
  char      cmd[255];
  char    * pChar = cmd;
  int       first=1;
  uint8_t   layout;
  uint16_t  vid;

  int delay = time(NULL);
  
  *pChar = 0;
  
  for (idx=0; idx<nb_rbs_entry; idx++) {

	// Get the list of storages for this cluster ID
	pExport_host = rbs_get_cluster2_list(&rpcclt_export, parameter.rbs_export_hostname, 
                                    	parameter.storaged_geosite, 
										rbs_stor_configs[idx].cid, 
										&cluster_entries,
										&layout, &vid);
	if (pExport_host == NULL) {					
      severe("rbs_get_cluster2_list failed exportd %s cid %d %s", 
		      parameter.rbs_export_hostname, rbs_stor_configs[idx].cid, strerror(errno));
      continue;
	}  

    // Initialize the storage to rebuild
    if (rbs_initialize(rbs_stor_configs[idx].cid, 
	                   rbs_stor_configs[idx].sid, 
					   rbs_stor_configs[idx].root, 
					   rbs_stor_configs[idx].device.total, 
					   rbs_stor_configs[idx].device.mapper, 
					   rbs_stor_configs[idx].device.redundancy) != 0) {
        severe("can't init. storage to rebuild (cid:%u;sid:%u;path:%s)",
                rbs_stor_configs[idx].cid, rbs_stor_configs[idx].sid, rbs_stor_configs[idx].root);
        continue;
    }
	
    strcpy(storage_config.export_hostname,parameter.rbs_export_hostname);
    strcpy(storage_config.config_file,parameter.storaged_config_file);
    storage_config.site   = parameter.storaged_geosite;
    storage_config.layout = layout;
    storage_config.device = parameter.rbs_device_number;
    rbs_stor_configs[idx].status = RBS_STATUS_PROCESSING_LIST;
    rbs_write_storage_config_file(parameter.rebuildRef, &storage_config);
	
    rbs_monitor[idx].layout = layout;
           
		  
    if (first) {	
	  pChar += sprintf(pChar,"rozo_make_rebuild_lists.py -d -e %s -p %d -r %d -E %s -S %s -c %d:%d", 
                	   pExport_host, 
			   (int) parameter.parallel, 
			   (int) parameter.rebuildRef,
			   common_config.export_temporary_dir,
			   common_config.storage_temporary_dir,
                           (int) rbs_stor_configs[idx].cid, 
			   (int) rbs_stor_configs[idx].sid);
	  first = 0;
	}			
	else {
  	  pChar += sprintf(pChar, ",%d:%d",rbs_stor_configs[idx].cid, rbs_stor_configs[idx].sid);
    }
	
    // Free cluster(s) list
    rbs_release_cluster_list(&cluster_entries);

  }
  
  if (first) {
    severe("No CID/SID to rebuild");
	return -1;
  }

  if (parameter.simu != NULL) {
    pChar += sprintf(pChar, " -s %s -d",parameter.simu);
  }

  if (system(cmd)==0) {}
  delay = time(NULL) - delay;
  info("(%d s) %s",delay, cmd);
    
  for (idx=0; idx<nb_rbs_entry; idx++) {
  
    rbs_monitor[idx].list_building_sec = delay/nb_rbs_entry;  
	rbs_monitor[idx].nb_files = rbs_read_file_count(parameter.rebuildRef,
	                                            rbs_stor_configs[idx].cid,
												rbs_stor_configs[idx].sid);
												
  }
  
  return 0;  
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
    list_t       *p, *q;
    int            status = -1;
    char         * dir;
    char           filename[FILENAME_MAX];
    int            idx;
    int            cfgfd[MAXIMUM_PARALLEL_REBUILD_PER_SID];
    int            entry_size=0;
	uint64_t       count=0;
    char         * pChar;
	   
    /*
    ** Create FID list file files
    */
    dir = get_rebuild_sid_directory_name(parameter.rebuildRef,cid,sid);
    for (idx=0; idx < parameter.parallel; idx++) {

      pChar = filename;
      pChar += rozofs_string_append(pChar,dir);
      pChar += rozofs_string_append(pChar,"/job");
      pChar += rozofs_u32_append(pChar,idx);

      cfgfd[idx] = open (filename,O_CREAT | O_TRUNC | O_WRONLY, 0640);
      if (cfgfd[idx] == -1) {
        severe("Can not open file %s %s", filename, strerror(errno));
        return -1;
      }
    }    


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
                count = rbs_get_rb_entry_list_one_storage(rb_stor, cid, sid,cfgfd, failed, &entry_size);
				if (count == -1) {
                    severe("rbs_get_rb_entry_list_one_storage failed: %s\n",
                            strerror(errno));
                    goto out;
                }
            }
        }
    }

    status = 0;
out:
    for (idx=0; idx < parameter.parallel; idx++) {
      close(cfgfd[idx]);
    }  
	
	/*
	** Write the count file
	*/
	rbs_write_count_file(cid,sid,count);
    return status;
}

/** Build a list with just one FID
 *
 * @param cid: unique id of cluster that owns this storage.
 * @param sid: the unique id for the storage to rebuild.
 * @param fid2rebuild: the FID to rebuild
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
static int rbs_build_one_fid_list(cid_t cid, sid_t sid, uint8_t layout, uint8_t bsize, uint8_t * dist) {
  int            fd; 
  rozofs_rebuild_entry_file_t file_entry;
  char         * dir;
  char           filename[FILENAME_MAX];
  int            ret;
  int            i;
  /*
  ** Create FID list file files
  */
  dir = get_rebuild_sid_directory_name(parameter.rebuildRef,cid,sid);

  char * pChar = filename;
  pChar += rozofs_string_append(pChar,dir);
  pChar += rozofs_string_append(pChar,"/job0");
      
  fd = open(filename,O_CREAT | O_TRUNC | O_WRONLY, 0640);
  if (fd == -1) {
    severe("Can not open file %s %s", filename, strerror(errno));
    return -1;
  }

  rbs_write_st2rebuild(layout);    

  memcpy(file_entry.fid, parameter.fid2rebuild, sizeof (fid_t));
  file_entry.bsize       = bsize;  
  file_entry.todo        = 1;      
  file_entry.relocate    = 1;  
  file_entry.block_start = 0;  
  file_entry.block_end   = -1;  
  file_entry.error       = rozofs_rbs_error_none;
  
  for(i=0; i<ROZOFS_SAFE_MAX; i++) {
    file_entry.dist_set_current[i] = dist[i];
  }    

  int entry_size = rbs_entry_size_from_layout(layout);
  
  ret = write(fd,&file_entry,entry_size); 
  if (ret != entry_size) {
    severe("can not write file cid%d sid%d %s",cid,sid,strerror(errno));
    return -1;
  }  
  
  close(fd);
 
  /*
  ** Write the count file
  */
  rbs_write_count_file(cid,sid,1);  
  
  return 0;   
}
/** During rebuild update periodicaly the stats
 *
 */
void periodic_stat_update(int * fd) {
  RBS_MONITOR_S         stat;
  int i;
  ROZOFS_RBS_COUNTERS_T counters;
    
  memcpy(&stat,&rbs_monitor[rbs_index], sizeof(stat));  
  stat.done_files      = 0;
  stat.deleted         = 0;    
  stat.written         = 0;
  stat.written_spare   = 0;
  stat.read            = 0;
  stat.read_spare      = 0;

  for (i=0; i< parameter.parallel; i++) {
    if (pread(fd[i], &counters, sizeof(counters), 0) == sizeof(counters)) {
      stat.done_files      += counters.done_files;
      stat.deleted         += counters.deleted;
      stat.written         += counters.written;
      stat.written_spare   += counters.written_spare;	
      stat.read            += counters.read;
      stat.read_spare      += counters.read_spare;
    }
  }

  memcpy(&rbs_monitor[rbs_index],&stat,sizeof(stat));
  rbs_monitor_file_update();
}
  
/** Rebuild list just produced 
 *
 */
int rbs_do_list_rebuild(int cid, int sid) {
  char         * dirName;
  char           cmd[FILENAME_MAX];
  int            status;
  int            failure;
  int            success;
  int            fd[128];
  char           fname[128];
  struct timespec timeout;
  sigset_t        mask;
  sigset_t        orig_mask; 
  pid_t           pid;
  int             instance;
  struct stat     buf;
  
  sigemptyset (&mask);
  sigaddset (&mask, SIGCHLD); 
  if (sigprocmask(SIG_BLOCK, &mask, &orig_mask) < 0) {
    severe("sigprocmask %s", strerror(errno));
    return 1;
  } 
   
  /*
  ** Start one rebuild process par rebuild file
  */
  dirName = get_rebuild_sid_directory_name(parameter.rebuildRef,cid,sid);  
  
  failure = 0;
  success = 0;
  	  
  /*
  ** Loop on distribution sub directories
  */
  for (instance=0; instance<parameter.parallel; instance++) {

    char * pChar = fname;
    pChar += rozofs_string_append(pChar,dirName);
    pChar += rozofs_string_append(pChar,"/stat");
    pChar += rozofs_u32_append(pChar,instance);
    
    fd[instance] = open(fname, O_RDONLY|O_CREAT, 0755);
	if (fd[instance]<0) {
	  severe("open(%s) %s",fname, strerror(errno));
	}  


    // Start a process for a job file
    pChar = fname;
    pChar += rozofs_string_append(pChar,dirName);
    pChar += rozofs_string_append(pChar,"/job");
    pChar += rozofs_u32_append(pChar,instance);
    
	// Look up for the job file
    if (stat(fname, &buf)<0) {
	  // No such job list any more
	  if (errno == ENOENT) {
		success++;
		continue;
	  }
      // Other errors
	  severe("stat(%s) %s",fname, strerror(errno));	  
	  failure++;
	  continue;	  
	}
	
	// File exist but is empty
	if (buf.st_size == 0) {
	  unlink(fname);
	  success++;
	  continue;	  
	}
	
    pid = fork();
    
    if (pid == 0) {
      char * pChar = cmd;
      pChar += rozofs_string_append(pChar,"storage_list_rebuilder -c ");
	  pChar += rozofs_u32_append(pChar,cid);
      pChar += rozofs_string_append(pChar," -s ");
	  pChar += rozofs_u32_append(pChar,sid);
      pChar += rozofs_string_append(pChar," -r ");
	  pChar += rozofs_u32_append(pChar,parameter.rebuildRef);
      pChar += rozofs_string_append(pChar," -i ");
	  pChar += rozofs_u32_append(pChar,instance);
      if (quiet) {
        pChar += rozofs_string_append(pChar," --quiet");
      }
      status = system(cmd);
      if (status == 0) exit(0);
      exit(-1);
    }	
  }


  periodic_stat_update(fd);
  
  while (parameter.parallel > (failure+success)) {
    int                   ret;

    timeout.tv_sec  = 20;
    timeout.tv_nsec = 0;
    
    ret = sigtimedwait(&mask, NULL, &timeout);
    if (ret < 0) {
      if (errno != EAGAIN) continue;
    }  

     
    /* Check for rebuild sub processes status */    
    while ((pid = waitpid(-1,&status,WNOHANG)) > 0) {
	
      status = WEXITSTATUS(status);

      if (status != 0) failure++;
      else             success++;
	  
	  periodic_stat_update(fd);
    }
     
	 
    periodic_stat_update(fd);
    
    // Rebuild is paused. Forward signal to every child
    if (sigusr_received) {
	  kill(0,SIGUSR1);
    }
  }
  
  
  for (instance=0; instance<parameter.parallel; instance++) {
    if (fd[instance]>0) close(fd[instance]);
  }

  if (failure != 0) {
    severe("%d list rebuild processes failed upon %d",failure,parameter.parallel);
    return -1;
  }
  return 0;
}
/** Retrieves the list of bins files to rebuild from the available disks
 *
 * @param cluster_entries: list of cluster(s).
 * @param cid: unique id of cluster that owns this storage.
 * @param sid: the unique id for the storage to rebuild.
 * @param device: the missing device identifier 
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
static int rbs_build_device_missing_list_one_cluster(cid_t cid, 
						     sid_t sid,
						     int device_to_rebuild) {
  char           dir_path[FILENAME_MAX];						     
  char           slicepath[FILENAME_MAX];						     
  char           filepath[FILENAME_MAX];						     
  int            device_it;
  int            spare_it;
  DIR           *dir1;
  struct dirent *file;
  int            fd; 
  size_t         nb_read;
  rozofs_stor_bins_file_hdr_t file_hdr; 
  rozofs_rebuild_entry_file_t file_entry;
  int            idx;
  char         * dir;
  char           filename[FILENAME_MAX];
  int            cfgfd[MAXIMUM_PARALLEL_REBUILD_PER_SID];
  int            ret;
  int            slice;
  uint8_t        chunk;  
  int            entry_size=0;
  char * pChar;

  /*
  ** Create FID list file files
  */
  dir = get_rebuild_sid_directory_name(parameter.rebuildRef,cid,sid);
  for (idx=0; idx < parameter.parallel; idx++) {

    pChar = filename;
    pChar += rozofs_string_append(pChar,dir);
    pChar += rozofs_string_append(pChar,"/job");
    pChar += rozofs_u32_append(pChar,idx);
      
    cfgfd[idx] = open(filename,O_CREAT | O_TRUNC | O_WRONLY, 0755);
    if (cfgfd[idx] == -1) {
      severe("Can not open file %s %s", filename, strerror(errno));
      return 0;
    }
  }   

  // Loop on all the devices
  for (device_it = 0; device_it < storage_to_rebuild->device_number;device_it++) {

    // Do not read the disk to rebuild
    if (device_it == device_to_rebuild) continue;

    // For spare and no spare
    for (spare_it = 0; spare_it < 2; spare_it++) {

      // Build path directory for this layout and this spare type        	
      char * pChar = dir_path;
      pChar += rozofs_string_append(pChar,storage_to_rebuild->root);
      *pChar++ = '/';
      pChar += rozofs_u32_append(pChar,device_it); 
      pChar += rozofs_string_append(pChar,"/hdr_");
      pChar += rozofs_u32_append(pChar,spare_it); 
         
      
      // Check that this directory already exists, otherwise it will be create
      if (access(dir_path, F_OK) == -1) continue;

      for (slice=0; slice < (common_config.storio_slice_number); slice++) {

        storage_build_hdr_path(slicepath, storage_to_rebuild->root, device_it, spare_it, slice);

        // Open this directory
        dir1 = opendir(slicepath);
        if (dir1 == NULL) continue;


        // Loop on header files in slice directory
        while ((file = readdir(dir1)) != NULL) {
          int i;
          
	  if (file->d_name[0] == '.') continue;

          // Read the file
	  pChar = filepath;
	  pChar += rozofs_string_append(pChar,slicepath);
	  *pChar++ = '/';
	  pChar += rozofs_string_append(pChar,file->d_name);
 
	  fd = open(filepath, ROZOFS_ST_NO_CREATE_FILE_FLAG, ROZOFS_ST_BINS_FILE_MODE);
	  if (fd < 0) continue;

          nb_read = pread(fd, &file_hdr, sizeof(file_hdr), 0);
	  close(fd);	    

          // What to do with such an error ?
	  if (nb_read != sizeof(file_hdr)) continue;
	  
	  // When not in a relocation case, rewrite the file header on this device if it should
	  if (!parameter.relocate) {
            for (i=0; i < storage_to_rebuild->mapper_redundancy; i++) {
	          int dev;

              dev = storage_mapper_device(file_hdr.v0.fid,i,storage_to_rebuild->mapper_modulo);

 	      if (dev == device_to_rebuild) {
		// Let's re-write the header file  	      
        	storage_build_hdr_path(filepath, storage_to_rebuild->root, device_to_rebuild, spare_it, slice);
        	ret = storage_write_header_file(NULL,dev,filepath,&file_hdr);
		if (ret != 0) {
	          severe("storage_write_header_file(%s) %s",filepath,strerror(errno))
		}	
		break;
	      } 
	    } 
	  }

          // Check whether this file has some chunk of data on the device to rebuild
	  for (chunk=0; chunk<ROZOFS_STORAGE_MAX_CHUNK_PER_FILE; chunk++) {
	  
	      if (file_hdr.v0.device[chunk] == ROZOFS_EOF_CHUNK)  break;
	      
              if (file_hdr.v0.device[chunk] != device_to_rebuild) continue;
	   
              /*
	      ** This file has a chunk on the device to rebuild
	      ** Check whether this FID is already set in the list
	      */
	      if (rb_hash_table_search_chunk(file_hdr.v0.fid,chunk) == 0) {
	        rb_hash_table_insert_chunk(file_hdr.v0.fid,chunk);	
	      }
	      else {
		continue;
	      }	      

	      /*
	      ** Layout not yet filled in st2rebuild. It is time
	      ** to update it
	      */
	      if (entry_size == 0) {
	        ret = rbs_write_st2rebuild(file_hdr.v0.layout);
		if (ret != 0) goto out;
		entry_size = rbs_entry_size_from_layout(file_hdr.v0.layout);
              }

	      memcpy(file_entry.fid,file_hdr.v0.fid, sizeof (fid_t));
	      file_entry.bsize       = file_hdr.v0.bsize;
              file_entry.todo        = 1;     
	      file_entry.relocate    = parameter.relocate;
	      file_entry.block_start = chunk * ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(file_hdr.v0.bsize);  
	      file_entry.block_end   = file_entry.block_start + ROZOFS_STORAGE_NB_BLOCK_PER_CHUNK(file_hdr.v0.bsize) -1;  
              file_entry.error       = rozofs_rbs_error_none;
	      
              memcpy(file_entry.dist_set_current, file_hdr.v0.dist_set_current, sizeof (sid_t) * ROZOFS_SAFE_MAX);	    

              ret = write(cfgfd[current_file_index],&file_entry,entry_size); 
	      if (ret != entry_size) {
	        severe("can not write file cid%d sid%d %d %s",cid,sid,current_file_index,strerror(errno));
	      }
	      current_file_index++;
	      if (current_file_index >= parameter.parallel) current_file_index = 0; 
	  }
	      
	} // End of loop in one slice 
	closedir(dir1);  
      } // End of slices
    }
  } 

out:
  for (idx=0; idx < parameter.parallel; idx++) {
    close(cfgfd[idx]);
  } 

  /*
  ** Write the count file
  */
  rbs_write_count_file(cid,sid,rb_fid_table_count);  
  return 0;   
}
int rbs_build_job_lists(cid_t cid, sid_t sid, const char *root, uint32_t dev, uint32_t dev_mapper, uint32_t dev_red) {
    int status = -1;
    int failed,available;

    DEBUG_FUNCTION;

    int time_start = time(NULL);

    rb_hash_table_initialize();

    // Initialize the storage to rebuild
    if (rbs_initialize(cid, sid, root, dev, dev_mapper, dev_red) != 0) {
        severe("can't init. storage to rebuild (cid:%u;sid:%u;path:%s)",
                cid, sid, root);
        goto out;
    }
    strcpy(storage_config.export_hostname,parameter.rbs_export_hostname);
    strcpy(storage_config.config_file,parameter.storaged_config_file);
    storage_config.site = parameter.storaged_geosite;
    storage_config.layout = 0xFF;
    storage_config.device = parameter.rbs_device_number;

    // Get the list of storages for this cluster ID
    pExport_host = rbs_get_cluster_list(&rpcclt_export, parameter.rbs_export_hostname, 
                                        parameter.storaged_geosite, cid, &cluster_entries);
    if (pExport_host == NULL) {					
        severe("rbs_get_cluster_list failed (cid: %u) : %s", cid, strerror(errno));
        goto out;
    }

    // Check the list of cluster
    if (rbs_check_cluster_list(&cluster_entries, cid, sid) != 0)
        goto out;

    // Get connections for this given cluster
    rbs_init_cluster_cnts(&cluster_entries, cid, sid,&failed,&available);

    // One FID to rebuild
    if (parameter.type == rbs_rebuild_type_fid) {
      uint32_t   bsize;
      uint8_t    layout; 
      ep_mattr_t attr;
      
      // Resolve this FID thanks to the exportd
      if (rbs_get_fid_attr(&rpcclt_export, pExport_host, parameter.fid2rebuild, &attr, &bsize, &layout) != 0)
      {
        if (errno == ENOENT) {
	  status = -2;
	  REBUILD_FAILED("Unknown FID");
	}
	else {
	  REBUILD_FAILED("Can not get attributes from export \"%s\" %s",pExport_host,strerror(errno));
	}
	goto out;
      }
      
      if (rbs_build_one_fid_list(cid, sid, layout, bsize, (uint8_t*) attr.sids) != 0)
        goto out;
      rb_fid_table_count = 1;	
      parameter.parallel = 1;
      storage_config.layout  = layout;
      rbs_monitor[rbs_index].layout = layout;
    }
    else if (parameter.type == rbs_rebuild_type_storage) {
      // Build the list from the remote storages
      if (rbs_get_rb_entry_list_one_cluster(&cluster_entries, cid, sid, failed) != 0)
        goto out;  	 	 	 
    }
    else {
      // The device number is to big for theis storage
      if (parameter.rbs_device_number >= storage_config.storage.device_number) {
        REBUILD_FAILED("No such device number %d.",parameter.rbs_device_number);
	status = -2;	
	goto out;
      }
      // The storage has only on device, so this is a complete storage rebuild
      if (storage_config.storage.device_number == 1) {
	// Build the list from the remote storages
	if (rbs_get_rb_entry_list_one_cluster(&cluster_entries, cid, sid,failed) != 0)
          goto out;         
      }
      else {
	// Build the list from the available data on local disk
	if (rbs_build_device_missing_list_one_cluster(cid, sid, parameter.rbs_device_number) != 0)
          goto out;
      }		    		
    }
    
    // No file to rebuild
    if (rb_fid_table_count==0) {
      REBUILD_MSG("No file to rebuild");
      rbs_empty_dir (get_rebuild_sid_directory_name(parameter.rebuildRef,cid,sid));
      unlink(get_rebuild_sid_directory_name(parameter.rebuildRef,cid,sid));
    }
    else { 
      REBUILD_MSG("%llu files to rebuild by %d processes",
           (unsigned long long int)rb_fid_table_count,parameter.parallel);
    }	   
     
    status = 0;
    rbs_monitor[rbs_index].list_building_sec = time(NULL) - time_start;
out:    
    rb_hash_table_delete();    
    rbs_monitor[rbs_index].nb_files = rb_fid_table_count;
    // Free cluster(s) list
    rbs_release_cluster_list(&cluster_entries);
    return status;
}
/* Empty and remove a directory
*
* @param dirname: Name of the directory to cleanup
*/
void clean_dir(char * name) {
  DIR           *dir;
  struct dirent *file;
  char           fname[256];
  struct stat    st;

#if 0
  #warning NO CLEAN DIR
  return;
#endif
  
  if (name==NULL) return;
    
  if (stat(name,&st)<0) {
    return;
  }
  
  if (!S_ISDIR(st.st_mode)) {
    if (unlink(name)<0) {
      severe("unlink(%s) %s",name,strerror(errno));
    }  
    return;
  }
      
    
  /*
  ** Open this directory
  */
  dir = opendir(name);
  if (dir == NULL) {
    severe("opendir(%s) %s", name, strerror(errno));
    return;
  } 	  
  /*
  ** Loop on distribution sub directories
  */
  while ((file = readdir(dir)) != NULL) {
  
    if (strcmp(file->d_name,".")==0)  continue;
    if (strcmp(file->d_name,"..")==0) continue;
    
    char * pChar = fname;
    pChar += rozofs_string_append(pChar,name);
    *pChar++ = '/';
    pChar += rozofs_string_append(pChar,file->d_name);
    
    clean_dir(fname);
  }
  closedir(dir); 
  rmdir(name);
  return;
}
/*
** Display the list of remaining FID in a given rebuild job list
** given by its file name
*/
void storaged_rebuild_list_read(char * fid_list) {
  int        fd = -1;
  uint64_t   offset;
  rozofs_rebuild_entry_file_t   file_entry;
  char fidString[40];
      
  fd = open(fid_list,O_RDONLY);
  if (fd < 0) {
      printf("Can not open file %s %s",fid_list,strerror(errno));
      goto error;
  }
  
  int entry_size = rbs_entry_size_from_layout(storage_config.layout);

  offset = 0;

  while (pread(fd,&file_entry,entry_size,offset) == entry_size) {
  
    offset += entry_size;    
    
    /* Next file to rebuild */ 
    if (file_entry.todo) {
      rozofs_uuid_unparse(file_entry.fid,fidString);
      printf("%s  \"%s\"\n",fidString,rozofs_rbs_error_2_string(file_entry.error));
    }
  }


error: 
  if (fd != -1) close(fd);   
}
/*
** Display the list of remaining FID in a given rebuild job list
** given by its file name
*/
int storaged_rebuild_list_count(char * fid_list,uint64_t * count) {
  int        fd = -1;
  uint64_t   offset;
  rozofs_rebuild_entry_file_t   file_entry;
      
  fd = open(fid_list,O_RDONLY);
  if (fd < 0) {
    severe("Can not open file %s %s",fid_list,strerror(errno));
    goto error;
  }
  
  int entry_size = rbs_entry_size_from_layout(storage_config.layout);
  
  offset = 0;
  
  while (pread(fd,&file_entry,entry_size,offset) == entry_size) {
    offset += entry_size;    
    *count += 1;
  }


error: 
  if (fd != -1) close(fd);   
  return storage_config.layout;
}
/** Display the list of remaining FIDs
 *
 */
void rbs_list_remaining_fid(void) {
  char         * dirName;
  DIR           *dir0;
  DIR           *dir1;
  struct dirent *file0;
  struct dirent *file1;
  char           fname[512];
     
  /*
  ** Get rebuild job directory
  */
  dirName = get_rebuild_directory_name(parameter.rebuildRef);
  
  /*
  ** Open this directory
  */
  dir0 = opendir(dirName);
  if (dir0 == NULL) {
    if (errno==ENOENT) {
      REBUILD_FAILED("Rebuild identifier %d does not exist any more.",parameter.rebuildRef);
    }
    else {
      severe("opendir(%s) %s", dirName, strerror(errno));
    }  
    return;
  } 	  
  /*
  ** Loop on distribution sub directories
  */
  while ((file0 = readdir(dir0)) != NULL) {
  
    if (strcmp(file0->d_name,".")==0)  continue;
    if (strcmp(file0->d_name,"..")==0) continue;    
    if (strcmp(file0->d_name,"command")==0) continue;    
    if (strcmp(file0->d_name,"pid")==0) continue;    
    
    /*
    ** For each subdirectory : ie cid/sid
    */
    char * pChar = fname;
    pChar += rozofs_string_append(pChar,dirName);
    *pChar++ = '/';
    pChar += rozofs_string_append(pChar,file0->d_name);    
    dir1 = opendir(fname);
    if (dir1 == NULL) {
      severe("opendir(%s) %s", fname, strerror(errno));
      continue;
    }

    /*
	** Read the storage configuration file
	*/
	if (rbs_read_storage_config_file(fname, &storage_config) == NULL) {
      severe("rbs_read_storage_config_file(%s) %s", fname, strerror(errno));
      continue;
	}
	
    /*
    ** Loop on distribution sub directories
    */
    while ((file1 = readdir(dir1)) != NULL) {

      if (strcmp(file1->d_name,".")==0)  continue;
      if (strcmp(file1->d_name,"..")==0) continue;   
      
      /*
      ** For each file of each subdirectory
      */
      char * pChar = fname;
      pChar += rozofs_string_append(pChar,dirName);
      *pChar++ = '/';
      pChar += rozofs_string_append(pChar,file0->d_name);    
      *pChar++ = '/';
      pChar += rozofs_string_append(pChar,file1->d_name);          
      storaged_rebuild_list_read(fname);
    }
    
    closedir(dir1);  	
  }
  closedir(dir0);
}
/** Stop a running rebuild
 *
 */
pid_t rbs_get_running_pid() {
  pid_t pid;
  char  fname[512];
  int   fd;  
    
  pid = read_pid();
  if (pid == 0) { 
    return 0;
  }
      
  sprintf(fname,"/proc/%d/cmdline",pid);
  
  /*
  ** Save command 
  */
  if ((fd = open(fname, O_RDONLY, S_IRWXU | S_IROTH)) < 0) {
    if (errno != ENOENT) severe("can't open %s %s", fname,strerror(errno));
    return 0;    
  }

  if (read(fd,fname,sizeof(fname))<=0) {
    severe("read(%s) %s",fname,strerror(errno));
    return 0;
  }
  close(fd);
  
  if (strstr(fname, "storage_rebuild")==NULL) {
    return 0;
  }

  return pid;
}
/** Pause a running rebuild
 *
 */
void rbs_rebuild_pause() {
  pid_t pid;

  pid = rbs_get_running_pid();
  if (pid == 0) {
    REBUILD_MSG("Rebuild %d is not running",parameter.rebuildRef); 
    return;
  }

  kill(pid,SIGUSR1);

  REBUILD_MSG("Rebuild %d will be paused within few minutes",parameter.rebuildRef); 
  printf("Resume this rebuild   : storage_rebuild -id %d -resume\n", parameter.rebuildRef); 

  return;
}
/** Abort a running rebuild
 *
 */
void rbs_rebuild_abort() {
  pid_t pid;

  pid = rbs_get_running_pid();
  if (pid != 0) {
    kill(pid,SIGINT);
    sleep(1);
  }
  clean_dir(get_rebuild_directory_name(parameter.rebuildRef));

  REBUILD_MSG("Rebuild %d will be aborted within few minutes",parameter.rebuildRef); 
  return;
}
/** Starts a thread for rebuild given storage(s)
 *
 * @param nb: Number of entries.
 * @param v: table of storages configurations to rebuild.
 */
static inline int rebuild_storage_thread(rbs_stor_config_t *stor_confs) {
  int    result;
  int    delay=1;
  int    cid, sid;

  rbs_monitor_update("initiated",0,0);


  while (run_loop < parameter.max_reloop) {

    run_loop++;

    /*
    ** When relooping, let some time for things to get repaired magicaly
    */
    if (run_loop != 1) {

      rbs_monitor_display();  
   
      REBUILD_MSG("Rebuild failed ! Attempt #%u/%u will start in %d minutes", run_loop, parameter.max_reloop, delay);      
      	
      sleep(delay * 60);       
      if (delay < 60) delay = 2 *delay;
      if (delay > 60) delay = 60;
    }

    /*
    ** Let's process the clusters one after the other
    */
    for (rbs_index = 0; rbs_index < nb_rbs_entry; rbs_index++) {

      cid = stor_confs[rbs_index].cid;
      sid = stor_confs[rbs_index].sid;

      /* 
      ** Depending on the rebuild status
      */
      switch(stor_confs[rbs_index].status) {

	  /*
	  ** The list of rebuilding jobs is not yet done for this sid
	  */
	  case RBS_STATUS_BUILD_JOB_LIST:

      rbs_monitor_update("building list",cid, sid);	    
      REBUILD_MSG("Start rebuild process (cid=%u;sid=%u).",cid, sid);

	  result = rbs_build_job_lists(cid, sid, stor_confs[rbs_index].root,
				     stor_confs[rbs_index].device.total,
				     stor_confs[rbs_index].device.mapper, 
				     stor_confs[rbs_index].device.redundancy);  
				     
	  if (sigusr_received) {
	    /* Rebuild is interupted */
            goto paused;
	  }
	  /*
	  ** Failure
	  */
      if (result == -1) {
        rbs_monitor_update("failed",cid, sid);
	    REBUILD_MSG("cid %d sid %d building list failed.", cid, sid);	      
	    continue; /* Process next sid */
	  }

	  /*
	  ** Abort
	  */	    
	  if (result < 0) {
        // Abort 
        rbs_monitor_update("abort",cid, sid);	      
        REBUILD_MSG("cid %d sid %d building list abort.",cid, sid);
	    stor_confs[rbs_index].status = RBS_STATUS_ERROR;
	    continue;				
	  }
	  
	  /*
	  ** No file o rebuild 
	  */
	  if (rbs_monitor[rbs_index].nb_files == 0) {
            rbs_monitor_update("success",cid, sid);
	    REBUILD_MSG("cid %d sid %d No file to rebuild !!!", cid, sid);
	    stor_confs[rbs_index].status = RBS_STATUS_SUCCESS;
	    /*
	    ** Remove cid/sid directory 
	    */
	    clean_dir(get_rebuild_sid_directory_name(parameter.rebuildRef,cid,sid));  
	    /*
	    ** Clear errors
	    */
	    if (parameter.type == rbs_rebuild_type_device) {
	      rbs_storio_reinit(cid, sid, parameter.rbs_device_number, 0);
	    }   
	    else {
	      rbs_storio_reinit(cid, sid, 0xFF, 0); 
	    }   
	    continue;
	  }  	  

	/*
	** Try or retry to rebuild the job list
	*/ 
	case RBS_STATUS_PROCESSING_LIST:
        case RBS_STATUS_FAILED:

          rbs_monitor_update("running",cid, sid);

#if 1
		if (rbs_monitor[rbs_index].nb_files == 0) {
          REBUILD_MSG("cid %d sid %d No file to rebuild. Check ssh connection toward exportd host.", cid, sid);
          rbs_monitor_update("No file to rebuild (Check ssh cnx with export).",cid, sid);
          stor_confs[rbs_index].status = RBS_STATUS_SUCCESS;
          /*
          ** Remove cid/sid directory 
          */
          clean_dir(get_rebuild_sid_directory_name(parameter.rebuildRef,cid,sid));  
          /*
          ** Clear errors
          */
          if (parameter.type == rbs_rebuild_type_device) {
            rbs_storio_reinit(cid, sid, parameter.rbs_device_number, 0);
          }   
          else {
	        rbs_storio_reinit(cid, sid, 0xFF, 0); 
	      }   
	      continue;
	    }
#endif		  
	  stor_confs[rbs_index].status = RBS_STATUS_PROCESSING_LIST;	      	    
	  result = rbs_do_list_rebuild(cid, sid);

	  if (sigusr_received) {
	    /* Rebuild is interupted */
            goto paused;
	    
	  }		      
	  
	  /*
	  ** Success 
	  */
          if (result == 0) {
            rbs_monitor_update("success",cid, sid);
	    REBUILD_MSG("cid %d sid %d Rebuild success.", cid, sid);
	    stor_confs[rbs_index].status = RBS_STATUS_SUCCESS;
	    /*
	    ** Remove cid/sid directory 
	    */
	    clean_dir(get_rebuild_sid_directory_name(parameter.rebuildRef,cid,sid));  
	    /*
	    ** Clear errors
	    */
	    if (parameter.type == rbs_rebuild_type_device) {
	      rbs_storio_reinit(cid, sid, parameter.rbs_device_number, 0);
	    }   
	    else {
	      rbs_storio_reinit(cid, sid, 0xFF, 0); 
	    }   
	    continue;
	  }
	  
	  /*
	  ** Failure
	  */
          if (result == -1) {
            rbs_monitor_update("failed",cid, sid);
	    REBUILD_MSG("cid %d sid %d Rebuild failed.", cid, sid);
	    stor_confs[rbs_index].status = RBS_STATUS_FAILED;
	    continue;	      
	  }


	  /*
	  ** Error
	  */
          rbs_monitor_update("error",cid, sid);
	  REBUILD_MSG("cid %d sid %d Rebuild error.", cid, sid);
          stor_confs[rbs_index].status = RBS_STATUS_ERROR;	    
	  continue;	      

	default:
	  continue;
      }
    }

    /*
    ** Check whether some reloop is to be done
    */
    for (rbs_index = 0; rbs_index < nb_rbs_entry; rbs_index++) {
      if ((stor_confs[rbs_index].status != RBS_STATUS_ERROR)
      &&  (stor_confs[rbs_index].status != RBS_STATUS_SUCCESS)) break;
    }
    if (rbs_index == nb_rbs_entry) {
      /*
      ** Everything is finished 
      */
      rbs_monitor_update("completed",0,0);
      return 0;
    }
    sprintf(rebuild_status,"waiting %d min before reloop",delay);
    rbs_monitor_update(rebuild_status,0,0);
  }  	    

  rbs_monitor_update("failed",0,0);
  {
    char mystring[1024];
    char * pChar = mystring;
    
    pChar += sprintf(pChar,"\nTo get the list of files that failed to rebuild,\nplease enter :\n  storage_rebuild -id %d -list\n",
		     parameter.rebuildRef);
    pChar += sprintf(pChar,"\nTo re-attempt the rebuild of remaining files,\nplease enter :\n  storage_rebuild -id %d -resume\n",
		     parameter.rebuildRef); 
    rbs_monitor_string_append(mystring);		     
  } 		     
  return -1;
  
paused:
  REBUILD_MSG("Rebuild %d paused.",parameter.rebuildRef);
  rbs_monitor_update("paused",0,0);
  {
    char mystring[1024];
    char * pChar = mystring;
    
    pChar += sprintf(pChar,"\nTo get the list of files remaining to rebuild,\nplease enter :\n  storage_rebuild -id %d -list\n",
		     parameter.rebuildRef);
    pChar += sprintf(pChar,"\nTo re-attempt the rebuild of remaining files,\nplease enter :\n  storage_rebuild -id %d -resume\n",
		     parameter.rebuildRef); 
    rbs_monitor_string_append(mystring);     		     
  }
  /*
  ** Save elpased delay in order 
  ** to reread it on resume
  */
  save_consummed_delay();   		     
  return -1;       
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
    pChar += rozofs_string_append(pChar,"/");   
               
    if (statfs(path, &sfs) != -1) {
      if (sfs.f_bfree > max) {
        max         = sfs.f_bfree;
	choosen_dev = dev;
      }
    }
  }  
  return choosen_dev;
}

/*
**____________________________________________________

  Save command in command file
*/
void save_command() {
  char fname[1024];
  int  fd = -1;
    
  sprintf(fname,"%s/%s",get_rebuild_directory_name(parameter.rebuildRef),"command");
  
  /*
  ** Save command 
  */
  if ((fd = open(fname, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IROTH)) < 0) {
    severe("can't open %s %s", fname,strerror(errno));
    return;
  }
  
  dprintf(fd,"%s",command);
  close(fd);
  //info("COMMAND %s written",fname);
}

/*
**____________________________________________________

  read command in command file
*/
char saved_command[1024];
int preload_command(int rebuildRef, rbs_parameter_t * par) {
  char fname[1024];
  int  fd = -1;
  char * pChar;
  char *argv[32];
  int   argc;
    
  sprintf(fname,"%s/%s",get_rebuild_directory_name(rebuildRef),"command");
  
  /*
  ** Save command 
  */
  if ((fd = open(fname, O_RDONLY , S_IRWXU | S_IROTH)) < 0) {
    severe("can't open %s %s", fname,strerror(errno));
    return -1;
  }
  
  if (pread(fd, &saved_command, sizeof(saved_command), 0)<0) {
    severe("can't read %s %s", fname, strerror(errno));
    return -1;
  }

  info("preload %s",saved_command);
    
  argc       = 0;
  pChar      = saved_command;
  
  while (*pChar != 0) {
  
    while ((*pChar == ' ')||(*pChar == '\t')) *pChar++ = 0;
    if (*pChar == 0) break;

    argv[argc++] = pChar;
    pChar++;
        
    while ((*pChar != 0)&&(*pChar != ' ')&&(*pChar != '\t')) pChar++;
    if (*pChar == 0) break;    

  }
  close(fd);
  

  parse_command(argc, argv, par); 
  return 0;
}
/** Start one rebuild process for each storage to rebuild
 */
static inline int rbs_rebuild_process() {
    list_t *p = NULL;
    int found_cid_sid=0;
    int ret;
    int status = -1;
    char * dir;

    /*
    ** Create a temporary directory to receive the list files
    */  
    clean_dir(get_rebuild_directory_name(parameter.rebuildRef));
    ret = mkdir(get_rebuild_directory_name(parameter.rebuildRef),ROZOFS_ST_BINS_FILE_MODE);
    if (ret != 0) {
      severe("Can not create directory %s : %s",get_rebuild_directory_name(parameter.rebuildRef), strerror(errno));
      goto out;
    }  
    
    /*
    ** Save command in file "command" of the directory
    */
    save_command();
    
    /*
    ** Save pid
    */
    save_pid();

    memset(&rbs_stor_configs, 0,
            STORAGES_MAX_BY_STORAGE_NODE * sizeof(rbs_stor_config_t));

    DEBUG_FUNCTION;
    

    // For each storage in configuration file

    list_for_each_forward(p, &storaged_config.storages) {

      storage_config_t *sc = list_entry(p, storage_config_t, list);
	
	  /*
	  ** If a specific sid is to be rebuilt, skip the other
	  */
	  if ((parameter.cid!=-1)&&(parameter.sid!=-1)) {
		if ((parameter.cid != sc->cid) || (parameter.sid != sc->sid)) continue; 
	  }
	  found_cid_sid = 1;

	  /*
	  ** If a specific device is to be rebuilt, checkt its number is valid
	  */
	  if ((parameter.type == rbs_rebuild_type_device)&&(parameter.rbs_device_number >= sc->device.total)) {
		continue;
	  } 

      // Copy the configuration for the storage to rebuild
      strncpy(rbs_stor_configs[nb_rbs_entry].export_hostname, parameter.rbs_export_hostname,
      ROZOFS_HOSTNAME_MAX);
      rbs_stor_configs[nb_rbs_entry].cid = sc->cid;
      rbs_stor_configs[nb_rbs_entry].sid = sc->sid;
      rbs_stor_configs[nb_rbs_entry].stor_idx = nb_rbs_entry;
	  rbs_stor_configs[nb_rbs_entry].device.total      = sc->device.total;
	  rbs_stor_configs[nb_rbs_entry].device.mapper     = sc->device.mapper;
	  rbs_stor_configs[nb_rbs_entry].device.redundancy = sc->device.redundancy;
	  rbs_stor_configs[nb_rbs_entry].status            = RBS_STATUS_BUILD_JOB_LIST;

      strncpy(rbs_stor_configs[nb_rbs_entry].root, sc->root, PATH_MAX);

      rbs_monitor[nb_rbs_entry].cid        = sc->cid;
      rbs_monitor[nb_rbs_entry].sid        = sc->sid;
      rbs_monitor[nb_rbs_entry].nb_files   = 0;	
      rbs_monitor[nb_rbs_entry].done_files = 0;
      rbs_monitor[nb_rbs_entry].deleted    = 0;	
	  rbs_monitor[nb_rbs_entry].layout     = 0xFF;
	  strcpy(rbs_monitor[nb_rbs_entry].status,"to do");
      rbs_monitor[rbs_index].list_building_sec = 0;	
	  nb_rbs_entry++;

	  // Create a temporary directory to receive the list files 
	  dir = get_rebuild_sid_directory_name(parameter.rebuildRef,sc->cid,sc->sid);
	  ret = mkdir(dir,ROZOFS_ST_BINS_FILE_MODE);
	  if ((ret != 0)&&(errno!=EEXIST)) {
		severe("mkdir(%s) %s", dir, strerror(errno));
		goto out;
	  }   
    }

    if (nb_rbs_entry==0) {
      if (found_cid_sid == 0) {
        REBUILD_FAILED("No such cid/sid %d/%d.", parameter.cid, parameter.sid);          
      }
      else if (parameter.type == rbs_rebuild_type_device) {
        REBUILD_FAILED("No such device number %d.",parameter.rbs_device_number); 
      }	 
      goto out;  
    }

#if 1
	/*
	** Try to get list of FID to rebuild from export
	*/
	while (1) {

      // FID rebuild
      if (parameter.type == rbs_rebuild_type_fid) break;     

      // When rebuilding a whole storage, request list from export
      if (parameter.type == rbs_rebuild_type_storage) {
    	rbs_build_job_list_from_export();
		break;
      }   

      /*
	  ** Only one device to rebuild.
	  */

	  // Bad device number
      if (parameter.rbs_device_number >= storage_config.storage.device_number) break;

	  // This is the only device of this storio so rebuild the whole storage
      if (storage_config.storage.device_number == 1) {
	    rbs_build_job_list_from_export();
      }
	  break;           		
	}
#endif

    /*
    ** Process to the rebuild
    */    
    status = rebuild_storage_thread(rbs_stor_configs);
    rbs_monitor_display();  

   
    forget_pid();

    /*
    ** Remove temporary directory
    */
    if (status!=-1) {
      clean_dir(get_rebuild_directory_name(parameter.rebuildRef));
    } 
    
out:
    /*
    ** Purge excedent old rebuild result files
    */
    rbs_monitor_purge();
    return status;
}
/** Start one rebuild process for each storage to rebuild
 */
static inline int rbs_rebuild_resume() {
  char             * dirName;
  DIR              * dir0;
  struct dirent    * file0;
  DIR              * dir1;
  int                cid,sid;
  int                ret;
  int                status = -1;
  storage_t        * sc=NULL;
  char               fname[1024];
  
  
  memset(&rbs_stor_configs, 0,
          STORAGES_MAX_BY_STORAGE_NODE * sizeof(rbs_stor_config_t));

  
     
  /*
  ** Start one rebuild process par rebuild file
  */
  dirName = get_rebuild_directory_name(parameter.rebuildRef);
  
  /*
  ** Open this directory
  */
  dir0 = opendir(dirName);
  if (dir0 == NULL) {
    if (errno==ENOENT) {
      REBUILD_FAILED("Rebuild identifier %d does not exist any more.",parameter.rebuildRef);
    }
    else {
      severe("opendir(%s) %s", dirName, strerror(errno));
    }  
    return status;
  } 	  
  /*
  ** Loop on distribution sub directories
  */
  while ((file0 = readdir(dir0)) != NULL) {
  
    if (strcmp(file0->d_name,".")==0)  continue;
    if (strcmp(file0->d_name,"..")==0) continue;    
    
    /*
    ** Scan cid/sid
    */
    ret = sscanf(file0->d_name, "cid%d_sid%d",&cid, &sid);
    if (ret != 2) {
      //severe("Unexpected directory name %s/%s", dirName, file0->d_name);
      continue;
    }

    // For each storage in configuration file
    sc = storaged_lookup(cid,sid);
    if (sc == NULL) {
      severe("Unexpected cid/sid %d/%d", cid, sid);
      continue;
    }

    // Copy the configuration for the storage to rebuild
    strncpy(rbs_stor_configs[nb_rbs_entry].export_hostname, parameter.rbs_export_hostname, ROZOFS_HOSTNAME_MAX);
    rbs_stor_configs[nb_rbs_entry].cid = cid;
    rbs_stor_configs[nb_rbs_entry].sid = sid;
    rbs_stor_configs[nb_rbs_entry].stor_idx = nb_rbs_entry;
    
    
    rbs_stor_configs[nb_rbs_entry].device.total      = sc->device_number;
    rbs_stor_configs[nb_rbs_entry].device.mapper     = sc->mapper_modulo;
    rbs_stor_configs[nb_rbs_entry].device.redundancy = sc->mapper_redundancy;
    rbs_stor_configs[nb_rbs_entry].status            = RBS_STATUS_FAILED;
    strncpy(rbs_stor_configs[nb_rbs_entry].root, sc->root, PATH_MAX);

    rbs_monitor[nb_rbs_entry].cid        = cid;
    rbs_monitor[nb_rbs_entry].sid        = sid;
	rbs_monitor[nb_rbs_entry].nb_files   = rbs_read_file_count(parameter.rebuildRef,cid,sid); 
    rbs_monitor[nb_rbs_entry].done_files = 0;
    rbs_monitor[nb_rbs_entry].deleted    = 0;	
    rbs_monitor[nb_rbs_entry].layout     = 0xFF;
    strcpy(rbs_monitor[nb_rbs_entry].status,"to do");
        
    sprintf(fname,"%s/%s",get_rebuild_directory_name(parameter.rebuildRef),file0->d_name);
    dir1 = opendir(fname);
    if (dir1 == NULL) {
      severe("opendir(%s) %s", fname, strerror(errno));
      nb_rbs_entry++;
      continue;
    }

    /*
	** Read the storage configuration file
	*/
	if (rbs_read_storage_config_file(fname, &storage_config) == NULL) {
      severe("rbs_read_storage_config_file(%s) %s", fname, strerror(errno));
      continue;
	}
	
	rbs_monitor[nb_rbs_entry].layout = storage_config.layout;

    if (rbs_monitor[nb_rbs_entry].nb_files == 0) {
      /* No file to rebuild => the list is to be built */
      rbs_stor_configs[nb_rbs_entry].status = RBS_STATUS_BUILD_JOB_LIST;
    }    
    nb_rbs_entry++; 
    closedir(dir1);     
    
  }
  closedir(dir0);
  
  /*
  ** Save pid
  */
  save_pid();
    
  // Read previously elapsed delay 
  read_previous_delay();   
	
  /*
  ** Process to the rebuild
  */   
  status = rebuild_storage_thread(rbs_stor_configs);
  rbs_monitor_display();  
  forget_pid();
  
  /*
  ** Remove temporary directory
  */
  if (status!=-1) {
    clean_dir(get_rebuild_directory_name(parameter.rebuildRef));
  } 

  /*
  ** Purge excedent old rebuild result files
  */
  rbs_monitor_purge();
  return status;
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

    list_for_each_forward_safe(p, q, &storaged_config.storages) {

        storage_config_t *s = list_entry(p, storage_config_t, list);
        free(s);
    }
}


static void on_stop() {
    DEBUG_FUNCTION;   
    
    storaged_release();
    closelog();
    // Kill all sub-processes
    if (sigusr_received) {
      rozofs_session_leader_killer(1000000);
    }  
}
/*__________________________________________________________________________
*/
void rbs_cath_sigusr(int sig){
  sigusr_received = 1;
  signal(SIGUSR1, rbs_cath_sigusr);  
}

int main(int argc, char *argv[]) {
    int ret;
    int status = -1;
    
    /*
    ** Change local directory to "/"
    */
    if (chdir("/")!= 0) {}

    rozofs_layout_initialize();
    
    /*
    ** read common config file
    */
    common_config_read(NULL);         

    uma_dbg_record_syslog_name("RBS");
    
    // Init of the timer configuration
    rozofs_tmr_init_configuration();

    /*
    ** Check whether this is a resume command
    */
    int rebuildRef = is_command_resume(argc, argv);

    /*
    ** Read parameters
    */
    rbs_conf_init(&parameter);
    if (rebuildRef != 0) {
      /* 
      ** When resume command, pre-initialize the parameters with the 
      ** ones of the original command
      */
      if (preload_command(rebuildRef, &parameter) == -1) {
        printf("No such rebuild id %d\n",rebuildRef);	 
        exit(EXIT_FAILURE);		   
	  } 
    }
    /*
    ** Parse command parameters
    */
    parse_command(argc, argv, &parameter);

    /*
    ** Process listing of failed FID
    */
    if (parameter.list) {
      rbs_list_remaining_fid();
      exit(EXIT_SUCCESS);
    }

    /*
    ** Process listing of failed FID
    */
    if (parameter.pause) {
      quiet = 0;
      rbs_rebuild_pause();
      exit(EXIT_SUCCESS);
    }    
    /*
    ** Process listing of failed FID
    */
    if (parameter.abort) {
      quiet = 0;
      rbs_rebuild_abort();
      exit(EXIT_SUCCESS);
    }   
        
    if (parameter.resume) {
      /*
      ** Check the rebuild is not running
      */
      if (rbs_get_running_pid()!=0) {
        printf("Rebuild %d is already running\n",parameter.rebuildRef);
        exit(EXIT_FAILURE);
      }   
    } 
    
    /*
    ** Initalize the rebuild monitoring file name
    */
    {
      char * p = command;
      int i;
      
      for (i=0; i< argc; i++) {
        p += rozofs_string_append(p, argv[i]);
	*p++ = ' ';
      }
      *p = 0;	
      info("%s",command);
      rbs_status_file_name(NULL);
    }      

    /*
    ** SIGUSR handler
    */
    signal(SIGUSR1, rbs_cath_sigusr);  

    
    /*
    ** Declare the signal handler that may generate core files,
    ** and attach a crash callback 
    */
    setsid(); // Start a new session
    rozofs_signals_declare("storage_rebuild", common_config.nb_core_file);
    rozofs_attach_crash_cbk(on_crash);
        
    /*
    ** Clear errors and reinitialize disk
    */
    if (parameter.clear) {
      
      if (rbs_storio_reinit(parameter.cid, parameter.sid, parameter.rbs_device_number, 1)!=0) {
        REBUILD_MSG("Can't reset error on cid %d sid %d .",parameter.cid,parameter.sid);
        exit(EXIT_FAILURE);  
      }
      
      REBUILD_MSG("cid %d sid %d device %d re-initialization",parameter.cid,parameter.sid,parameter.rbs_device_number);
    }
       
    
    // Initialize the list of storage config
    if (sconfig_initialize(&storaged_config) != 0) {
        quiet = 0;
        REBUILD_FAILED("Can't initialize storaged config.");
        goto error;
    }
    // Read the configuration file
    if (parameter.cid == -1) {
      ret = sconfig_read(&storaged_config, parameter.storaged_config_file,0);
    }
    else {
      ret = sconfig_read(&storaged_config, parameter.storaged_config_file,parameter.cid);    
    }  
    if (ret < 0) {
        quiet = 0;
        REBUILD_FAILED("Failed to parse storage configuration file %s.",parameter.storaged_config_file);
        goto error;
    }
    // Check the configuration
    if (sconfig_validate(&storaged_config) != 0) {
        quiet = 0;
        REBUILD_FAILED("Inconsistent storage configuration file %s.", parameter.storaged_config_file);
        goto error;
    }
    // Check rebuild storage configuration if necessary
    if (rbs_check() != 0) goto error;


    // Initialization of the storage configuration
    if (storaged_initialize() != 0) {
        quiet = 0;
        REBUILD_FAILED("Can't initialize storaged.");
        goto error;
    }
    
    // Check the requested cid/sid exist
    if (parameter.cid!=-1) {
      if (storaged_lookup(parameter.cid,parameter.sid)== NULL) {
        quiet = 0;
        REBUILD_FAILED("No such cid/sid %d/%d.",parameter.cid,parameter.sid);
        goto error;
      }
    }

    syslog(EINFO,"Rebuild %d monitoring file is %s\n", parameter.rebuildRef, rbs_monitor_file_path); 
    if (!quiet) {
      printf("Check rebuild status : watch -n 10 cat %s\n", rbs_monitor_file_path); 
      printf("Abort this rebuild   : storage_rebuild -id %d -abort\n", parameter.rebuildRef); 
      printf("Pause this rebuild   : storage_rebuild -id %d -pause\n", parameter.rebuildRef); 
    }

    /*
    ** Daemonize
    */
    if (parameter.background) {
      if (daemon(0, 0)==-1) {
        severe("daemon %s",strerror(errno));        
      }
      quiet = 1;
    }
    
    // Start rebuild storage   
    if (parameter.resume) 
      status = rbs_rebuild_resume();
    else       
      status = rbs_rebuild_process();
    
    on_stop();  
    if (status == 0) exit(EXIT_SUCCESS);
    else             exit(EXIT_FAILURE);
    
error:
    REBUILD_MSG("Can't start storage_rebuild. See logs for more details.");
    exit(EXIT_FAILURE);
}
