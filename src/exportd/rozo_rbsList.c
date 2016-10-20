#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <uuid/uuid.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <getopt.h>
#include <sys/types.h>
#include <dirent.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/mattr.h>
#include <libconfig.h>
#include "export.h"
#include "rozo_inode_lib.h"
#include "exp_cache.h"
#include "econfig.h"
#include "rozofs/common/log.h"

int rozofs_no_site_file;

char * pDir = NULL;
char working_dir[ROZOFS_FILENAME_MAX];
  
econfig_t exportd_config;

int    parallel=0;
char * configFileName = EXPORTD_DEFAULT_CONFIG;
int    rebuildRef=0;

typedef struct _sid_info_t {  
  // Count of FID that match
  uint64_t     count;
  // Next job file to write
  int          fd_idx;
  // One file descriptor opened per job file
  int          fd[MAXIMUM_PARALLEL_REBUILD_PER_SID];
} sid_info_t;

typedef sid_info_t * sid_tbl_t[SID_MAX];
sid_tbl_t          * cid_tbl[ROZOFS_CLUSTERS_MAX] = {0};

lv2_cache_t  cache;
int rozofs_fwd  = -1;
int rozofs_safe = -1;

int     nb_requested_vid = 0;
uint8_t requested_vid[256] = {0};

int debug = 0;

uint64_t total=0;
int      entry_size;

#define TRACE if (debug) info

/*
**_______________________________________________________________________
*/
/**
*   Empty and remove a directory
*
* @param dirname: Name of the directory to cleanup
*/
void clean_dir(char * name) {
  DIR           *dir;
  struct dirent *file;
  char           fname[256];
  struct stat    st;

  
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
  ** Loop on distibrution sub directories
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
**_______________________________________________________________________
*/
/**
*   RozoFS specific function for visiting

   @param inode_attr_p: pointer to the inode data
   @param exportd : pointer to exporthd data structure
   @param p: always NULL
   
   @retval 0 no match
   @retval 1 match
*/

int rozofs_visit(void *exportd,void *inode_attr_p,void *p) {
  int i;
  ext_mattr_t *inode_p = inode_attr_p;
  int cid;
  int sid;
  int idx;
  sid_tbl_t   * pCid;
  sid_info_t  * pSid;
  char          name[64];

  if (debug) {
     i = 0;
     if (inode_p->s.fname.name_type == ROZOFS_FNAME_TYPE_DIRECT) {
       for (i=0;i<inode_p->s.fname.len;i++) name[i] = inode_p->s.fname.name[i];
     }
     name[i] = 0;	
  } 

  /*
  ** Check whether this CID/SID is interresting
  */
  cid = inode_p->s.attrs.cid-1;
  pCid = cid_tbl[cid];
  if (pCid == NULL) return 0;

  if (rozofs_fwd < 0) {
    /*
    ** compute the layout on the first file
    */
    rozofs_safe = 0;
    for (i=0; i < ROZOFS_SAFE_MAX; i++,rozofs_safe++) {
      if (inode_p->s.attrs.sids[i]==0) break;
    }
    switch (rozofs_safe) {
      case 4:
	rozofs_fwd = 3;
	break;
      case 8:
	rozofs_fwd = 6;
	break;
      case 16:
	rozofs_fwd = 12;
	break;
      default:
	exit(-1);
    }
    entry_size =  sizeof(rozofs_rebuild_entry_file_t) - ROZOFS_SAFE_MAX + rozofs_safe;
  }	

  /*
  ** check for sid
  */
  for (i = 0; i < rozofs_safe; i++) {
    rozofs_rebuild_entry_file_t entry;
    int                         j;

    sid = inode_p->s.attrs.sids[i]-1;
    pSid = (*pCid)[sid];
    if (pSid == NULL) continue;

    memset(&entry,0,sizeof(entry));
    memcpy(entry.fid,inode_p->s.attrs.fid,sizeof(fid_t));
    entry.todo      = 1;
    entry.block_end = -1;
    for (j=0; j<rozofs_safe;j++) {
      entry.dist_set_current[j] = inode_p->s.attrs.sids[j];
    }

    idx = pSid->fd_idx;	
    if (write(pSid->fd[idx],&entry,entry_size) < entry_size) {
      severe("write(cid %d sid %d job %d) %s\n", cid+1, sid+1, idx, strerror(errno));
    }
    else {
      TRACE("-> added size %d to cid %d sid %d job %d : %s\n",entry_size, cid+1,sid+1,idx,name);
    }  
    idx++;
    if (idx>=parallel) idx = 0;
    pSid->fd_idx = idx;
    pSid->count++;
    total++;
  }  
  return 0;
}
/*
**_______________________________________________________________________
*/
/** Find out which volume hods a given cid/sid
*   
    @param cid
    @param sid
    
    @retval -1 on error
*/
int get_cid_sid_volume(cid_t cid, sid_t sid) {
  list_t * v, * c, * s;

  list_for_each_forward(v, &exportd_config.volumes) {

    volume_config_t * volume = list_entry(v, volume_config_t, list);

    list_for_each_forward(c, &volume->clusters) {
	
      cluster_config_t *cluster = list_entry(c, cluster_config_t, list);

      if (cluster->cid != cid) continue;

      list_for_each_forward(s, (&cluster->storages[0])) {
	  
        storage_node_config_t *storage = list_entry(s, storage_node_config_t, list);

        if (storage->sid == sid) return volume->vid;
      }
    }
  }
  return -1;
}
/*
**_______________________________________________________________________
*/
/**
*   
    @param line: inout line to parse
    @param p: pointer to the cid/sid list represenation in memory
    @param buffer : pointer to the error buffer
    
    @retval 0  on success
    @retval -1 on error
*/
int parse_cidsid_list(char *line,int rebuildRef, int parallel)
{
  int ret;
  char * pch;
  int cid = -1;
  int sid = -1;
  int i;
  char fName[ROZOFS_FILENAME_MAX];
  sid_tbl_t      * pCid;
  sid_info_t     * pSid;
  int              vid;

  pch = line;
  while (*pch != 0) {
    /*
    ** convert to integer
    */
    ret = sscanf(pch, "%d:%d", &cid, &sid); 
    if (ret!=2) {
      severe("Bad conversion for cid:sid %s\n",pch);
      return -1;
    }    
    while ((cid > 0) && (cid <= ROZOFS_CLUSTERS_MAX) && 
	    (sid > 0) && (sid <= SID_MAX)) 
    {

      /*
      ** Find out the volume tha hold this cid/sid
      */
      vid = get_cid_sid_volume(cid,sid);
      if (vid < 0) {
	    severe("cid/sid %d/%d not found in configuration file\n", cid, sid);
	    break;
      }
      // Is this volume already recorded	
      for (i=0; i < nb_requested_vid; i++) {
	if (requested_vid[i] == vid) break;
      }
      if (i==nb_requested_vid) {
	// record it
	requested_vid[nb_requested_vid++] = vid;
      }

      cid--;
      sid--;

      pCid = cid_tbl[cid];
      if (pCid == NULL) {
    	pCid = xmalloc(sizeof(sid_tbl_t));
	memset(pCid,0,sizeof(sid_tbl_t));
	cid_tbl[cid] = pCid;
      }
	  
      pSid = (*pCid)[sid];
      if (pSid == NULL) {
	pSid = xmalloc(sizeof(sid_info_t));
	memset(pSid,0,sizeof(sid_info_t));
	(*pCid)[sid] = pSid;
      }

      // Create directory
      sprintf(fName,"%scid%d_sid%d",pDir,cid+1,sid+1);
      if (mkdir(fName,766)<0) {
        severe("mkdir(%s) %s\n",fName,strerror(errno));
      }  

      for (i=0; i< parallel; i++) {
	sprintf(fName,"%scid%d_sid%d/job%d",pDir,cid+1,sid+1,i);
	pSid->fd[i] = open(fName, O_CREAT | O_TRUNC | O_APPEND | O_WRONLY,0755);
	if (pSid->fd[i] == -1) {
	  severe("open(%s) %s\n",fName,strerror(errno));
	}
	TRACE("%s opened\n",fName);
      }
      break;		
    }

    while((*pch!=',')&&(*pch!=0)) pch++;
    if(*pch==',') pch++;
  }
  return 0;   
}
/*
**_______________________________________________________________________
*/
/** Close every job file
*   
*/
void close_all() {
  int              cid, sid, job;
  sid_tbl_t      * pCid;
  sid_info_t     * pSid;
  char             fname[ROZOFS_FILENAME_MAX];
  int              fd;
    
  for (cid=0; cid<ROZOFS_CLUSTERS_MAX; cid++) {
  
    pCid = cid_tbl[cid];
    if (pCid == NULL) continue;

    for (sid=0; sid<SID_MAX; sid++) {
	
      pSid = (*pCid)[sid];
      if (pSid == NULL) continue;

      for (job=0; job<parallel; job++) {
	if (pSid->fd[job]>0) {
	  close(pSid->fd[job]);
	  pSid->fd[job] = 0;
	}
      }

      sprintf(fname,"%s/cid%d_sid%d/count",pDir,cid+1,sid+1);
      fd = open(fname, O_WRONLY | O_APPEND | O_CREAT | O_TRUNC,0755);
      if (fd<0) {
        severe("open(%s) %s\n",fname,strerror(errno));
	continue;
      }	
      if (write(fd,&(pSid->count), sizeof(uint64_t)) != sizeof(uint64_t)) {
        severe("write(%d,%s) %s\n",fd,fname,strerror(errno));	  
      }
      close(fd);
      
      sprintf(fname,"%s/cid%d_sid%d",pDir,cid+1,sid+1);
      info("cid/sid %d/%d : %llu files in %s",
            cid+1,
	    sid+1,
	    (unsigned long long)pSid->count,
	    fname);
    }
  }
}
/*
 *_______________________________________________________________________
 */
static void usage() {
    printf("Usage: ./rzsave [OPTIONS]\n\n");
    printf("\t-h,--help                    print this message.\n\n");
    printf("\t-i,--input      <cid/sid>    mandatory list <cid>:<sid>,<sid>,<sid>.... \n");
    printf("\t-p,--parallel   <parallel>   mandatory rebuild parallelism number.\n");
    printf("\t-r,--rebuildRef <rebuildRef> mandatory rebuild reference\n");
    printf("\t-E,--expDir     <cfgFile>    optional result directory.\n");
    printf("\t-c,--config     <cfgFile>    optional configuration file name.\n");
    printf("\t-d,--debug                   display debugging information.\n");

};




int main(int argc, char *argv[]) {
  int c;
  void *rozofs_export_p;
  char *cidsid_p= NULL;
  int   i;



  int ret;

  static struct option long_options[] = {
      {"help", no_argument, 0, 'h'},
      {"input", required_argument, 0, 'i'},
      {"parallel", required_argument, 0, 'p'},
      {"rebuildRef", required_argument, 0, 'r'},
      {"expDir", required_argument, 0, 'E'},      
      {"config", required_argument, 0, 'c'},
      {"debug", no_argument, 0, 'd'},
      {0, 0, 0, 0}
  };

  openlog("rozo_rbsList", LOG_PID, LOG_DAEMON);

  while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "hdv:i:p:r:c:E:", long_options, &option_index);

      if (c == -1)
          break;

      switch (c) {

        case 'h':
          usage();
          exit(EXIT_SUCCESS);
          break;
		  
        case 'c':
          configFileName = optarg;
          break;	
		  
        case 'E':
          pDir = optarg;
          break;	
		  		  
        case 'p':
          if (sscanf(optarg,"%d",&parallel)!= 1) {
		    severe("Bad -p value %s\n",optarg);	  
            usage();
            exit(EXIT_FAILURE);			  
		  }
          break;
		  		  
        case 'd':
          debug = 1;
          break;		  			  			  

        case 'r':
          if (sscanf(optarg,"%d",&rebuildRef)!= 1) {
		    severe("Bad -r value %s\n",optarg);	  		  
            usage();
            exit(EXIT_FAILURE);			  
		  }
          break;
		  			  			  
        case 'i':
	      if (cidsid_p!= NULL) {
	        severe("cid/sid list already defined : %s\n",cidsid_p);
        	usage();
        	exit(EXIT_FAILURE);	        

	      }
          cidsid_p = optarg;
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

  if (cidsid_p == NULL) 
  {
       severe("cid/sid list missing!!\n");
       usage();
       exit(EXIT_FAILURE);  
  }  
  if (rebuildRef == 0)
  {
       severe("rebuildRef missing!!\n");
       usage();
       exit(EXIT_FAILURE);  
  } 
  if (parallel == 0)
  {
       severe("parallel missing!!\n");
       usage();
       exit(EXIT_FAILURE);  
  }        
  /*
  ** Read configuration file
  */
  if (econfig_initialize(&exportd_config) != 0) {
       severe("can't initialize exportd config %s.\n",strerror(errno));
       exit(EXIT_FAILURE);  
  }    
  if (econfig_read(&exportd_config, configFileName) != 0) {
        severe("failed to parse configuration file %s %s.\n",
            configFileName,strerror(errno));
       exit(EXIT_FAILURE);  
  }   
  if (econfig_validate(&exportd_config) != 0) {
       severe("inconsistent configuration file %s %s.\n",
            configFileName, strerror(errno));
       exit(EXIT_FAILURE);  
  } 
  
  /*
  ** Create a clean directory
  */
  if (pDir == NULL) {
    sprintf(working_dir,"/tmp/rebuild.%d/",rebuildRef);
    pDir = working_dir;
  }
  else {
    sprintf(working_dir,"%s/rebuild.%d/",pDir,rebuildRef);
    pDir = working_dir;
  }
  clean_dir(pDir);
  if (mkdir(pDir,766)<0) {
    severe("mkdir(%s) %s\n",pDir,strerror(errno));
    exit(EXIT_FAILURE); 	
  }  
  
  /*
  ** Parse cdi:sid list
  */
  ret = parse_cidsid_list(cidsid_p, rebuildRef, parallel);
  if (ret < 0) {
     severe("erreur while parsing cid/sid list\n");
     usage();
     exit(EXIT_FAILURE);       
  }
  /*
  ** Loop on export
  */
  export_config_t * econfig;
  list_t          * p;

  list_for_each_forward(p, &exportd_config.exports) {

    econfig = list_entry(p, export_config_t, list);

    TRACE("export %d in volume %d path %s\n", econfig->eid, econfig->vid, econfig->root);
	
    // Check whether the volume of this export is interresting
    for (i=0; i < nb_requested_vid; i++) {
      if (requested_vid[i] == econfig->vid) break;
    }
    // No interest
    if (i==nb_requested_vid) continue;

    // Interresting
    rozofs_export_p = rz_inode_lib_init(econfig->root);
    if (rozofs_export_p == NULL) {
      severe("RozoFS: error while reading %s\n",econfig->root);
      continue;  
    }

    /*
    ** init of the lv2 cache
    */
    lv2_cache_initialize(&cache);   
    rozofs_fwd = -1;
    rz_scan_all_inodes(rozofs_export_p,ROZOFS_REG,1,rozofs_visit,NULL,NULL,NULL);	
  }
  
  close_all();
  econfig_release(&exportd_config);
  exit(0);
}
