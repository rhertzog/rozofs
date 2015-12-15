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
#include <rozofs/rozofs.h>
#include <rozofs/common/mattr.h>
#include "export.h"
#include "rozo_inode_lib.h"
#include "exp_cache.h"


lv2_cache_t cache;


/**
*  API to get the pathname of the objet: @rozofs_uuid@<FID_parent>/<child_name>

   @param export : pointer to the export structure
   @param inode_attr_p : pointer to the inode attribute
   @param buf: output buffer
   
   @retval buf: pointer to the beginning of the outbuffer
*/
static char buf_uuid[]="./@rozofs_uuid@";
static int buf_uuid_len = -1;
static int buf_dir_len = -1;
char *rozo_get_full_path(void *exportd,void *inode_p,char *buf,int lenmax)
{
   lv2_entry_t *plv2;
   char name[1024];
   char *pbuf = buf;
   int cur_len= 0;
   int name_len=0;
   int first=1;
   ext_mattr_t *inode_attr_p = inode_p;
   rozofs_inode_t *inode_val_p;
   
   pbuf +=lenmax;
   
   export_t *e= exportd;
   
   inode_val_p = (rozofs_inode_t*)inode_attr_p->s.pfid;
   if ((inode_val_p->fid[0]==0) && (inode_val_p->fid[1]==0))
   {
      pbuf-=2;
      pbuf[0]='.';   
      pbuf[1]=0;      
   } 
   
   buf[0] = 0;
   first = 1;
   while(1)
   {
      /*
      ** get the name of the directory
      */
      name[0]=0;
      get_fname(e,name,&inode_attr_p->s.fname,inode_attr_p->s.pfid);
      name_len = strlen(name);
      if (name_len == 0) break;
      if (first == 1) {
	name_len+=1;
	first=0;
      }
      pbuf -=name_len;
      memcpy(pbuf,name,name_len);
      pbuf--;
      *pbuf='/';

      if (memcmp(e->rfid,inode_attr_p->s.pfid,sizeof(fid_t))== 0)
      {
	 /*
	 ** this the root
	 */
	 pbuf--;
	 *pbuf='.';
	 return pbuf;
      }
      /*
      ** get the attributes of the parent
      */
      if (!(plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,&cache, inode_attr_p->s.pfid))) {
	break;
      }  
      inode_attr_p=  &plv2->attributes;
    }

    return pbuf;
}

/**
*   RozoFS specific function for visiting

   @param inode_attr_p: pointer to the inode data
   @param exportd : pointer to exporthd data structure
   @param p: always NULL
   
   @retval 0 no match
   @retval 1 match
*/
char rzofs_path_bid[]="rozofs";
int rozofs_visit(void *exportd,void *inode_attr_p,void *p)
{
   char name[4096];
   int ret= 0;
   char *pbuf;
   ext_mattr_t *inode_p = inode_attr_p;
   lv2_entry_t *plv2;
   export_t *e= exportd;   
   
//  if ((inode_p->s.attrs.sids[0] == 1) || (inode_p->s.attrs.sids[0] == 2))
  {
    /*
    ** Get the pathname of the object
    */
//    rozo_get_parent_child_path(exportd,inode_attr_p,name,4096);
    pbuf = rozo_get_full_path(exportd,inode_attr_p,name,4096);
    /* Apply the predicates to this path. */
    printf("%s\n",pbuf);
    ret = 1;
  }
  return ret;
}


/*
 *_______________________________________________________________________
 */
static void usage() {
    printf("Usage: ./rzsave [OPTIONS]\n\n");
    printf("\t-h, --help\tprint this message.\n");
    printf("\t-p,--path <export_root_path>\t\texportd root path \n");

};




int main(int argc, char *argv[]) {
    int c;
    int read_attr_flag = 0;
    int ret;
    void *rozofs_export_p;
    char *root_path=NULL;
    
    static struct option long_options[] = {
        {"help", no_argument, 0, 'h'},
        {"path", required_argument, 0, 'p'},
        {0, 0, 0, 0}
    };
    
  
    while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "hlrc:p:", long_options, &option_index);

      if (c == -1)
          break;

      switch (c) {

          case 'h':
              usage();
              exit(EXIT_SUCCESS);
              break;
          case 'p':
              root_path = optarg;
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
  if (root_path == NULL) 
  {
       usage();
       exit(EXIT_FAILURE);  
  }

  /*
  ** init of the RozoFS data structure on export
  ** in order to permit the scanning of the exportd
  */
  rozofs_export_p = rz_inode_lib_init(root_path);
  if (rozofs_export_p == NULL)
  {
    printf("RozoFS: error while reading %s\n",root_path);
    exit(EXIT_FAILURE);  
  }
  /*
  ** init of the lv2 cache
  */
  lv2_cache_initialize(&cache);
  
  ret = rz_scan_all_inodes(rozofs_export_p,ROZOFS_DIR,1,rozofs_visit,NULL,NULL,NULL);


}
