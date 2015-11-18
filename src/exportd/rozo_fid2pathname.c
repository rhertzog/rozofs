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

/*
 *_______________________________________________________________________
 */
/**
*  API to get the full pathname of the objet

   @param export : pointer to the export structure
   @param inode_attr_p : pointer to the inode attribute
   @param buf: output buffer
   
   @retval buf: pointer to the beginning of the outbuffer
*/

char *rozo_get_full_path(void *exportd,void *inode_p,char *buf,int lenmax)
{
   lv2_entry_t *plv2;
   char name[1024];
   char *pbuf = buf;
   int name_len=0;
   int first=1;
   char buf_fid[64];

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
      get_fname((void*)e,name,(void*)&inode_attr_p->s.fname,inode_attr_p->s.pfid);
      name_len = strlen(name);
      if (name_len == 0) 
      {
         /*
	 ** put the value of the i-node
	 */
	 uuid_unparse(inode_attr_p->s.attrs.fid,buf_fid);
	 printf(name,"@rozofs_uuid@%s",buf_fid); 	 
         name_len = strlen(name);      
      }
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

/*
 *_______________________________________________________________________
 */
 /**
*   That function is intended to return the relative path to an object:
    @rozofs_uuid@<FID_parent>/<child_name>
    example:
    @rozofs_uuid@1b4e28ba-2fa1-11d2-883f-0016d3cca427
    
    @param exportd: pointer to exportd data structure
    @param inode_p: pointer to the inode
    @param buf : pointer to the output buffer
    @param lenmax: max length of the output buffer
*/
char *rozo_get_relative_path(void *exportd,void *inode_p,char *buf,int lenmax)
{
   char name[1024];
   char *pbuf = buf;
   char buf_fid[64];
   ext_mattr_t *inode_attr_p = inode_p;
   rozofs_inode_t *inode_val_p;
   
   
   export_t *e= exportd;
   
   inode_val_p = (rozofs_inode_t*)inode_attr_p->s.pfid;
   if ((inode_val_p->fid[0]==0) && (inode_val_p->fid[1]==0))
   {
      pbuf += sprintf(pbuf,"./"); 
   } 
   else
   {
     uuid_unparse(inode_attr_p->s.pfid,buf_fid);
     pbuf += sprintf(pbuf,"./@rozofs_uuid@%s/",buf_fid); 
   } 
   /*
   ** get the object name
   */
   name[0] = 0;     
   get_fname(e,name,&inode_attr_p->s.fname,inode_attr_p->s.pfid);
   if (name[0]== 0)
   {
     uuid_unparse(inode_attr_p->s.attrs.fid,buf_fid);
     pbuf += sprintf(pbuf,"@rozofs_uuid@%s",buf_fid);    
   
   }
   else
   {
     pbuf += sprintf(pbuf,"%s",name);       
   }
   return buf;
}

/*
 *_______________________________________________________________________
 */
static void usage() {
    printf("\nUsage: rozo_fid2pathname [-r] -p export_root_path -i <input_filename | fid> [-o output_filename] \n\n");
    printf("Options:\n");
    printf("\t-h,--help:     print this message.\n");
    printf("\t-p,--path:     exportd root pathname \n");
    printf("\t-r,--relative: when asserted the output is generated with @rozofs_uuid@<FID_parent>/<child_name> format  \n");
    printf("\t-i,--input:    fid of the objet or input filename \n");
    printf("\t               that contains a list of fid in the 1b4e28ba-2fa1-11d2-883f-0016d3cca427 format \n");
    printf("\t-o,--output:   output filename for full path translation (optional) \n\n");

};




int main(int argc, char *argv[]) {
    int c;
    void *rozofs_export_p;
    char *root_path=NULL;
    char *input_path=NULL;
    char *output_path=NULL;
    fid_t fid;
    int one_fid_only = 0;
    lv2_entry_t *plv2;
    int res;
    int count;
    int failures;
    FILE *fd_in = NULL;
    FILE *fd_out= NULL;
    char name[4096];
    char *pbuf;  
    char fid_buf[64]; 
    export_t *e;    
    int relative= 0;
      
    static struct option long_options[] = {
        {"help", no_argument, 0, 'h'},
        {"path", required_argument, 0, 'p'},
        {"input", required_argument, 0, 'i'},
        {"output", required_argument, 0, 'o'},
        {"relative", no_argument, 0, 'r'},
        {0, 0, 0, 0}
    };
    
  
    while (1) {

      int option_index = 0;
      c = getopt_long(argc, argv, "hlrc:p:i:o:r", long_options, &option_index);

      if (c == -1)
          break;

      switch (c) {

          case 'h':
              usage();
              exit(EXIT_SUCCESS);
              break;
          case 'r':
              relative = 1;
              break;
          case 'p':
              root_path = optarg;
              break;
          case 'i':
              input_path = optarg;
              break;
          case 'o':
              output_path = optarg;
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
  if (input_path == NULL) 
  {
       printf("Neither an valid input file nor valid FID\n");
       usage();
       exit(EXIT_FAILURE);  
  }
  /*
  ** check if the input is a FID or a filename
  */
  if (rozofs_uuid_parse(input_path, fid)<0) 
  {
    /*
    **  check the presence of the input file
    */    
    if ((fd_in = fopen(input_path,"r")) < 0)
    {
       printf("not a valid file: %s: %s\n",input_path,strerror(errno));
       usage();
       exit(EXIT_FAILURE);      
    }      
  }
  else
  {
    one_fid_only = 1;
  }
  /*
  ** Check for ouput file
  */
  if (output_path != NULL)
  {
    if ((fd_out = fopen(output_path,"w")) < 0)
    {
       printf("not a valid file: %s: %s\n",input_path,strerror(errno));
       usage();
       exit(EXIT_FAILURE);      
    }       
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
  failures = 0;
  count = 0;
  e =  rozofs_export_p;
  res = 1;

  if (one_fid_only == 0) res= fscanf(fd_in,"%s",fid_buf);
  while ((res != EOF) || (one_fid_only))
  {
      /*
      ** Parse the fid
      */
     count++;
     while (1)
     {
       if (one_fid_only == 0)
       {
	 if (rozofs_uuid_parse(fid_buf, fid)<0) 
	 {
            if (fd_out==NULL) {
	      printf("%s : cannot be translated\n",fid_buf);
	    }
	    else
	    {
	      fprintf(fd_out,"%s : cannot be translated\n",fid_buf);	
	    }
	    failures +=1;
	    break;
	  }
	}
	/*
	** Get the attributes of the file
	*/
	if (!(plv2 = EXPORT_LOOKUP_FID(e->trk_tb_p,&cache, fid))) {
	  if (fd_out) fprintf(fd_out,"@rozofs_uuid@%s : cannot be translated\n",fid_buf);	
	  else  printf("@rozofs_uuid@%s : cannot be translated\n",fid_buf);
	  failures +=1;
	  break;
        }
	/*
	** Get the full pathname
	*/
        if (relative)
	  pbuf = rozo_get_relative_path(rozofs_export_p,&plv2->attributes,name,4096);
	else
          pbuf = rozo_get_full_path(rozofs_export_p,&plv2->attributes,name,4096);
        if (fd_out==NULL) {
          printf("%s\n",pbuf);
	}
	else
	{
	  fprintf(fd_out,"%s\n",pbuf);	
	}
	break;
     }  
     if (one_fid_only) break;
     res= fscanf(fd_in,"%s",fid_buf);  
  }
  printf("Number of scanned/failed FID: %d/%d\n",count,failures);
  if (fd_out) fclose(fd_out);
  if (fd_in) fclose(fd_in);
  
  return 0;
}
