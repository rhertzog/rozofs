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
#include <unistd.h>
#include <libintl.h>
#include <libconfig.h>
#include <getopt.h>
#include <inttypes.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/vfs.h>
#include <malloc.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdarg.h>
#include <mntent.h>
#include <sys/mount.h>
#include <attr/xattr.h>

#include <rozofs/rozofs_srv.h>
#include <rozofs/common/log.h>
#include <rozofs/common/xmalloc.h>
#include <rozofs/common/list.h>
#include <rozofs/common/common_config.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/core/rozofs_core_files.h>

#include "rozofs_mover.h"

/*
** The name of the utility to display on help
*/
char * utility_name=NULL;


/*-----------------------------------------------------------------------------
**
**  MAIN file mover
**
**----------------------------------------------------------------------------
*/
int main(int argc, char *argv[]) {
  list_t jobs;     
  int    i;
  char   name[128];

  /*
  ** Get utility name and record it for syslog
  */
  utility_name = basename(argv[0]);   
  uma_dbg_record_syslog_name(utility_name);

  /*
  ** Set a signal handler
  */
  rozofs_signals_declare(utility_name, 1); 
  
  rozofs_mover_init();
  
  list_init(&jobs);
  
  for (i=1;i<11; i++) {
    rozofs_mover_job_t * job;
    
    job = malloc(sizeof(rozofs_mover_job_t));
    memset(job,0,sizeof(rozofs_mover_job_t));
    
    job->cid    = 2;
    job->sid[0] = 8;
    job->sid[1] = 7;
    job->sid[2] = 3;
    job->sid[3] = 4;
    job->sid[4] = 5;
    job->sid[5] = 6;
    job->sid[6] = 1;
    job->sid[7] = 2;
    
    sprintf(name,"@rozofs_uuid@00000000-0000-4000-0000-000000000018/f%d",i);
    job->name = strdup(name);
    
    list_init(&job->list);
    list_push_back(&jobs,&job->list);    
  }

  rozofs_do_move_one_export("localhost", 
                            "/home/jean-pierre/rozoClients/develop/tests/SIMU/export_1", 
			    10 /* 10 MB/s */,
			    &jobs);
			    
  /*
  ** Display statistics
  */
  rozofs_mover_print_stat(uma_dbg_get_buffer());
  printf("%s",uma_dbg_get_buffer()); 
  return 0;
}
