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
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <inttypes.h>
#include <rozofs/core/uma_dbg_api.h>
#include <semaphore.h>
#include <pthread.h>
#include <rozofs/common/log.h>

#define TRC_MAX_ENTRY_SZ  80

typedef struct _trc_entry_t
{
    uint64_t ts;
    char     entry[TRC_MAX_ENTRY_SZ];
}trc_entry_t;

int trc_buf_sz;
int trc_buf_idx;
int trc_buf_enable =1;
pthread_rwlock_t trc_buf_lock;

trc_entry_t *trc_buf_p = NULL;

/*__________________________________________________________________________
*/
/**
*   Show the status od the fuse trace service

    @param pChar : pointer to the result buffer
    @retval none
*/
void trc_buf_status(char *pChar)
{
   pChar+=sprintf(pChar,"trace status      :%s \n",(trc_buf_enable==0)?"Disabled":"Enabled");
   pChar+=sprintf(pChar,"trace buffer size :%d entries \n",(trc_buf_p==NULL)?0:trc_buf_sz);

}

/*
**________________________________________________________
*/
void trc_buf_insert(char *data)
{
  if (trc_buf_p== NULL) return;
  if (trc_buf_enable == 0) return;
  int size = strlen(data);

//  severe("trace insert");  
  if (size > (TRC_MAX_ENTRY_SZ-1)) 
  {
     size = TRC_MAX_ENTRY_SZ-1;
  }
  pthread_rwlock_wrlock(&trc_buf_lock);
  
  memcpy(trc_buf_p[trc_buf_idx].entry,data,size);
  trc_buf_p[trc_buf_idx].entry[size] = 0;
  trc_buf_p[trc_buf_idx].ts = 1;
  trc_buf_idx = (trc_buf_idx+1)%trc_buf_sz;

  pthread_rwlock_unlock(&trc_buf_lock);

}
/*
**________________________________________________________
*/
void trc_buf_reset()
{
  if (trc_buf_p!= NULL) 
  {
   memset(trc_buf_p,0,sizeof(trc_entry_t)*trc_buf_sz);
   trc_buf_idx = 0;    
  }

}
/*
**________________________________________________________
*/
void show_trc_buf(char *pChar)
{

  if (trc_buf_p== NULL) 
  {
     sprintf(pChar,"No trace buffer\n");
     return;
  }
  sprintf(pChar,"Empty\n");
  int i;
  int j = trc_buf_sz;
  for (i = trc_buf_idx; j != 0; i=(i+1)%trc_buf_sz,j--)
  {
    if (trc_buf_p[i].ts==0) continue;
    pChar +=sprintf(pChar,"%4.4d - %s\n",i, trc_buf_p[i].entry);
  
  }
}

/*
**________________________________________________________
*/


static char * show_trc_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"trc reset         : reset trace buffer\n");
  pChar += sprintf(pChar,"trc enable        : enable trace mode\n");  
  pChar += sprintf(pChar,"trc disable       : disable trace mode\n");  
  pChar += sprintf(pChar,"trc status        : current status of the trace buffer\n");  
  pChar += sprintf(pChar,"trc count <count> : allocate a trace buffer with <count> entries\n");  
  pChar += sprintf(pChar,"trc               : display trace buffer\n");  
  return pChar; 
}  


void show_trc(char * argv[], uint32_t tcpRef, void *bufRef) {
    char *pChar = uma_dbg_get_buffer();
    int   new_val;   
     
    if (argv[1] != NULL)
    {
        if (strcmp(argv[1],"reset")==0) {
	  trc_buf_reset();
	  uma_dbg_send(tcpRef, bufRef, TRUE, "Reset Done\n");    
	  return;
	}
        if (strcmp(argv[1],"enable")==0) {
	  if (trc_buf_enable != 1)
	  {
            trc_buf_enable= 1;
            trc_buf_reset();
            uma_dbg_send(tcpRef, bufRef, TRUE, " trace is now enabled\n");    
	  }
	  else
	  {
            uma_dbg_send(tcpRef, bufRef, TRUE, " trace is already enabled\n");    
	  }
	  return;
	}  
        if (strcmp(argv[1],"status")==0) {
	  trc_buf_status(pChar);
	  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	  return;
	}  

        if (strcmp(argv[1],"disable")==0) {
	  if (trc_buf_enable != 0)
	  {
            trc_buf_enable=0;
            uma_dbg_send(tcpRef, bufRef, TRUE, " trace is now disabled\n");    
	  }
	  else
	  {
            uma_dbg_send(tcpRef, bufRef, TRUE, " trace is already disabled\n");    
	  }
	  return;
        }
	pChar = show_trc_help(pChar);
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	return;   	
    }
    show_trc_buf(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
 }


/*
**________________________________________________________
*/
void trc_buf_init(int count)
{

   trc_buf_sz = count;
   trc_buf_idx = 0;
   
    pthread_rwlock_init(&trc_buf_lock, NULL);

   trc_buf_p = malloc(sizeof(trc_entry_t)*count);
   memset(trc_buf_p,0,sizeof(trc_entry_t)*count);
   uma_dbg_addTopic_option("trace",show_trc,UMA_DBG_OPTION_RESET);
}
