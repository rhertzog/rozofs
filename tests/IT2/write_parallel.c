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
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <inttypes.h>
#include <errno.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <ctype.h>
#include <sys/wait.h>
#include <pthread.h> 

#define DEFAULT_FILENAME    "mnt1_1/this_is_the_default_rw_2_proc_test_file_name"

#define BUFFER_SIZE   (256*1024)

#define BLKSIZE   26
#define LOOP      3000
#define NBWRITER    23
char block[BLKSIZE];
char blockread[BLKSIZE];

#define HEXDUMP_COLS 16
void hexdump(void *mem, unsigned int offset, unsigned int len)
{
        unsigned int i, j;
        
        for(i = 0; i < len + ((len % HEXDUMP_COLS) ? (HEXDUMP_COLS - len % HEXDUMP_COLS) : 0); i++)
        {
                /* print offset */
                if(i % HEXDUMP_COLS == 0)
                {
                        printf("0x%06x: ", i+offset);
                }
 
                /* print hex data */
                if(i < len)
                {
                        printf("%02x ", 0xFF & ((char*)mem)[i+offset]);
                }
                else /* end of block, just aligning for ASCII dump */
                {
                        printf("   ");
                }
                
                /* print ASCII dump */
                if(i % HEXDUMP_COLS == (HEXDUMP_COLS - 1))
                {
                        for(j = i - (HEXDUMP_COLS - 1); j <= i; j++)
                        {
                                if(j >= len) /* end of block, not really printing */
                                {
                                        putchar(' ');
                                }
                                else if(isprint(((char*)mem)[j+offset])) /* printable char */
                                {
                                        putchar(0xFF & ((char*)mem)[j+offset]);        
                                }
                                else /* other char */
                                {
                                        putchar('.');
                                }
                        }
                        putchar('\n');
                }
        }
}


char FILENAME[500];
uint32_t loopCount;


static void usage() {
    printf("Parameters:\n");
    printf("[ -file <name> ]           file to do the test on (default %s)\n", DEFAULT_FILENAME);
    exit(-100);
}

static void read_parameters(argc, argv)
int argc;
char *argv[];
{
    unsigned int idx;
    int ret;
    int val;

    strcpy(FILENAME, DEFAULT_FILENAME);

    idx = 1;
    while (idx < argc) {

        /* -file <name> */
        if (strcmp(argv[idx], "-file") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx-1]);
                usage();
            }
            ret = sscanf(argv[idx], "%s", FILENAME);
            if (ret != 1) {
                printf("%s option but bad value \"%s\"!!!\n", argv[idx-1], argv[idx]);
                usage();
            }
            idx++;
            continue;
        }
        /* -loop <loop count> */
        if (strcmp(argv[idx], "-loop") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx-1]);
                usage();
            }
            ret = sscanf(argv[idx], "%d", &loopCount);
            if (ret != 1) {
                printf("%s option but bad value \"%s\"!!!\n", argv[idx-1], argv[idx]);
                usage();
            }
            idx++;
            continue;
        }			
        printf("Unexpected parameter %s\n", argv[idx]);
        usage();
    }
}

typedef struct _test_ctx_t {
  uint32_t   start;
  uint32_t   result;
} test_ctx_t;



void * loop_writer_thread(void * ctx) {
  int      fd;
  uint64_t size;
  uint32_t idx;
  off_t offset;
  test_ctx_t * pCtx = (test_ctx_t *) ctx;

  fd = open(FILENAME, O_RDWR);
  if (fd == -1) {      
      printf("READER - ERROR !!! open(%s) %s\n",FILENAME,strerror(errno));
      pCtx->result = -1;      
      return NULL;
  }
     
  
  for (idx=0; idx<LOOP; idx++) {
  
    offset = BLKSIZE*(pCtx->start+(NBWRITER*idx)); 
        
    size = pwrite(fd, block, BLKSIZE, offset);    
    if (size != BLKSIZE) {
        printf("WRITER - ERROR !!! write(%s,offset %d) %s\n", FILENAME,offset,strerror(errno));
	pCtx->result = -1;
        return NULL;
    }

    usleep(random()%30000);
  }
  if (close(fd)<0) {
        printf("WRITER - ERROR !!! close(%s) %s\n", FILENAME,strerror(errno));
	pCtx->result = -1;
        return NULL;  
  }  
  pCtx->result = 0;
  return NULL;
}  

int loop_reader() {
  int      fd;
  int      idx;
  size_t   size;
  int      res = 0;
  
  fd = open(FILENAME, O_RDWR);
  if (fd == -1) {
      printf("READER - ERROR !!! open(%s) %s\n",FILENAME,strerror(errno));
      return -1;
  }
          
  for (idx=0; idx<(LOOP*NBWRITER); idx++) {
        
    size = pread(fd, blockread, BLKSIZE, BLKSIZE*idx);    
    if (size != BLKSIZE) {
        printf("READER - ERROR !!! write(%s,idx %d) %s\n", FILENAME,idx,strerror(errno));
	close(fd);
        return -1;
    }
    if (memcmp(block, blockread, BLKSIZE)!= 0) {
        printf("READER - ERROR !!! block %d differs %s\n", idx, FILENAME);
	res = -1;     
    }

    
  }
  if (close(fd)<0) {
        printf("WRITER - ERROR !!! close(%s) %s\n", FILENAME,strerror(errno));
        return -1;  
  }  
  //if (res == 0) printf("SUCCESS\n");
  return res;
}  
int start_thread_create(test_ctx_t * ctx) {
   int                        i;
   int                        err;
   pthread_attr_t             attr;
   pthread_t                  thread;
   
   err = pthread_attr_init(&attr);
   if (err != 0) {
     printf("pthread_attr_init %d %s",ctx->start,strerror(errno));
     return -1;
   }  

   err = pthread_create(&thread,&attr,loop_writer_thread,ctx);       
   return 0;
}

test_ctx_t ctx[NBWRITER];
int loop_test_process() {
  int        i,ret;  
  int        size;
  int        fd;
  
  fd = open(FILENAME, O_RDWR | O_CREAT | O_TRUNC, 0640);
  if (fd == -1) {
      printf("open(%s) %s\n",FILENAME, strerror(errno));
      return -1;
  }  
  close(fd);
 
   
  for (i=0; i<NBWRITER; i++) {
    ctx[i].start  = i;
    ctx[i].result = -2;
  }  
     
  for (i=0; i<NBWRITER; i++) {    
    /* Launch writer /reader threads */
    ret = start_thread_create(&ctx[i]);
    if (ret != 0) return ret;
  }

  while (1) {
  
      sleep(1);
      for (i=0; i<NBWRITER; i++) {    
	if (ctx[i].result == -2) break; 
      }      
      if (i==NBWRITER) break;
  }  

  return loop_reader();
}  

int main(int argc, char **argv) {
  pid_t pid[2000];
  int proc;
  int ret;
    
  read_parameters(argc, argv);
  
  {
    int i;
    for (i=0; i<BLKSIZE; i++) { 
      block[i] = ('a'+(i%27));
    }
  }
    
  ret = loop_test_process();
  return ret;

}
