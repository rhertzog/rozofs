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
#include <sys/stat.h>
#include <sys/wait.h>
#include <dirent.h>




#define DEFAULT_NB_PROCESS    20
#define DEFAULT_LOOP         200

int shmid;
#define SHARE_MEM_NB 7541

int nbProcess       = DEFAULT_NB_PROCESS;
int myProcId;
int loop=DEFAULT_LOOP;
int * result;
char mount[256];
static void usage() {
    printf("Parameters:\n");
    printf("-mount <mount point> ]  The mount point\n");
    printf("[ -process <nb> ]      The test will be done by <nb> process simultaneously (default %d)\n", DEFAULT_NB_PROCESS);
    printf("[ -loop <nb> ]        <nb> test operations will be done (default %d)\n",DEFAULT_LOOP);
    exit(-100);
}


char cmd[1024];

static void read_parameters(argc, argv)
int argc;
char *argv[];
{
    unsigned int idx;
    int ret;
    
    mount[0] = 0;

    idx = 1;
    while (idx < argc) {
	
        /* -process <nb>  */
        if (strcmp(argv[idx], "-process") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx-1]);
                usage();
            }
            ret = sscanf(argv[idx], "%u", &nbProcess);
            if (ret != 1) {
                printf("%s option but bad value \"%s\"!!!\n", argv[idx-1], argv[idx]);
                usage();
            }
            idx++;
            continue;
        }
        /* -mount <mount point>  */
        if (strcmp(argv[idx], "-mount") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx-1]);
                usage();
            }
            ret = sscanf(argv[idx], "%s", mount);
            if (ret != 1) {
                printf("%s option but bad value \"%s\"!!!\n", argv[idx-1], argv[idx]);
                usage();
            }
            idx++;
            continue;
        }
        /* -loop <nb>  */
        if (strcmp(argv[idx], "-loop") == 0) {
            idx++;
            if (idx == argc) {
                printf("%s option set but missing value !!!\n", argv[idx-1]);
                usage();
            }
            ret = sscanf(argv[idx], "%u", &loop);
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

int read_directory(char * d,char * f) {
  DIR * dir;
  int   ret;
  struct dirent  * file;
  int found=0;
  int i;
    
  dir = opendir(d);
  if (dir == NULL) {
    printf("Can not open directory %s %s\n",strerror(errno));
    return -1;
  }

  ret = 0;
  while (1) {
  
    file = readdir(dir);
    if (file == NULL) break;

    if (strcmp(file->d_name,"..")==0) continue;
    if (strcmp(file->d_name,".")==0) continue;
    if (f != NULL) {
      if (strcmp(file->d_name,f) == 0) {
        found = 1;
        continue;
      }
    }
    printf("%s found in directory %s\n",file->d_name,d);
    ret = -1;
    break;
  }
  closedir(dir);
  if (f != NULL) {
    if (found == 0) return -1;
  }  
  return ret; 
}

#define CHECK_DIR_EMPTY(d)  ret = read_directory(d, NULL);\
                            if (ret < 0) {\
                              printf("%d %s is not empty\n", __LINE__,d);\
                              return ret;\
                            }
#define CHECK_FILE_IN_DIR(d,f)    ret = read_directory(d, f);\
                                  if (ret < 0) {\
                                    printf("%d %s should contain %s\n", __LINE__,d,f);\
                                    return ret;\
                                  }
int do_one_test(char * d1,char * d2) {
  int ret = 0;
  int i;
  char f1[256];
  char f2[256];
   

  CHECK_DIR_EMPTY(d1);
  CHECK_DIR_EMPTY(d2);   
  
  sprintf(f1, "%s/f1", d1);  
  sprintf(f2, "%s/f2", d1);  
  ret = rename(f1,f2);
  if ((ret >= 0) || (errno != ENOENT)) {
    printf("%d rename non existant file %s to %s %s\n", __LINE__,f1, f2, strerror(errno));
    return -1;
  }
  
  sprintf(cmd, "echo A > %s", f1);
  system(cmd);  
  ret = rename(f1,f2);
  if (ret < 0) {
    printf("%d rename %s to %s %s\n", __LINE__,f1, f2, strerror(errno));
    return -1;
  } 
  CHECK_FILE_IN_DIR(d1, "f2");


  sprintf(cmd, "echo A > %s", f1);
  system(cmd);  
  ret = rename(f1,f2);
  if (ret < 0) {
    printf("%d rename %s to %s %s\n", __LINE__, f1, f2, strerror(errno));
    return -1;
  } 
  CHECK_FILE_IN_DIR(d1, "f2");
  ret = rename(f2,f1);
  if (ret < 0) {
    printf("%d rename %s to %s %s\n", __LINE__, f2, f1, strerror(errno));
    return -1;
  } 
  CHECK_FILE_IN_DIR(d1, "f1");
      
  sprintf(f2, "/tmp/f2");  
  ret = rename(f1,f2);
  if ((ret >= 0) || (errno != EXDEV)) {
    printf("%d rename file %s to %s %s\n",__LINE__, f1, f2, strerror(errno));
    return -1;
  }   
  
  sprintf(f2, "%s/f2", d2);  
  ret = rename(f1,f2);
  if (ret < 0) {
    printf("%d rename %s to %s %s\n", __LINE__,f1, f2, strerror(errno));
    return -1;
  }   
  CHECK_FILE_IN_DIR(d2, "f2");
  CHECK_DIR_EMPTY(d1);
  
  ret = rename(f2,f1);
  if (ret < 0) {
    printf("%d rename %s to %s %s\n", __LINE__,f2, f1, strerror(errno));
    return -1;
  }  
  CHECK_FILE_IN_DIR(d1, "f1");
  CHECK_DIR_EMPTY(d2);
  
  ret = rename(d1,d2);
  if (ret < 0) {
    printf("%d rename %s to %s %s\n", __LINE__,f2, f1, strerror(errno));
    return -1;
  }  
  CHECK_FILE_IN_DIR(d2, "f1");
  
  ret = rename(d2,d1);
  if (ret < 0) {
    printf("%d rename %s to %s %s\n", __LINE__,f2, f1, strerror(errno));
    return -1;
  }  
  CHECK_FILE_IN_DIR(d1, "f1");
  unlink(f1);    
  rmdir(d1);
  return 0;
}
int loop_test_process() {
  char directoryName1[256];
  char directoryName2[256];
  pid_t pid = getpid();
  int ret;
  int count = 0;
  
  
  while (1) {
    count ++;
       
    sprintf(directoryName1, "%s/d1_%u", mount, pid);
    ret = mkdir(directoryName1,0755);
    if (ret < 0) {
      printf("proc %3d - mkdir(%s) %s\n", myProcId, directoryName1,strerror(errno));  
      return -1;       
    }
    sprintf(directoryName2, "%s/d2_%u", mount,pid);
    ret = mkdir(directoryName2,0755);
    if (ret < 0) {
      printf("proc %3d - mkdir(%s) %s\n", myProcId, directoryName2,strerror(errno));  
      return -1;       
    }

    ret = do_one_test(directoryName1,directoryName2);   
    if (ret < 0) {
      printf("proc %3d - test failed in loop %d(%s) %s\n", myProcId, count);  
      return ret;
    }

    if (count == loop) {
      return 0;
    }
  }    
}  
void free_result(void) {
  struct shmid_ds   ds;
  shmctl(shmid,IPC_RMID,&ds); 
}
int * allocate_result(int size) {
  struct shmid_ds   ds;
  void            * p;
      
  /*
  ** Remove the block when it already exists 
  */
  shmid = shmget(SHARE_MEM_NB,1,0666);
  if (shmid >= 0) {
    shmctl(shmid,IPC_RMID,&ds);
  }
  
  /* 
  * Allocate a block 
  */
  shmid = shmget(SHARE_MEM_NB, size, IPC_CREAT | 0666);
  if (shmid < 0) {
    perror("shmget(IPC_CREAT)");
    return 0;
  }  

  /*
  * Map it on memory
  */  
  p = shmat(shmid,0,0);
  if (p == 0) {
    shmctl(shmid,IPC_RMID,&ds);  
       
  }
  memset(p,0,size);  
  return (int *) p;
}
int main(int argc, char **argv) {
  pid_t pid[2000];
  int proc;
  int ret;
    
  read_parameters(argc, argv);
  
  if (mount[0] == 0) {
    printf("-mount is mandatory\n");
    exit(-100);
  }
  
  if (nbProcess <= 0) {
    printf("Bad -process option %d\n",nbProcess);
    exit(-100);
  }

  result = allocate_result(4*nbProcess);
  if (result == NULL) {
    printf(" allocate_result error\n");
    exit(-100);
  }  
  for (proc=0; proc < nbProcess; proc++) {
  
     pid[proc] = fork();     
     if (pid[proc] == 0) {
       myProcId = proc;
       result[proc] = loop_test_process();
       exit(0);
     }  
  }

  for (proc=0; proc < nbProcess; proc++) {
    waitpid(pid[proc],NULL,0);        
  }
  
  ret = 0;
  for (proc=0; proc < nbProcess; proc++) {
    if (result[proc] != 0) {
      ret--;
    }
  }
  free_result();
  if (ret) printf("OK %d / FAILURE %d\n",nbProcess+ret, -ret);
  exit(ret);
}
