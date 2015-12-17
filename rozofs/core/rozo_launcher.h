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


#ifndef ROZO_LAUNCHER_H
#define ROZO_LAUNCHER_H

#include <rozofs/common/types.h>
#include <rozofs/common/log.h>

#if 1

/*__________________________________________________________________________
**
** Parse a command line and split it an argv[] array of parameters
**
** @param cmd   the command line to parse
** @param argv  the array of pointer to the parameters
**
*/
static inline void rozo_launcher_parse_command(char * cmd, char * argv[]) {
  char * pChar = cmd;
  int    argc=0;

  while (*pChar) {

    /* Skip spaces */
    while((*pChar==' ')||(*pChar=='\t')||(*pChar=='\n')) pChar++;

    /* Begin of argument */
    argv[argc++] = pChar;

    /* search either space or end of string */
    while ((*pChar!=0)&&(*pChar!=' ')&&(*pChar!='\t')&&(*pChar!='\n')) pChar++;

    /* Space encountered */
    if (*pChar==' ') {
      /* Space is replaced by an end of string */
      *pChar = 0;
      /* Next character in the string */
      pChar++; 
    }	 
  }
  argv[argc] = NULL;
} 
/*__________________________________________________________________________
**
** Start a launcher 
**
** @param pidfile   pid file name to save the new process pid
** @param cmd       command line to run
**
** @retval the number of parameters or 0
*/
static inline int rozo_launcher_start(char *pidfile, char * cmd) { 
  char * argv[64];

  argv[0] = "rozolauncher";
  argv[1] = "start";
  argv[2] = pidfile;
  
  rozo_launcher_parse_command(cmd,&argv[3]);
    
  pid_t pid = vfork();
    
  if (pid < 0) {
    severe("vfork() %s",strerror(errno));
    return -1;
  }
  
  if (pid!=0) return 0;
  /*
  ** clean up of the file descriptor inherited from the parent
  */
  {
     int i;
     for (i= 4; i < 4096; i++) close(i);
  }  
  
  if (execvp(argv[0],argv)<0) {
    severe("rozo_launcher_start(%s,%s) %s",pidfile, cmd, strerror(errno));
  }
  exit(0);
}
/*__________________________________________________________________________
**
** Stop a process using the launcher 
**
** @param pidfile   pid file name conaining process pid to stop
**
** @retval the number of parameters or 0
*/
static inline int rozo_launcher_stop(char *pidfile) {  
  char * argv[4];

  argv[0] = "rozolauncher";
  argv[1] = "stop";
  argv[2] = pidfile;
  argv[3] = NULL;
        
  pid_t pid = vfork();
  
  if (pid < 0) {
    severe("vfork() %s",strerror(errno));
    return -1;
  }
  
  if (pid!=0) return 0; 


  if (execvp(argv[0],argv)<0) {
    severe("rozo_launcher_stop(%s) %s",pidfile, strerror(errno));
  }
  exit(0);
}

/*__________________________________________________________________________
**
** Reload a process using the launcher 
**
** @param pidfile   pid file name conaining process pid to stop
**
** @retval the number of parameters or 0
*/
static inline int rozo_launcher_reload(char *pidfile) {  
  char * argv[4];

  argv[0] = "rozolauncher";
  argv[1] = "reload";
  argv[2] = pidfile;
  argv[3] = NULL;
        
  pid_t pid = vfork();
  
  if (pid < 0) {
    severe("vfork() %s",strerror(errno));
    return -1;
  }
  
  if (pid!=0) return 0; 


  if (execvp(argv[0],argv)<0) {
    severe("rozo_launcher_reload(%s) %s",pidfile, strerror(errno));
  }
  exit(0);
}  
#else
/*__________________________________________________________________________
**
** Start a launcher 
**
** @param pidfile   pid file name to save the new process pid
** @param cmd       command line to run
**
** @retval the number of parameters or 0
*/
static inline int rozo_launcher_start(char *pidfile, char * cmd) { 
  char   fork_cmd[1024];

  sprintf(fork_cmd,"rozolauncher start %s %s",pidfile,cmd);
  system(fork_cmd);
  return 0;
}  
/*__________________________________________________________________________
**
** Stop a process using the launcher 
**
** @param pidfile   pid file name conaining process pid to stop
**
** @retval the number of parameters or 0
*/
static inline int rozo_launcher_stop(char *pidfile) {  
  char   fork_cmd[1024];

  sprintf(fork_cmd,"rozolauncher stop %s",pidfile);
  system(fork_cmd);
  return 0;
}
#endif

#endif
