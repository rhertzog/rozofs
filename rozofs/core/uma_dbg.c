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
#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <errno.h>
#include <stdarg.h>
#include <unistd.h>
#include <time.h>
#include <ctype.h>
 
#include <rozofs/common/types.h>
#include <rozofs/common/log.h>

#include "ruc_common.h"
#include "ruc_list.h"
#include "ruc_sockCtl_api.h"
#include "uma_tcp_main_api.h"
#include "ruc_tcpServer_api.h"
#include "uma_dbg_api.h"
#include "uma_dbg_msgHeader.h"
#include "config.h"
#include "../rozofs_service_ports.h"

uint32_t   uma_dbg_initialized=FALSE;
char     * uma_gdb_system_name=NULL;
static time_t uptime=0;

typedef struct uma_dbg_topic_s {
  char                     * name;
  uint16_t                   option;
  uint16_t                   len:15;
  uma_dbg_topic_function_t funct;
} UMA_DBG_TOPIC_S;

#define UMA_DBG_MAX_TOPIC 128
UMA_DBG_TOPIC_S uma_dbg_topic[UMA_DBG_MAX_TOPIC] = {};
uint32_t        uma_dbg_nb_topic = 0;
uint32_t          uma_dbg_topic_initialized=FALSE;

#define            MAX_ARG   64

uma_dbg_catcher_function_t	uma_dbg_catcher = uma_dbg_catcher_DFT;

typedef struct uma_dbg_session_s {
  ruc_obj_desc_t            link;
  void 		            * ref;
  uint32_t                    ipAddr;
  uint16_t                    port;
  uint32_t                    tcpCnxRef;
//64BITS  uint32_t                    recvPool;
  void                      *recvPool;
  char                     *argv[MAX_ARG];
  char                      argvBuffer[2050];
  char                      last_valid_command[2000];
} UMA_DBG_SESSION_S;

UMA_DBG_SESSION_S *uma_dbg_freeList = (UMA_DBG_SESSION_S*)NULL;
UMA_DBG_SESSION_S *uma_dbg_activeList = (UMA_DBG_SESSION_S*)NULL;
#define UMA_DBG_MAX_CMD_LEN 127
static char rcvCmdBuffer[UMA_DBG_MAX_CMD_LEN+1];

char uma_dbg_temporary_buffer[UMA_DBG_MAX_SEND_SIZE];

void uma_dbg_listTopic(uint32_t tcpCnxRef, void *bufRef, char * topic);
static uint32_t do_not_send = 0;

/*__________________________________________________________________________
 */
/**
*  Format an ASCII dump
* @param mem   memory area to dump
* @param len   size to dump mem on
* @param p     where to output the dump
*
*  return the address of the end of the dump 
*/
/*__________________________________________________________________________
 */ 
#define HEXDUMP_COLS 16
char * uma_dbg_hexdump(void *ptr, unsigned int len, char * p)
{
        unsigned int i, j;
	char * mem =(char *) ptr;
        
        for(i = 0; i < len + ((len % HEXDUMP_COLS) ? (HEXDUMP_COLS - len % HEXDUMP_COLS) : 0); i++)
        {
                /* print offset */
                if(i % HEXDUMP_COLS == 0)
                {
                        p += sprintf(p,"%8p: ", mem+i);
                }
 
                /* print hex data */
                if(i < len)
                {
                        p += sprintf(p,"%02x ", 0xFF & ((char*)mem)[i]);
                }
                else /* end of block, just aligning for ASCII dump */
                {
                        p += sprintf(p,"   ");
                }
                
                /* print ASCII dump */
                if(i % HEXDUMP_COLS == (HEXDUMP_COLS - 1))
                {
                        for(j = i - (HEXDUMP_COLS - 1); j <= i; j++)
                        {
                                if(j >= len) /* end of block, not really printing */
                                {
                                        p += sprintf(p," ");
                                }
                                else if(isprint(((char*)mem)[j])) /* printable char */
                                {
					p += sprintf(p,"%c", 0xFF & ((char*)mem)[j]);     
                                }
                                else /* other char */
                                {
                                        p += sprintf(p,".");
                                }
                        }
                        p += sprintf(p,"\n");
                }
        }
	return p;
}

/*__________________________________________________________________________
 */
/**
*  Run a system command and return the result 
*/
int uma_dbg_run_system_cmd(char * cmd, char *result, int len) {
  pid_t  pid;
  char   fileName[32];
  int    fd;
  int ret = -1;
  
  pid = getpid();
  sprintf(fileName,"/tmp/rozo.%d",pid);
  
  strcat(cmd," > ");
  strcat(cmd,fileName);
  
  ret = system(cmd);
  if (-1 == ret) {
    DEBUG("%s returns -1",cmd);
  }
  
  fd = open(fileName, O_RDONLY);
  if (fd < 0) {
    unlink(fileName);
    return 0;    
  }
  
  len = read(fd,result, len-1);
  result[len] = 0;
  
  close(fd);
  unlink(fileName);  
  return len;
} 

/*__________________________________________________________________________
 */
/**
*  Display the system name if any has been set thanks to uma_dbg_set_name()
*/
void uma_dbg_system_cmd(char * argv[], uint32_t tcpRef, void *bufRef) {
  char * cmd;
  int    len;

  if(argv[1] == NULL) {  
    uma_dbg_listTopic(tcpRef, bufRef, NULL);
    return; 
  }
  
  cmd = rcvCmdBuffer;
  while (*cmd != 's') cmd++;
  cmd += 7;

  len = uma_dbg_run_system_cmd(cmd, uma_dbg_get_buffer(), uma_dbg_get_buffer_len());
  if (len == 0)  uma_dbg_send(tcpRef, bufRef, TRUE, "No response\n");    
  else           uma_dbg_send(tcpRef, bufRef, TRUE, "%s",uma_dbg_get_buffer());
  return ;
} 
/*__________________________________________________________________________
 */
/**
*  Display the system name if any has been set thanks to uma_dbg_set_name()
*/
void uma_dbg_system_ps(char * argv[], uint32_t tcpRef, void *bufRef) {
  int    len;
  pid_t  pid;
  
  pid = getpid();
  
  sprintf(uma_dbg_get_buffer(),"ps -p %d ", pid);
  strcat(uma_dbg_get_buffer(), "-o%p -o%C -o%t -o%z -o%a");

  len = uma_dbg_run_system_cmd(uma_dbg_get_buffer(), uma_dbg_get_buffer(), uma_dbg_get_buffer_len());
  if (len == 0)  uma_dbg_send(tcpRef, bufRef, TRUE, "No response\n");    
  else           uma_dbg_send(tcpRef, bufRef, TRUE, "%s",uma_dbg_get_buffer());
  return ;
} 
/*__________________________________________________________________________
 */
/**
*  Display the system name if any has been set thanks to uma_dbg_set_name()
*/
void uma_dbg_show_uptime(char * argv[], uint32_t tcpRef, void *bufRef) {
    time_t elapse;
    int days, hours, mins, secs;

    // Compute uptime for storaged process
    elapse = (int) (time(0) - uptime);
    days = (int) (elapse / 86400);
    hours = (int) ((elapse / 3600) - (days * 24));
    mins = (int) ((elapse / 60) - (days * 1440) - (hours * 60));
    secs = (int) (elapse % 60);
    uma_dbg_send(tcpRef, bufRef, TRUE, "uptime = %d days, %d:%d:%d\n", days, hours, mins, secs);
}

/*__________________________________________________________________________
 */
/**
*  Display the ports that should be reserved
*/
void uma_dbg_reserved_ports(char * argv[], uint32_t tcpRef, void *bufRef) {
  char * pt = uma_dbg_get_buffer();
  char cmd[512];
        
  pt += show_ip_local_reserved_ports(pt);
  pt += sprintf(pt,"\n");
  strcpy(cmd,"grep ip_local_reserved_ports /etc/sysctl.conf");
  pt += sprintf(pt,"%s\n",cmd);    
  pt += uma_dbg_run_system_cmd(cmd, pt, 1024);
  pt += sprintf(pt,"\n");
  
  strcpy(cmd,"cat /proc/sys/net/ipv4/ip_local_reserved_ports");
  pt += sprintf(pt,"%s\n",cmd);  
  pt += uma_dbg_run_system_cmd(cmd, pt, 1024);
  
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}
/*__________________________________________________________________________
 */
/**
*  Display the system name if any has been set thanks to uma_dbg_set_name()
*/
void uma_dbg_show_name(char * argv[], uint32_t tcpRef, void *bufRef) {  
    
  if (uma_gdb_system_name == NULL) {
    uma_dbg_send(tcpRef, bufRef, TRUE, "system : NO NAME\n");
  }  
  else {  
    uma_dbg_send(tcpRef, bufRef, TRUE, "system : %s\n", uma_gdb_system_name);
  }
}

/*__________________________________________________________________________
 */
/**
*  Display the version of the library 
*/
void uma_dbg_show_version(char * argv[], uint32_t tcpRef, void *bufRef) {  
  uma_dbg_send(tcpRef, bufRef, TRUE, "version : %s\n", VERSION);
}
/*__________________________________________________________________________
 */
/**
*  Reset every resetable command
*/
void uma_dbg_counters_reset(char * argv[], uint32_t tcpRef, void *bufRef) {
  int topicNum;
  UMA_DBG_TOPIC_S * p;
  int count=0;
  
  if ((argv[1] == NULL)||(strcmp(argv[1],"reset")!=0)) {  
    uma_dbg_send(tcpRef, bufRef, TRUE, "counters requires \"reset\" as parameter\n");
    return; 
  }
  
  /*
  ** To prevent called function to send back a response
  */ 
  do_not_send = 1;
  
  p = uma_dbg_topic;
  for (topicNum=0; topicNum <uma_dbg_nb_topic; topicNum++,p++) {
    if (p->option & UMA_DBG_OPTION_RESET) {
      count++;
      p->funct(argv,tcpRef,bufRef);
    }
  }  

  do_not_send = 0;

  uma_dbg_send(tcpRef, bufRef, TRUE, "%d reset done\n", count);
} 
/*-----------------------------------------------------------------------------
**
**  #SYNOPSIS
**   Default catcher function
**
**  IN:
**   OUT :
**
**----------------------------------------------------------------------------
*/
//64BITS uint32_t uma_dbg_catcher_DFT(uint32 tcpRef, uint32 bufRef)
uint32_t uma_dbg_catcher_DFT(uint32_t tcpRef, void *bufRef)
{
	return FALSE;
}

/*-----------------------------------------------------------------------------
**
**  #SYNOPSIS
**   Change the catcher function
**
**  IN:
**   OUT :
**
**----------------------------------------------------------------------------
*/
void uma_dbg_setCatcher(uma_dbg_catcher_function_t funct)
{
	uma_dbg_catcher = funct;
}

/*-----------------------------------------------------------------------------
**
**  #SYNOPSIS
**   Send a message
**
**  IN:
**   OUT :
**
**----------------------------------------------------------------------------
*/
//64BITS void uma_dbg_send(uint32_t tcpCnxRef, uint32 bufRef, uint8_t end, char *fmt, ... )
void uma_dbg_send(uint32_t tcpCnxRef, void  *bufRef, uint8_t end, char *fmt, ... ) {
  va_list         vaList;
  UMA_MSGHEADER_S *pHead;
  char            *pChar;
  uint32_t           len;

  /* 
  ** May be in a specific process such as counter reset
  ** and so do not send any thing
  */
  if (do_not_send) return;
  
  /* Retrieve the buffer payload */
  if ((pHead = (UMA_MSGHEADER_S *)ruc_buf_getPayload(bufRef)) == NULL) {
    severe( "ruc_buf_getPayload(%p)", bufRef );
    /* Let's tell the caller fsm that the message is sent */
    return;
  }
  pChar = (char*) (pHead+1);
  
  len    = sprintf(pChar, "____[%s]__[ %s]____\n", uma_gdb_system_name, rcvCmdBuffer);
  pChar += len;
  len   += sizeof(UMA_MSGHEADER_S);

  /* Format the string */
  va_start(vaList,fmt);
  len += vsprintf(pChar, fmt, vaList)+1;
  va_end(vaList);

  if (len > UMA_DBG_MAX_SEND_SIZE)
  {
    severe("debug response exceeds buffer length %u/%u",len,(int)UMA_DBG_MAX_SEND_SIZE);
  }

  pHead->len = htonl(len-sizeof(UMA_MSGHEADER_S));
  pHead->end = end;

  ruc_buf_setPayloadLen(bufRef,len);
  uma_tcp_sendSocket(tcpCnxRef,bufRef,0);
}
/*-----------------------------------------------------------------------------
**
**  #SYNOPSIS
**   Free a debug session
**
**  IN:
**   OUT :
**
**----------------------------------------------------------------------------
*/
void uma_dbg_free(UMA_DBG_SESSION_S * pObj) {
  /* Free the TCP connection context */
  if (pObj->tcpCnxRef != -1) uma_tcp_deleteReq(pObj->tcpCnxRef);
  /* remove the context from the active list */
  ruc_objRemove((ruc_obj_desc_t*)pObj);
  /* Set the context in the free list */
  ruc_objInsertTail((ruc_obj_desc_t*)uma_dbg_freeList,(ruc_obj_desc_t*)pObj);
}
/*-----------------------------------------------------------------------------
**
**  #SYNOPSIS
**   Find a debug session context from its IP address and TCP port
**
**  IN:
**       - IP addr : searched logical IP address
**       - TCP port
**   OUT :
**      !=-1 : object reference
**     == -1 error
**
**----------------------------------------------------------------------------
*/

UMA_DBG_SESSION_S *uma_dbg_findFromAddrAndPort(uint32_t ipAddr, uint16_t port) {
  ruc_obj_desc_t    * pnext;
  UMA_DBG_SESSION_S * p;

  pnext = (ruc_obj_desc_t*)NULL;
  while ((p = (UMA_DBG_SESSION_S*)ruc_objGetNext(&uma_dbg_activeList->link, &pnext))
	 !=(UMA_DBG_SESSION_S*)NULL) {

    if ((p->ipAddr == ipAddr) && (p->port == port)) return p;

  }
  /* not found */
  return (UMA_DBG_SESSION_S *) NULL;
}
/*-----------------------------------------------------------------------------
**
**  #SYNOPSIS
**   Find a debug session context from its IP address and TCP port
**
**  IN:
**       - IP addr : searched logical IP address
**       - TCP port
**   OUT :
**      !=-1 : object reference
**     == -1 error
**
**----------------------------------------------------------------------------
*/

UMA_DBG_SESSION_S *uma_dbg_findFromRef(void * ref) {
  ruc_obj_desc_t    * pnext;
  UMA_DBG_SESSION_S * p;

  pnext = (ruc_obj_desc_t*)NULL;
  while ((p = (UMA_DBG_SESSION_S*)ruc_objGetNext(&uma_dbg_activeList->link, &pnext))
	 !=(UMA_DBG_SESSION_S*)NULL) {

    if (p->ref == ref) return p;

  }
  /* not found */
  return (UMA_DBG_SESSION_S *) NULL;
}
/*-----------------------------------------------------------------------------
**
**  #SYNOPSIS
**   Find a debug session context from its connection ref
**
**  IN:
**       - ref : TCP connection reference
**       - TCP port
**   OUT :
**      !=-1 : object reference
**     == -1 error
**
**----------------------------------------------------------------------------
*/

UMA_DBG_SESSION_S *uma_dbg_findFromCnxRef(uint32_t ref) {
  ruc_obj_desc_t    * pnext;
  UMA_DBG_SESSION_S * p;

  pnext = (ruc_obj_desc_t*)NULL;
  while ((p = (UMA_DBG_SESSION_S*)ruc_objGetNext(&uma_dbg_activeList->link, &pnext))
	 !=(UMA_DBG_SESSION_S*)NULL) {

    if (p->tcpCnxRef == ref) return p;

  }
  /* not found */
  return (UMA_DBG_SESSION_S *) NULL;
}
/*
**--------------------------------------------------------------------------
**  #SYNOPSIS
**  called by any SWBB that wants to add a topic on the debug interface

**   IN:
**       topic : a string representing the topic
**       allBack : the function to be called when a request comes in
**                 for this topic
**   OUT : none
**
**
**--------------------------------------------------------------------------
*/
void uma_dbg_insert_topic(int idx, char * topic, uint16_t option, uint16_t length, uma_dbg_topic_function_t funct) {
  /* Register the topic */
  uma_dbg_topic[idx].name         = topic;
  uma_dbg_topic[idx].len          = length;
  uma_dbg_topic[idx].funct        = funct;
  uma_dbg_topic[idx].option       = option;
}  
/*
**--------------------------------------------------------------------------
**  #SYNOPSIS
**  called by any SWBB that wants to add a topic on the debug interface

**   IN:
**       topic : a string representing the topic
**       allBack : the function to be called when a request comes in
**                 for this topic
**       option : a bit mask of options
**   OUT : none
**
**
**--------------------------------------------------------------------------
*/
void uma_dbg_addTopic_option(char * topic, uma_dbg_topic_function_t funct, uint16_t option) {
  int    idx,idx2;
  uint16_t length;
  char * my_topic = NULL;

  if (uma_dbg_topic_initialized == FALSE) {
    /* Reset the topic table */
    for (idx=0; idx <UMA_DBG_MAX_TOPIC; idx++) {
      uma_dbg_topic[idx].len   = 0;
      uma_dbg_topic[idx].name  = NULL;
      uma_dbg_topic[idx].funct = NULL;
    }
    uma_dbg_topic_initialized = TRUE;
    uma_dbg_nb_topic = 0;
  }

  /* Get the size of the topic */
  length = strlen(topic);
  if (length == 0) {
    severe( "Bad topic length %d", length );
    return;
  }
  
  /* Check a place is left */
  if (uma_dbg_nb_topic == UMA_DBG_MAX_TOPIC) {
    severe( "Too much topic %d. Can not insert %s", UMA_DBG_MAX_TOPIC, topic );
    return;    
  }

  /* copy the topic */
  my_topic = malloc(length + 1) ;
  if (my_topic == NULL) {
    severe( "Out of memory. Can not insert %s",topic );    
    return;
  }
  strcpy(my_topic, topic);

  /* Find a free entry in the topic table */
  for (idx=0; idx <uma_dbg_nb_topic; idx++) {
    int order;   
    
    order = strcasecmp(topic,uma_dbg_topic[idx].name);
    
    /* check the current entry has got a different key word than
       the one we are to add */
    if (order == 0) {
      severe( "Trying to add topic %s that already exist", topic );
      free(my_topic);
      return;
    }
    
    /* Insert here */
    if (order < 0) break;
  }
  
  for (idx2 = uma_dbg_nb_topic-1; idx2 >= idx; idx2--) {
     uma_dbg_insert_topic(idx2+1,uma_dbg_topic[idx2].name,uma_dbg_topic[idx2].option,uma_dbg_topic[idx2].len, uma_dbg_topic[idx2].funct);
  }
  uma_dbg_insert_topic(idx,my_topic,option,length, funct);
  uma_dbg_nb_topic++;
}
/*-----------------------------------------------------------------------------
**
**  #SYNOPSIS
**   Send a message
**
**  IN:
**   OUT :
**
**----------------------------------------------------------------------------
*/
//64BITS void uma_dbg_listTopic(uint32_t tcpCnxRef, uint32 bufRef, char * topic)
void uma_dbg_listTopic(uint32_t tcpCnxRef, void *bufRef, char * topic) {
  UMA_MSGHEADER_S *pHead;
  char            *p;
  uint32_t           idx,topicNum;
  int             len=0;               

  /* Retrieve the buffer payload */
  if ((pHead = (UMA_MSGHEADER_S *)ruc_buf_getPayload(bufRef)) == NULL) {
    severe( "ruc_buf_getPayload(%p)", bufRef );
    return;
  }

  p = (char*) (pHead+1);
  idx = 0;
  /* Format the string */
  if (topic) {
    idx += sprintf(&p[idx], "No such topic \"%s\" !!!\n\n",topic);
    len = strlen(topic);                
  }

  /* Build the list of topic */
  if (len == 0) idx += sprintf(&p[idx], "List of available topics :\n");
  else          idx += sprintf(&p[idx], "List of %s... topics:\n",topic);
  
  for (topicNum=0; topicNum <uma_dbg_nb_topic; topicNum++) {
  
    if (uma_dbg_topic[topicNum].option & UMA_DBG_OPTION_HIDE) continue;
  
    if (len == 0) {
      idx += sprintf(&p[idx], "  %s\n",uma_dbg_topic[topicNum].name);
    }
    else if (strncmp(topic,uma_dbg_topic[topicNum].name, len) == 0) {
      idx += sprintf(&p[idx], "  %s\n",uma_dbg_topic[topicNum].name);      
    }  
  }
  if (len == 0) idx += sprintf(&p[idx], "  exit / quit / q\n");

  idx ++;

  pHead->len = htonl(idx);
  pHead->end = TRUE;

  ruc_buf_setPayloadLen(bufRef,idx + sizeof(UMA_MSGHEADER_S));
  uma_tcp_sendSocket(tcpCnxRef,bufRef,0);
}
/*
**--------------------------------------------------------------------------
**  #SYNOPSIS
**   callback used by the TCP connection receiver FSM
**   when a message has been fully received on the
**   TCP connection.
**
**   When the application has finsihed to process the message, it must
**   release it
**
**   IN:
**       user reference provide at TCP connection creation time
**       reference of the TCP objet on which the buffer has been allocated
**       reference of the buffer that contains the message
**
**   OUT : none
**
**
**--------------------------------------------------------------------------
*/
//64BITS void uma_dbg_receive_CBK(uint32_t userRef,uint32 tcpCnxRef,uint32 bufRef)
void uma_dbg_receive_CBK(void *opaque,uint32_t tcpCnxRef,void *bufRef) {
  int              topicNum;
  char           * pBuf, * pArg;
  uint16_t           length;
  uint32_t           argc;
  uint32_t           found=0;
  UMA_MSGHEADER_S *pHead;
  uint32_t           idx;
  UMA_DBG_SESSION_S * p;
  int                 replay=0;

  /*
  ** clear the received command buffer content
  */
  rcvCmdBuffer[0] = 0;

  /* Retrieve the session context from the referecne */

  if ((p = uma_dbg_findFromRef(opaque)) == NULL) {
    uma_dbg_send(tcpCnxRef,bufRef,TRUE,"Internal error");
    return;
  }

  /* Retrieve the buffer payload */
  if ((pHead = (UMA_MSGHEADER_S *)ruc_buf_getPayload(bufRef)) == NULL) {
    severe( "ruc_buf_getPayload(%p)", bufRef );
    return;
  }

  /*
  ** Call an optional catcher in order to redirect the message.
  ** This must be done after having checked the validity of the buffer,
  ** in order to avoid confusion : a bad bufRef means disconnection.
  */
  if(uma_dbg_catcher(tcpCnxRef, bufRef) )
  {
    return;
  }

  /* Scan the command line */
  argc = 0;
  pBuf = (char*)(pHead+1);
  
  /* Is it a replay request i.e "!!" */
  if ((pBuf[0] =='!') && (pBuf[1] =='!')) {
    /* let's get as input the last saved command */
    pBuf = p->last_valid_command;
    replay = 1;
  }
  /*
  ** save the current received command
  */
  memcpy(rcvCmdBuffer,pBuf,UMA_DBG_MAX_CMD_LEN);
  rcvCmdBuffer[UMA_DBG_MAX_CMD_LEN] = 0;
  pArg = p->argvBuffer;
  while (1) {
    /* Skip blanks */
//  (before FDL)  while ((*pBuf == ' ') || (*pBuf == '\t')) *pBuf++;
  while ((*pBuf == ' ') || (*pBuf == '\t')) pBuf++;
    if (*pBuf == 0) break; /* end of command line */
    p->argv[argc] = pArg;     /* beginning of a parameter */
    argc++;
    /* recopy the parameter */
    while ((*pBuf != ' ') && (*pBuf != '\t') && (*pBuf != 0)) *pArg++ = *pBuf++;
    *pArg++ = 0; /* End the parameter with 0 */
    if (*pBuf == 0) break;/* end of command line */
  }

  /* Set to NULL parameter number not filled in the command line */
  for (idx=argc; idx < MAX_ARG; idx++) p->argv[idx] = NULL;

  /* No topic */
  if (argc == 0) {
    uma_dbg_listTopic(tcpCnxRef, bufRef, NULL);
    return;
  }

  /* Search exact match in the topic list the one requested */
  length = strlen(p->argv[0]);
  for (topicNum=0; topicNum <uma_dbg_nb_topic; topicNum++) {
    if (uma_dbg_topic[topicNum].len == length) {
        
      int order = strcasecmp(p->argv[0],uma_dbg_topic[topicNum].name);
      
      if (order == 0) {
	found = 1;
	idx = topicNum;	
	break;
      }
      if (order < 0) break;  
    }
  }

  /* Search match on first characters */
  if (found == 0) {
    for (topicNum=0; topicNum <uma_dbg_nb_topic; topicNum++) {
      if (uma_dbg_topic[topicNum].option & UMA_DBG_OPTION_HIDE) continue;
      if (uma_dbg_topic[topicNum].len > length) {
        int order = strncasecmp(p->argv[0],uma_dbg_topic[topicNum].name, length);
        if (order < 0) break;  	
	if (order == 0) {
	  found++;
	  idx = topicNum;
	   /* Several matches Display possibilities */
	  if (found > 1) {
            uma_dbg_listTopic(tcpCnxRef, bufRef, p->argv[0]);  
            return; 	  
	  }
	}  
      }	 
    } 
  }

  /* No such command. List everything */
  if (found == 0) {
    uma_dbg_listTopic(tcpCnxRef, bufRef, NULL);  
    return;  
  }
  
  
  /* We have found one command */

  /* Save this existing command for later replay */
  if (replay == 0) {
    strcpy(p->last_valid_command,(char*)(pHead+1));
  } 
  
  uma_dbg_topic[idx].funct(p->argv,tcpCnxRef,bufRef);
}
/*
**-------------------------------------------------------
  void upc_nse_ip_disc_uph_ctl_CBK(uint32_t nsei,uint32 tcpCnxRef)
**-------------------------------------------------------
**  #SYNOPSIS
**   That function allocates all the necessary
**   resources for UPPS TCP connections management
**
**   IN:
**       refObj : reference of the NSE context
**       tcpCnxRef : reference of the tcpConnection
**
**
**   OUT :
**        none
**
**-------------------------------------------------------
*/
//64BITS void uma_dbg_disc_CBK(uint32_t refObj,uint32 tcpCnxRef) {
void uma_dbg_disc_CBK(void *opaque,uint32_t tcpCnxRef) {
  UMA_DBG_SESSION_S * pObj;

  if ((pObj = uma_dbg_findFromRef(opaque)) == NULL) {
    return;
  }

  uma_dbg_catcher(tcpCnxRef, NULL);
  uma_dbg_free(pObj);
}
/*
**-------------------------------------------------------
**  #SYNOPSIS
**   called when a debug session open is requested
**
**   IN:
**
**   OUT :
**
**
**-------------------------------------------------------
*/
uint32_t uma_dbg_accept_CBK(uint32_t userRef,int socketId,struct sockaddr * sockAddr) {
  uint32_t              ipAddr;
  uint16_t              port;
  UMA_DBG_SESSION_S * pObj;
  uma_tcp_create_t    conf;
  struct  sockaddr_in vSckAddr;
  int                 vSckAddrLen=14;
  char                name[32];

  uma_tcp_create_t *pconf = &conf;

  /* Get the source IP address and port */
  if((getpeername(socketId, (struct sockaddr *)&vSckAddr,(socklen_t*) &vSckAddrLen)) == -1){
    return RUC_NOK;
  }
  ipAddr = (uint32_t) ntohl((uint32_t)(/*(struct sockaddr *)*/vSckAddr.sin_addr.s_addr));
  port   = ntohs((uint16_t)(vSckAddr.sin_port));
  sprintf(name,"DBG %u.%u.%u.%u", (ipAddr>>24)&0xFF, (ipAddr>>16)&0xFF,(ipAddr>>8)&0xFF,(ipAddr)&0xFF);

  /* Search for a debug session with this IP address and port */
  if ((pObj = uma_dbg_findFromAddrAndPort(ipAddr,port)) != NULL) {

    /* Session already exist. Just update it */
    if (uma_tcp_updateTcpConnection(pObj->tcpCnxRef,socketId,name) != RUC_OK) {
      uma_dbg_free(pObj);
      return RUC_NOK;
    }
    return RUC_OK;
  }

  /* Find a free debug session context */
  pObj = (UMA_DBG_SESSION_S*)ruc_objGetFirst((ruc_obj_desc_t*)uma_dbg_freeList);
  if (pObj == (UMA_DBG_SESSION_S*)NULL) {
//    INFO8 "No more free debug session" EINFO;
    return RUC_NOK;
  }

  /* Initialize the session context */
  pObj->ipAddr    = ipAddr;
  pObj->port      = port;
  pObj->tcpCnxRef = (uint32_t) -1;
  
  pObj->last_valid_command[0] = 0;

  /* Allocate a TCP connection descriptor */
  pconf->headerSize       = sizeof(UMA_MSGHEADER_S);
  pconf->msgLenOffset     = 0;
  pconf->msgLenSize       = sizeof(uint32_t);
  pconf->bufSize          = 2048*4;
  pconf->userRcvCallBack  = uma_dbg_receive_CBK;
  pconf->userDiscCallBack = uma_dbg_disc_CBK;
  pconf->userRef          =  pObj->ref;
  pconf->socketRef        = socketId;
  pconf->xmitPool         = NULL; /* use the default XMIT pool ref */
  pconf->recvPool         = pObj->recvPool; /* Use a big buffer pool */
  pconf->userRcvReadyCallBack = (ruc_pf_sock_t)NULL ;

  if ((pObj->tcpCnxRef = uma_tcp_create_rcvRdy_bufPool(pconf)) == (uint32_t)-1) {
    severe( "uma_tcp_create" );
    return RUC_NOK;
  }

  /* Tune the socket and connect with the socket controller */
  if (uma_tcp_createTcpConnection(pObj->tcpCnxRef,name) != RUC_OK) {
    severe( "uma_tcp_createTcpConnection" );
    uma_dbg_free(pObj);
    return RUC_NOK;
  }

  /* Remove the debud session context from the free list */
  ruc_objRemove((ruc_obj_desc_t*)pObj);
  /* Set the context in the active list */
  ruc_objInsertTail((ruc_obj_desc_t*)uma_dbg_activeList,(ruc_obj_desc_t*)pObj);

  return RUC_OK;
}
/*
**-------------------------------------------------------
**  #SYNOPSIS
**   creation of the TCP server connection for debug sessions
**   IN:    void
**   OUT :  void
**-------------------------------------------------------
*/
void uma_dbg_init(uint32_t nbElements,uint32_t ipAddr, uint16_t serverPort) {
  ruc_tcp_server_connect_t  inputArgs;
  UMA_DBG_SESSION_S         *p;
  ruc_obj_desc_t            *pnext ;
  void                      *idx;
  uint32_t                    tcpCnxServer;

  /* Service already initialized */
  if (uma_dbg_initialized) {
    severe( "Service already initialized" );
    return;
  }
  uma_dbg_initialized = TRUE;
  
  uptime = time(0);

  /* Create a distributor of debug sessions */
  uma_dbg_freeList = (UMA_DBG_SESSION_S*)ruc_listCreate(nbElements,sizeof(UMA_DBG_SESSION_S));
  if (uma_dbg_freeList == (UMA_DBG_SESSION_S*)NULL) {
    severe( "ruc_listCreate(%d,%d)", nbElements,(int)sizeof(UMA_DBG_SESSION_S) );
    return;
  }

  /* Loop on initializing the distributor entries */
  pnext = NULL;
  idx = 0;
  while (( p = (UMA_DBG_SESSION_S*) ruc_objGetNext(&uma_dbg_freeList->link, &pnext)) != NULL) {
    p->ref       = (void *) idx++;
    p->ipAddr    = (uint32_t)-1;
    p->port      = (uint16_t)-1;
    p->tcpCnxRef = (uint32_t)-1;
    p->recvPool  = ruc_buf_poolCreate(2,UMA_DBG_MAX_SEND_SIZE);
  }

  /* Initialize the active list */
  uma_dbg_activeList = (UMA_DBG_SESSION_S*) malloc(sizeof(UMA_DBG_SESSION_S));
  if (uma_dbg_activeList == ((UMA_DBG_SESSION_S*)NULL)) {
    severe( "uma_dbg_activeList = malloc(%d)", (int)sizeof(UMA_DBG_SESSION_S) );
    return;
  }
  ruc_listHdrInit(&uma_dbg_activeList->link);

  /* Create a TCP connection server */
  inputArgs.userRef    = 0;
  inputArgs.tcpPort    = serverPort /* UMA_DBG_SERVER_PORT */ ;
  inputArgs.priority   = 1;
  inputArgs.ipAddr     = ipAddr;
  inputArgs.accept_CBK = uma_dbg_accept_CBK;
  sprintf((char*)inputArgs.cnxName,"DBG SERVER");

  if ((tcpCnxServer = ruc_tcp_server_connect(&inputArgs)) == (uint32_t)-1) {
    severe("ruc_tcp_server_connect" );
  }
  
  uma_dbg_addTopic("who", uma_dbg_show_name);
  uma_dbg_addTopic("uptime", uma_dbg_show_uptime);
  uma_dbg_addTopic("version", uma_dbg_show_version);
  uma_dbg_addTopic_option("system", uma_dbg_system_cmd, UMA_DBG_OPTION_HIDE); 
  uma_dbg_addTopic("ps", uma_dbg_system_ps);
  uma_dbg_addTopic("reserved_ports", uma_dbg_reserved_ports);
  uma_dbg_addTopic("counters", uma_dbg_counters_reset);
}
/*
**-------------------------------------------------------
**  #SYNOPSIS
**   Give the system name to be display 
**   IN:    system_name
**   OUT :  void
**-------------------------------------------------------
*/  
void uma_dbg_set_name( char * system_name) {

  if (uma_gdb_system_name != NULL) {
    severe( "uma_dbg_set_name(%s) although name %s already set", system_name, uma_gdb_system_name );
    return;
  }  
  
  uma_gdb_system_name = malloc(strlen(system_name)+1);
  if (uma_gdb_system_name == NULL) {
    severe( "uma_dbg_set_name out of memory" );
    return;
  }
  
  strcpy(uma_gdb_system_name,system_name);
}
