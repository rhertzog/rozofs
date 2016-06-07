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
/*
**   I N C L U D E  F I L E S
*/

#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <errno.h>
#include <stdarg.h>
#include <unistd.h>
#include <time.h>

#include <rozofs/common/types.h>
#include <rozofs/core/ruc_trace_api.h>
#include <rozofs/core/ruc_common.h>
#include "rozofs_storcli_lbg_cnf_supervision.h"
#include <rozofs/core/ruc_sockCtl_api.h>
#include <rozofs/rpc/storcli_lbg_prototypes.h>
#include <rozofs/core/north_lbg_api.h>
#include "storcli_main.h"





uint32_t  storcli_sup_getIntSockIdxFromSocketId(storcli_lbg_sup_conf_t *p,int socketId);
/*
**   G L O B A L    D A T A
*/

/*
**  Call back function for socket controller
*/
ruc_sockCallBack_t storcli_sup_callBack_InternalSock=
  {
     storcli_sup_rcvReadyInternalSock,
     storcli_sup_rcvMsgInternalSock,
     storcli_sup_xmitReadyInternalSock,
     storcli_sup_xmitEvtInternalSock
  };

storcli_lbg_sup_conf_t   storcli_sup_lbg_ctx;


 uint32_t storcli_lbg_sup_conf_trace = TRUE;



/*----------------------------------------------
**  storcli_sup_getObjRef
**----------------------------------------------
**
**  that function builds the event message and
**  sends it to the internal socket.
**
**
**  IN :
**   none
**
**  OUT :ptr to the timer context
**
**-----------------------------------------------
*/

storcli_lbg_sup_conf_t *storcli_sup_getObjRef()
{
  return (storcli_lbg_sup_conf_t *)&storcli_sup_lbg_ctx;
}

/*
**_________________________________________________________________________
*/
/**
* send the configuration of a load balancing group to the non blocking side
  It is assumed that the load balancing group has been previously created

  The message is sent over the socket AF_UNIX pair.
  
  @param mstorage : pointer to the storage node data structure
  

  @retval 0 success
  @retval -1 error
*/
int storcli_sup_send_lbg_port_configuration(mstorage_t *mstorage)
{
  int nBytes;
  uint32_t  response;
  storcli_sup_msg_t msg;
  storcli_lbg_sup_conf_t *p = &storcli_sup_lbg_ctx;

  msg.opcode = STORCLI_LBG_ADD;
  msg.param = mstorage;

  nBytes = send(p->internalSocket[RUC_SOC_SEND],
                (const char *)&msg,
                sizeof(storcli_sup_msg_t),
                0);
  if (nBytes != sizeof(storcli_sup_msg_t))
  {
    /*
    **  message not sent
    */
    mstorage->error = errno;
    severe("storcli_sup_send_lbg_port_configuration : %s",strerror(errno));
    return -1;
  }
  /*
  ** OK now wait for the response
  */
    nBytes = recv(p->internalSocket[RUC_SOC_SEND],
                   (char *)&response,
                   sizeof(uint32_t),
                   0);
  if (nBytes != sizeof(uint32_t))
  {
    /*
    **  something wrong : (BUG)
    */
    mstorage->error = errno;
    severe("storcli_sup_send_lbg_port_configuration : %s",strerror(errno));
    return -1;
  }
  return (int)response;
}
#if 0
/*
**_________________________________________________________________________
*/
/**
*  send a message for creating a load balancing group without any configuration

  @param mstorage: pointer to the storage node data structure
  
  @retval 0 on success
  @retval -1 on error
*/
int storcli_sup_send_lbg_create(uint32_t opcode, void *mstorage , int index)
{
  int nBytes;
  storcli_sup_msg_t msg;
  uint32_t  response;
  storcli_lbg_sup_conf_t *p = &storcli_sup_lbg_ctx;
  mstorage_t *mstor = (mstorage_t*) mstorage ;

  msg.opcode = opcode;
  msg.param = mstorage;
  msg.filler = index;

  nBytes = send(p->internalSocket[RUC_SOC_SEND],
                (const char *)&msg,
                sizeof(storcli_sup_msg_t),
                0);
  if (nBytes != sizeof(storcli_sup_msg_t))
  {
    /*
    **  message not sent
    */
    severe("storcli_sup_send_lbg_create : %s",strerror(errno));
    return -1;

  }
  /*
  ** OK now wait for the response
  */
    nBytes = recv(p->internalSocket[RUC_SOC_SEND],
                   (char *)&response,
                   sizeof(uint32_t),
                   0);
  if (nBytes != sizeof(uint32_t))
  {
    /*
    **  something wrong : (BUG)
    */
    severe("storcli_sup_send_lbg_create : %s",strerror(errno));
    return -1;
  }
  return (int)response;
} 
#endif

/*----------------------------------------------
**  storcli_sup_getIntSockIdxFromSocketId
**----------------------------------------------
**
**   That function returns the internal
**   socket index associated to socketId.
**   If the socketId is not found it
**   return -1.
**
**
**  IN :
**     p : Relci object pointer
**     socketId : socket Identifier to search
**
**  OUT : -1:not found
**       <>-1: found (RUC_SOC_SEND or RUC_SOC_RECV)
**
**-----------------------------------------------
*/

uint32_t  storcli_sup_getIntSockIdxFromSocketId(storcli_lbg_sup_conf_t *p,int socketId)
{
   int i;


   for (i = 0;i < 2;i++)
   {
     if (p->internalSocket[i]==socketId) return (uint32_t)i;
   }
   return -1;
}



/*----------------------------------------------
**  storcli_sup_rcvReadyInternalSock
**----------------------------------------------
**
**   receive ready function: only for
**   receiver socket. Nothing expected
**   on sending socket
**
**
**  IN :
**     not_significant : Relci instance index
**     socketId : socket Identifier
**
**  OUT : always TRUE for RUC_SOC_RECV
**        always FALSE for RUC_SOC_SEND
**
**-----------------------------------------------
*/
uint32_t storcli_sup_rcvReadyInternalSock(void * not_significant,int socketId)
{
  storcli_lbg_sup_conf_t *p;
  uint32_t      socketIdx;

  /*
  **  Get the pointer to the timer Object
  */
  p = &storcli_sup_lbg_ctx;
  socketIdx = storcli_sup_getIntSockIdxFromSocketId(p,socketId);
  if (socketIdx == -1)
  {
    /*
    ** something really wrong
    */
    RUC_WARNING(p);
    return FALSE;
  }
  if (socketIdx == RUC_SOC_SEND)
    return FALSE;
  else
    return TRUE;
}

/*----------------------------------------------
**  storcli_sup_rcvMsgInternalSock
**----------------------------------------------
**
**   receive  function: only for
**   receiver socket. Nothing expected
**   on sending socket. It indicates
**   that there is an internal message
**   pending for the Relci instance
**
**
**  IN :
**     not_significant : Relci instance index
**     socketId : socket Identifier
**
**  OUT : always TRUE for RUC_SOC_RECV
**        always FALSE for RUC_SOC_SEND
**
**-----------------------------------------------
*/
uint32_t storcli_sup_rcvMsgInternalSock(void * not_significant,int socketId)
{
  storcli_lbg_sup_conf_t *p;
  uint32_t      socketIdx;
  int         bytesRcvd;
  storcli_sup_msg_t msg;
  int ret;
  uint32_t retcode;
  int nBytes;

  /*
  **  Get the pointer to the timer Object
  */
  p = &storcli_sup_lbg_ctx;
  socketIdx = storcli_sup_getIntSockIdxFromSocketId(p,socketId);
  if (socketIdx == -1)
  {
    /*
    ** something really wrong
    */
    RUC_WARNING(p);
    return FALSE;
  }

  if (socketIdx == RUC_SOC_SEND)
  {
    /*
    **  should not occur
    */
    RUC_WARNING(socketId);
    return FALSE;
  }
  /*
  **  Ticker received
  **
  */
  bytesRcvd = recv(socketId,
                   (char *)&msg,
                   sizeof(storcli_sup_msg_t),
                   0);
  if (bytesRcvd != sizeof(storcli_sup_msg_t))
  {
    /*
    **  something wrong : (BUG)
    */
    RUC_WARNING(errno);
    return TRUE;
  }
  /*
  **  process the signal
  */
  switch ( msg.opcode)
  {
    case STORCLI_LBG_ADD:
      ret = rozofs_storcli_setup_all_lbg_of_storage( (mstorage_t*)msg.param);
      if (ret < 0)
      {
        fatal("Cannot configure Load Balancing Group");                       
      }   
      retcode = (uint32_t) ret;   
      nBytes = send(p->internalSocket[RUC_SOC_RECV],
                    (const char *)&retcode,
                    sizeof(retcode),
                    0);
      if (nBytes != sizeof(retcode))
      {
        /*
        **  message not sent
        */
        fatal("error on sending response");

      }         
      break;
      
#if 0      
    case STORCLI_LBG_CREATE:
      mstorage_t *storage_p = (mstorage_t*)msg.param;
      retcode = 0;
      storage_p->lbg_id[msg.filler] = north_lbg_create_no_conf();
      if (storage_p->lbg_id[msg.filler] < 0)
      {
        fatal("Cannot configure Load Balancing Group");      
        retcode = (uint32_t) -1;                 
      }  
      
      nBytes = send(p->internalSocket[RUC_SOC_RECV],
                    (const char *)&retcode,
                    sizeof(retcode),
                    0);
      if (nBytes != sizeof(retcode))
      {
        /*
        **  message not sent
        */
        fatal("error on sending response");

      }         
      break;
#endif

    default:
      RUC_WARNING(msg.opcode);
      break;
  }

  return TRUE;
}


/*----------------------------------------------
**  storcli_sup_xmitReadyInternalSock
**----------------------------------------------
**
**   xmit ready function: only for
**   xmit socket. Nothing expected
**   on receiving socket.
**
**
**  IN :
**     not_significant : Relci instance index
**     socketId : socket Identifier
**
**  OUT : always FALSE for RUC_SOC_RECV
**        always FALSE for RUC_SOC_SEND
**  There is not congestion on the internal socket
**
**-----------------------------------------------
*/
uint32_t storcli_sup_xmitReadyInternalSock(void * not_significant,int socketId)
{
  storcli_lbg_sup_conf_t *p;
  uint32_t      socketIdx;

  /*
  **  Get the pointer to the timer Object
  */
  p = &storcli_sup_lbg_ctx;
  socketIdx = storcli_sup_getIntSockIdxFromSocketId(p,socketId);
  if (socketIdx == -1)
  {
    /*
    ** something really wrong
    */
    RUC_WARNING(p);
    return FALSE;
  }

  if (socketIdx == RUC_SOC_RECV)
    return FALSE;
  else
    return FALSE;
}


/*----------------------------------------------
**  storcli_sup_xmitEvtInternalSock
**----------------------------------------------
**
**   xmit event  function: only for
**   xmit socket.
**   That function should never be encountered
**
**
**  IN :
**     intSockRef : either RUC_SOC_SEND or
**                         RUC_SOC_RECV
**     socketId : socket Identifier
**
**  OUT : always FALSE for RUC_SOC_RECV
**        always TRUE for RUC_SOC_SEND
**
**-----------------------------------------------
*/
uint32_t storcli_sup_xmitEvtInternalSock(void * not_significant,int socketId)
{
  return FALSE;
}
/*
**    I N T E R N A L   S O C K E T
**    CREATION/DELETION
*/


/*----------------------------------------------
**  storcli_sup_createInternalSocket (private)
**----------------------------------------------
**
**  That function is intented to create a
**  socket pair. That socket pair is used
**  for sending internal event back to the
**  timer instance
**
**  IN :
**     p : pointer to the timer instance
**
**  OUT :
**    RUC_OK : all is fine
**    RUC_NOK : unable to create the internal
**              socket.
**
**  note : the socket is configured as an asynchronous
**         socket.(sending socket only)
**-----------------------------------------------
*/

uint32_t storcli_sup_createInternalSocket(storcli_lbg_sup_conf_t *p)
{
  int    ret;
  uint32_t retcode = RUC_NOK;
//  int    fileflags;


  /*
  **  1 - create the socket pair
  */

  ret = socketpair(  AF_UNIX,
                  SOCK_DGRAM,
                  0,
                  &p->internalSocket[0]);

  if (ret < 0)
  {
    /*
    ** unable to create the sockets
    */
    RUC_WARNING(errno);
    return RUC_NOK;
  }
  while (1)
  {
#if 0
    /*
    ** change socket mode to asynchronous
    */
    if((fileflags=fcntl(p->internalSocket[RUC_SOC_SEND],F_GETFL,0))==-1)
    {
      RUC_WARNING(errno);
      break;
    }
    if(fcntl(p->internalSocket[RUC_SOC_SEND],F_SETFL,fileflags|O_NDELAY)==-1)
    {
      RUC_WARNING(errno);
      break;
    }
#endif
    /*
    ** 2 - perform the connection with the socket controller
    */
    p->intSockconnectionId[RUC_SOC_SEND]=
                                     ruc_sockctl_connect(p->internalSocket[RUC_SOC_SEND],
                                     "CNF_SOCK_XMIT",
                                      0,
                                      p,
                                      &storcli_sup_callBack_InternalSock);
    if (p->intSockconnectionId[RUC_SOC_SEND]== NULL)
    {
      RUC_WARNING(RUC_SOC_SEND);
      break;
    }
    p->intSockconnectionId[RUC_SOC_RECV]=
                 ruc_sockctl_connect(p->internalSocket[RUC_SOC_RECV],
                                     "CNF_SOCK_RECV",
                                      0,
                                      p,
                                      &storcli_sup_callBack_InternalSock);
    if (p->intSockconnectionId[RUC_SOC_RECV]== NULL)
    {
      RUC_WARNING(RUC_SOC_SEND);
      break;
    }
    /*
    **  done
    */
    retcode = RUC_OK;
    break;

  }
  if (retcode != RUC_OK)
  {
    /*
    ** something wrong: close the sockets and disconnect
    **  from socket controller
    */
    close (p->internalSocket[RUC_SOC_SEND]);
    close (p->internalSocket[RUC_SOC_RECV]);
    if (p->intSockconnectionId[RUC_SOC_RECV] != NULL)
    {
      ruc_sockctl_disconnect(p->intSockconnectionId[RUC_SOC_RECV]);
      p->intSockconnectionId[RUC_SOC_RECV] = NULL;
    }
    if (p->intSockconnectionId[RUC_SOC_SEND] != NULL)
    {
      ruc_sockctl_disconnect(p->intSockconnectionId[RUC_SOC_SEND]);
      p->intSockconnectionId[RUC_SOC_SEND] = NULL;
    }
    return RUC_NOK;
  }
  return RUC_OK;
}


/*----------------------------------------------
**  storcli_sup_deleteInternalSocket (private)
**----------------------------------------------
**
** That function is called when a Recli
** instance is deleted:
**
**   That function performs:
**    -  the closing of the socket pair
**    -  the socket controller disconnection
**    -  the purge of the signal queue list
**
**
**  IN :
**     p : pointer to the timer instance
**
**  OUT :
**    RUC_OK : all is fine
**    RUC_NOK :
**-----------------------------------------------
*/

uint32_t storcli_sup_deleteInternalSocket(storcli_lbg_sup_conf_t *p)
{

  if (p->internalSocket[RUC_SOC_SEND] != -1)
  {
    close (p->internalSocket[RUC_SOC_SEND]);
    p->internalSocket[RUC_SOC_SEND] = -1;
  }
  if (p->internalSocket[RUC_SOC_RECV] != -1)
  {
    close (p->internalSocket[RUC_SOC_RECV]);
    p->internalSocket[RUC_SOC_SEND] = -1;
  }
  if (p->intSockconnectionId[RUC_SOC_RECV] != NULL)
  {
    ruc_sockctl_disconnect(p->intSockconnectionId[RUC_SOC_RECV]);
    p->intSockconnectionId[RUC_SOC_RECV] = NULL;
  }
  if (p->intSockconnectionId[RUC_SOC_SEND] != NULL)
  {
    ruc_sockctl_disconnect(p->intSockconnectionId[RUC_SOC_SEND]);
    p->intSockconnectionId[RUC_SOC_SEND] = NULL;
  }

  return RUC_OK;
}



/*----------------------------------------------
**  storcli_sup_moduleInit (public)
**----------------------------------------------
**
**
**  IN :
*
**
**  OUT :
**    RUC_OK/RUC_NOK
**
**-----------------------------------------------
*/
uint32_t storcli_sup_moduleInit()

{
  storcli_lbg_sup_conf_t *p;
  uint32_t      ret;
  /*
  **  Get the pointer to the timer Object
  */
  p = storcli_sup_getObjRef();

   /*
   ** create the internal socket
   */

   ret = storcli_sup_createInternalSocket(p);
   return ret;
}
