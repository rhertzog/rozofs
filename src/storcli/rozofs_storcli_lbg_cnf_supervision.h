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

#ifndef ROZOFS_STORCLI_LBG_CNF_SUPERVISION_H
#define ROZOFS_STORCLI_LBG_CNF_SUPERVISION_H
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <errno.h>
#include <stdarg.h>
#include <unistd.h>
#include <time.h>

#include "config.h"
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>

#include <rozofs/common/types.h>
#include <rozofs/core/ruc_common.h>
#include <rozofs/core/ruc_sockCtl_api.h>
#include <rozofs/rpc/eclient.h>

typedef struct storcli_lbg_sup_conf_t
{
  int       internalSocket[2];  /* -1 if N.S */
  void *    intSockconnectionId[2];  /* -1: connection id returned by the sock Ctrl */
} storcli_lbg_sup_conf_t;


#define STORCLI_LBG_ADD     1  /**< opcode for adding a storage node LBGs  */
#define STORCLI_LBG_CREATE  2  /**< deprecated !! opcode for creating a load balancing group with no config. */

/**
* message structue echanged between the thread that monitor the port configuration of
* a load balancing group and the non blocking entity
* The goal of the exchange is to trigger the configuration of the ports once the 
* the storage provides its listening ports
*/
typedef struct _storcli_sup_msg_t
{
    uint32_t  opcode;
    uint32_t  filler;
    void      *param;
} storcli_sup_msg_t;


storcli_lbg_sup_conf_t *storcli_sup_getObjRef();
int storcli_sup_send_lbg_port_configuration(mstorage_t *mstorage);
uint32_t  storcli_sup_getIntSockIdxFromSocketId(storcli_lbg_sup_conf_t *p,int socketId);
uint32_t storcli_sup_rcvReadyInternalSock(void * not_significant,int socketId);
uint32_t storcli_sup_rcvMsgInternalSock(void * not_significant,int socketId);
uint32_t storcli_sup_xmitReadyInternalSock(void * not_significant,int socketId);
uint32_t storcli_sup_xmitEvtInternalSock(void * not_significant,int socketId);
uint32_t storcli_sup_createInternalSocket(storcli_lbg_sup_conf_t *p);
uint32_t storcli_sup_deleteInternalSocket(storcli_lbg_sup_conf_t *p);
int storcli_sup_send_lbg_create(uint32_t opcode, void *mstorage , int index);
uint32_t storcli_sup_moduleInit();

#endif
