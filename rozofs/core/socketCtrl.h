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
#ifndef SOCKET_CTRL_H
#define SOCKET_CTRL_H

#include <rozofs/common/types.h>
#include <sys/select.h>
#include "ruc_common.h"
#include "ruc_list.h"
#include "ruc_sockCtl_api.h"


/*
** file descriptor for receiving and transmitting events
*/
//extern fd_set  rucRdFdSet;   
//extern fd_set  rucWrFdSet;   
extern rozo_fd_set  rucWrFdSetCongested;
/*
**  private API
*/

void ruc_sockCtl_checkRcvBits();
void ruc_sockCtl_prepareRcvBits();
void ruc_sockCtl_checkXmitBits();
void ruc_sockCtl_prepareXmitBits();

#endif
