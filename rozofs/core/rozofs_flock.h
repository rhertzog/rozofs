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

#ifndef _ROZOFS_FLOCK_H
#define _ROZOFS_FLOCK_H

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#include <rozofs/common/log.h>
#include <rozofs/rpc/eproto.h>

/*
**______________________________________________________________________________
*/
/** Prepare lock information for tracing
 *
 * @param e: the export managing the file or directory.
 * @param fid: the id of the file or directory.
 * @param lock: the lock to set/remove
 * 
 * @return: On success, the size of the extended attribute value.
 * On failure, -1 is returned and errno is set appropriately.
 */
char * flock_request2string(char * buffer, fid_t fid, ep_lock_t * lock) {
  char * pChar = buffer;

  fid2string(fid,pChar);
  pChar += 37;
  pChar += sprintf(pChar," client=%"PRIu64" owner=%"PRIu64" ", lock->client_ref, lock->owner_ref);
  switch(lock->mode) {
    case EP_LOCK_FREE:  pChar += sprintf(pChar,"FR "); break;
    case EP_LOCK_READ:  pChar += sprintf(pChar,"RD "); break;
    case EP_LOCK_WRITE: pChar += sprintf(pChar,"WR "); break;
    default:            pChar += sprintf(pChar,"%d ",lock->mode);
  }  
  pChar += sprintf(pChar,"[%"PRIu64":%"PRIu64"[",
	       (uint64_t) lock->user_range.offset_start,
	       (uint64_t) lock->user_range.offset_stop);
  return pChar;
}

#endif  
