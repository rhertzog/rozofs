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
#include <stdint.h>
#include <stdio.h>
#include <uuid/uuid.h>
#include <stdlib.h>
#include <string.h>
#include <rozofs/rozofs.h>
#include <rozofs/common/log.h>
#include "config.h"
#include "storcli_ring.h"


typedef struct _stc_rng_entry_t
{
   fid_t fid;     /**< unique file identifier    */
   uint64_t bid;  /**< first block index         */
   uint64_t nb_blks;  /**< number of blocks                   */
   void   *obj_ptr;   /**< object context pointer             */
   uint8_t opcode;   /**< opcode associated with the request */
   uint8_t state;   /**< 0: RUN/1:WAIT */
} stc_rng_entry_t;


stc_rng_entry_t *stc_rng_request_p = NULL;
uint16_t stc_rng_rd_idx;
uint16_t stc_rng_wr_idx;

uint64_t stc_rng_collision_count;
uint64_t stc_rng_submit_count;
uint64_t stc_rng_parallel_count;
int      stc_rng_serialize; 

/*
**____________________________________________________________
*/
/**
*   init of the ring 

   @param none
   
   @retval 0 on success
   @retval < 0 error, see errno for details

*/
int stc_rng_init()
{
    
    stc_rng_request_p = malloc(sizeof(stc_rng_entry_t)*STORCLI_RING_SZ);
    if (stc_rng_request_p == NULL)
    {
       severe("Out of memory ");
       return -1;
    }
    memset(stc_rng_request_p,0,sizeof(stc_rng_entry_t)*STORCLI_RING_SZ);
    stc_rng_rd_idx = 0;
    stc_rng_wr_idx = 0;
    stc_rng_collision_count = 0;
    stc_rng_submit_count = 0;
    stc_rng_serialize = 0;
    stc_rng_parallel_count = 0;
    
    return 0;
}
/*
**____________________________________________________________
*/
/**
*     ring insertion:

    insert the new entry in the ring. During the insert the service
    check if the request is runnable.
    
    @param obj_ptr : pointer to the object to insert
    @param opcode : opcode of the request
    @param fid : unique identifier of the object (key)
    @param bid : block index (key)
    @param nb_blks : number of blocks (key)
    @param entry_idx_p : pointer to the entry idx that has been allocated in the ring
    
    @retval 0 : non runnable
    @retval 1 : runnable
*/
int stc_rng_insert(void *obj_ptr,uint8_t opcode, fid_t fid,uint64_t bid,uint64_t nb_blks,int *entry_idx_p)
{
   int runnable = 1;
   stc_rng_entry_t *p;
   /*
   ** check if there is some collosion with the requests that are in progress
   */
   stc_rng_submit_count++;
   uint16_t cur_idx = stc_rng_rd_idx;
   int found = 0;
   for (cur_idx = stc_rng_rd_idx;cur_idx!= stc_rng_wr_idx;cur_idx = (cur_idx+1)%STORCLI_RING_SZ )
   {
      p = &stc_rng_request_p[cur_idx];
      if (p->obj_ptr == NULL) continue;
      if (memcmp(p->fid,fid,sizeof(fid_t))!= 0) continue;
      if (stc_rng_serialize)
      {
        runnable = 0;
	break;
      }
      /*
      ** we have the same FID check the block offset and the number of blocks
      */
      if ((bid+nb_blks <= p->bid) || (bid >= p->bid+p->nb_blks))
      {
        /*
	** no collision -> check the next entry
	*/
	found++;
	continue;
      }
      /*
      ** there is a collision
      */
      found = 0;
      stc_rng_collision_count++;
      runnable = 0;
      break;
   }

   /*
   **  insert the entry
   */
   if (found) stc_rng_parallel_count++;
   p = &stc_rng_request_p[stc_rng_wr_idx];
   memcpy(p->fid,fid,sizeof(fid_t));
   p->bid = bid;
   p->nb_blks = nb_blks;
   if (p->obj_ptr != NULL)
   {
      severe("!!!!!ring is FULL");
   }
   p->obj_ptr = obj_ptr;
   p->opcode = opcode;
   p->state = runnable;
   *entry_idx_p = stc_rng_wr_idx;
   stc_rng_wr_idx = (stc_rng_wr_idx+1)%STORCLI_RING_SZ;
   return runnable;
}
/*
**____________________________________________________________
*/
/**
*  remove an entry from the ring

   removing the entry consist in clearing the object pointer associated with the 
   entry to remove.
   If there is a new runnable request, the service returns the pointer and the opcode
   associated with that entry
   
   @param entry_idx : index of the ring entry to remove

   @retval 0 on success
   @retval < 0 on error
*/
int stc_rng_release_entry(int entry_idx, void **next_running_p,
        uint8_t *opcode_p) {

    stc_rng_entry_t *p;
    stc_rng_entry_t *cur_p;
    int cur_idx = 0;
    *next_running_p = NULL;

    if (entry_idx >= STORCLI_RING_SZ) {
        /*
         ** the entry index is out of range
         */
        severe("out of range ring index %d", entry_idx);
        return -1;
    }
    cur_p = &stc_rng_request_p[entry_idx];
    cur_p->obj_ptr = NULL;
   
    /*
     ** update read index of the entry match the current one
     */
    if (stc_rng_rd_idx == entry_idx) {
        stc_rng_rd_idx = (stc_rng_rd_idx + 1) % STORCLI_RING_SZ;
    }

    int read_idx_increment_allowed = 1;
    for (cur_idx = stc_rng_rd_idx; cur_idx != stc_rng_wr_idx;
            cur_idx = (cur_idx + 1) % STORCLI_RING_SZ) {
        p = &stc_rng_request_p[cur_idx];
        if (p->obj_ptr == NULL) {
            if (read_idx_increment_allowed) {
                stc_rng_rd_idx = (stc_rng_rd_idx + 1) % STORCLI_RING_SZ;
            }
            continue;
        }
        read_idx_increment_allowed = 0;
        if (memcmp(p->fid, cur_p->fid, sizeof(fid_t)) != 0)
            continue;
        /**
         * exit from loop if the entry to release is not the first for this FID
         */
        if (p->state == STORCLI_RUN)
            break;

        /*
         ** the entry is waiting: schedule it
         */
        *next_running_p = p->obj_ptr;
        p->state = STORCLI_RUN;
        *opcode_p = p->opcode;
        break;

    }
    return 0;
}


