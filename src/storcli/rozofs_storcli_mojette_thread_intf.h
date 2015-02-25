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
 
#ifndef STORIO_MOJETTE_THREAD_INTF_H
#define STORIO_MOJETTE_THREAD_INTF_H

#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <errno.h>
#include <rozofs/rozofs.h>
#include "config.h"
#include <rozofs/common/log.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/core/rozofs_socket_family.h>
#include "rozofs_storcli.h"


#define ROZOFS_MAX_DISK_THREADS  4


typedef struct _rozofs_disk_thread_stat_t {
  uint64_t            MojetteInverse_count;
  uint64_t            MojetteInverse_Byte_count;
  uint64_t            diskRead_error;
  uint64_t            diskRead_error_spare;  
  uint64_t            diskRead_nosuchfile;
  uint64_t            diskRead_badCidSid;
  uint64_t            MojetteInverse_time;
  uint64_t            MojetteInverse_cycle;
  
  uint64_t            MojetteForward_count;
  uint64_t            MojetteForward_Byte_count;
  uint64_t            diskWrite_error;
  uint64_t            diskWrite_badCidSid;  
  uint64_t            MojetteForward_time;
  uint64_t            MojetteForward_cycle;

} rozofs_disk_thread_stat_t;
/*
** Disk thread context
*/
typedef struct _rozofs_mojette_thread_ctx_t
{
  pthread_t                    thrdId; /* of disk thread */
  int                          thread_idx;
  char                       * hostname;  
  int                          eid;  
  int                          storcli_idx;  
  int                          sendSocket;
  rozofs_disk_thread_stat_t    stat;
} rozofs_mojette_thread_ctx_t;

extern rozofs_mojette_thread_ctx_t rozofs_mojette_thread_ctx_tb[];
extern int rozofs_stcmoj_thread_write_enable;
extern int rozofs_stcmoj_thread_read_enable;
extern uint32_t rozofs_stcmoj_thread_len_threshold;

/**
* Message sent/received in the af_unix disk sockets
*/

typedef enum _rozofs_stcmoj_thread_request_e {
  STORCLI_MOJETTE_THREAD_INV = 0,
  STORCLI_MOJETTE_THREAD_FWD = 1
} rozofs_stcmoj_thread_request_e;

typedef struct _rozofs_stcmoj_thread_msg_t
{
  uint32_t            msg_len;
  uint32_t            opcode;
  uint32_t            status;
  uint32_t            transaction_id;
  uint64_t            timeStart;
  uint64_t            size;
  void              * working_ctx;
} rozofs_stcmoj_thread_msg_t;

/*__________________________________________________________________________
* Initialize the disk thread interface
*
* @param hostname    storio hostname (for simulation)
* @param eid   index of the exportd
* @param storcli    relative of the storcli 
* @param nb_threads  Number of threads that can process the disk requests
* @param nb_buffer   Number of buffer for sending and number of receiving buffer
*
*  @retval 0 on success -1 in case of error
*/
int rozofs_stcmoj_thread_intf_create(char * hostname,int eid,int storcli_idx, int nb_threads, int nb_buffer);

/*__________________________________________________________________________
*/
/**
*  Send a disk request to the disk threads
*
* @param opcode     the request operation code
* @param working_ctx     pointer to the generic rpc context
* @param timeStart  time stamp when the request has been decoded
*
* @retval 0 on success -1 in case of error
*  
*/
int rozofs_stcmoj_thread_intf_send(rozofs_stcmoj_thread_request_e   opcode, 
                                 rozofs_storcli_ctx_t            * working_ctx,
				 uint64_t                       timeStart) ;

/*
**__________________________________________________________________________
*/
/**
*  Thar API is intended to be used by a disk thread for sending back a 
   disk response (read/write or truncate) towards the main thread
   
   @param thread_ctx_p: pointer to the thread context (contains the thread source socket )
   @param msg: pointer to the message that contains the disk response
   @param status : status of the disk operation
   
   @retval none
*/
void storio_send_response (rozofs_mojette_thread_ctx_t *thread_ctx_p, rozofs_stcmoj_thread_msg_t * msg, int status);


/*__________________________________________________________________________
* Enable/disable the mojette threads for write
*
* @param enable      1 to enable, 0 to disable 
*/
void rozofs_stcmoj_thread_enable_write(int enable) ;
/*__________________________________________________________________________
* Enable/disable the mojette threads for read
*
* @param enable      1 to enable, 0 to disable 
*/
void rozofs_stcmoj_thread_enable_read(int enable) ;
/*__________________________________________________________________________
* Set the threshold to call the mojette threads
*
* @param threshold      The threshold in number of blocks
*/
void rozofs_stcmoj_thread_set_threshold(int nbBlocks) ;
/*__________________________________________________________________________
* Reset to the default parameters
*
*/
void rozofs_stcmoj_thread_set_default() ;
#endif
