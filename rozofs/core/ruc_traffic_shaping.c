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
#include <stdlib.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <fcntl.h> 
#include <sys/un.h>             
#include <sys/time.h>
#include <errno.h>  
#include <rozofs/common/types.h>
#include <rozofs/common/log.h>
#include "ruc_common.h"
#include "ruc_list.h"
#include "ruc_buffer_api.h"
#include "uma_dbg_api.h"
#include "ruc_traffic_shaping.h"
#include "ruc_sockCtl_api.h"

/*
** Define MICROLONG if not yet done
*/
#ifndef MICROLONG
#define MICROLONG(time) ((unsigned long long)time.tv_sec * 1000000 + time.tv_usec)
#endif

/**
* Global data
*/

trshape_ctx_t storcli_traffic_shaper[TRSHAPE_MAX_CTX]; 

/**
*  fake function
*/
trshape_ctx_t *trshape_get_ctx_from_idx(int idx)
{
  return &storcli_traffic_shaper[0];
 
}

#define TRAFFIC_SHAPER_COUNTER(name) pchar += sprintf(pchar," %-20s : %llu\n",#name,(long long unsigned int)p->stats.name);
static char * show_traffic_shaper_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"shaper set <value> : set shaper value\n");
  pChar += sprintf(pChar,"shaper reset       : reset statistics\n");
  pChar += sprintf(pChar,"shaper             : display statistics\n");
  return pChar; 
}  
void show_traffic_shaper(char * argv[], uint32_t tcpRef, void *bufRef) 
{
    char *pchar = uma_dbg_get_buffer();
   trshape_ctx_t *p = trshape_get_ctx_from_idx(0) ;
    int byte_time = 0;
    
    if (argv[1] != NULL)
    {
      if (strcmp(argv[1],"reset")==0) 
      {
        memset(&p->stats,0,sizeof(trshape_stats_t));     
        uma_dbg_send(tcpRef, bufRef, TRUE, "Reset Done\n");    
        return;      
      }
      if (strcmp(argv[1],"set")==0) 
      {
        errno = 0;       
        byte_time = (int) strtol(argv[2], (char **) NULL, 10);   
        if (errno != 0) {
	 pchar += sprintf(pchar,"Bad byte time value %s\n",argv[2]);    
	 pchar = show_traffic_shaper_help(pchar);
         uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
         return;     
        }  
        p->byte_time_ns = (uint64_t) byte_time; 
        p->byte_time_ns_adjusted = (uint64_t) byte_time; 
        uma_dbg_send(tcpRef, bufRef, TRUE, "Done\n"); 
        return;   
      }  
      pchar = show_traffic_shaper_help(pchar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
      return;                
    }

   pchar += sprintf(pchar,"Byte time     (1/10 ns): %llu\n", (long long unsigned int) p->byte_time_ns);
   pchar += sprintf(pchar,"Estimated time(1/10 ns): %llu\n", (long long unsigned int) p->byte_time_ns_adjusted);
   pchar += sprintf(pchar,"Next time (us)    : %llx\n",(long long unsigned int)  p->next_time);
   
   TRAFFIC_SHAPER_COUNTER(submit_count);
   TRAFFIC_SHAPER_COUNTER(enqueued_count);
   TRAFFIC_SHAPER_COUNTER(dequeued_count);
   TRAFFIC_SHAPER_COUNTER(anticipation_count);
   TRAFFIC_SHAPER_COUNTER(deadline_count);
   TRAFFIC_SHAPER_COUNTER(forcedeq_count);
   TRAFFIC_SHAPER_COUNTER(send_violation_count);
      

  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}

/*__________________________________________________________________________
*/
/**
* Initialize a traffic shaping context

  @param p : the traffic shaping context
  @param
  
  retval none
*/
void trshape_ctx_init(trshape_ctx_t *p, int index,uint64_t byte_time_ns)
{
  struct timeval     timeDay;

  ruc_listHdrInit((ruc_obj_desc_t *) &p->link);
  p->index = index;
  p->free = 0;
  p->integrity = 0; 
  gettimeofday(&timeDay,(struct timezone *)0);  
  p->next_time = MICROLONG(timeDay);
  p->byte_time_ns = byte_time_ns;
  p->byte_time_ns_adjusted = byte_time_ns;
  p->noempty2empty = 0;
  p->quantum_percent = RUC_BYTE_QUANTUM_PERCENT_DEFAULT;
  p->sent = 0;
   memset(&p->stats,0,sizeof(trshape_stats_t)); 

}
/*__________________________________________________________________________
*/
/**
* schedule a traffic shaping queue

  @param p : traffic shaping queue 
  @param cur_time: current time in us
  
  retval 0 : OK
  retval < 0 error (no scheduler queue)
*/
int trshape_schedule_queue(trshape_ctx_t *p,uint64_t cur_time)
{
   ruc_buf_shaping_ctx_t *buf_shaping_ctx_p;  
   int8_t inuse;
   ruc_obj_desc_t *pnext;
   void *buf_p;
   struct timeval     timeDay;
   int ret;
   uint64_t new_time;
   uint64_t rsp_time;
   uint64_t check_time;
   uint64_t *pstat;
   
   
   pnext = (ruc_obj_desc_t*)NULL;
   while ((buf_p = (void*) ruc_objGetNext(&p->link,&pnext))!=NULL) 
   { 
      /*
      ** get the traffic shaping context
      */
      buf_shaping_ctx_p = ruc_buffer_get_shaping_ctx(buf_p);
      if (p->sent == 0) 
      {  
        check_time = p->next_time;
        pstat = &p->stats.anticipation_count;
      }
      else 
      {
        check_time = p->next_time + (buf_shaping_ctx_p->rsp_time * p->byte_time_ns_adjusted)/10000 ;
        pstat = &p->stats.deadline_count;
      } 
//       info("p->sent %d check_time %llu cur_time %llu delta %llu ",p->sent,check_time,cur_time,(cur_time - check_time));

      if (cur_time >= check_time)
      {
        /*
        ** we can send
        */
        ruc_objRemove((ruc_obj_desc_t*)buf_p);
        /*
        ** update stats
        */
        p->stats.dequeued_count++;  
        (*pstat)+=1;
//        p->next_time = cur_time + buf_shaping_ctx_p->rsp_time;
        ret = (*buf_shaping_ctx_p->sched_callBackFct)(buf_shaping_ctx_p->lbg_id,buf_p);
        /*
        ** the pnext time MUST be updated after the sending of the frame since the system
        ** might be preempted before being able to send the frame
        ** if we don't do that, we might face the situation where 2 read request are sent
        ** at the same time
        */
        gettimeofday(&timeDay,(struct timezone *)0);  
        new_time = MICROLONG(timeDay);  
        rsp_time = buf_shaping_ctx_p->rsp_time * p->byte_time_ns_adjusted/10000; 
        p->next_time = new_time + rsp_time;
        if (ret < 0)
        {
          /*
          ** check the inuse of the buffer and release it for inuse ==1
          */
          inuse = ruc_buf_inuse_get(buf_p);
          if (inuse == 1) 
          {
             ruc_buf_freeBuffer(buf_p);  
          } 
          return -1;    
        }
//        if (p->sent == 1) 
        {
          /*
          ** adjust byte time
          */
          p->anticipation++;
          RUC_TRAFFIC_SHAPING_ADJUST_BYTE_TIME_ADD(p);    
        }
        p->sent = 1;
        /*
        ** check if the queue is empty, in that case we assert noempty2empty flag
        */
        if (ruc_objIsEmptyList((ruc_obj_desc_t*)&p->link))
        {
           p->noempty2empty = 1;      
        }
      }
      return 0;      
   }
   return 0;
}  


/*__________________________________________________________________________
*/
/**
* schedule a traffic shaping queue

  @param p : traffic shaping queue 
  @param cur_time: current time in us
  
  retval 0 : OK
  retval < 0 error (no scheduler queue)
*/
int trshape_force_schedule_queue(trshape_ctx_t *p,uint64_t cur_time)
{
   ruc_buf_shaping_ctx_t *buf_shaping_ctx_p;  
   int8_t inuse;
   ruc_obj_desc_t *pnext;
   void *buf_p;
   int ret;

   
   pnext = (ruc_obj_desc_t*)NULL;
   while ((buf_p = (void*) ruc_objGetNext(&p->link,&pnext))!=NULL) 
   { 
      /*
      ** get the traffic shaping context
      */
      buf_shaping_ctx_p = ruc_buffer_get_shaping_ctx(buf_p);

      ruc_objRemove((ruc_obj_desc_t*)buf_p);
      /*
      ** update stats
      */
      p->stats.forcedeq_count++;  
      p->next_time = cur_time + buf_shaping_ctx_p->rsp_time;
      ret = (*buf_shaping_ctx_p->sched_callBackFct)(buf_shaping_ctx_p->lbg_id,buf_p);
      if (ret < 0)
      {
        /*
        ** check the inuse of the buffer and release it for inuse ==1
        */
        inuse = ruc_buf_inuse_get(buf_p);
        if (inuse == 1) 
        {
           ruc_buf_freeBuffer(buf_p);  
        } 
        return -1;    
      }
      /*
      ** check if the queue is empty, in that case we assert noempty2empty flag
      */
      if (ruc_objIsEmptyList((ruc_obj_desc_t*)&p->link))
      {
         p->noempty2empty = 1;      
      }
      break;
   } 
   return 0;  
}  


/*__________________________________________________________________________
*/
/**
* reschedule on frame receive

  @param p : traffic shaping queue 
  @param cur_time: current time in us
  
  retval 0 : OK
  retval < 0 error (no scheduler queue)
*/
int trshape_schedule_on_response()
{
  struct timeval     timeDay;
  gettimeofday(&timeDay,(struct timezone *)0);  
  uint64_t cur_time = MICROLONG(timeDay);    
    /*
   ** get the reference of the traffic shaping queue
   */
   trshape_ctx_t *p = trshape_get_ctx_from_idx(0);
   if ( p == NULL) return -1;
   /*
   **  if the queue is empty adjust next_time to now
   ** and then exit
   */ 
   if (ruc_objIsEmptyList((ruc_obj_desc_t*)&p->link))
   {
      p->next_time = cur_time;  
      p->noempty2empty = 0;
      if (p->sent == 1)
      {
        if (p->anticipation == 0)
        {
          /*
          ** readjust time
          */
          RUC_TRAFFIC_SHAPING_ADJUST_BYTE_TIME_SUB(p);        
        }
      }
      p->anticipation = 0;   
             
      p->sent = 0;
      return 0; 
   }
   /*
   ** check the case of the last frame of a burst: this is indicated
   ** thanks the noemption2empty flag assertion
   */
   if (p->noempty2empty == 1)
   {
      if (p->sent == 1)
      {
        if (p->anticipation == 0)
        {
          /*
          ** readjust time
          */
          RUC_TRAFFIC_SHAPING_ADJUST_BYTE_TIME_SUB(p);        
        }
      }
      /*
      ** clear it and grant the next direct send on response (forced_dequeue)
      */
      p->anticipation = 0;         
      p->noempty2empty = 0;
      p->sent = 0;
      return 0;       
   }
   if (p->sent == 1)
   {
     /*
     ** the next frame has already been sent by anticipation by the scheduler
     ** just clear sent to allow direct send on next response 
     */
     if (p->anticipation == 0)
     {
       /*
       ** readjust time
       */
       RUC_TRAFFIC_SHAPING_ADJUST_BYTE_TIME_SUB(p);        
     }

     p->sent = 0;
     p->anticipation = 0;         
     return 0;
   }
   /*
   ** there is no pending xmit: check the queue and send a frame if there is
   ** something in the queue
   */
   RUC_TRAFFIC_SHAPING_ADJUST_BYTE_TIME_SUB(p);
   p->anticipation = 0;              

   trshape_force_schedule_queue(p,cur_time);  
   return 0;   
}

/*__________________________________________________________________________
*/
/**
* queue a buffer in the traffic shaping queue

  @param traffic_shaper_idx 
  @param *buf_p
  @param disk_time
  @param rsp_size
  
  retval 0 : send is granted
  retval 1 : packet is queued
  retval < 0 error (no scheduler queue)
*/
int trshape_queue_buf(int traffic_shaper_idx,void *buf_p,
                      uint32_t rsp_size,uint32_t disk_time,sched_pf_buf_t cbk,int lbg_id)
{
  struct timeval     timeDay;
  ruc_buf_shaping_ctx_t *buf_shaping_ctx_p;  
  int8_t inuse;
  uint64_t cur_time;
  int ret;
  
  buf_shaping_ctx_p = ruc_buffer_get_shaping_ctx(buf_p);

   /*
   ** get the reference of the traffic shaping queue
   */
   trshape_ctx_t *p = trshape_get_ctx_from_idx(traffic_shaper_idx);
   if ( p == NULL) return -1;
   /*
   ** update stats
   */
   p->stats.submit_count++;     
   /*
   ** compute the response time in microseconds
   */
   uint64_t rsp_time = p->byte_time_ns_adjusted*rsp_size/10000;

   /*
   ** save the parameter in the traffic shaping context
   */
   buf_shaping_ctx_p->rsp_time = rsp_size;
   buf_shaping_ctx_p->disk_time = disk_time;
   buf_shaping_ctx_p->lbg_id = lbg_id;
   buf_shaping_ctx_p->sched_callBackFct = cbk;
   /*
   ** check if the scheduler queue is empty
   ** when the scheduler queue is not empty, just insert the new request in the queue
   */
     /*
  ** check if the main queue is empty if the queue is not empty just queue our message
  ** at the tail of the load balancer main queue
  */
  gettimeofday(&timeDay,(struct timezone *)0);  
  cur_time = MICROLONG(timeDay);    
//       info("FDL rsp_time %llu cur_time %llu next_time %llu rsp_size %d",rsp_time,cur_time,p->next_time,rsp_size);

  if (!ruc_objIsEmptyList((ruc_obj_desc_t*)&p->link))
  {
     /*
     ** queue the message at the tail
     */
     ruc_objRemove((ruc_obj_desc_t*)buf_p);
     ruc_objInsertTail(&p->link,(ruc_obj_desc_t*)buf_p);  
    /*
    ** update statistics
    */
    p->stats.enqueued_count++;  
    /*
    ** check the scheduler queue to figure out if some frame can be extracted
    */   
    return trshape_schedule_queue(p,cur_time);
  }
   /*
   ** Queue is empty get the current time and convert it in microseconds
   */
   

  if (cur_time >= p->next_time)
  {
    /*
    ** we can send
    */
    p->next_time = cur_time + rsp_time;
    ret = (*cbk)(lbg_id,buf_p);
    if (ret < 0)
    {
      /*
      ** check the inuse of the buffer and release it for inuse ==1
      */
      inuse = ruc_buf_inuse_get(buf_p);
      if (inuse == 1) 
      {
         ruc_buf_freeBuffer(buf_p);  
      } 
      return -1;    
    }
    if (p->noempty2empty) p->stats.send_violation_count++;
    if (p->sent == 1)
    {
       /*
       ** adjust byte time
       */
       RUC_TRAFFIC_SHAPING_ADJUST_BYTE_TIME_ADD(p);    
    }
    p->sent = 0;
    return 0;
  }
  /*
  ** Queue the buffer
  */
  ruc_objRemove((ruc_obj_desc_t*)buf_p);
  ruc_objInsertTail(&p->link,(ruc_obj_desc_t*)buf_p);
  p->stats.enqueued_count++;  
  return 1;
}

/*__________________________________________________________________________
*/
/**
*   Init of the traffic shapping function
*

   @param current_time : current time provided by the socket controller
   
   
   @retval none
*/
void trshape_scheduler_entry_point(uint64_t current_time)
{
  trshape_schedule_queue(&storcli_traffic_shaper[0],current_time);

}
/*__________________________________________________________________________
*/
/**
*   Init of the traffic shapping function
*

   @param byte_time_ns : default byte time in 1/10 of ns
   
   @retval 0
*/
void trshape_module_init(uint64_t byte_time_ns)
{
 int i;
 trshape_ctx_t *p = storcli_traffic_shaper;
 
 for (i = 0; i < TRSHAPE_MAX_CTX; i++,p++)
 {
   trshape_ctx_init(p,i,byte_time_ns);
 }
 /*
 ** attach the callback on socket controller
 */
 ruc_sockCtrl_attach_traffic_shaper(trshape_scheduler_entry_point);
 
     uma_dbg_addTopic_option("shaper", show_traffic_shaper,UMA_DBG_OPTION_RESET);

 
 
}
