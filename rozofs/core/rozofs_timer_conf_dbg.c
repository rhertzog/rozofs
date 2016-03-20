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
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <rozofs/common/log.h>
#include <rozofs/core/uma_dbg_api.h>
#include <rozofs/rozofs_timer_conf.h>



/*__________________________________________________________________________
*/
void dbg_show_tmr_man(char * pt) {
  pt += sprintf(pt,"Displays the configurable timers.\n");
  pt += sprintf(pt,"1rst column : the timer name.\n");
  pt += sprintf(pt,"2nd  column : the timer index.\n");
  pt += sprintf(pt,"3rd  column : the timer default value.\n");
  pt += sprintf(pt,"4th  column : the timer minimum value.\n");
  pt += sprintf(pt,"5th  column : the timer maximum value.\n");
  pt += sprintf(pt,"6th  column : the timer curent value.\n");
  pt += sprintf(pt,"7th  column : the timer unit.\n");
}

void dbg_show_tmr(char * argv[], uint32_t tcpRef, void *bufRef) 
{

   rozofs_tmr_display(uma_dbg_get_buffer());
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}

/*__________________________________________________________________________
*/
static char * dbg_set_tmr_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"tmr_set <num#> <value>   : set timer number <num#> to <value>\n");
  pChar += sprintf(pChar,"tmr_set <name> <value>   : set timer name <name> to <value>\n");  
  return pChar; 
} 
void dbg_set_tmr_man(char * pt) {
  pt += sprintf(pt,"To modify a configurable timer value.\n");
  dbg_set_tmr_help(pt);
} 
void dbg_set_tmr(char * argv[], uint32_t tcpRef, void *bufRef) 
{
  char           *pChar=uma_dbg_get_buffer();

   int timer_id,val;
   
   if (argv[2] ==NULL)
   {
      pChar = dbg_set_tmr_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
      return;         
   }
   /* Check 1rst a string name */
   timer_id = rozofs_tmr_get_idx_from_name(argv[1]);
   /* Check for an index */
   if (timer_id < 0) {
     errno = 0;
     timer_id = (int) strtol(argv[1], (char **) NULL, 10);   
     if (errno != 0) {
	pChar = dbg_set_tmr_help(pChar);
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	return;     
     }
   }  
   if (timer_id >= TMR_MAX_ENTRY)
   {
    pChar += sprintf(pChar,"invalid timer index (max %d)\n",(TMR_MAX_ENTRY-1));    
    pChar = dbg_set_tmr_help(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
    return;        
   }
   errno = 0;
   val = (int) strtol(argv[2], (char **) NULL, 10);   
   if (errno != 0) {
     pChar += sprintf(pChar,"bad value %s\n",argv[2]);    
     pChar = dbg_set_tmr_help(pChar);
     uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
     return; 
   }
   if (rozofs_tmr_configure(timer_id,val) < 0)
   {
     pChar += sprintf(pChar,"timer value out of range\n");    
     pChar = dbg_set_tmr_help(pChar);
     uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
     return;    
   }   
   uma_dbg_send(tcpRef, bufRef, TRUE, "Success\n");
}


/*__________________________________________________________________________
*/
static char * dbg_set_tmr_default_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"tmr_default all    : set every timer to its default value\n");
  pChar += sprintf(pChar,"tmr_default <num#> : set timer number <num#> to its default value\n");
  pChar += sprintf(pChar,"tmr_default <name> : set timer name <name> to itsdefault value\n");  
  return pChar; 
} 
void  dbg_set_tmr_default_man(char * pt) {
  pt += sprintf(pt,"To reset the configurable timer.\n");
  dbg_set_tmr_default_help(pt);  
}
void dbg_set_tmr_default(char * argv[], uint32_t tcpRef, void *bufRef) 
{
  char           *pChar=uma_dbg_get_buffer();

   
   int timer_id;
   
   if (argv[1] ==NULL)
   {
      pChar = dbg_set_tmr_default_help(pChar);
      uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
      return;         
   }
   if (strcmp("all",argv[1]) ==0)  
   {
    rozofs_tmr_init_configuration();
    uma_dbg_send(tcpRef, bufRef, TRUE, "Success\n");    
    return;     
   }
   /* Check 1rst a string name */
   timer_id = rozofs_tmr_get_idx_from_name(argv[1]);
   /* Check for an index */
   if (timer_id < 0) {
     errno = 0;
     timer_id = (int) strtol(argv[1], (char **) NULL, 10);   
     if (errno != 0) {
	pChar = dbg_set_tmr_default_help(pChar);
	uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
	return;     
     }
   }  
   if (timer_id >= TMR_MAX_ENTRY)
   {
    pChar += sprintf(pChar,"invalid timer index (max %d)\n",(TMR_MAX_ENTRY-1));    
    pChar = dbg_set_tmr_default_help(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
    return;             
   }
   rozofs_tmr_set_to_default(timer_id);
   uma_dbg_send(tcpRef, bufRef, TRUE, "Success\n");    
   return;   
}



/*__________________________________________________________________________
*/
void rozofs_timer_conf_dbg_init()
{
  uma_dbg_addTopicAndMan("tmr_show", dbg_show_tmr, dbg_show_tmr_man, 0);
  uma_dbg_addTopicAndMan("tmr_set", dbg_set_tmr, dbg_set_tmr_man, 0);
  uma_dbg_addTopicAndMan("tmr_default", dbg_set_tmr_default, dbg_set_tmr_default_man, 0);
}
