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

#ifndef _XMALLOC_H
#define _XMALLOC_H

#include <stdlib.h>

#include <string.h>
#include "log.h"
#include <stdint.h>
#include <sys/types.h>

typedef struct _xmalloc_stats_t
{
   uint64_t  count;
   int       size;
} xmalloc_stats_t;

#define XMALLOC_MAX_SIZE  512
#define check_memory(p) if (p == 0) {\
    fatal("null pointer detected -- exiting.");\
}

extern xmalloc_stats_t *xmalloc_size_table_p;
extern uint32_t         xmalloc_entries;
extern unsigned int     xmalloc_entries;

static inline int xmalloc_stats_insert(int n)
{
   int i;
   xmalloc_stats_t *p = xmalloc_size_table_p;
   
   for (i = 0; i < xmalloc_entries; i++,p++)
   {
      if (p->size == n)
      {
         p->count++;
         return i;      
      }
   }  
   
   p->size  = n;
   p->count = 1; 
   xmalloc_entries++;
   return i;                 
}
static inline void xmalloc_stats_release(int n)
{
   int i;
   xmalloc_stats_t *p = xmalloc_size_table_p;
   
   for (i = 0; i < xmalloc_entries; i++,p++)
   {
      if (p->size == n)
      {
         p->count--;
	 if (p->count != 0) return;
	 xmalloc_entries--; 
	 break;
      } 
   }
   
   for (;i < xmalloc_entries; i++,p++) {   
      memcpy(p,&p[1],sizeof(xmalloc_stats_t));
   } 
}


#define xmalloc(n) xmalloc_internal(__FILE__,__LINE__,n) 
void *xmalloc_internal(char * file, int line, size_t n);

#define xstrdup(str) xstrdup_internal(__FILE__,__LINE__,(size_t)strlen(str),str)
void *xstrdup_internal(char * file, int line, size_t n, char * src); 

void xfree(void * p);

void *xcalloc(size_t n, size_t s);

void *xrealloc(void *p, size_t n);

#endif
