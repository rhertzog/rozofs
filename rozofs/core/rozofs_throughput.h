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
#ifndef rozofs_thr_H
#define rozofs_thr_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "rozofs_string.h"
#include <malloc.h>

#define ROZOFS_THR_CNTS_NB    64

typedef struct _rozofs_thr_1_cnt_t {
  uint64_t     ts;
  uint64_t     count;
} rozofs_thr_1_cnt_t;


typedef struct _rozofs_thr_cnts_t {
  char                   * name;
  rozofs_thr_1_cnt_t       counters[ROZOFS_THR_CNTS_NB];
} rozofs_thr_cnts_t;


/*_______________________________________________________________________
* Update throughput counter
*
* @param counters the counter structure
* @param count    a count to increment counters with
* @param t        the time in seconds
*/
static inline void rozofs_thr_cnt_update_with_time(rozofs_thr_cnts_t * counters, uint64_t count, uint32_t t) {
  int    rank;
  if (counters == NULL) return;
  
  rank = t % ROZOFS_THR_CNTS_NB;
  
  if (counters->counters[rank].ts == t) {
    counters->counters[rank].count += count;
  }
  else {
    counters->counters[rank].ts    = t;
    counters->counters[rank].count = count;
  }
}  
/*_______________________________________________________________________
* Update throughput counter
*
* @param counters the counter structure
* @param count    a count to increment counters with
*/
static inline void rozofs_thr_cnt_update(rozofs_thr_cnts_t * counters, uint64_t count) {
  if (counters == NULL) return;

  uint32_t t = time(NULL);
  rozofs_thr_cnt_update_with_time(counters,count,t);
}

/*_______________________________________________________________________
* Display throughput counters
*
* @param pChar    Where to format the ouput
* @param counters array of the counters
* @param nb       number of entries in the array
*/
char * rozofs_thr_display(char * pChar, rozofs_thr_cnts_t * counters[], int nb);
/*_______________________________________________________________________
* Initialize a thoughput measurement structure
*
* @param counters  The structure to initialize of NULL if it is to be allocated
*
* @retval the initialized structure address
*/
static inline rozofs_thr_cnts_t * rozofs_thr_cnts_allocate(rozofs_thr_cnts_t * counters, char * name) {

  /*
  ** Allocate counters when needed
  */
  if (counters == NULL) {
    counters = memalign(32,sizeof(rozofs_thr_cnts_t));
    counters->name = strdup(name);
  }
    
  /*
  ** Reset counters
  */
  memset(counters->counters,0,sizeof(rozofs_thr_1_cnt_t)*ROZOFS_THR_CNTS_NB);
  return counters;
}
/*_______________________________________________________________________
* Free a thoughput measurement structure
*
* @param counters  The structure to free
*
*/
static inline void rozofs_thr_cnts_free(rozofs_thr_cnts_t ** counters) {

  if (*counters == NULL) return;
  
  if ((*counters)->name) {
    free((*counters)->name);
    (*counters)->name = NULL;
  }

  free(*counters);
  *counters = NULL;
}
#endif


