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

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "rozofs_throughput.h"


/*_______________________________________________________________________
* Display throughput counters
*
* @param pChar    Where to format the ouput
* @param pChar    The counters
* @param pChar    The counters
*/
char * rozofs_thr_display(char * pChar, rozofs_thr_cnts_t * counters[], int nb) {
  uint32_t t;
  struct timeval tv;
  int    rank;
  int    idx,line,col;
  rozofs_thr_1_cnt_t *p;
  uint32_t COLS=4;
  uint32_t LINES;
  int      value;

  if (counters==NULL) {
    pChar += rozofs_string_append(pChar,"Counters not initialized\n");
    return pChar;
  }   
  for (value=0; value< nb; value++) {
    if (counters[value] == NULL) {
      pChar += rozofs_string_append(pChar,"Counters not initialized\n");
      return pChar;
    }
  }

  switch(nb) {
    case 1: 
    case 2: 
      COLS = 4;
      break;
    case 3:
      COLS=3;
    case 4:
    case 5:
      COLS=2;  
      break;
    default:
      COLS=1;
  }           
  LINES=60/COLS;

    
  gettimeofday(&tv,(struct timezone *)0);
  t = tv.tv_sec-1;
  rank = t % ROZOFS_THR_CNTS_NB;  

  for (col=0; col<COLS; col++) {
    pChar += rozofs_string_append(pChar," _____ ");
    for (value=0; value< nb; value++) {
      pChar += rozofs_string_append(pChar,"_________ ");
    }  
  }  
  pChar += rozofs_eol(pChar);
 
  for (col=0; col<COLS; col++) {  
    pChar += rozofs_string_append(pChar,"| T.S |");
    for (value=0; value< nb; value++) {  
      pChar += rozofs_string_append(pChar," ");    
      pChar += rozofs_string_padded_append(pChar, 8, rozofs_left_alignment, counters[value]->name);
      pChar += rozofs_string_append(pChar,"|");    
    }  
  }  
  pChar += rozofs_eol(pChar);

  for (col=0; col<COLS; col++) {  
    pChar += rozofs_string_append(pChar,"|_____|");
    for (value=0; value< nb; value++) {  
      pChar += rozofs_string_append(pChar,"_________|");   
    }  
  }  
  pChar += rozofs_eol(pChar);
     
  
  for (line=0; line< LINES; line++) {
    
    for (col=0; col<COLS; col++) {
    
      idx = (ROZOFS_THR_CNTS_NB+rank-line-(col*LINES))%ROZOFS_THR_CNTS_NB;

      pChar += rozofs_string_append(pChar,"|");
      pChar += rozofs_i32_padded_append(pChar, 4, rozofs_right_alignment, -1-line-(col*LINES));
      pChar += rozofs_string_append(pChar," |");

      for (value=0; value< nb; value++) {  
	p = &(counters[value]->counters[idx]);
	if (p->ts == (t-line-(col*LINES))) {
	  pChar += rozofs_string_append(pChar," ");	
	  pChar += rozofs_bytes_padded_append(pChar,7, p->count);
	  pChar += rozofs_string_append(pChar," |");
	}
	else {
	  pChar += rozofs_string_append(pChar,"         |");
	}
      }    
    }
    pChar += rozofs_eol(pChar);  
  }
  
  for (col=0; col<COLS; col++) {  
    pChar += rozofs_string_append(pChar,"|_____|");
    for (value=0; value< nb; value++) {  
      pChar += rozofs_string_append(pChar,"_________|");
    }  
  }  
  pChar += rozofs_eol(pChar);
  return pChar;    
}
