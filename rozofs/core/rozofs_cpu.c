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
#include <string.h>
#include <errno.h>
#include <ctype.h>

#include <rozofs/core/rozofs_string.h>
#include <rozofs/common/log.h>
#include <rozofs/core/rozofs_cpu.h>
#include <rozofs/core/uma_dbg_api.h>

#define _PATH_PROC_CPUINFO	"/proc/cpuinfo"
#define MYBUFSIZ 4096
#define PATTERN "cpu MHz"

static uint64_t cpu_frequency=0;

/*
**_______________________________________________________________
*/
void show_cpu_frequency_man(char * pt) {
  pt += sprintf(pt,"Display the CPU frequency\n");
}
void show_cpu_frequency(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();

  pChar += sprintf(pChar,"cpu frequency : %llu Hz\n",
               (long long unsigned int)rozofs_get_cpu_frequency()); 
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer()); 
  return;  	  
}  

/*
**_______________________________________________________________
* Parse a line in the system file to find out the CPU frequency
*
* @param line    The line of the file to parse
*
* @retval 1 when the CPU frequency has benn found and parsed
* @retval 0 when the CPU frequency           
*/
static int parse_cpu_frequency(char *line) {
  char    * p;
  int       len      = strlen(PATTERN);
  uint64_t  units    = 1000000; // Frequency units is MHz
  int       dotFound = 0; 
  

  if (!*line) return 0;

  if (strncmp(line, PATTERN, len)) return 0;
  p = line + len;
  
  // skip spaces 
  while (isspace(*p)) p++;
  
  // skip ':'
  if (*p != ':') return 0;
  p++;

  // skip spaces 
  while (isspace(*p)) p++;

  // value
  while (*p) {
  
    if ((*p>='0')&&(*p<='9')) {
    
      cpu_frequency *= 10;
      cpu_frequency += *p-'0';

      if (dotFound) {
        units/= 10; 
      }

      p++;
      continue;
    }
    
    if (*p == '.') {
      dotFound = 1;
      p++;
      continue;
    }  
    
    cpu_frequency *= units;
    return 1;
  }
  return 0;
} 
 
uint64_t rozofs_get_cpu_frequency(void) {
  FILE *fp;
  char buf[MYBUFSIZ];
  
  if (cpu_frequency) return cpu_frequency;
  
  uma_dbg_addTopicAndMan("frequency",show_cpu_frequency, show_cpu_frequency_man, 0);

  fp = fopen(_PATH_PROC_CPUINFO,"r");
  if (fp == NULL) {
    severe("fopen(%s) %s", _PATH_PROC_CPUINFO,strerror(errno));
    return 0;
  }
  
  while (fgets(buf, sizeof(buf), fp) != NULL) {
  
    if (parse_cpu_frequency(buf)) {
      break;
    }
  } 
  
  if (cpu_frequency==0) {
    cpu_frequency = 2*GIGA;
  }  
  return cpu_frequency;
  
}
  
