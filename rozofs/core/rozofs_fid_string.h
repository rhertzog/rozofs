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
 
#ifndef ROZOFS_FID_STRING_H
#define ROZOFS_FID_STRING_H

#include <uuid/uuid.h>
#include <stdint.h>
#include <stdio.h>
#include <rozofs/core/rozofs_string.h>

#ifdef __cplusplus
extern "C" {
#endif /*__cplusplus*/


/*
**___________________________________________________________
** Parse a string representing an FID
**
** @param pChar   The string containing the FID
** @param fid     The parsed FID
**
** @retval The next place to parse in the string after the FID 
**         or NULL when the string do not represent an FID
*/
static inline char * rozofs_string2fid(char * pChar, uuid_t fid) {
  uint8_t *pFid = (uint8_t*) fid;

  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return NULL;
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return NULL;
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return NULL;
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return NULL;
     
  if (*pChar++ != '-') return NULL;  
  
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return NULL;
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return NULL;
  
  if (*pChar++ != '-') return NULL;  

  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return NULL;
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return NULL;
  
  if (*pChar++ != '-') return NULL;  

  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return NULL;
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return NULL;
  
  if (*pChar++ != '-') return NULL;  
  
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return NULL;  
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return NULL;  
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return NULL;  
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return NULL;  
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return NULL;  
  pChar = rozofs_2char2uint8(pChar,pFid++);
  if (pChar == NULL) return NULL;  
   
  return pChar;
}

/*
**___________________________________________________________
** Get a FID value and display it as a string. An end of string
** is inserted at the end of the string.
**
** @param fid     The FID value
** @param pChar   Where to write the ASCII translation
**
** @retval The end of string
*/
static inline char * rozofs_fid2string(uuid_t fid, char * pChar) {
  uint8_t * pFid = (uint8_t *) fid;
  
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);   
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  
  *pChar++ = '-';
  
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);
   
  *pChar++ = '-';
  
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  
  *pChar++ = '-';
  
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  
  *pChar++ = '-';
  
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);   
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);
  pChar = rozofs_u8_2_char(*pFid++,pChar);   

  *pChar = 0; 
  
  return pChar;  
}

#ifdef __cplusplus
}
#endif /*__cplusplus */

#endif

