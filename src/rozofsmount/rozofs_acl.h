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
#ifndef ROZOFS_ACL_H
#define ROZOFS_ACL_H

#include <string.h>
#include <linux/fs.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>


/**
**________________________________________________________________
*/
/**
*  Check if the extended attributes are related to acl access

   @param name: name of the extended attribute
   @param value: value of the extended attribute
   @param size: size of the extended attributes
   
   @retval -1 error
   @retval 0 extended attribute is acl_access with equivalent mode
   @retval 1 extended attribute is acl_access with no equivalent mode
   
*/
int rozofs_acl_access_check(const char *name, const char *value, size_t size,mode_t *mode_p);

#endif
