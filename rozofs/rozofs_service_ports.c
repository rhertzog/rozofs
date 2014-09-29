/*
 Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
 This file is part of Rozofs.

 Rozofs is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published
 by the Free Software Foundation; either version 2 of the License,
 or (at your option) any later version.

 Rozofs is distributed in the hope that it will be useful, but
 WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see
 <http://www.gnu.org/licenses/>.
 */


#include "rozofs_service_ports.h"

ROZOFS_SERVICE_PORT_RANGE_DESC_T rozofs_service_port_range[ROZOFS_SERVICE_PORT_MAX] = {
  [ROZOFS_SERVICE_PORT_EXPORT_DIAG] = {
    .defaultValue = 52000,
    .rangeSize    = 1,
    .name         = "rozofs_export_diag",
    .service      = "Export master diagnostic",
  },
  
  [ROZOFS_SERVICE_PORT_EXPORT_EPROTO] = {
    .defaultValue = 53000,
    .rangeSize    = 1,
    .name         = "rozofs_export_eproto",
    .service      = "Export master eproto",
  },
   
  [ROZOFS_SERVICE_PORT_MOUNT_DIAG] = {
    .defaultValue = 50003,
    .rangeSize    = NB_FSMOUNT*3,
    .name         = "rozofs_mount_diag",
    .service      = "rozofsmount & storcli diagnostic",
  },
  
  [ROZOFS_SERVICE_PORT_STORAGED_DIAG] = {
    .defaultValue = 50027,
    .rangeSize    = NB_STORIO+1,
    .name         = "rozofs_storaged_diag",
    .service      = "Storaged & storio diagnostic",
  }, 
  
  [ROZOFS_SERVICE_PORT_STORAGED_MPROTO] = {
    .defaultValue = 51000,
    .rangeSize    = 1,
    .name         = "rozofs_storaged_mproto",
    .service      = "Storaged mproto",
  }  
};
