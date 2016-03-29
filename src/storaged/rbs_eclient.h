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

#ifndef RBS_ECLIENT_H
#define RBS_ECLIENT_H

#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>

/** Initialize a storage structure to reach a cid/sid by interogating
 *  the exportd as well as the storaged
 *
 * @param clt: RPC connection to export server
 * @param export_host_list: IP or hostname of export server
 * @param site: the site identifier
 * @param cid: the unique ID of cluster
 * @param sid: the storage identifier
 * @param stor: the storage structure to initialize
 *
 * @return: NULL on error, valid export host name on success
 */
int rbs_get_storage(rpcclt_t * clt, const char *export_host_list, int site, cid_t cid, sid_t sid, rb_stor_t * stor);


/** Send a request to export server for get the list of member storages
 *  of cluster with a given cid and add this storage list to the list
 *  of clusters
 *
 * @param clt: RPC connection to export server
 * @param export_host: IP or hostname of export server
 * @param site: the site identifier
 * @param cid: the unique ID of cluster
 * @param cluster_entries: list of cluster(s)
 *
 * @return: NULL on error, valid export host name on success
 */
char * rbs_get_cluster_list(rpcclt_t * clt, const char *export_host_list, int site, cid_t cid,
        list_t * cluster_entries) ;
/** Send a request to export server for get the list of member storages
 *  of cluster with a given cid and add this storage list to the list
 *  of clusters
 *
 * @param clt: RPC connection to export server
 * @param export_host: IP or hostname of export server
 * @param site: the site identifier
 * @param cid: the unique ID of cluster
 * @param cluster_entries: list of cluster(s)
 *
 * @return: NULL on error, valid export host name on success
 */
char * rbs_get_cluster2_list(rpcclt_t * clt, const char *export_host_list, int site, cid_t cid,
        list_t * cluster_entries, uint8_t * layout, uint16_t * vid) ;

#endif
