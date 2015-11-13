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

#ifndef RBS_SCLIENT_H
#define RBS_SCLIENT_H

#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>

#include <rozofs/rozofs.h>
#include <rozofs/rozofs_srv.h>
#include <rozofs/rpc/eproto.h>

#include "rbs_transform.h"
#include "rbs.h"

#define ROZOFS_READ_BUFFER_SIZE (256*1024) 
#define ROZOFS_BLOCKS_IN_BUFFER(bsize) (ROZOFS_READ_BUFFER_SIZE/ROZOFS_BSIZE_BYTES(bsize))
#define ROZOFS_BLOCKS_MAX (ROZOFS_BLOCKS_IN_BUFFER(ROZOFS_BSIZE_MIN))



typedef struct rbs_storcli_ctx {
    rbs_projection_ctx_t prj_ctx[ROZOFS_SAFE_MAX];
    rbs_inverse_block_t block_ctx_table[ROZOFS_BLOCKS_MAX];
    uint8_t redundancy_stor_idx_current;
    char *data_read_p;
} rbs_storcli_ctx_t;

/** Get connections to storage servers for a given rebuild entry and
 *  verifies that the number of connections to storage servers is sufficient
 *
 *  To verify that number of connections is sufficient from a given distribution
 *  then we must pass the nb. of connection required as parameter
 *
 * This function computes a pseudo-random integer in the range 0 to RAND_MAX
 * inclusive to select a connection among all those available for
 * one storage server (CID;SID).
 * That allows you to load balancing connections between proccess
 * for one storage server
 *
 * @param *rb_entry: pointer to entry to rebuild
 * @param *clusters_list: list of clusters
 * @param cid_to_search: cluster ID attached at this entry
 * @param sid_to_rebuild: the sid of storage sto rebuild
 * @param nb_cnt_required: nb. of connections required
 *
 * @return: 0 on success -1 otherwise (errno: EIO is set)
 */
int rbs_get_rb_entry_cnts(rb_entry_t * rbs_restore_one_rb_entry,
        list_t * clusters_list, cid_t cid_to_search, sid_t sid_to_rebuild,
        uint8_t nb_cnt_required);

/** Send a request to a given storage (CID;SID) for get the list of entries
 *  to rebuild for a given storage (rebuild_sid)
 *
 * @param *mclt: mproto connection to storage server
 * @param cid: the unique cluster ID of storage to contacted
 * @param sid: the unique storage ID of storage to contacted
 * @param rebuild_sid: the unique storage ID of storage to rebuild
 * @param device: index indication
 * @param spare: index indication
 * @param cookie: index indication
 * @param **children: pointer to the list of rebuild entries
 * @param eof: end of list indication
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int rbs_get_rb_entry_list(mclient_t * mclt, cid_t cid, sid_t sid,
        sid_t rebuild_sid, uint8_t * device, uint8_t * spare, uint16_t * slice,
        uint64_t * cookie,
        bins_file_rebuild_t ** children, uint8_t * eof);

int rbs_read_blocks(sclient_t **storages, int local_idx, uint8_t layout, uint32_t bsize, cid_t cid,
        sid_t dist_set[ROZOFS_SAFE_MAX], fid_t fid, bid_t first_block_idx,
        uint32_t nb_blocks_2_read, uint32_t * nb_blocks_read, int retry_nb,
        rbs_storcli_ctx_t * working_ctx_p,
	uint64_t          * size_read);
int rbs_read_all_available_proj(sclient_t **storages, int spare_idx, uint8_t layout, uint32_t bsize, cid_t cid,
        sid_t dist_set[ROZOFS_SAFE_MAX], fid_t fid, bid_t first_block_idx,
        uint32_t nb_blocks_2_read, uint32_t * nb_blocks_read, 
        rbs_storcli_ctx_t * working_ctx_p,
	uint64_t          * size_read);
/** Send a request to export server for get the list of member storages
 *  of cluster with a given cid and add this storage list to the list
 *  of clusters
 *
 * @param clt: RPC connection to export server
 * @param export_host: IP or hostname of export server
 * @param cid: the unique ID of cluster
 * @param cluster_entries: list of cluster(s)
 *
 * @return: 0 on success -1 otherwise (errno is set)
 */
int rbs_get_fid_attr(rpcclt_t * clt, const char *export_host, fid_t fid, ep_mattr_t * attr, uint32_t * bsize, uint8_t * layout);	
#endif
