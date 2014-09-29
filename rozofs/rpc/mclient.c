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

#include <string.h>
#include <errno.h>

#include <rozofs/common/log.h>
#include <rozofs/core/rozofs_ip_utilities.h>

#include "mproto.h"
#include "rpcclt.h"
#include "mclient.h"

uint16_t mproto_service_port = 0;

int mclient_initialize(mclient_t * clt, struct timeval timeout) {
    int status = -1;
    DEBUG_FUNCTION;

    clt->status = 0;

    if (mproto_service_port == 0) {
      /* Try to get debug port from /etc/services */    
      mproto_service_port = rozofs_get_service_port_storaged_mproto();
    }
    
    if (rpcclt_initialize(&clt->rpcclt, clt->host, MONITOR_PROGRAM,
            MONITOR_VERSION, ROZOFS_RPC_BUFFER_SIZE, ROZOFS_RPC_BUFFER_SIZE,
            mproto_service_port, timeout) != 0) {
        // storageclt_release can change errno
        int xerrno = errno;
        //storageclt_release(clt);
        clt->status = 0;
        errno = xerrno;
        goto out;
    }
    clt->status = 1;

    status = 0;
out:
    return status;
}

// XXX Useless

void mclient_release(mclient_t * clt) {
    DEBUG_FUNCTION;
    if (clt && clt->rpcclt.client)
        rpcclt_release(&clt->rpcclt);
}

int mclient_stat(mclient_t * clt, sstat_t * st) {
    int status = -1;
    mp_stat_ret_t *ret = 0;
    mp_stat_arg_t args;
    DEBUG_FUNCTION;

    args.cid = clt->cid;
    args.sid = clt->sid;

    if (!(clt->rpcclt.client) || !(ret = mp_stat_1(&args, clt->rpcclt.client))) {
        errno = EPROTO;
        goto out;
    }
    if (ret->status != 0) {
        errno = ret->mp_stat_ret_t_u.error;
        goto out;
    }
    memcpy(st, &ret->mp_stat_ret_t_u.sstat, sizeof (sstat_t));

    status = 0;
out:
    if (ret)
        xdr_free((xdrproc_t) xdr_mp_stat_ret_t, (char *) ret);
    return status;
}

int mclient_remove(mclient_t * clt, uint8_t layout,
        sid_t dist_set[ROZOFS_SAFE_MAX], fid_t fid) {
    int status = -1;
    mp_status_ret_t *ret = 0;
    mp_remove_arg_t args;
    DEBUG_FUNCTION;

    args.cid = clt->cid;
    args.sid = clt->sid;
    args.layout = layout;
    memcpy(args.dist_set, dist_set, sizeof (sid_t) * ROZOFS_SAFE_MAX);
    memcpy(args.fid, fid, sizeof (fid_t));

    if (!(clt->rpcclt.client) || !(ret = mp_remove_1(&args, clt->rpcclt.client))) {
        errno = EPROTO;
        goto out;
    }
    if (ret->status != 0) {
        errno = ret->mp_status_ret_t_u.error;
        goto out;
    }
    status = 0;
out:
    if (ret)
        xdr_free((xdrproc_t) xdr_mp_status_ret_t, (char *) ret);
    return status;
}

int mclient_ports(mclient_t * mclt, mp_io_address_t * io_address_p) {
    int status = -1;
    mp_ports_ret_t *ret = 0;
    DEBUG_FUNCTION;

    if (!(mclt->rpcclt.client) || !(ret = mp_ports_1(NULL, mclt->rpcclt.client))) {
        errno = EPROTO;
        goto out;
    }
    if (ret->status != 0) {
        errno = ret->mp_ports_ret_t_u.error;
        goto out;
    }
    memcpy(io_address_p, &ret->mp_ports_ret_t_u.io_addr, STORAGE_NODE_PORTS_MAX * sizeof (struct mp_io_address_t));

    status = 0;
out:
    if (ret)
        xdr_free((xdrproc_t) xdr_mp_ports_ret_t, (char *) ret);
    return status;
}
