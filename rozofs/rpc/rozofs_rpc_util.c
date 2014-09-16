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

#define FUSE_USE_VERSION 26

#include <stdio.h>
#include <assert.h>
#include <stddef.h>
#include <stdlib.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <fcntl.h> 
#include <sys/un.h>             
#include <errno.h>  
#include <rpc/rpc.h>

/*
 * XDR the MSG_ACCEPTED part of a reply message union
 */
bool_t
rozofs_xdr_accepted_reply(XDR *xdrs, struct accepted_reply *ar)
{

	/* personalized union, rather than calling xdr_union */
    /*
    ** skip the verifier: flavor and length
    */
//      Add 2 uint32_t with 0 value to replace auth field
//	if (! xdr_opaque_auth(xdrs, &(ar->ar_verf)))
//		return (FALSE); 
        uint32_t val=0;
        if (! xdr_uint32_t(xdrs, (uint32_t*)&val))
			return (FALSE);
        if (! xdr_uint32_t(xdrs, (uint32_t*)&val))
			return (FALSE);	
					
	if (! xdr_enum(xdrs, (enum_t *)&(ar->ar_stat)))
		return (FALSE);
	switch (ar->ar_stat) {

	case SUCCESS:
        if (ar->ar_results.proc != NULL)
        {
		   return ((*(ar->ar_results.proc))(xdrs, ar->ar_results.where));
        }
        return TRUE;

	case PROG_MISMATCH:
		if (! xdr_uint32_t(xdrs, (uint32_t*)&(ar->ar_vers.low)))
			return (FALSE);
		return (xdr_uint32_t(xdrs, (uint32_t*)&(ar->ar_vers.high)));

	case GARBAGE_ARGS:
	case SYSTEM_ERR:
	case PROC_UNAVAIL:
	case PROG_UNAVAIL:
		break;
	}
	return (TRUE);  /* TRUE => open ended set of problems */
}

/*
 * XDR the MSG_DENIED part of a reply message union
 */
bool_t 
rozofs_xdr_rejected_reply(XDR *xdrs, struct rejected_reply *rr)
{
	/* personalized union, rather than calling xdr_union */
	if (! xdr_enum(xdrs, (enum_t *)&(rr->rj_stat)))
		return (FALSE);
	switch (rr->rj_stat) {

	case RPC_MISMATCH:
		if (! xdr_uint32_t(xdrs, (uint32_t*)&(rr->rj_vers.low)))
			return (FALSE);
		return (xdr_uint32_t(xdrs, (uint32_t*)&(rr->rj_vers.high)));

	case AUTH_ERROR:
		return (xdr_enum(xdrs, (enum_t *)&(rr->rj_why)));
	}
	/* NOTREACHED */
//	assert(0);
	return (FALSE);
}

static const struct xdr_discrim reply_dscrm[3] = {
	{ (int)MSG_ACCEPTED, (xdrproc_t)rozofs_xdr_accepted_reply },
	{ (int)MSG_DENIED, (xdrproc_t)rozofs_xdr_rejected_reply },
	{ __dontcare__, NULL_xdrproc_t } };


/*
 * XDR a reply message
 */
bool_t rozofs_xdr_replymsg(XDR *xdrs, struct rpc_msg *rmsg)

{

	if (
	    xdr_uint32_t(xdrs,(uint32_t *) &(rmsg->rm_xid)) && 
	    xdr_enum(xdrs, (enum_t *)&(rmsg->rm_direction)) &&
	    (rmsg->rm_direction == REPLY) )
		return (xdr_union(xdrs, (enum_t *)&(rmsg->rm_reply.rp_stat),
		   (caddr_t)(void *)&(rmsg->rm_reply.ru), reply_dscrm,
		   NULL_xdrproc_t));
	return (FALSE);
}
