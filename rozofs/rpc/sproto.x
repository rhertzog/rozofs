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

%#include <rozofs/rozofs.h>

typedef uint32_t sp_uuid_t[ROZOFS_UUID_SIZE_RPC];

enum sp_status_t {
    SP_SUCCESS = 0,
    SP_FAILURE = 1
};

union sp_status_ret_t switch (sp_status_t status) {
    case SP_FAILURE:    int error;
    default:            void;
};

struct sp_write_arg_t {
    uint16_t    cid;
    uint8_t     sid;          
    uint8_t     layout;
    uint8_t     spare;
    uint32_t     dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid;        
    uint8_t     proj_id;     
    uint64_t    bid;
    uint32_t    nb_proj;
    uint32_t    alignment;
    opaque      bins<>;
};


/*
** write structure without the bins -> use for storcli
*/
struct sp_write_arg_no_bins_t {
    uint16_t    cid;
    uint8_t     sid;          
    uint8_t     layout;
    uint8_t     spare;
    uint32_t     dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid;        
    uint8_t     proj_id;     
    uint64_t    bid;
    uint32_t    nb_proj;
    uint32_t    alignment;
    uint32_t    len;
};

struct sp_write_repair_arg_t {
    uint16_t    cid;
    uint8_t     sid;          
    uint8_t     layout;
    uint8_t     spare;
    uint32_t     dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid;        
    uint8_t     proj_id;     
    uint64_t    bid;
    uint32_t    nb_proj;
    uint32_t    alignment;
    uint64_t    bitmap;
    opaque      bins<>;
};


/*
** write repair structure without the bins -> use for storcli
*/
struct sp_write_repair_arg_no_bins_t {
    uint16_t    cid;
    uint8_t     sid;          
    uint8_t     layout;
    uint8_t     spare;
    uint32_t     dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid;        
    uint8_t     proj_id;     
    uint64_t    bid;
    uint32_t    nb_proj;
    uint32_t    alignment;
    uint64_t    bitmap;
    uint32_t    len;
};


struct sp_read_arg_t {
    uint16_t    cid;
    uint8_t     sid;
    uint8_t     layout;
    uint8_t     spare;
    uint32_t     dist_set[ROZOFS_SAFE_MAX_RPC];
    sp_uuid_t   fid; 
    uint64_t    bid;
    uint32_t    nb_proj;
};

struct sp_truncate_arg_no_bins_t {
    uint16_t    cid;
    uint8_t     sid;
    uint8_t     layout;
    uint8_t     spare;
    uint8_t     dist_set[ROZOFS_SAFE_MAX];
    sp_uuid_t   fid; 
    uint8_t     proj_id;
    uint16_t    last_seg;
    uint64_t    last_timestamp; 
    uint64_t    bid; 
    uint32_t    len;
};
    
struct sp_truncate_arg_t {
    uint16_t    cid;
    uint8_t     sid;
    uint8_t     layout;
    uint8_t     spare;
    uint8_t     dist_set[ROZOFS_SAFE_MAX];
    sp_uuid_t   fid; 
    uint8_t     proj_id;
    uint16_t    last_seg;
    uint64_t    last_timestamp; 
    uint64_t    bid;    
    opaque      bins<>;    
};

struct sp_read_t {
    uint32_t    filler;
    opaque      bins<>;
    uint64_t    file_size;
};

union sp_read_ret_t switch (sp_status_t status) {
    case SP_SUCCESS:    sp_read_t  rsp;
    case SP_FAILURE:    int     error;
    default:            void;
};

union sp_write_ret_t switch (sp_status_t status) {
    case SP_SUCCESS:    uint64_t    file_size;
    case SP_FAILURE:    int         error;
    default:            void;
};

program STORAGE_PROGRAM {
    version STORAGE_VERSION {
        void
        SP_NULL(void)                   = 0;

        sp_write_ret_t
        SP_WRITE(sp_write_arg_t)        = 1;

        sp_read_ret_t
        SP_READ(sp_read_arg_t)          = 2;

        sp_status_ret_t
        SP_TRUNCATE(sp_truncate_arg_t)  = 3;

        sp_write_ret_t
        SP_WRITE_REPAIR(sp_write_repair_arg_t)        = 4;
    }=1;
} = 0x20000002;
