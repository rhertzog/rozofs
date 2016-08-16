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

#ifndef _EXPORT_VOLUME_STAT_H
#define _EXPORT_VOLUME_STAT_H

#include <stdint.h>
#include <rozofs/rozofs.h>

typedef struct _export_vol_sid_in_cluster_t
{
   uint16_t sid;                   /**< Storage  identifier                           */
   uint16_t  state;                /**< state of the storage                          */
   uint16_t free_percent;          /**< pourcentage of available space                */
   uint16_t rebalance_state;       /**< rebalance state                               */
   uint8_t  hostname[ROZOFS_HOSTNAME_MAX];         /**< hostname or hostname list     */
   uint64_t total_size_bytes;     /**< sid total size in bytes        */
   uint64_t free_size_bytes;      /**< sid free size in bytes         */
   
} export_vol_sid_in_cluster_t;

typedef struct _export_vol_cluster_stat_t
{
   uint16_t cluster_id;            /**< cluster identifier                   */
   uint16_t nb_sid;                /**< number of sid in the cluster         */
   uint16_t free_percent;          /**< pourcentage of available space       */
   uint16_t rebalance_state;       /**< rebalance state                      */
   uint64_t total_size_bytes;      /**< cluster total size in bytes          */
   uint64_t free_size_bytes;       /**< cluster free size in bytes           */
} export_vol_cluster_stat_t;


typedef struct _export_vol_cluster_stat2_t
{
   uint16_t cluster_id;      /**< cluster identifier              */
   uint16_t nb_sid;         /**< number of sid in the cluster     */
   uint64_t total_size_bytes;     /**< cluster total size in bytes          */
   uint64_t free_size_bytes;      /**< cluster free size in bytes           */
   export_vol_sid_in_cluster_t  sid_tab[SID_MAX];
} export_vol_cluster_stat2_t;


typedef struct _export_vol_stat_t
{
   uint16_t volume_id;          /**< volume identifier              */
   uint16_t nb_cluster;         /**< number of clusters in the volume    */
   export_vol_cluster_stat_t cluster_tab[ROZOFS_CLUSTERS_MAX];
} export_vol_stat_t;

#endif
