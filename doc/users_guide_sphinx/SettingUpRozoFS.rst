-----------------
Setting up RozoFS
-----------------

Networking Considerations
=========================

Vlan/port Segregation
---------------------

It is recommended to separate core traffic (application) from the SAN
traffic (RozoFS/Storage) with VLANs. It is recommended to use separate
ports for application and RozoFS/Client. When RozoFS and Storage are
co-located, they can share the same ports. However, if there are enough
available ports, it is better that each entity (RozoFS, Storage) has its
own set of ports.

Flow Control (802.3x)
---------------------

It is **mandatory** to enable Flow Control on the switch ports that
handle RozoFS/Storage traffic. In addition, one must also enable Flow
Control on the NICs used by RozoFS/Storage to obtain the performance
benefit. On many networks, there can be an imbalance in the network
traffic between the devices that send network traffic and the devices
that receive the traffic. This is often the case in SAN configurations
in which many hosts (initiators such as RozoFS) are communicating with
storage devices. If senders transmit data simultaneously, they may
exceed the throughput capacity of the receiver. When this occurs, the
receiver may drop packets, forcing senders to retransmit the data after
a delay. Although this will not result in any loss of data, latency will
increase because of the retransmissions, and I/O performance will
degrade.

Spanning-tree Protocol
----------------------

It is recommended to disable spanning-tree protocol (STP) on the switch
ports that connect end nodes (RozoFS clients and storage array network
interfaces). If it is still decide to enable STP on those switch ports,
one need to check for a STP vendor feature, such as PortFast, which
allows immediate transition of the ports into forwarding state.

Storage and RozoFS Network Configuration
----------------------------------------

RozoFS Clients/Storages node connections to the SAN network switches are
always in active-active mode. In order to leverage to Ethernet ports
utilization, the balancing among the ports is under the control of the
application and not under the control of a bonding driver (there is no
need for bonding driver with RozoFS storage node). When operating in the
default mode of RozoFs (no LACP), it is recommended that each SAN port
belongs to different VLANs. Configuration with 802.3ad (LACP) trunks is
supported, however the Ethernet ports usage will not be optimal since
the selection of a port depends on a hash applied either an MAC or IP
level.

Mutli-link Configuration
~~~~~~~~~~~~~~~~~~~~~~~~

That configuration is the recommended one for RozoFS where there is one
separate Vlan per physical port. The following diagram describes how
storage nodes are connected toward the ToR switches. It is assumed that
the RozoFS clients reside on nodes that are connected towards the
northbound of the ToR SAN switches

.. figure:: pics/multi_link_1.png
   :align: center
   :alt:

.. figure:: pics/multi_link_2.png
   :align: center
   :alt:

LACP Configuration
~~~~~~~~~~~~~~~~~~

In that case, the ports dedicated to the SAN (RozoFS and Storage) are
grouped in one or two LACP groups, depending if we want to separate the
RozoFS and Storage traffic or not. They can be either reside on the same
or different VLANs.

.. figure:: pics/lacp.png
   :align: center
   :alt:

Preparing Nodes
===============

Enable HA for RozoFS metadata servers
-------------------------------------

RozoFS must be combined with a High-Availability (HA) software to enable a
complete storage failover solution. Indeed, unlike storage data daemons
(storaged), the exportd daemon can not be running on several nodes at the same
time. So, if you want a High-available exportd server you need to replicate the
metadata to another node and coordinate exportd actions between nodes.

Rozo Systems made the choice to use these 3 softwares for the ``rozofs-exportd``
service:

 - `DRBD <http://www.drbd.org>`_ - a replicated storage solution mirroring the content of block devices between hosts

 - `Corosync <http://corosync.github.io/corosync/>`_ - a Group Communication System

 - `Pacemaker <http://clusterlabs.org/>`_ - an open source cluster resource manager (CRM)

This chapter explain how setting up a complete RozoFS metadata failover with 2
nodes.

Prerequisites
~~~~~~~~~~~~~

The following section informs you about system requirements, and some
prerequisites for setting up a High Availability ``rozofs-exportd`` service.
It also includes recommendations for this setup.

**Hardware requirements**

The following list specifies hardware requirements:

- At **least two Linux node**.

- At **least two TCP/IP communication media** per node.

- A `STONITH <https://en.wikipedia.org/wiki/STONITH>`_ mechanism. A **STONITH
  device** is a power switch which the cluster uses to reset nodes that are
  thought to be dead or behaving in a strange manner. This is the only reliable
  way to ensure that no data corruption is performed
  by nodes that hang and only appear to be dead.

- **One empty data block device by node** (partition or complete hard disk,
  Logical Volume LVM..) separate from OS Hard Disk.

.. note::
 For more information about STONITH device, see the
 `Fencing and Stonith documentation <http://clusterlabs.org/doc/crm_fencing.html>`_.


**Software requirements**

- One Linux OS, Rozo Systems has only deployed this setup on the following OS:

  - CentOS 6.5, 6.6 and 7

  - Debian 7 and 8

- Enable Network Time Protocol daemon: If nodes are not synchronized, log files
  and cluster reports are very hard to analyze.

**Other recommendations**

For a useful High Availability setup, consider the following recommendations:

- **Redundant Communication Paths**: it's strongly recommended to set up cluster
  communication (corosync) via two or more redundant paths. This can be done
  via:

  - Network Device Bonding.

  - A second communication channel in Corosync.

- We strongly recommend **multiple STONITH devices per node**.

The following diagram describes a ideal network configuration for a exportd HA
setup:

- DRBD and Corosync (Primary interface: ``ring 0``) use a bonding network device

- A second network interface (``ring 1``) is used by corosync if the first interface failed.

- 2 STONITH devices are used:

  - One with the Light Out Management interface (ilo, idrac, IPMI...)

  - One with a Switched PDU

.. figure:: pics/exportd-HA-ideal-setup.png
   :align: center

Meta-data Replication with DRBD 8.4
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**About DRBD**

DRBD replicates data from the primary device to the
secondary device in a way which ensures that both copies of the data remain
identical. Think of it as a networked RAID 1. It mirrors data in real-time, so
its replication occurs continuously. Applications do not need to know that in
fact their data is stored on different disks.

.. note::
    For more information, see the
    `DRBD 8.4 online documentation <http://www.drbd.org/en/doc/users-guide-84>`_.

The following example uses two servers named ``node-1`` and ``node-2``, and the
DRBD resource named ``r0``. Each node use the device ``/dev/sdd`` for low level
device. It sets up ``node-1`` as the primary node. Be sure to
modify the instructions relative to your own configuration.

**Installing DRBD**

On both servers, install DRBD packages

For installing DRBD with *apt* :

.. code-block:: bash

    apt-get install drbd8-utils

For install DRBD with *yum*:

.. code-block:: bash

    yum install kmod-drbd84 drbd84-utils

.. note::
    For more information about DRBD installation, see the
    `DRBD documentation <http://www.drbd.org/docs/about/>`_.

**Configuring a DRBD resource**

The DRBD configuration files are stored in the directory ``/etc/drbd.d/``. There
are two configuration files which are created:

-  ``/etc/drbd.d/r0.res`` corresponds to the configuration for resource *r0*;

-  ``/etc/drbd.d/global_common.conf`` corresponds to the global configuration of
   DRBD.

Create files ``/etc/drbd.d/global_common.conf`` and ``/etc/drbd.d/r0.res`` on
``node-1``, changes the lines according to your parameters, and save them.

Examples of configuration files for DRBD 8.4:

.. code-block:: ini
   :linenos:
   :caption: /etc/drbd.d/global_common.conf

    global {
      usage-count no;
    }

    common {

      handlers {
          # handlers
          pri-lost "/usr/lib/drbd/notify-pri-lost.sh; \
                    /usr/lib/drbd/notify-emergency-reboot.sh; \
                    echo b > /proc/sysrq-trigger ; \
                    reboot -f";
          pri-on-incon-degr "/usr/lib/drbd/notify-pri-on-incon-degr.sh; \
                             /usr/lib/drbd/notify-emergency-reboot.sh; \
                             echo b > /proc/sysrq-trigger; \
                             reboot -f";
          pri-lost-after-sb "/usr/lib/drbd/notify-pri-lost-after-sb.sh; \
                             /usr/lib/drbd/notify-emergency-reboot.sh; \
                             echo b > /proc/sysrq-trigger; \
                             reboot -f";
          fence-peer "/usr/lib/drbd/crm-fence-peer.sh";
          after-resync-target "/usr/lib/drbd/crm-unfence-peer.sh";
          split-brain "/usr/lib/drbd/notify-split-brain.sh root";
      }

      disk {
          on-io-error detach; # If a hard drive fails
          # The handler is supposed to reach the other node over
          # alternative communication paths and call 'drbdadm outdate res' there
          fencing resource-only; # 2 rings should be configured in corosync
          ## Tuning recommendations
          al-extents 3389;
      }

      net {
          verify-alg crc32c;
          csums-alg crc32c;
          rr-conflict call-pri-lost;
          ### Automatic split brain recovery policies
          after-sb-0pri discard-zero-changes ;
          after-sb-1pri call-pri-lost-after-sb ;
          after-sb-2pri call-pri-lost-after-sb ;
          always-asbp;
          ## Tuning recommendations
          max-buffers 8000;
          max-epoch-size 8000;
          sndbuf-size 0;
          unplug-watermark 16;
      }
    }

.. code-block:: ini
   :linenos:
   :caption: /etc/drbd.d/r0.res
   :emphasize-lines: 4-15

    resource r0 {

      protocol C;
      on node-1 {
        device      /dev/drbd0; # Block device name
        disk        /dev/sdd;   #  Lower level device
        address     192.168.1.1:7788; # IP address:port for data transfer
        meta-disk   internal;  # Meta-data are stored on lower level device
      }
      on node-2 {
        device      /dev/drbd0;
        disk        /dev/sdd;
        address     192.168.1.2:7788;
        meta-disk   internal;
      }
    }


This file configure a DRBD resource named ``r0`` which uses an underlying local
disk named ``/dev/sdd`` on both nodes ``node-1`` and ``node-2``.
In this example, we configure the resource to use internal meta-data (means that
DRBD stores its meta data on the same physical lower-level device as the actual
production data) and it uses TCP port ``7788`` for its network connections, and
binds to the IP addresses ``192.168.1.1`` and ``192.168.1.2``, respectively.
This resource is configured to use fully synchronous replication (protocol C).

Copy DRBD configuration files manually to the other node (``node-2``):

.. code-block:: bash

    scp /etc/drbd.d/* node-2:/etc/drbd.d/

**Enabling the DRBD resource**

Each of the following steps must be completed on both nodes.

Initializes the DRBD meta-data:

.. code-block:: bash

    drbdadm -- --ignore-sanity-checks create-md r0

Attach resource ``r0`` to the backing device, set the replication parameters and
connect the resource to its peer:

.. code-block:: bash

    drbdadm up r0

Start the resync process and put the device into the primary role (``node-1`` in
this case) by entering the following command only on ``node-1``:

.. code-block:: bash

    drbdadm --force primary r0

**Creating a file system**

Create desired file system on top of your DRBD device (for example *ext4*):

.. code-block:: bash

    mkfs.ext4 -b 4096 -i 4096 -I 128 /dev/drbd0

**Testing the metadata filesystem**

If the install and configuration procedures worked as expected, you are
ready to run a basic test of the DRBD functionality. Create a mount
point on ``node-1``, such as ``/srv/rozofs/exports``:

.. code-block:: bash

    mkdir -p /srv/rozofs/exports

Mount the DRBD device:

.. code-block:: bash

    mount /dev/drbd0 /srv/rozofs/exports

In the following section, we will configure the management of high availability
with Pacemaker. So it will be necessary to have the rozofs-exportd
configuration file on both servers and this file should be identical. For that
we will move this configuration file to meta-data filesystem  and create
symbolic links on each node.

On ``node-1`` (the current primary node):

.. code-block:: bash

    mv /etc/rozofs/export.conf /srv/rozofs/exports/export.conf

On ``node-1`` and ``node-2``:

.. code-block:: bash

    ln -sf /srv/rozofs/exports/export.conf /etc/rozofs/export.conf

Unmount the DRBD device on ``node-1``:

.. code-block:: bash

    umount /srv/rozofs/exports

To verify that synchronization is performed:

.. code-block:: bash

  cat /proc/drbd
  version: 8.4.3 (api:1/proto:86-101)
  srcversion: 1A9F77B1CA5FF92235C2213
   0: cs:Connected ro:Primary/Secondary ds:UpToDate/UpToDate C r-----
      ns:64 nr:0 dw:24 dr:93981 al:1 bm:5 lo:0 pe:0 ua:0 ap:0 ep:1 wo:f oos:0


The two resources are now synchronized (``UpToDate``). The initial
synchronization is performed, it is necessary to stop the DRBD service
and remove the link for the initialization script not to start the
service automatically DRBD. The service will be controlled by the
Pacemaker service.

Disable DRBD and rozofs-exportd init script on each meta-data node
(depending on your distribution):

Debian Wheezy, CentOS 6 (system V):

.. code-block:: bash

    /etc/init.d/drbd stop
    /etc/init.d/rozofs-exportd stop
    insserv -vrf drbd rozofs-exportd

Debian Jessie, CentOS 7 (systemd):

.. code-block:: bash

    systemctl stop rozofs-exportd drbd
    systemctl disable rozofs-exportd drbd


It's also necessary to create the DRBD device mountpoint directory on
``node-2`` :

.. code-block:: bash

    mkdir -p /srv/rozofs/exports


High Availability with Pacemaker
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pacemaker is an open-source high availability resource management tool
suitable for clusters of Linux machines. This tool can detect machine
failures with a communication system based on an exchange of UDP packets
and migrate services (resource) from one server to another.

The configuration of Pacemaker can be done with the
`crmsh <http://crmsh.github.io/>`_ utility (Cluster Management Shell). It
allows you to manage different resources and propagates changes on each
server automatically. The creation of a resource is done with an entry
named primitive in the configuration file. This primitive uses a script
corresponding to the application to be protected.

In the case of the platform, Pacemaker manages the following resources:

-  rozofs-exportd service;

-  Mounting the file system used to store meta-data;

-  DRBD resource (``r0``), roles (master or slave);

-  Server connectivity.

The following diagram describes the different resources configured and
controlled via Pacemaker. In this case, 2 servers are configured and
``node-1`` is the master server.

.. image :: pics/pacemaker.png
  :align: center


.. note::
   In our case we want setting up the cluster to move all the resources when
   we don't have enough connectivity with the storaged nodes. Therefore we use
   ping resource with the list of storaged nodes IP.


**Installing Corosync and Pacemaker packages**

On both servers, install the following packages:

 - pacemaker

 - crmsh

 - corosync

 - fence-agents, ipmitool (STONITH)

 - resource-agents

 - fping

 - rozofs-exportd

**Create Cluster Authorization Key**

The first component to configure is Corosync. It manages the
infrastructure of the cluster, i.e. the status of nodes and their
operation. For this, we must generate an authentication key that is
shared by all the machines in the cluster. The ``corosync-keygen``
utility can be use to generate this key and then copy it to the other
nodes.

Create key on ``node-1``:

.. code-block:: bash

    corosync-keygen

Copy the key manually to the other node:

.. code-block:: bash

    scp /etc/corosync/authkey root@node-2:/etc/corosync/authkey

**Configuring Corosync**

Besides copying the key, you also have to modify the corosync
configuration file which stored in ``/etc/corosync/corosync.conf``.

Edit your ``corosync.conf`` with the following (example with **unicast** and
**corosync version 2**):

.. code-block:: ini
   :linenos:
   :emphasize-lines: 14-26,36-49
   :caption: /etc/corosync/corosync.conf
   :name: corosync.conf

    totem {
        version: 2

        # How long before declaring a token lost (ms)
        token: 6000
        # How many token retransmits before forming a new configuration
        token_retransmits_before_loss_const: 10

        clear_node_high_bit: yes

        # This specifies the mode of redundant ring
        rrp_mode: passive

        # The following values need to be set based on your environment
        interface {
            ringnumber: 0
            bindnetaddr: 192.168.1.0
            mcastport: 5405
        }

        interface {
            ringnumber: 1
            bindnetaddr: 192.168.2.0
            mcastport: 5407
        }
        transport: udpu
    }

    quorum {
        provider: corosync_votequorum
        # Only valid with 2 nodes
        expected_votes: 2
        two_node: 1
    }

    nodelist {
        node {
            ring0_addr: 192.168.1.1
            ring1_addr: 192.168.2.1
            name: node-1
            nodeid: 1
        }
        node {
            ring0_addr: 192.168.1.2
            ring1_addr: 192.168.2.2
            name: node-2
            nodeid: 2
        }
    }

    logging {
        fileline: off
        to_stderr: no
        to_logfile: no
        #logfile: /var/log/corosync/corosync.log
        to_syslog: yes
        syslog_facility: daemon
        debug: off
        timestamp: on
        logger_subsys {
          subsys: QUORUM
          debug: off
        }
    }

Copy the ``corosync.conf`` manually to the other node:

.. code-block:: bash

    scp /etc/corosync/corosync.conf root@node-2:/etc/corosync/corosync.conf


By default, the Corosync service is disabled. On both servers, change that
by editing ``/etc/default/corosync`` and change the value of START to yes if
needed:

.. code-block:: bash
   :linenos:
   :caption: /etc/default/corosync
   :name: corosync

   START=yes


**Starting Corosync**

Corosync is started as a regular system service. Depending on your
distribution, it may ship with a LSB init script, an upstart job, or a
systemd unit file. Either way, the service is usually named corosync.

Examples:

.. code-block:: bash

    /etc/init.d/corosync start
    service corosync start
    start corosync
    systemctl start corosync

You can now check the ring status manually with ``corosync-cfgtool``:

.. code-block:: bash

  corosync-cfgtool -s
  Printing ring status.
  Local node ID 1
  RING ID 0
          id      = 192.168.1.1
          status  = ring 0 active with no faults
  RING ID 1
          id      = 192.168.2.1
          status  = ring 1 active with no faults


**Configuring Pacemaker**

Once the Pacemaker cluster is set up and before configuring the
different resources and constraints of the Pacemaker cluster, it is
necessary to have the OCF scripts for exportd on each server. This script is
enable to start, stop and monitor the exportd daemon. This script is installed
by default with the rozofs-exportd package
(``/usr/lib/ocf/resource.d/heartbeat/exportd``).

To set the cluster properties, create cluster resources configuration file
``crm.conf`` and changes the lines according to your parameters, and save it.

.. code-block:: bash

  property stonith-enabled="false" no-quorum-policy="ignore"

  rsc_defaults migration-threshold=10 failure-timeout=60

  primitive p-ping ocf:pacemaker:ping params  \
  host_list="192.168.1.1 192.168.1.2 192.168.1.3 192.168.1.4" \
  multiplier="100" dampen="5s" \
  op start timeout="60" op monitor interval="5s" timeout="60"

  clone c-ping p-ping meta interleave="true"

  primitive p-drbd-r0 ocf:linbit:drbd params drbd_resource="r0" \
  adjust_master_score="0 10 1000 10000" op start timeout="240" \
  op stop timeout="100" op notify interval="0" timeout="90" \
  op monitor interval="10" timeout="20" role="Master" \
  op monitor interval="20" timeout="20" role="Slave"

  ms ms-drbd-r0 p-drbd-r0 meta master-max="1" master-node-max="1" \
  clone-max="2" clone-node-max="1" notify="true" \
  globally-unique="false" interleave="true"

  primitive p-fs-exportd ocf:heartbeat:Filesystem params device="/dev/drbd0" \
  directory="/srv/rozofs/exports" fstype="ext4" options="user_xattr,noatime" \
  op start timeout="60" op stop timeout="60" op monitor interval="10"

  primitive exportd-rozofs ocf:heartbeat:exportd params  \
  conffile="/etc/rozofs/export.conf" op monitor interval="10s"

  group grp-exportd p-fs-exportd exportd-rozofs

  colocation c-grp-exportd-on-drbd-r0 inf: grp-exportd ms-drbd-r0:Master

  order o-drbd-r0-before-grp-exportd inf: ms-drbd-r0:promote grp-exportd:start

  location loc-ms-drbd-r0-needs-ping ms-drbd-r0 \
  rule -inf: not_defined pingd or pingd lt 200

Load this configuration with the following command:

.. code-block:: bash

   crm configure load replace crm.conf


Once all the primitives and constraints are loaded, it is possible to
check the correct operations of the cluster with the following command:

.. code-block:: bash

    crm_mon -1

    Last updated: Mon Jun  6 09:46:39 2016
    Last change: Wed Jun  1 15:14:04 2016 by root via cibadmin on node-2
    Stack: corosync
    Current DC: node-1 (version 1.1.14-70404b0) - partition with quorum
    2 nodes and 6 resources configured

    Online: [ node-1 node-2 ]

     Resource Group: grp-exportd
         p-fs-exportd       (ocf::heartbeat:Filesystem):    Started node-1
         exportd-rozofs     (ocf::heartbeat:exportd):       Started node-1
     Master/Slave Set: ms-drbd-r0 [p-drbd-r0]
         Masters: [ node-1 ]
         Slaves: [ node-2 ]
     Clone Set: c-ping [p-ping]
         Started: [ node-1 node-2 ]

**Adding a STONITH resource (example with IPMI)**

Before using STONITH with IPMI, you must configure the network used by the IPMI
devices and the IPMI devices on each node.

After doing this you can add the STONITH resources in the Pacemaker cluster
configuration:

.. code-block:: bash

  crm configure primitive fence-node-1 stonith:fence_ipmilan params \
  pcmk_host_list="node-1" ipaddr="192.168.100.1" \
  login="login" passwd="passwd" lanplus="true" \
  pcmk_reboot_action="off" op monitor interval="3600s"

  crm configure location loc-fence-node-1 fence-node-1 -inf: node-1

  crm configure primitive fence-node-2 stonith:fence_ipmilan params \
  pcmk_host_list="node-2" ipaddr="192.168.100.2" \
  login="login" passwd="passwd" lanplus="true" \
  pcmk_reboot_action="off" op monitor interval="3600s"

  crm configure location loc-fence-node-2 fence-node-2 -inf: node-2

Set the global cluster option stonith-enabled to true:

.. code-block:: bash

  crm configure property stonith-enabled=true


**Testing your cluster configuration**

You can testing your STONITH configuration by typing one of the following
commands on one node:

.. code-block:: bash

  crm node fence <nodename>
  killall -9 corosync


Storaged Nodes
--------------

Storaged Storaged nodes should have appropriate free space on disks. The
storaged service stores transformed data as files on a common file
system (ext4). It is important to dedicate file systems used by storaged
service exclusively to it (use a Logical Volume or dedicated partition).
It is necessary to manage the free space properly.

Configuration Files
===================

Exportd Configuration File
--------------------------

The configuration file of exportd (``export.conf``) consists of 3 types
of information :

-  the redundancy configuration chosen (layout)

-  the list of storage volumes used to store data (volumes)

-  list of file systems exported (exports)

Redundancy Configuration (layout): the **layout** allows you to specify
the configuration of redundancy RozoFS. There are 3 redundancy
configurations that are possible :

-  layout=0; cluster(s) of 4 storage locations, 3 are used for each
   write and 2 for each read

-  layout=1; cluster(s) of 8 storage locations, 6 are used for each
   write and 4 for each read

-  layout=2; cluster(s) 16 storage locations, 12 are used for each write
   and 8 for each read

List of storage volumes (volumes): The list of all the storage
**volumes** used by exportd is grouped under the volumes list. A volume
in the list is identified by a unique identification number (VID) and
contains one or more **clusters** identified by a unique identification
number (CID) consisting of 4, 8 or 16 storage locations according to the
layout you have chosen. Each storage location in a cluster is defined
with the SID (the storage unique identifier within the cluster) and its
network name (or IP address).

List of exported file systems (exports): The exportd daemon can export
one or more file systems. Each exported file system is defined by the
absolute path to the local directory that contains specific metadata for
this file system.

Here is the an example of configuration file (``export.conf``) for
exportd daemon:

.. code-block:: bash

    # rozofs export daemon configuration file

    layout = 0 ; # (inverse = 2, forward = 3, safe = 4)

    volumes = # List of volumes
    (
        {
            # First volume
            vid = 1 ; # Volume identifier = 1
            cids=     # List of clusters for the volume 1
            (
                {
                    # First cluster of volume 1
                    cid = 1;  # Cluster identifier = 1
                    sids =    # List of storages for the cluster 1
                    (
                        {sid = 01; host = "storage-node-1-1";},
                        {sid = 02; host = "storage-node-1-2";},
                        {sid = 03; host = "storage-node-1-3";},
                        {sid = 04; host = "storage-node-1-4";}
                    );
                },
                {
                    # Second cluster of volume 1
                    cid = 2; # Cluster identifier = 2
                    sids =   # List of storages for the cluster 2
                    (
                        {sid = 01; host = "storage-node-2-1";},
                        {sid = 02; host = "storage-node-2-2";},
                        {sid = 03; host = "storage-node-2-3";},
                        {sid = 04; host = "storage-node-2-4";}
                    );
                }
            );
        },
        {
            # Second volume
            vid = 2; # Volume identifier = 2
            cids =   # List of clusters for the volume 2
            (
                {
                    # First cluster of volume 2
                    cid = 3; # Cluster identifier = 3
                    sids =   # List of storages for the cluster 3
                    (
                        {sid = 01; host = "storage-node-3-1";},
                        {sid = 02; host = "storage-node-3-2";},
                        {sid = 03; host = "storage-node-3-3";},
                        {sid = 04; host = "storage-node-3-4";}
                    );
                }
            );
        }
    );

    # List of exported filesystem
    exports = (

      # First filesystem exported
      {eid = 1; root = "/srv/rozofs/exports/export_1"; md5="AyBvjVmNoKAkLQwNa2c";
       squota="128G"; hquota="256G"; vid=1;},
      # Second filesystem exported
      {eid = 2; root = "/srv/rozofs/exports/export_2"; md5="";
      squota=""; hquota = ""; vid=2;}
    );

Storaged Configuration File
---------------------------

The configuration file of the **storaged** daemon (``storage.conf``)
must be completed on each physical server storage where storaged daemon
is used. It contains two informations:

-  ports; list of TCP ports used to receive requests to write and read
   from clients using rozofsmount

-  storages; list of local storage locations used to store the
   transformed data (projections)

List of local storage locations (storages): All of storage locations
used by the storaged daemon on a physical server are grouped under the
storages list. The storages list consists of one or more storage
locations. Each storage location is defined by the CID (unique
identification number of the cluster to which it belongs) and SID (the
storage unique identifier within the cluster) and the absolute path to
the local directory that will contain the specific encoded data for this
storage.

Configuration file example (``storage.conf``) for one storaged daemon:

.. code-block:: bash

    # rozofs storage daemon configuration file.

    # listen: (mandatory)
    #   Specifies list of IP(s) (or hostname(s)) and port(s) the storio
    #   process should listen on for receive write and read requests from
    #   clients.

    listen = (
       {
          addr = "*";
          port = 41001;
       }
    );

    # storages:
    #   It's the list of local storage managed by this storaged.

    storages = (
      {cid = 1; sid = 1; root = "/srv/rozofs/storages/storage_1-1";},
      {cid = 2; sid = 1; root = "/srv/rozofs/storages/storage_2-1";}
    );
