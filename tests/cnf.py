#!/usr/bin/python
# -*- coding: utf-8 -*-

#rozofs.set_trace()

# Change number of core files
# rozofs.set_nb_core_file(1);

# Enable FID recycling
rozofs.set_fid_recycle(10)
#--------------STORIO GENERAL

# Set original RozoFS file distribution
# rozofs.set_file_distribution_origin()

# Set single storio mode
# rozofs.storio_mode_single()

# Disable CRC32
# rozofs.set_crc32(False)

# Disable self healing
# rozofs.set_self_healing(0)

# Modify number of listen port/ per storio
# rozofs.set_nb_listen(4)

# Modify number of storio threads
# rozofs.set_threads(8)

# Use fixed size file mounted through losetup for devices
#rozofs.set_ext4(300)
#rozofs.set_xfs(1000,None)
#rozofs.set_xfs(1000,"4096")
#rozofs.set_xfs(1000,"64K")
#rozofs.set_xfs(1000,"128M")

#--------------CLIENT GENERAL

# Enable mojette thread for read
# rozofs.enable_read_mojette_threads()

# Disable mojette thread for write
# rozofs.disable_write_mojette_threads()

# Modify mojette threads threshold
# rozofs.set_mojette_threads_threshold(32*1024)

# Dual STORCLI
# rozofs.dual_storcli()

# Disable POSIX lock
# rozofs.no_posix_lock

# Disable BSD lock
# rozofs.no_bsd_lock


#--------------Layout
# -- Layout 1
layout = rozofs.layout_4_6_8()
# -- Layout 0
#layout = rozofs.layout_2_3_4()


#-------------- NB devices
devices    = 3
mapper     = 2
redundancy = 2


# Create a volume
v1 = volume_class(layout,rozofs.failures(layout))

# Create 2 clusters on this volume
c1 = v1.add_cid(devices,mapper,redundancy)  
c2 = v1.add_cid(devices,mapper,redundancy)  
#c3 = v1.add_cid(devices,mapper,redundancy)  
#c4 = v1.add_cid(devices,mapper,redundancy)  

# Create the required number of sid on each cluster
# The 2 clusters use the same host for a given sid number
for s in range(rozofs.min_sid(layout)):
  c1.add_sid_on_host(s+1)
  c2.add_sid_on_host(s+1)
#  c3.add_sid_on_host(s+1)
#  c4.add_sid_on_host(s+1)    	  
# Create on export for 4K, and one moun point
e1 = v1.add_export(rozofs.bsize4K())
#e1.set_squota("1G")
#e1.set_hquota("3G")
m1 = e1.add_mount()

# Create on export for 8K, and one moun point
#e2 = v1.add_export(rozofs.bsize8K())
#m2 = e2.add_mount()

# Set host 1 faulty
#h1 = host_class.get_host(1)
#if h1 == None:
#  print "Can find host 1"
#else:
#  h1.set_admin_off()  
  
