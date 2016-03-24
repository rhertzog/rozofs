#!/usr/bin/python
# -*- coding: utf-8 -*-
cnf_clusters=[]
global layout
global nbclusters
global clients_nb

#_____________________________________
def layout1_4servers():
  global layout
  global nbclusters

  layout = rozofs.layout_4_6_8()
  
  # Create a volume
  v1 = volume_class(layout,1)

  # Create 2 clusters on this volume
  for i in range(nbclusters):    
    c = v1.add_cid(devices,mapper,redundancy)  
    cnf_clusters.append(c)

  # Create the required number of sid on each cluster
  # The 2 clusters use the same host for a given sid number
  for s in range(rozofs.min_sid(layout)/2):
    for c in cnf_clusters:    
      c.add_sid_on_host(s+1)
      c.add_sid_on_host(s+1)
    	  
  # Create on export for 4K, and one moun point
  e1 = v1.add_export(rozofs.bsize4K())
  m1 = e1.add_mount()
#_____________________________________
def layout2_4servers():
  global layout
  global nbclusters

  layout = rozofs.layout_8_12_16()
  
  # Create a volume
  v1 = volume_class(layout,1)

  # Create 2 clusters on this volume
  for i in range(nbclusters):    
    c = v1.add_cid(devices,mapper,redundancy)  
    cnf_clusters.append(c)

  # Create the required number of sid on each cluster
  # The 2 clusters use the same host for a given sid number
  for s in range(rozofs.min_sid(layout)/4):
    for c in cnf_clusters:    
      c.add_sid_on_host(s+1)
      c.add_sid_on_host(s+1)
      c.add_sid_on_host(s+1)
      c.add_sid_on_host(s+1)
    	  
  # Create on export for 4K, and one moun point
  e1 = v1.add_export(rozofs.bsize4K())
  m1 = e1.add_mount()  
#_____________________________________
def layout2_8servers():
  global layout
  global nbclusters

  layout = rozofs.layout_8_12_16()
  
  # Create a volume
  v1 = volume_class(layout,1)

  # Create 2 clusters on this volume
  for i in range(nbclusters):    
    c = v1.add_cid(devices,mapper,redundancy)  
    cnf_clusters.append(c)

  # Create the required number of sid on each cluster
  # The 2 clusters use the same host for a given sid number
  for s in range(rozofs.min_sid(layout)/2):
    for c in cnf_clusters:    
      c.add_sid_on_host(s+1)
      c.add_sid_on_host(s+1)
    	  
  # Create on export for 4K, and one moun point
  e1 = v1.add_export(rozofs.bsize4K())
  m1 = e1.add_mount()    

#_____________________________________ 
def createNbSids(nbSid):
  global layout
  global nbclusters
  global clients_nb
  
  # Create a volume
  v1 = volume_class(layout,rozofs.failures(layout))

  # Create clusters on this volume
  for i in range(nbclusters):
    c = v1.add_cid(devices,mapper,redundancy)  
    cnf_clusters.append(c)

    # Create the required number of sid on each cluster
    # The 2 clusters use the same host for a given sid number
    for s in range(nbSid):
	  c.add_sid_on_host(s+1,s % rozofs.site_number)

  # Create on export for 4K, and one mount point
  e1 = v1.add_export(rozofs.bsize4K())
  
  for i in range(1,clients_nb+1): 
    m = e1.add_mount((i-1) % rozofs.site_number)

#_____________________________________ 
def layout0nbSids(nbSid):
  global layout

  layout = rozofs.layout_2_3_4()
  createNbSids(nbSid)
#_____________________________________ 
def layout1nbSids(nbSid):
  global layout

  layout = rozofs.layout_4_6_8()
  createNbSids(nbSid)

#_____________________________________ 
def layout2nbSids(nbSid):
  global layout

  layout = rozofs.layout_8_12_16()
  createNbSids(nbSid)

#_____________________________________ 

# Number of sites
#rozofs.set_site_number(4)

#rozofs.set_trace()

rozofs.set_alloc_mb(0);

# Change number of core files
# rozofs.set_nb_core_file(1);

# Enable FID recycling
#rozofs.set_fid_recycle(10)
#--------------STORIO GENERAL

# Set original RozoFS file distribution
rozofs.set_file_distribution(2)

# Set single storio mode
# rozofs.storio_mode_single()

# Disable CRC32
# rozofs.set_crc32(False)

# Disable self healing
rozofs.set_self_healing(1)

# Modify number of listen port/ per storio
# rozofs.set_nb_listen(4)

# Modify number of storio threads
# rozofs.set_threads(8)

# Use fixed size file mounted through losetup for devices
#rozofs.set_ext4(100)
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
#rozofs.no_posix_lock

# Disable BSD lock
#rozofs.no_bsd_lock


#-------------- NB devices
devices    = 4
mapper     = 2
redundancy = 2


# Nb cluster per volume
nbclusters = 2

# default is to have one mount point per site
clients_nb = rozofs.site_number


#__Specific layout with small number of server
#layout2_4servers()
#layout2_8servers()
#layout1_4servers()

#__Choose a layout as well as a number of server__
#layout0nbSids(4)
layout1nbSids(8)
#layout2nbSids(16)

# Set host 1 faulty
#h1 = host_class.get_host(1)
#if h1 == None:
#  print "Can find host 1"
#else:
#  h1.set_admin_off()  
  
