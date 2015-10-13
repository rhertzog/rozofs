#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os.path
import subprocess
import time
import re
import shlex
import datetime
import shutil
from adaptative_tbl import *


# Read configuratino file
#import * from setup.config

cids = []
hosts = []
volumes= []
mount_points = []

vid_nb=0
cid_nb=0
eid_nb=0

#____________________________________
# Class host
#____________________________________
class host_class:
  def __init__(self, number, site):
    global hosts
    self.number = number
    self.addr=""
    self.addr += "192.168.%s.%s"%(10,self.number)
    for i in range(1,rozofs.nb_listen): self.addr += "/192.168.%s.%s"%(int(i+10),self.number)
    self.sid = []
    self.site = site
    self.admin = True
    hosts.append(self)

  @staticmethod    
  def get_host(val):
    global hosts
    for h in hosts:
      if h.number == val: return h
    return None
   
  def get_sid(self,val):
    for s in self.sid:
      if s.sid == val: return s
    return None  
    
  def add_sid(self,sid):
    self.sid.append(sid)

  def nb_sid(self): return len(self.sid)

  def set_admin_off(self):
    self.admin = False

  def set_admin_on(self):
    self.admin = True    

  def display(self):
    d = adaptative_tbl(2,"Hosts") 
    d.new_center_line()
    d.set_column(1,"#")
    d.set_column(2,"@")
    d.set_column(3,"Site")      
    d.set_column(4,"Vid")
    d.set_column(5,"Cid/Sid")
    d.end_separator()   
    for h in hosts:
      d.new_line()
      d.set_column(1,"%s"%(h.number))   
      d.set_column(2,"%s"%(h.addr))     
      d.set_column(3,"%s"%(h.site))  
      my_vols = []
      string=""
      for s in h.sid:
        string+="%s/%s "%(s.cid.cid,s.sid)
	if s.cid.volume not in my_vols: my_vols.append(s.cid.volume)
      d.set_column(5,"%s"%(string))
      string=""
      for v in my_vols: 
        string+="%s "%(v.vid)
      d.set_column(4,"%s"%(string))
    d.display()       

  def get_config_name(self): return "%s/storage_%s.conf"%(rozofs.get_config_path(),self.number)

  def create_config (self):
    save_stdout = sys.stdout
    sys.stdout = open(self.get_config_name(),"w")
    self.display_config()
    sys.stdout.close()
    sys.stdout = save_stdout

  def delete_config (self):
    try: os.remove(self.get_config_name())
    except: pass 
          
  def start(self):
    if self.admin == False: return
    self.add_if() 
    cmd_system("rozolauncher start /var/run/launcher_storaged_%s.pid storaged -c %s -H %s &"%(self.number,self.get_config_name(),self.addr))

  def stop(self):
    cmd_system("rozolauncher stop /var/run/launcher_storaged_%s.pid storaged"%(self.number))

  def del_if(self,nb=None):
    # Delete one interface
    if nb != None:
      silent_system("ip addr del 192.168.%s.%s/32 dev %s"%(int(nb)+10,self.number,rozofs.interface))  
      return
    # Delete all interfaces
    for i in range(rozofs.nb_listen): self.del_if(i)  

  def add_if(self,nb=None):
    # Add one interface
    if nb != None:
      silent_system("ip addr add 192.168.%s.%s/32 dev %s"%(int(nb)+10,self.number,rozofs.interface))
      return
    # Add all interfaces
    for i in range(rozofs.nb_listen): self.add_if(i)  
     
  def reset(self): 
    self.stop()
    self.start()

  def display_config(self):
    global rozofs    
    if rozofs.self_healing != 0:
      print "self-healing \t= %s;"%(rozofs.self_healing)
    print "export-hosts \t= \"%s\";"%(exportd.export_host)
    print "listen = ( "
    nextl=" "
    for i in range(rozofs.nb_listen):
      print "\t%s{addr = \"192.168.%s.%s\"; port = 41000;}"%(nextl,int(i+10),self.number)
      nextl=","
    print ");"
    nexts=" "
    print "storages = ("
    for s in self.sid:
      print "\t%s{cid = %s; sid = %s; root =\"%s\"; device-total = %s; device-mapper = %s; device-redundancy = %s;}"%(nexts,s.cid.cid,s.sid,s.get_root_path(self.number),s.cid.dev_total,s.cid.dev_mapper,s.cid.dev_red)
      nexts=","
    print "); "
       
  def process(self,opt):
    string="ps -fC rozolauncher"
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in cmd.stdout:
      if not "storaged" in line: continue
      if not "storage_%s.conf"%(self.number) in line: continue
      pid=line.split()[1]
      print "\n_______________STORAGE localhost%s"%(self.number)     
      cmd_system("pstree %s %s"%(opt,pid))
    return
    
  def rebuild(self):  
    cmd_system("storage_rebuild -c %s -H localhost%s -r %s -l 8"%(self.get_config_name(),self.number,exportd.export_host))  

#____________________________________
# Class sid
#____________________________________
class sid_class:

  def __init__(self, cid, sid, host_site0, host_site1):  
    self.cid        = cid
    self.sid        = sid 
    self.host       = []   
    self.add_host(0,host_site0)
    self.add_host(1,host_site1)
 
  def add_host(self,site,name):
    if name == None: return
    h = host_class.get_host(name)
    if h == None: 
      h = host_class(name,site)
    else:
      if site != h.site:
        print "host localhost%s can not be used on site 0 as well as site 1"%(h.number)
	sys.exit(1)
    self.host.append(h)
    h.add_sid(self)    
       
  def get_root_path(self,host_number):
    return "%s/storage_%s_%s_%s"%(rozofs.get_config_path(),host_number,self.cid.cid,self.sid)  

  def get_site_root_path(self,site):
    if len(self.host) < (int(site)+1): return None
    return self.get_root_path(self.host[site].number)  
    
  def create_path(self):
    for h in self.host:    
      root_path=self.get_root_path(h.number)   
      try:os.mkdir(root_path)
      except: pass
      self.create_device("all")    

  def delete_path(self):
    for h in self.host:    
      try:self.delete_device("all")
      except: pass     
      root_path=self.get_root_path(h.number)   
      try: shutil.rmtree(root_path)
      except: pass 
      

  def delete_device(self,device):
    h = self.host[0]   
    if device == "all":
      for dev in range(self.cid.dev_total): self.delete_device(dev)
    else:	
      self.delete_device_file(device)
      path=self.get_root_path(h.number)+"/%s"%(device) 
      try: shutil.rmtree(path)
      except: pass 
    
  def get_device_file_path(self,site): 
    if len(self.host) < (int(site)+1): return None  
    return "%s/devices/cid%s/sid%s/"%(rozofs.get_config_path(),self.cid.cid,self.sid)
 

  def mount_device_file(self,dev):
    if rozofs.disk_size_mb == None: return  
    
    if dev == "all":
      for dev in range(self.cid.dev_total): self.mount_device_file(dev)
      return
      
    path=self.get_device_file_path(int(0))
    h = self.host[0]   
    cmd_system("touch %s/%s/X"%(self.get_root_path(h.number),dev))
       
    string="losetup -j %s%s "%(path,dev)
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = cmd.communicate()
    if output != "":
      loop=output.split(':')[0]  
      print "%s%s -> %s -> %s/%s"%(path,dev,loop,self.get_root_path(h.number),dev)
      if rozofs.fstype == "ext4":
        cmd_system("mount -t ext4 %s %s/%s"%(loop,self.get_root_path(h.number),dev))
      else:
        if rozofs.allocsize == None:
          cmd_system("mount -t xfs %s %s/%s"%(loop,self.get_root_path(h.number),dev))      		
	else:
          cmd_system("mount -t xfs -o allocsize=%s %s %s/%s"%(rozofs.allocsize,loop,self.get_root_path(h.number),dev))      	
    else:
      print "No /dev/loop for %s%s"%(path,dev)  
    return
     	  
  def umount_device_file(self,dev):
    h = self.host[0]    
    silent_system("umount %s/%s"%(self.get_root_path(h.number),dev))
      
  def create_device_file(self,device):
  
    if rozofs.disk_size_mb == None: return

    if device == "all":
      for dev in range(self.cid.dev_total): self.create_device_file(dev)
      return
          
    path=self.get_device_file_path(int(0)) 
    try: os.makedirs(path)
    except: pass 
    
    if os.path.exists("%s/%s"%(path,device)): return
    cmd_system("dd if=/dev/zero of=%s/%s bs=1MB count=%s 2>&1"%(path,device,rozofs.disk_size_mb))
    for loop in range(1,128):
      try:
	string="losetup /dev/loop%s %s/%s "%(loop,path,device)
        parsed = shlex.split(string)
        cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	output, error = cmd.communicate()
	if error == "":
	  if rozofs.fstype == "ext4" :
	    cmd_system("mkfs.ext4 -q /dev/loop%s"%(loop))
	  else:  
	    cmd_system("mkfs.xfs -f -q /dev/loop%s"%(loop))	    
	  return
      except: 
        continue      
    print "Can not find /dev/loop for %s/%s"%(path,device)

  def delete_device_file(self,device):

    if rozofs.disk_size_mb == None: return

    if device == "all":
      for dev in range(self.cid.dev_total): self.delete_device_file(dev)
      return
      
    self.umount_device_file(device)      
    path="%s%s"%(self.get_device_file_path(int(0)),device)

    if not os.path.exists(path): return
    
    string="losetup -j %s"%(path)
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = cmd.communicate()
    if output != "":
      loop=output.split(':')[0] 
      cmd_system("losetup -d %s"%(loop))
    else:
      print "No /dev/loop for %s"%(path) 
    try:os.remove(path)
    except: pass    
          
  def create_device(self,device):
    h = self.host[0]   
    if device == "all":
      for dev in range(self.cid.dev_total): self.create_device(dev)
    else:      
      self.create_device_file(device)
      path=self.get_root_path(h.number)+"/%s"%(device)   
      try: os.makedirs(path)
      except: pass 
      self.mount_device_file(device)
      
  def rebuild(self,device=None):
    h = self.host[0] 
    if device == None:  
      cmd_system("storage_rebuild -c %s -H localhost%s -r %s -s %d/%d -l 8"%(h.get_config_name(),h.number,exportd.export_host,self.cid.cid,self.sid,))  
    else:
      cmd_system("storage_rebuild -c %s -H localhost%s -r %s -s %d/%d -l 8 -d %s"%(h.get_config_name(),h.number,exportd.export_host,self.cid.cid,self.sid,device))  
                      
  def info(self):
    print "cid = %s"%(self.cid.cid)
    print "sid = %s"%(self.sid)
    print "site0 = %s"%(self.host[0].number)
    print "@site0 = %s"%(self.host[0].addr)
    print "path0 = %s"%(self.get_root_path(self.host[0].number))
    if len(self.host) > 1:
      print "site1 = %s"%(self.host[1].number)
      print "@site1 = %s"%(self.host[1].addr)      
      print "path1 = %s"%(self.get_root_path(self.host[1].number))
#____________________________________
# Class cid
#____________________________________
class cid_class:

  def __init__(self, volume, dev_total, dev_mapper, dev_red, dev_size):
    global cids 
    global cid_nb
    cid_nb+=1
    self.cid        = cid_nb
    self.sid        = []
    self.dev_total  = dev_total
    self.dev_mapper = dev_mapper
    self.dev_red    = dev_red 
    self.volume     = volume
    self.georep     = None 
    self.dev_size   = 0;
    cids.append(self) 
    
  @staticmethod
  def get_cid(val):
    global cids
    for c in cids:
      if c.cid == val: return c
    return None  
   
  def get_sid(self,val):
    for s in self.sid:
      if s.sid == val: return s
    return None  
    
  def set_georep(self,georep):
    if self.georep == None:  self.georep = georep
    else:
      if self.georep != georep:
        print "gereplication inconsistency on cid %s"%(self.cid)
	sys.exit(1)
	    
  def add_sid_on_host(self,host_site0, host_site1=None):
    sid=len(self.sid)
    sid+=1  
    s = sid_class(self, sid, host_site0, host_site1) 
    self.sid.append(s)
    if host_site1 == None: self.set_georep(False)
    else                 : self.set_georep(True)    	
    return s   
  
  def create_path(self):
    for s in self.sid: s.create_path()     

  def delete_path(self):
    for s in self.sid: s.delete_path()     

  def nb_sid(self): return len(self.sid)
       


#____________________________________
# Class mount
#____________________________________
class mount_point_class:

  def __init__(self, eid, site=0):
    global mount_points
    instance = len(mount_points)    
    self.instance = instance
    self.eid = eid
    self.site= site    
    mount_points.append(self)

  def info(self):
    print "instance = %s"%(self.instance)
    print "eid = %s"%(self.eid.eid)
    print "vid = %s"%(self.eid.volume.vid)
    print "site = %s"%(self.site)
    print "path = %s"%(self.get_mount_path())
    l=rozofs.layout(self.eid.volume.layout).split('_')
    print "layout = %s %s %s"%(l[1],l[2],l[3])
    print "failures = %s"%(self.eid.volume.failures)
    list=[]
    string=""
    for h in hosts:
      for s in h.sid:
        if s.cid.volume.vid == self.eid.volume.vid:
	  if h.site == self.site:
	    if not h in list:
	      list.append(h)
	      string += " %s"%(h.number)
    print "hosts = %s"%(string)	        
    list=[]
    string=""
    for h in hosts:
      for s in h.sid:
        if s.cid.volume.vid == self.eid.volume.vid:
	  if h.site == self.site:
	    string += " %s-%s-%s"%(h.number,s.cid.cid,s.sid)
    print "sids = %s"%(string)	    

  def get_mount_path(self):
    return "%s/mnt%s_eid%s_site%s"%(rozofs.get_config_path(),self.instance,self.eid.eid,self.site)
    
  def create_path(self):
    global rozofs
    try:os.mkdir(self.get_mount_path())
    except: pass
    
  def delete_path(self):
    try: shutil.rmtree(self.get_mount_path())
    except: pass 
               
  def start(self):
    global rozofs
    options="-o rozofsexporttimeout=24"
    #options += " -o rozofsstoragetimeout=4"
    options += " -o rozofsstorclitimeout=11" 
    options += " -o rozofsnbstorcli=%s"%(rozofs.nb_storcli)
    options += " -o rozofsbufsize=256" 
    options += " -o rozofsminreadsize=256"
    options += " -o rozofsshaper=0"
    if rozofs.posix_lock == True: options += " -o posixlock"
    if rozofs.bsd_lock == True  : options += " -o bsdlock"
    options += " -o rozofsrotate=3"	
    options += " -o site=%s"%(self.site)	    
    options += " -o instance=%s"%(self.instance)
    if rozofs.read_mojette_threads == True: options += " -o mojThreadRead=1"
    if rozofs.write_mojette_threads == False: options += " -o mojThreadWrite=0"
    if rozofs.mojette_threads_threshold != None: options += " -o mojThreadThreshold=%s"%(rozofs.mojette_threads_threshold)

    cmd_system("rozofsmount -H %s -E %s %s %s"%(exportd.export_host,self.eid.get_root_path(),self.get_mount_path(),options))
    
  def stop(self):
    if os.path.exists(self.get_mount_path()):
      cmd_system("umount %s"%(self.get_mount_path()))
      if os.path.exists(self.get_mount_path()): 
        cmd_system("umount -l %s"%(self.get_mount_path()))
     
  def reset(self): 
    self.stop()
    self.start()
       
  def display(self):   
    d = adaptative_tbl(2,"Mount points") 
    d.new_center_line()
    d.set_column(1,"Instance")
    d.set_column(2,"Volume")      
    d.set_column(3,"Export")
    d.set_column(4,"layout")
    d.set_column(5,"Block")
    d.set_column(6,"Site") 
    d.set_column(7,"Mount path") 
    d.new_center_line()
    d.set_column(2,"id")      
    d.set_column(3,"id")
    d.set_column(5,"size")
    d.set_column(6,"number") 
    d.end_separator()    
    for m in mount_points:
      d.new_line()
      d.set_column(1,"%s"%(m.instance))
      d.set_column(2,"%s"%(m.eid.volume.vid))      
      d.set_column(3,"%s"%(m.eid.eid))
      d.set_column(4,"%s"%(rozofs.layout(m.eid.volume.layout)))
      d.set_column(5,"%s"%(rozofs.bsize(m.eid.bsize))) 
      d.set_column(6,"%s"%(m.site))   
      d.set_column(7,"%s"%(m.get_mount_path())) 
    d.display()
    
  def process(self,opt):
    string="ps -fC rozofsmount"
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in cmd.stdout:
      if not "instance=%s"%(self.instance) in line: continue
      pid=line.split()[1]
      print "\n_______________FS %s eid %s vid %s %s"%(self.instance,self.eid.eid,self.eid.volume.vid,self.get_mount_path())     
      cmd_system("pstree %s %s"%(opt,pid))
    return    
#____________________________________
# Class export
#____________________________________
class export_class:

  def __init__(self, bsize, volume):
    global eid_nb
    eid_nb += 1
    self.eid   = eid_nb
    self.bsize = bsize
    self.volume= volume
    self.hquota= ""
    self.squota= "" 
    self.mount =[]
    
  def set_hquota(self,quota):
    self.hquota= quota
    
  def set_squota(self,quota):
    self.squota= quota            

  def get_root_path(self):
    return "%s/export_%s"%(rozofs.get_config_path(),self.eid)  
     
  def add_mount(self,site=0):
    if site != 0:
      if self.volume.georep() != True:
         print "adding mount point on site 1 for eid %s of volume %s without georeplication"%(self.eid,self.volume.vid)
	 sys.exit(1)
    m = mount_point_class(self,site)
    self.mount.append(m)
  
  def create_path(self):  
    try:os.mkdir(self.get_root_path())
    except: pass 
    for m in self.mount: m.create_path()   

  def delete_path(self):
    for m in self.mount: m.delete_path()   
    try: shutil.rmtree(self.get_root_path())
    except: pass 

  def nb_mount_point(self): return len(self.mount)
          
  def display(self):
    for m in self.mount: m.display()
#____________________________________
# Class volume
#____________________________________
class volume_class:

  def __init__(self,layout,failures):
    global vid_nb
    vid_nb+=1
    self.vid        = vid_nb
    self.cid        = [] 
    self.eid        = []  
    self.layout     = layout
    self.failures   = failures;
    volumes.append(self)
    
  def add_cid(self, dev_total, dev_mapper, dev_red, dev_size=0):
    c = cid_class(self,dev_total, dev_mapper, dev_red, dev_size)
    self.cid.append(c)
    return c

  def georep(self):
    georep = None
    for c in self.cid:
      if georep == None: georep = c.georep
      elif c.georep != georep:
	  print "inconsistent georeplication on volume %s"%(v.vid)
	  exit(1)
    return georep
     
  def add_export(self, bsize):
    e = export_class(bsize,self)
    self.eid.append(e)
    return e

  def create_path(self):  
    for c in self.cid: c.create_path()
    for e in self.eid: e.create_path()    

  def delete_path(self):
    for c in self.cid: c.delete_path()
    for e in self.eid: e.delete_path()    

  def nb_cid(self): return len(self.cid)
  def nb_eid(self): return len(self.eid)
     
  def display(self):
    d = adaptative_tbl(2,"Volumes") 
    d.new_center_line()
    d.set_column(1,"Vid")
    d.set_column(2,"Cid")      
    d.set_column(3,"Export")
    d.set_column(4,"layout")
    d.end_separator()    
    d.new_line()
    for v in volumes:
      d.set_column(1,"%s"%(v.vid))
      string=""
      for c in v.cid: string += "%s "%(c.cid)
      d.set_column(2,"%s"%(string))
      string=""
      for e in v.eid: string += "%s "%(e.eid)
      d.set_column(3,"%s"%(string))
      d.set_column(4,"%s"%(rozofs.layout(v.layout)))
    d.display()

#____________________________________
# Class rozo_fs
#____________________________________
class exportd_class:

  def __init__(self,hosts=None):
    if hosts == None:
      self.export_host="localhost/192.168.36.15"
    else:
      self.export_host=hosts  

  def get_config_name(self): return "%s/export.conf"%(rozofs.get_config_path())

  def delete_config(self):
    try: os.remove(self.get_config_name())
    except: pass     
        
  def create_config (self):
    save_stdout = sys.stdout
    sys.stdout = open(self.get_config_name(),"w")
    self.display_config()
    sys.stdout.close()
    sys.stdout = save_stdout
       
  def pid(self):
    string="ps -fC exportd"
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    pid=0
    for line in cmd.stdout:
      if not "exportd" in line: continue
      if not "-i" in line: pid=line.split()[1]
    return pid
           
  def start(self):
    pid=self.pid()
    if pid != 0: 
      print "exportd is already started as process %s"%(pid)
      return
    cmd_system("exportd -c %s"%(self.get_config_name()))    
    
  def stop(self):
    pid=self.pid()
    if pid == 0: return
    cmd_system("kill %s"%(pid))
    pid=self.pid()
    if pid == 0: return
    cmd_system("kill -9 %s"%(pid))    

  def reset(self):
    pid=self.pid()
    if pid == 0: self.start_exportd()
    cmd_system("kill -HUP %s"%(pid))

  def process(self,opt):
    pid = self.pid()
    if pid != 0: 
      print "\n_______________EXPORTD"    
      cmd_system("pstree %s %s"%(opt,pid))
     
  def display_config (self):  
    global volumes
    print "layout = 1;"
    print "volumes ="
    print "("
    nextv=" "
    for v in volumes:
      print "  %s{"%(nextv)
      nextv=","
      print "    vid = %s;"%(v.vid)
      print "    layout = %s;"%(v.layout)
      print "    georep = %s;"%(v.georep())
      print "    cids = "
      print "    ("
      nextc=" "
      for c in v.cid:
	print "     %s{"%(nextc)
	nextc=","      
	print "        cid = %s;"%(c.cid)
	print "        sids = "
	print "        ("
	nexts=" "
	for s in c.sid:
	  if len(s.host) == 1 :
	    print "          %s{sid=%s; host=\"%s\";}"%(nexts,s.sid,s.host[0].addr)
	  else:
	    print "          %s{sid=%s; site0=\"%s\"; site1=\"%s\";}"%(nexts,s.sid,s.host[0].addr,s.host[1].addr)
	  nexts=","
	print "        );"
	print "      }"    
      print "    );"
      print "  }"
    print ");"
    print "exports ="
    print "("
    nexte=" "
    for v in volumes:
      for e in v.eid:
        root_path=e.get_root_path()	
	print "  %s{eid=%s; bsize=\"%s\"; root=\"%s\"; md5=\"\"; squota=\"%s\"; hquota=\"%s\"; vid=%s;}"%(nexte,e.eid,rozofs.bsize(e.bsize),root_path,e.squota,e.hquota,v.vid)
	nexte=","	
    print ");"

  def display(self): 
    print "EXPORTD:"
    print "  . %-12s : %s"%("Hosts",self.export_host)    

#____________________________________
# Class geomgr_class
#____________________________________
class geomgr_class:

  def display(self):
    print "- %s"%(self.number)
    for s in self.sid: print "    cid %s sid %s"%(s.cid.cid,s.sid)

  def get_config_name(self): return "%s/geomgr.conf"%(rozofs.get_config_path())
  def get_saved_config_name(self): return "%s/geomgr.conf"%(os.getcwd())

  def create_config (self):
    save_stdout = sys.stdout
    sys.stdout = open(self.get_config_name(),"w")
    self.display_config()
    sys.stdout.close()
    sys.stdout = save_stdout
   
  def modify(self):
    # If loval conf file does not exists create it
    if os.path.exists(self.get_saved_config_name()):
      save_stdout = sys.stdout
      sys.stdout = open(self.get_saved_config_name(),"w")
      self.display_config()
      sys.stdout.close()
      sys.stdout = save_stdout
    cmd_system("nedit %s"%(self.get_saved_config_name()))
    if os.path.exists(self.get_config_name()):
      shutil.copy(self.get_saved_config_name(),self.get_config_name())
      
  def delete(self): 
    try: os.remove(self.get_saved_config_name())
    except: pass
    
  def  display_config(self):
    
    # When there is a saved geomgr configuration
    # use it
    if os.path.exists(self.get_saved_config_name()):
      os.sytem("cat %s"%(self.get_saved_config_name()))
      return

    print "active = True ;"
    print "export-daemons = (" 
    print "   {" 
    print "	active = True;" 
    print "	host   = \"%s\";"%(exportd.export_host)
    print "	exports="   
    print "	("  
    nexte=" " 
    for v in volumes:
      if v.georep() != True: continue
      for e in v.eid:
	print "         %s{"%(nexte)
	nexte="," 
	print "               active = True;"  
	print "               path   = \"%s\";"%(e.get_root_path()) 
	print "               site   = 1;" 
	print "               nb     = 1;" 
	print "          }," 
	print "          {" 
	print "               active = True;"  
	print "               path   = \"%s\";"%(e.get_root_path())  
	print "               site   = 0;" 
	print "               nb     = 1;" 
	print "               calendar =" 
	print "		   (" 
	print "		     { start=\"8:00\"; stop=\"12:15\";  },"
	print "		     { start=\"14:15\"; stop=\"17:30\"; }"
	print "		   );" 
    print "	);"   
    print "   }" 
    print ');' 

  def delete_config (self):
    try: os.remove(self.get_config_name())
    except: pass 
          
  def start(self):
    cmd_system("rozolauncher start /var/run/launcher_geomgr.pid geomgr -c %s -t 5 &"%(self.get_config_name()))

  def stop(self):
    cmd_system("rozolauncher stop /var/run/launcher_geomgr.pid geomgr")
     
  def reset(self): 
    self.stop()
    self.start()
       
  def process(self,opt):
    string="ps -fC rozolauncher"
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in cmd.stdout:
      if not "geomgr" in line: continue
      pid=line.split()[1]
      print "\n_______________GEOMGR"
      cmd_system("pstree %s %s"%(opt,pid))
    return
  
#____________________________________
# Class rozo_fs
#____________________________________
class rozofs_class:

  def __init__(self):
    self.threads = 4
    self.nb_core_file = 2
    self.crc32 = True
    self.self_healing = 1
    self.nb_listen=2;
    self.storio_mode="multiple";
    self.interface = "eth0"
    self.read_mojette_threads = False
    self.write_mojette_threads = True
    self.mojette_threads_threshold = None
    self.nb_storcli = 1
    self.posix_lock = True
    self.bsd_lock = True
    self.disk_size_mb = None
    self.trace = False
    self.storio_slice = 8
    self.spin_down_allowed = False
    self.file_distribution = 1
    
  def allow_disk_spin_down(self): self.spin_down_allowed = True    
  def set_trace(self): self.trace = True
  def storio_mode_single(self):self.storio_mode = "single"  
  def set_storio_slice(self,sl):self.storio_slice = sl 
  def set_nb_listen(self,nb_listen):self.nb_listen = nb_listen  
  def set_nb_core_file(self,nb_core_file):self.nb_core_file = nb_core_file     
  def set_threads(self,threads):self.threads = threads  
  def set_self_healing(self,self_healing):self.self_healing = self_healing      
  def set_crc32(self,crc32):self.crc32 = crc32  
  def enable_read_mojette_threads(self): self.read_mojette_threads = True
  def disable_write_mojette_threads(self): self.read_mojette_threads = False
  def set_mojette_threads_threshold(self,threshold): self.mojette_threads_threshold = threshold
  def dual_storcli(self): self.nb_storcli = 2
  def no_posix_lock(self): self.posix_lock = False
  def no_bsd_lock(self): self.bsd_lock = False
  def set_file_distribution_origin(self): self.file_distribution = 0
  def set_xfs(self,mb,allocsize=None):
    self.fstype       = "xfs"
    self.disk_size_mb = mb
    self.allocsize    = allocsize
  def set_ext4(self,mb):
    self.fstype = "ext4"
    self.disk_size_mb = mb
  
  def get_config_path(self):
    path = "%s/SIMU"%(os.getcwd())
    if not os.path.exists(path): os.makedirs(path)
    return path
    
  def core_dir(self)        : return "/var/run/rozofs_core"  
  def layout_2_3_4(self)    : return 0
  def layout_4_6_8(self)    : return 1
  def layout_8_12_16(self)  : return 2
  def layout_4_6_9(self)    : return 3 
  def layout(self,val):
    if val == 0: return "layout_2_3_4"
    if val == 1: return "layout_4_6_8"
    if val == 2: return "layout_8_12_16"
    if val == 3: return "layout_4_6_9"
   
  def min_sid(self,val):
    if val == 0: return 4
    if val == 1: return 8
    if val == 2: return 16
    if val == 3: return 9

  def failures(self,val):
    if val == 0: return 1
    if val == 1: return 2
    if val == 2: return 4
    if val == 3: return 1
       
  def bsize4K(self)    : return 0
  def bsize8K(self)    : return 1
  def bsize16K(self)   : return 2
  def bsize32K(self)   : return 3 
  def bsize(self,val):
    if val == 0: return "4K"
    if val == 1: return "8K"
    if val == 2: return "16K"
    if val == 3: return "32K"

  def display_common_config(self):        
    print "nb_disk_thread       = %s;"%(rozofs.threads)
    print "nb_core_file         = %s;"%(rozofs.nb_core_file)
    print "crc32c_check         = %s;"%(rozofs.crc32)
    print "crc32c_generate      = %s;"%(rozofs.crc32)
    print "crc32c_hw_forced     = True;"
    print "storio_slice_number  = %s;"%(rozofs.storio_slice)
    if self.storio_mode == "multiple": print "storio_multiple_mode = True;"
    else:                              print "storio_multiple_mode = False;"
    if rozofs.spin_down_allowed == True: print "allow_disk_spin_down = True;"
    print "file_distribution_rule= %s;"%(self.file_distribution)
    print "fid_recycle          = True;"
    print "trash_high_threshold = 10;"


  def create_common_config(self):
    save_stdout = sys.stdout
    sys.stdout = open("/usr/local/etc/rozofs/rozofs.conf","w")
    self.display_common_config()
    sys.stdout.close()
    sys.stdout = save_stdout    
    shutil.copy2('/usr/local/etc/rozofs/rozofs.conf', '/etc/rozofs/rozofs.conf')

  def create_config(self):
    global hosts
    self.create_common_config()
    exportd.create_config()
    for h in hosts: h.create_config()
    geomgr.create_config()
    
  def delete_config(self):
    global hosts
    exportd.delete_config()
    for h in hosts: h.delete_config()
    geomgr.delete_config()

  def display(self):
    exportd.display()
    print "STORCLI:" 
    print "  . %-12s : %s "%("Nb",self.nb_storcli)
    print "  . %-12s : %s "%("POSIX lock",self.posix_lock)
    print "  . %-12s : %s "%("BSD lock",self.bsd_lock)
    print "  * Mojette threads"
    print "    . %-10s : %s"%("Read",self.read_mojette_threads)
    print "    . %-10s : %s"%("Write",self.write_mojette_threads)
    if self.mojette_threads_threshold == None:
      print "    . %-10s : %s"%("Threshold","default")    
    else:  
      print "    . %-10s : %s bytes"%("Threshold",self.mojette_threads_threshold)
    print "STORIO:"
    print "  . %-12s : %s"%("Mode",self.storio_mode)
    print "  . %-12s : %s"%("CRC32",self.crc32)
    print "  . %-12s : %s minutes"%("Self healing",self.self_healing)
    print "  . %-12s : %s ports"%("Listen",self.nb_listen)
    print "  . %-12s : %s "%("Threads",self.threads)
    if self.disk_size_mb == None:
      print "  . %-12s : %s "%("Device size","no limit")
    else:
      print "  . %-12s : %s MB (%s)"%("Device size",self.disk_size_mb,self.fstype)
        
    if len(volumes) != int(0):
      volumes[0].display()
    if len(hosts) != int(0):
      hosts[0].display()
    if len(mount_points) != int(0):      
      mount_points[0].display()    

  def create_path(self):
    for v in volumes: v.create_path()
    
  def delete_path(self):
    for v in volumes: v.delete_path()

  def resume(self):
    self.create_config()  
    check_build()  
    for h in hosts: h.start()
    exportd.start()
    for m in mount_points: m.start() 
    geomgr.start()
       
  def start(self):  
    self.stop()
    self.create_path()
    self.resume()
    
  def pause(self):
    geomgr.stop()
    for m in mount_points: m.stop()
    for h in hosts: h.stop()
    exportd.stop()
    self.delete_config()

  def stop(self):
    self.pause()
    for h in hosts: h.del_if()
    self.delete_path()
        
  def process(self,opt): 
    geomgr.process(opt)
    exportd.process(opt)
    for h in hosts: h.process(opt)
    for m in mount_points: m.process(opt)

  def cou(self,f):
    if not os.path.exists(f):
      print "%s does not exist"%(f)
      exit(1) 
    string="attr -R -g rozofs %s"%(f)
    parsed = shlex.split(string)
    cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print " ___________ %s ___________"%(f)   
    for line in cmd.stdout:  
      line=line.split('\n')[0]
      print line
      words=line.split()
      if len(words) < int(3): continue      
      if words[0] =="MODE": 
        mode = words[2]
        if mode == "DIRECTORY" or mode == "SYMBOLIC": return 	
	continue
      if words[0] =="FID": 
        fid = words[2]
	continue  
      if words[0] =="STORAGE": 
        dist = words[2]
	continue  
      if words[0] =="CLUSTER": 
        cid = words[2]
	continue  
      if words[0] =="EID": 
        eid = words[2]
	continue  
      if words[0] =="VID": 
        vid = words[2]
	continue      	 
      if words[0] =="ST.NAME": 
        st_name = words[2]
	continue      	 

    SID_LIST=dist.split('-')
    
    c = cids[int(cid)-1]
    
    for site in range(0,2):
    
      print "__________________Site %s"%(site) 
      for sid in SID_LIST:
        print ""
        s = c.sid[int(sid)-1]
	path = s.get_site_root_path(site)
        string="find %s -name \"%s*\""%(path,st_name)
	parsed = shlex.split(string)
	cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for line in cmd.stdout: 
	  fname=line.split('\n')[0]
	  sz=os.path.getsize(fname) 
          print "%10s %s"%(sz,fname)	

  def exe_from_core_dir(self,dir):
    if dir == "storio": return "%s/build/src/%s/%s"%(os.getcwd(),"storaged",dir)
    if dir == "export_slave": return "%s/build/src/%s/%s"%(os.getcwd(),"exportd","exportd")
    if dir == "geomgr" : return "%s/build/src/%s/%s"%(os.getcwd(),"geocli",dir)    
    return "%s/build/src/%s/%s"%(os.getcwd(),dir,dir)

  def do_monitor_cfg (self): 
    global hosts
    global exportd
    global mount_points
    for v in volumes: print "VOLUME %s %s"%(exportd.export_host,v.vid)
    for h in hosts: print "STORAGE %s"%(h.addr) 
    for m in mount_points: print "FSMOUNT localhost %s"%(m.instance)

  def monitor(self): 
    save_stdout = sys.stdout
    sys.stdout = open("monitor.cfg","w")
    self.do_monitor_cfg()
    sys.stdout.close()
    sys.stdout = save_stdout  
    cmd_system("./monitor.py 5 -c monitor.cfg")
    
     
  def core(self,argv):
    if len(argv) == 2:
      for d in os.listdir(self.core_dir()):
        if os.path.isdir(os.path.join(self.core_dir(), d)):
          exe=self.exe_from_core_dir(d)
	  for f in os.listdir(os.path.join(self.core_dir(), d)):
	    name=os.path.join(self.core_dir(), d, f)
            if os.path.getmtime(name) < os.path.getmtime(exe):
	      print "(OLD) %s/%s"%(d,f)
	    else:
	      print "(NEW) %s/%s"%(d,f)  
      return  
    if argv[2] == "remove":
      if len(argv) == 3: return
      if argv[3] == "all":
        for d in os.listdir(self.core_dir()):
          if os.path.isdir(os.path.join(self.core_dir(), d)):
	    for f in os.listdir(os.path.join(self.core_dir(), d)):
	      try: os.remove(os.path.join(self.core_dir(), d, f))
	      except: pass
      else:
        try: os.remove(os.path.join(self.core_dir(), argv[3]))  
        except: pass
      return
    
    dir=argv[2].split('/')[0]
    exe=self.exe_from_core_dir(dir)
    if not os.path.exists(exe):
      print "No such executable"
      return
    cmd_system("ddd %s -core %s"%(exe,os.path.join(self.core_dir(), argv[2])))   
	
	      
#___________________________________________  
def cmd_system (string):
  if rozofs.trace: print string
  os.system(string)
	      
#___________________________________________  
def silent_system (string):
  # print string
  parsed = shlex.split(string)
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  output, error = cmd.communicate()	
        
#___________________________________________  
def check_build ():
  sucess=True
  if not os.path.exists("./build/src/exportd/exportd"):
    print "export is not built"
    sucess=False
  if not os.path.exists("./build/src/storaged/storaged"):
    print "storaged is not built"
    sucess=False
  if not os.path.exists("./build/src/storaged/storio"):
    print "storio is not built"
    sucess=False
  if not os.path.exists("./build/src/storaged/storage_rebuild"):
    print "storage_rebuild is not built"
  if not os.path.exists("./build/src/storaged/storage_list_rebuilder"):
    print "storage_list_rebuilder is not built"
  if not os.path.exists("./build/src/rozofsmount/rozofsmount"):
    print "rozofsmount is not built"
    sucess=False
  if not os.path.exists("./build/src/storcli/storcli"):
    print "storcli is not built"
    sucess=False
  if not os.path.exists("./build/src/rozodiag/rozodiag"):
    print "rozodiag is not built"
    sucess=False
  if not os.path.exists("./build/src/geocli/geocli"):
    print "geocli is not built"
    sucess=False
  if not os.path.exists("./build/src/geocli/geomgr"):
    print "geomgr is not built"
    sucess=False
  if not os.path.exists("./build/src/launcher/rozolauncher"):
    print "geomgr is not built"
    sucess=False
  if sucess==False: sys.exit(-1)
#_____________________________________________  
def syntax_export() :
  print  "./setup.py \tgeomgr  \tstart|stop|reset|pid|modify|delete"    

#_____________________________________________  
def syntax_geomgr() :
  print  "./setup.py \texportd \tstart|stop|reset|pid"
         
#_____________________________________________  
def syntax_mount() :
  print  "./setup.py \tmount   \tall|<instance> start|stop|reset|pid|info"
#_____________________________________________  
def syntax_storage() :
  print  "./setup.py \tstorage \tall|<host> <idx> start|stop|reset|rebuild|pid"
  print  "./setup.py \tstorage \tall|<host> <idx> ifup|ifdown <if#>"
  
#_____________________________________________  
def syntax_cou() :
  print  "./setup.py \tcou     \t<fileName>"
#_____________________________________________  
def syntax_config() :
  print  "./setup.py \tconfig  \t<confFileName>"  
#_____________________________________________  
def syntax_sid() :
  print  "./setup.py \tsid     \t<cid> <sid>\tdevice-delete all|<device>"
  print  "./setup.py \tsid     \t<cid> <sid>\tdevice-create all|<device>"
  print  "./setup.py \tsid     \t<cid> <sid>\trebuild [device]"
  print  "./setup.py \tsid     \t<cid> <sid>\tinfo"
#_____________________________________________  
def syntax_if() :
  print  "./setup.py \tifup|ifdown  \t<if#>"    


#_____________________________________________  
def syntax_all() :
  print  "Usage:"
  #print  "./setup.py \tsite    \t<0|1>"
  print  "./setup.py \t\t\tdisplay [conf. file]"
  print  "./setup.py \t\t\tstart|stop|pause|resume"
  syntax_export()
  syntax_geomgr()
  syntax_mount()
  syntax_storage() 
  syntax_sid() 
  syntax_cou() 
  syntax_config()  
  syntax_if()
  print  "./setup.py \tcore    \tremove all|<coredir>/<corefile>"
  print  "./setup.py \tcore    \t[<coredir>/<corefile>]"
  print  "./setup.py \tprocess \t[pid]"
  print  "./setup.py \tit..."
  print  "./setup.py \tmonitor"
  #print  "./setup.py reload"
  #print  "./setup.py build"
  #print  "./setup.py rebuild"
  #print  "./setup.py clean"
  sys.exit(-1)   
          
#_____________________________________________  
def syntax(string=None,topic=None) :

  if string != None: print "!!! %s !!!\n"%(string)

  if topic == None: syntax_all()
  
  func='syntax_%s'%(topic)
  try:
    getattr(sys.modules[__name__],func)() 
  except:
    pass
  sys.exit(-1)
  
         

def config_standard(layout,vols,cluster_per_vol,eid_per_vol,bsize):
  
  for idx1 in range(vols):

    v = volume_class(layout,rozofs.failures(layout))
    
    for idx2 in range(cluster_per_vol):
      c = v.add_cid(6,4,4)
      for s in range(rozofs.min_sid(layout)):
	c.add_sid_on_host(s+1)
	  
    for idx2 in range(eid_per_vol):
      e = v.add_export(bsize)
      m = e.add_mount()
      
def config_half_layout1(vols,bsize):
  
  for idx1 in range(vols):

    v = volume_class(rozofs.layout_4_6_8(),1)

    c = v.add_cid(4,2,2)
    for s in range(1,5):
      c.add_sid_on_host(s)
      c.add_sid_on_host(s)
      
    e = v.add_export(bsize)
    m = e.add_mount()

def config_4_6_9():
  
  v = volume_class(rozofs.layout_4_6_9())
  c = v.add_cid(1,1,1)
  
  h= 1
  s1 = c.add_sid_on_host(h)
  s2 = c.add_sid_on_host(h)
  s3 = c.add_sid_on_host(h)
  h= 2
  s1 = c.add_sid_on_host(h)
  s2 = c.add_sid_on_host(h)
  s3 = c.add_sid_on_host(h)
  h= 3
  s1 = c.add_sid_on_host(h)
  s2 = c.add_sid_on_host(h)
  s3 = c.add_sid_on_host(h)

  e = v.add_export(rozofs.bsize4K());
  m = e.add_mount()
   
             
def mix_config():
  
  v1 = volume_class(rozofs.layout_2_3_4())
  c1 = v1.add_cid(6,4,4);
  s11 = c1.add_sid_on_host(1)
  s12 = c1.add_sid_on_host(2)
  s13 = c1.add_sid_on_host(3)
  s14 = c1.add_sid_on_host(4)

  c2 = v1.add_cid(6,4,4);
  s21 = c2.add_sid_on_host(1)
  s22 = c2.add_sid_on_host(2)
  s23 = c2.add_sid_on_host(3)
  s24 = c2.add_sid_on_host(4)

  e1 = v1.add_export(0);
  m1=e1.add_mount()

  e2 = v1.add_export(0);
  m2=e2.add_mount()
  m3=e2.add_mount()

  v2 = volume_class(rozofs.layout_4_6_8())
  c3 = v2.add_cid(6,4,4);
  s31 = c3.add_sid_on_host(1,11)
  s32 = c3.add_sid_on_host(2,12)
  s33 = c3.add_sid_on_host(3,13)
  s34 = c3.add_sid_on_host(4,14)
  e3 = v2.add_export(1);
  m4=e3.add_mount()
  m4=e3.add_mount(1)	 
	 
	 
	 
	 
def test_parse(command, argv):	
  global rozofs
  global exportd
  global geomgr
   

  # Add path for rozofs executables
  try:
    for dir in os.listdir("%s/build/src"%(os.getcwd())):
      dir="%s/build/src/%s"%(os.getcwd(),dir)
      if os.path.isdir(dir):
        os.environ["PATH"] += (os.pathsep+dir)
  except: pass
      

  if   command == "display"            : rozofs.display()  
  elif command == "start"              : rozofs.start()  
  elif command == "stop"               : rozofs.stop()  
  elif command == "pause"              : rozofs.pause()  
  elif command == "resume"             : rozofs.resume()  
  elif command == "build"              : cmd_system("./setup.sh build")
  elif command == "rebuild"            : cmd_system("./setup.sh rebuild")
  elif command == "clean"              : cmd_system("./setup.sh clean")
  elif command == "monitor"            : rozofs.monitor()

  elif command == "ifup":
    itf=None 
    if len(argv) < 3 : syntax("Missing interface number","if")
    try:    itf=int(argv[2])
    except: syntax("Bad interface number","if")
    for h in hosts: h.add_if(itf)
    
  elif command == "ifdown": 
    itf=None
    if len(argv) < 3 : syntax("Missing interface number","if")
    try:    itf=int(argv[2])
    except: syntax("Bad interface number","if")
    for h in hosts: h.del_if(itf)

  elif command == "it"                 : 
    param=""
    i=int(2)
    while int(i) < len(argv): 
      param +=" %s"%(argv[i])
      i+=1
    cmd_system("%s/IT2/IT.py %s"%(os.getcwd(),param))
  elif command == "process"            : 
    if len(argv) == 2: rozofs.process('-a') 
    else:              rozofs.process('-ap') 
  elif command == "core"               : rozofs.core(argv)  

  elif command == "exportd"             :
       if len(argv) <= 2: syntax("export requires an action","export")     
       if argv[2] == "stop"        : exportd.stop()
       if argv[2] == "start"       : exportd.start()     
       if argv[2] == "reset"       : exportd.reset() 
       if argv[2] == "pid"         : exportd.process('-ap') 

  elif command == "geomgr"             :
       if len(argv) <= 2: syntax("geomgr requires an action","geomgr")  
       if argv[2] == "stop"        : geomgr.stop()
       if argv[2] == "start"       : geomgr.start()     
       if argv[2] == "reset"       : geomgr.reset() 
       if argv[2] == "pid"         : geomgr.process('-ap') 

       if argv[2] == "modify"      : geomgr.modify()
       if argv[2] == "delete"      : geomgr.delete()     

  elif command == "mount"             :
       if len(argv) <= 3: syntax("mount requires instance + action","mount")
       if argv[2] == "all":
	 first=0
	 last=len(mount_points)
       else:
	 try: instance = int(argv[2])  
	 except: syntax("mount requires an integer instance","mount")
	 if (len(mount_points)) <= int(instance):syntax("No such mount instance %s"%(argv[2]),"mount")
	 first=instance
	 last=instance+1
       for idx in range(first,last):
	 obj = mount_points[idx]       
	 if argv[3] == "stop"        : obj.stop()
	 if argv[3] == "start"       : obj.start()     
	 if argv[3] == "reset"       : obj.reset()          
	 if argv[3] == "pid"         : obj.process('-ap') 
	 if argv[3] == "info"        : obj.info() 

  elif command == "storage"             :
       if len(argv) <= 3: syntax("storage requires instance + action","storage")
       if argv[2] == "all":
	 first=0
	 last=len(hosts)
       else:
	 try: instance = int(argv[2])  
	 except: syntax("storage requires an integer instance","storage")
	 if instance == 0: syntax("No such storage instance","storage")
	 if (len(hosts)) < int(instance):syntax("No such storage instance","storage")
	 first=instance-1
	 last=instance
       for idx in range(first,last):
	 obj = hosts[idx]     
	 if argv[3] == "stop"        : obj.stop()
	 if argv[3] == "start"       : obj.start()     
	 if argv[3] == "reset"       : obj.reset() 
	 if argv[3] == "rebuild"     : obj.rebuild() 
	 if argv[3] == "ifdown"      :
	   if len(argv) <= 4: syntax("Missing interface#","storage")
	   
	   obj.del_if(argv[4])
	 if argv[3] == "ifup"      :
	   if len(argv) <= 4: syntax("Missing interface#","storage")
	   obj.add_if(argv[4])  
	 if argv[3] == "pid"         : obj.process('-ap')           

  elif command == "cou"                  : 
       if len(argv) <= 2: syntax("cou requires a file name","cou")
       rozofs.cou(argv[2]) 

  elif command == "get_nb_vol"         : 
       print "%d"%(len(volumes)) 

  elif command == "sid" : 
       if len(argv) <= 4: syntax("sid requires cid+sid numbers")

       try:     cid = int(argv[2])
       except:  syntax("get_cid_sid requires an integer for cluster id") 
       if cid == 0: syntax("No such cluster id")  
       if (len(cids)) < int(cid): syntax("No such cluster id")
       cid-=1            
       c = cids[cid]

       try:     sid = int(argv[3])
       except:  syntax("get_cid_sid requires an integer for storage id") 
       if sid == 0: syntax("No such storage id")  
       if sid > c.nb_sid(): syntax("No such storage id in this cluster")
       sid-= 1         
       s = c.sid[sid]

       if argv[4] == "device-delete" : 
	 if len(argv) <= 5: syntax("sid device-delete requires a device number","sid")
	 s.delete_device(argv[5])     
       if argv[4] == "device-create" : 
	 if len(argv) <= 5: syntax("sid device-create requires a device number","sid")
	 s.create_device(argv[5]) 
       if argv[4] == "rebuild":
         try:    s.rebuild(argv[5])
	 except: s.rebuild(None)              
       if argv[4] == "info"          : s.info()

  elif command == "config":
       if len(argv) < 3: syntax("config requires a configuration file name","config")
       if not os.path.exists(argv[2]): syntax("config file does not exist","config")
       # 1rst stop every thing
       rozofs.stop() 
       # copy new configuration file
       shutil.copy(argv[2],"cnf.py")
              
  elif command == "get_vol_clusters"   : 
       if len(argv) <= 2: syntax("get_vol_clusters requires a volume number")
       try:    idx = int(argv[2])
       except: syntax("get_vol_clusters requires an integer for volume number") 
       if idx == 0: syntax("No such volume number")
       if (len(volumes)) < int(idx):syntax("No such volume number")       
       idx-=1 
       v = volumes[idx]
       string=""
       for c in v.cid: string += " %s"%(c.cid)
       print string

  elif command == "get_cluster_sid_nb" : 
       if len(argv) <= 2: syntax("get_cluster_sid requires a cluster number")
       try:     idx = int(argv[2])
       except:  syntax("get_cluster_sid requires an integer for cluster number") 
       if idx == 0: syntax("No such cluster number")  
       if (len(cids)) < int(idx): syntax("No such cluster number")
       idx-=1            
       c = cids[idx]
       print "%s"%(c.nb_sid())

  else                                 : syntax("Unexpected command \"%s\n"%(command))

#____Initialize a few objects
def test_init():
  global rozofs
  global exportd
  global geomgr
  
  rozofs  = rozofs_class()
  exportd = exportd_class()
  geomgr  = geomgr_class()
  


if len(sys.argv) < int(2): syntax()
command = sys.argv[1]

# Some initializations
test_init()

# Read configuration file
if command == "display": 
  if len(sys.argv) == 2: 
    if os.path.exists("cnf.py"): execfile("cnf.py")
  else:
    if os.path.exists(sys.argv[2]): execfile(sys.argv[2])
else:  
  if os.path.exists("cnf.py"): execfile("cnf.py")


# Parse the command and execute it  
test_parse(command,sys.argv)
