#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os.path
import subprocess
import time
import re
import mmap
import shlex
import datetime
from optparse import OptionParser

exportds=[]
ROZODIAG=None
dis=None
error_table=None
version=None
symbol=int(0)
diagnostic=False
fulldiagnostic=False

error_counter=0
warning_counter=0

time_limit_minutes=0
#!/usr/bin/python
# -*- coding: utf-8 -*-

base=""

import sys

#_______________________________________________
class constants:

  def joined_column(self): return "#Zis_IZ_a_JoInED_ColUMn"
  
#_______________________________________________
class column_desc:

  def __init__(self,shift):  
    self.column_nb    = 0
    self.column_sizes = []
    self.shift        = shift
     
  def update_column(self, num, size):
  
    # Column number extension
    if int(num) > self.column_nb:
      for i in range(self.column_nb,num):
        self.column_sizes.append('0')
      self.column_nb = num 	
	
    # Column size extension
    if int(self.column_sizes[int(num)-1]) < int(size):
      self.column_sizes[int(num)-1] = int(size)

    
#_______________________________________________
class big_title:

  def __init__(self,text):  
    self.text = text

  def display(self,column_desc):
    l=0
    for col in range(column_desc.column_nb):
      l += (column_desc.column_sizes[col]+3)
    l -= (len(self.text) +3)
    
    line = ''    
    for i in range(int(column_desc.shift)): line+=' '		
    line+="| "
    start = int(l)/2
    end   = int(l)-start
    for i in range(start): line+=" "	    	
    line+=self.text
    for i in range(end): line+=" " 
    line+=" |"   
    print line  
#_______________________________________________
class separator_line:

  def __init__(self,extreme,separator,previous_line=None):  
    self.extreme    = extreme  
    self.separator  = separator
    self.separators = []
    if previous_line == None: return
      
    const = constants()    
    self.separators.append(extreme)
    skip=True
    for col in previous_line.column:
      if skip==True: 
        skip=False
	continue
      if col == const.joined_column(): self.separators.append('_')
      else:                            self.separators.append(separator)
    self.separators.append(extreme) 
    
  def display(self,column_desc):

    const = constants()
    line = ''    
    for i in range(int(column_desc.shift)): line+=' '
    
    if len(self.separators) != 0:
      for c in range(column_desc.column_nb):
	line += self.separators[c]
	line+='_'	
	for ci in range(int(column_desc.column_sizes[c])): line+='_'	
	line+='_' 
      line+=self.extreme 
      print line
      return       
    
    first=True
    for c in range(column_desc.column_nb):
      if first == True:
        # 1rst line begins with extreme separator
        first = False
	line += self.extreme
      else:	
        # Not a fisrt line
	line += self.separator
      line+='_'	
      for ci in range(int(column_desc.column_sizes[c])): line+='_'	
      line+='_' 
    line+=self.extreme   
    print line    
#_______________________________________________
class display_line:

  def __init__(self,centered=False):  
    self.column     = []   
    self.centered   = centered
      
  def set_column(self,column,value):
    # Extend column number
    if int(column) > len(self.column):
      for i in range(len(self.column),int(column)):
        self.column.append('')
    self.column[int(column)-1] = value

  def check_column(self,column,value):
    # Extend column number
    if int(column) > len(self.column): return False
    if self.column[int(column)-1] == value: return True
    return False
    
  # Join a colum with its preceding column  
  def join_preceding_column(self,column):
    const = constants()
    # Extend column number
    if int(column) > len(self.column):
      for i in range(len(self.column),int(column)):
        self.column.append('')
    self.column[int(column)-1] = const.joined_column()

  def display(self,column_desc):
    const = constants()
    line=''	
    for i in range(int(column_desc.shift)): line+=' '		
    line+="| "
    for col in range(column_desc.column_nb):
    
      try:     val=self.column[col]
      except:  val=''	
      
      if val == const.joined_column(): continue

      l = column_desc.column_sizes[col]-len(val)
      joined = 0
      for jc in range(col+1,column_desc.column_nb):
        try:    next = self.column[jc]
	except: next = ''
        if next != const.joined_column(): break
	l += column_desc.column_sizes[jc]+3
	joined += 1	
      if self.centered == True:
	start = int(l)/2
	end   = int(l)-start
      else:
	try:
	  float(val)	  
          start=l
	  end=0
	except:
	  start = 0
	  end = l

      for i in range(start): line+=" "	    	
      line+=val
      for i in range(end): line+=" " 
      line+=" | "  
      col+=joined 
    print line
        
#_______________________________________________
class adaptative_tbl:

  def __init__(self, shift, title=None):  
    self.row_nb      = int(0)
    self.row         = [] 
    self.current_row = None 
    self.column_desc = column_desc(shift)   
    if title == None: 
      self.separator(' ',' ')      
    else:
      self.separator(' ','_')
      self.row.append(big_title(title)) 
      self.row_nb += 1
      self.separator('|','_')
    
  def add_line(self,centered):
    line = display_line(centered)
    self.row.append(line) 
    self.row_nb += 1
    self.current_row = line
    
  def new_line(self):    self.add_line(False)
  def new_center_line(self): self.add_line(True)
  
  def separator(self,extreme,separator):
    self.row.append(separator_line(extreme,separator,self.current_row)) 
    self.row_nb = int(self.row_nb)+1
    self.current_row = None
            
  def end_separator(self): self.separator('|','|')	 
         
  def set_column(self,column,value):
    self.current_row.set_column(column,value)
    self.column_desc.update_column(column,len(value))      

  def join_preceding_column(self,column):
    self.current_row.join_preceding_column(column)
                	
  def display(self):
    # Must we add and end separator ?
    if self.current_row != None: self.end_separator()  
    for row in range(int(self.row_nb)):              
      self.row[row].display(self.column_desc)
      previous_line=self.row[row]


def increment_counter(parsed):
  global symbol
  global fulldiagnostic
  global debug
    
  if debug == True: 
    print parsed   
  else:
    if fulldiagnostic == True:
      symbol+=int(1)
      sys.stdout.write("\rTest nb %s"%(symbol))
      sys.stdout.flush()
  return
  
  
  
#_______________________________________________
class rozofs_module:

  def __init__(self,host,port,module):
    global modules
    self.host   = host
    self.addr   = self.host.split('/')
    self.port   = port
    self.module = module
    self.activeAddr = None
    self.ping()
    
  def id(self): return "[%s-%s-%s]"%(self.module, self.host, self.port) 

  def FATAL(self,string):
    print "%s FATAL: %s !!!\n"%(self.id(),string)
    exit(2)
    
  def get_error_table(self):
    global error_table
    if error_table == None: 
      error_table = adaptative_tbl(0,"ERROR/WARNING LIST")
      error_table.new_center_line()      
      error_table.set_column(1,'host') 
      error_table.set_column(2,'port') 
      error_table.set_column(3,'module') 
      error_table.set_column(4,'criticity') 
      error_table.set_column(5,'problem description') 
      error_table.set_column(6,'diagnostic command') 
      error_table.end_separator()
    return error_table
  
  @staticmethod 
  def display_error_table():
    global error_table
    global quiet
    if quiet == True: return
    print "      "
    if error_table != None: error_table.display() 
    else:                   print "No error detected"

  def check_error(self, criticity, string):
    tbl = self.get_error_table()
    for row in tbl.row:
      try:
	if row.check_column(1,self.host) == False: continue
	if row.check_column(2,"%s"%(self.port)) == False: continue
	if row.check_column(3,self.module) == False: continue 
	if row.check_column(4,criticity) == False: continue 
	if row.check_column(5,string) == False: continue
	return True 
      except:
        continue
    return False
           
  def add_error(self, criticity, string, tip=None):
    global   base
    global   error_counter
    global   warning_counter
    if self.check_error(criticity, string) == True: return 
    if criticity == 'WARNING': warning_counter +=1
    else:  error_counter += 1
    
    tbl = self.get_error_table()
    tbl.new_line()
    tbl.set_column(1,self.host) 
    tbl.set_column(2,"%s"%(self.port)) 
    tbl.set_column(3,self.module) 
    tbl.set_column(4,criticity) 
    tbl.set_column(5,string) 
    if tip == None: return
    try:
      port = int(self.port)
      tbl.set_column(6,"%sdiag -i %s -p %s -c %s"%(base,self.activeAddr,self.port,tip))
    except:
      tbl.set_column(6,"%sdiag -i %s -T %s -c %s"%(base,self.activeAddr,self.port,tip))
             
  def CRITICAL(self,string,tip=None): self.add_error('CRITICAL',string, tip)
  def ERROR(self,string,tip=None): self.add_error('ERROR',string,tip) 
  def WARNING(self,string,tip=None): self.add_error('WARNING',string,tip)

  def ping(self):
    """Sends a ping to destination
       sous forme d'une liste de lignes unicode
    """   
    #print "PING %s"%(self.id()) 
    self.activeAddr = None
    for addr in self.addr:
      if addr == '': continue
      parsed = ["ping", addr,"-c","1","-w","2"]
      increment_counter(parsed)
      cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      output, error = cmd.communicate()
      if error != '':
	self.WARNING("No response to ping on address %s"%(addr))
      else:
        for line in output.split('\n'):
	  words = line.split()
	  for i in range(len(words)):
	    if words[i] == "received,":
	      nb=int(words[i-1])
	      if nb == int(0):  
	         self.WARNING("No response to ping on address %s"%(addr))
              else:		        
                self.activeAddr = addr
    if self.activeAddr == None: 
      self.CRITICAL("does not respond to ping")    
      return False	
    return True

  def rozodiag_one_addr(self,cmd,addr):
    global ROZODIAG        
    if addr == '': return None

    
    # who is either a port number or a target name 
    try:
      port = int(self.port)
      parsed = [ROZODIAG, "-i",addr,"-p","%s"%(self.port),"-c",cmd] 
    except:
      parsed = [ROZODIAG, "-i",addr,"-T",self.port,"-c",cmd]  

    increment_counter(parsed)

    try:    command = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except: return None

    try:    output, error = command.communicate()
    except: return None

    if "error on connect Connection refused!!!" in output: return None
    if "List of available topics"  in output: return None   
    if error != '': return None

    self.activeAddr = addr
    lines=output.split('\n')
    return lines
            
  def rozodiag(self,cmd):
    """Run a rozodiag command toward this rozofs module
    """    
    global ROZODIAG    
    global base
    
    if self.activeAddr != None:
      lines = self.rozodiag_one_addr(cmd,self.activeAddr)
      if lines != None: return lines
      
    for addr in self.addr:
      lines = self.rozodiag_one_addr(cmd,addr)
      if lines != None: return lines
    
    if cmd == "uptime": self.CRITICAL("Not responding !!!")  
    if cmd == "version" and self.module == "exportd" : return None       
    else:               self.CRITICAL("Can not run %sdiag \"%s\"!!!"%(base,cmd),"%s"%(cmd))
    return None


  def check_version(self):
    """Get the version of a process
    """    
    global version
    if version == None: return True
    
    res = self.rozodiag("version")
    if res == None: return None

    for line in res:
      words = line.split()
      if len(words) < 3: continue
      if words[0] == "version":
	if version != words[2]:
          self.WARNING("has version %s (exportd has version %s)"%(words[2],version),"version")
	  return False
	return True  

  def get_git_tag(self):
    """Get the git tag
    """    
    global version
    res = self.rozodiag("git")
    if res == None: return None

    for line in res:
      words = line.split()
      if len(words) < 3: continue
      if words[0] == "git":
	tag = line.split(':')[1]
	return tag
    return None  
  
  def check_buffer(self):
    """Check whether some buffers are exhausted
    """    
    res = self.rozodiag("buffer")
    if res == None: return

    for line in res:
      words=line.split()
      if len(words) < 13: continue
      available=words[11]

      if available == "0/": self.WARNING("No buffer %s left"%(words[0]),"buffer")

  def check_rpc_resources(self):
     """Check whether some RPC errors have been detected
     """    
     TOCHECK=['SEND_ERR','ENCODING_ERROR','ENCODING_ERROR','NO_CTX_ERROR','NO_BUFFER_ERROR']
     failed = False

     res = self.rozodiag("rpc_resources")
     if res == None: return False

     for line in res:
       for what in TOCHECK:
	 if what in line:
           val= line.split(':')[1].split()[0]
           if int(val) > int(0):
             self.WARNING("%s RPC %s detected"%(val,what),"rpc_resources")	     
	     return False
     return True

  def check_tcp_info(self):
     """Check whether some TCP miss functionning have occured
     """    
     res = self.rozodiag("tcp_info")
     if res == None: return False

     for line in res:
       words=line.split()
       if len(words) < int(24): continue
       try:
         int(words[6]) 
         if int(words[6]) != 0 or int(words[18]) != 0 or int(words[20]) !=0:
	  self.WARNING("TCP retransmit or lost","tcp_info");	     
	  return True
       except:
         continue	 
     return True
     
  def check_uptime(self):
     """Check the system is up for more than 1 day"
     """    
     global time_limit_minutes
     res = self.rozodiag("uptime")
     if res == None: return False

     for line in res:
       words=line.split()
       try:
	 minutes  = int(words[2])*24*60 
	 minutes += int(words[4].split(':')[0])*60
	 minutes += int(words[4].split(':')[1])	 
	 if int(minutes) <= int(time_limit_minutes): self.WARNING("UP for only %s minutes"%(minutes),"uptime")
       except:
	 continue
       return True  
      
  def check_core_files(self):
     res = self.rozodiag("core")
     if res == None: return False
     if "None" in res: return False
     self.WARNING("Some core file exist","core");	    
     return True
    
  def check_logs(self):
    path = self.get_core_path()
    for file in os.listdir(path):
      self.WARNING("core file exist %s/%s"%(path,file))
    return	     
     
  def check_trx(self):
    """Check whether some tansaction error have occured
       on who at address addr
    """    
    TOCHECK=['SEND_ERR','TX_TIMEOUT',]

    res = self.rozodiag("trx")
    if res == None: return False

    status = True
    for line in res:

      if "number of transaction" in line:
	count=line.split(':')[1]
	initial=count.split('/')[0]
	allocated=count.split('/')[1]   
	if int(initial) ==  int(allocated):
          self.CRITICAL("All TRX context allocated","trx")
	  status = False
	else:
	  percent = int(allocated)*100/int(initial)
	  if int(percent) >= int(90):
            self.WARNING("%s/%s TRX context allocated"%(allocated,initial),"trx")	     
	continue

      words=line.split()
      if len(words) < 3: continue

      if words[1] in TOCHECK:
	if words[3] != 0: 
	  self.ERROR("%s %s"%(words[3],words[1]),"trx");	     
	  status = False
      
#_______________________________________________
class storio(rozofs_module):

  def __init__(self,host,port):
    rozofs_module.__init__(self,host,port,'storio')

  def check_storio_log(self):
    """
    """    
    res = self.rozodiag("log")
    if res == None: return False

    for line in res:
      if "nb log" not in line: continue
      val=line.split(':')[1].split('/')[0]
      if int(val) != int(0):
	self.WARNING("%s disk errors occured"%(val),"log")
	return False
    return True  

  def check_fid(self):
    """
    """    
    res = self.rozodiag("fid")
    if res == None: return False

    for line in res:
      if "out of ctx" not in line: continue
      val=line.split(':')[1].split()[0]
      if int(val) > int(0):
	self.ERROR("has %s out of FID ctx"%(val),"fid")
	return False
      return True  
    return False	

  def check_data_integrity(self):
    """Check whether CRC32 errors have been detected
    """    
    res = self.rozodiag("data_integrity")
    if res == None: return

    for line in res:
      words=line.split()  
      if "generation" in line:
	if words[3] != "ENABLED":
          self.WARNING("CRC32 not generated","data_integrity")
	continue
      if "control" in line:
	if words[3] != "ENABLED":
          self.WARNING("CRC32 not controled","data_integrity")
	  break
	continue
      if "computing" in line:
	if words[4] != "HARDWARE":
          self.WARNING("CRC32 computation is %s"%(words[4]),"data_integrity")
	continue
      if "error counter" in line:
	if words[4] != "0": 
          self.ERROR("%s CRC32 errors detected"%(words[4]),"data_integrity")
	continue	   


  def check(self):
    global fulldiagnostic
    if self.check_storio_log() == False: return False
    if fulldiagnostic == True:
      self.rozodiag("uptime")    
      self.check_version() 
      self.check_core_files()
      self.check_uptime()    
      self.check_buffer()
      self.check_data_integrity()
      self.check_rpc_resources()
      #self.check_tcp_info()
      self.check_fid()
    return True 
      

#_______________________________________________
class storage(rozofs_module):

  def __init__(self,site,cid,sid,host):
    global storaged
    rozofs_module.__init__(self,host,'storaged','storaged')    
    self.site    = site
    self.cid     = cid
    self.sid     = sid
    if self.activeAddr == None: self.failed  = True
    else                      : self.failed  = False

  def display(self):
    if self.failed == True: print "        sid %s \t%s\tFailed !!!"%(self.sid,self.host)
    else:                   print "        sid %s \t%s"%(self.sid,self.host)
                
  def check_devices(self):
    res = self.rozodiag("device")
    if res == None: return -1
    
    for line in res:
      words=line.split('|') 
      try: int(words[1])
      except: continue
      status = words[4].split()[0]
      cid=words[1].split()[0]
      sid=words[2].split()[0]
      dev=words[3].split()[0]		
      free= int(words[7]) 
      if status != "IS" and status != "DEG":
	self.ERROR("Device %s of cid%s/sid%s is %s"%(dev, cid, sid, status),"device") 
        self.failed = True 
      if free <= int(20):
	self.WARNING("Device %s of cid%s/sid%s has only %s%s free space"%(dev, cid, sid, free,'%'),"device")         	   
    return 0	
             
  def check_storaged(self):    
    global fulldiagnostic    
    res = self.check_devices() 
    if res == -1: return -1
         
    if fulldiagnostic == True:
      self.rozodiag("uptime")
      self.check_version()     
      self.check_uptime()
      self.check_buffer()
      self.check_rpc_resources()
      #self.check_tcp_info() 
      self.check_core_files()   
    return 0
                   
  def check_storios(self):
      res = self.rozodiag("storio_nb")
      if res == None:
        self.failed = True 
        return -1
      for line in res:
	words=line.split()
	if len(words) < 1:    continue
	if words[0] == "mode":
	  if words[2] == "single":
	    stio = storio(self.host,"storio:0")
	    if stio.check() == False: self.failed = True	    
	    return 0
	if words[0] == "cids":
	  for idx in range(2,len(words)):   
	    stio = storio(self.host,"storio:%s"%(words[idx]))
	    if stio.check() == False: self.failed = True	    
	  return 0
      return -1

#_______________________________________________
class cluster:

  def __init__(self,site,cid):
    self.site = site
    self.cid  = cid
    self.sids = []

  def equal(self,cid):     
    if int(self.cid) != int(cid): return False
    return True    

  def display(self):
    print "      cid %s"%(self.cid)
    for sid in sorted(self.sids,key=lambda storage:storage.sid): sid.display() 
   
#_______________________________________________
class volume:

  def __init__(self,vid,free,freeSz,InitSz):
    self.vid = vid
    self.clusters_0 = []
    self.clusters_1 = []
    self.free = free
    self.freeSz = int(freeSz)*1024
    self.InitSz = int(InitSz)*1024

  def get_cluster(self,site,cid):
    if int(site) == int(0): clusters = self.clusters_0
    else:                   clusters = self.clusters_1
    for c in clusters:
      if c.equal(cid): return c
    c=cluster(site,cid)
    clusters.append(c)
    return c
   
        	      
  def add_storage(self,site,cid,sid,host):
    c = self.get_cluster(site,cid)
    s = storage(site,cid,sid,host)
    c.sids.append(s)
    return 0      

  def display(self):
    print "\n  Volume %s - Free %s/%s %s%c"%(self.vid,display_unit(self.freeSz),display_unit(self.InitSz),self.free,'%')
    if len(self.clusters_0) != 0:
      print "    site: 0"
      for c in sorted(self.clusters_0,key=lambda cluster:cluster.cid): c.display()
    if len(self.clusters_1) != 0:      
      print "    site: 1"
      for c in sorted(self.clusters_1,key=lambda cluster:cluster.cid): c.display()
#_______________________________________________
class export_id:

  def __init__(self,eid,vid,path):
    self.eid  = eid
    self.vid  = vid
    self.path = path
#_______________________________________________
class storcli(rozofs_module):

  def __init__(self,host,port):
    rozofs_module.__init__(self,host,port,'storcli')          

  def check(self): 
    global fulldiagnostic
    status = True
    self.check_trx()
    if fulldiagnostic == True:
      self.check_uptime()
      self.check_version()
      self.check_buffer() 
      #self.check_tcp_info()
      self.check_core_files()
      res = self.rozodiag("start_config")
      if res == None:
	status = False
      else:  
	for line in res:
	  words=line.split()
	  if len(words) < 3: continue
	  if words[0] == "shaper":
	    if words[2] != "0":
	      self.failed = True 
	      self.ERROR("shaper is counfigured to %s"%(words[2]),"start_config")
	      status = False	          
    res = self.rozodiag("storaged_status")
    if res == None:
      status = False
    else:  
      for line in res:
	words=line.split()
	if len(words) < 20:   continue
	if words[0] == "cid": continue  
	if words[8] != "UP":
	  self.ERROR("LBG down toward SID %s %d/%d"%(words[4],int(words[0]),int(words[2])),"storaged_status")	    
	  status = False 
          continue
	if words[10] != "UP":
	  status = False  	  
	  self.ERROR("LBG down toward SID %s %d/%d"%(words[4],int(words[0]),int(words[2])),"storaged_status")	    	    
	if words[12]!= "YES":
	  sstatus = False  
	  self.ERROR("LBG down toward SID %s %d/%d"%(words[4],int(words[0]),int(words[2])),"storaged_status")	    	    
    return status

#_______________________________________________
class client(rozofs_module):

  def __init__(self,host,port,age):
    rozofs_module.__init__(self,host,port,'rozofsmount')          
    self.nbstorcli = "0" 
    self.status    = "OK"
    self.eid       = "0"
    self.vid       = "?"
    self.bsize     = "?"
    self.layout    = "?"
    self.age       = age
    self.site      = "?"
    
    res = self.rozodiag("start_config") 
    if res == None: 
      self.status = "Failed"
      return
    else:
     for line in res:
        if "running_site" in line:
	  self.site=line.split('=')[1].split()[0]
	  break        
             
    res = self.rozodiag("stclbg")
    if res == None: 
      self.status = "Failed"
      return
    else:
      for line in res:
        if "number of configured storcli" in line:
	  self.nbstorcli=line.split(':')[1].split()[0] 
	  break   
    res = self.rozodiag("exp_eid")
    if res != None:
      for line in res:
        words=line.split()
	if len(words) < 2: continue
	if words[0] == "eid":
	  self.eid="%d"%(int(words[1]))
	  break
    res = self.rozodiag("layout")
    if res != None:
      for line in res:
        if "LAYOUT_" in line:
	  words=line.split()
          self.layout = words[0].split('LAYOUT_')[1]
	  self.bsize  = words[4]
	  break
	  
  def add_vid(self,vid):
    self.vid = vid
      
  def check(self): 
    global diagnostic
    if self.status == "Failed": return
    self.check_trx()
    if diagnostic == True:
      self.check_version()  
      self.check_uptime()
      self.check_buffer()
      self.check_core_files()
    #self.check_tcp_info()
    
    for idx in range (1,int(self.nbstorcli)+1):
      try:
        storcli_port=int(self.port)+int(idx)
        stc = storcli(self.host,storcli_port)
      except:
        stc = storcli(self.host,self.port+':'+"%d"%(idx))        	
      if stc.check() == False: self.status = "STORCLI failed" 
       
  def display(self):
    global dis
    if dis == None:     
      dis = adaptative_tbl(2,"Clients")
      dis.new_center_line()
      dis.set_column(1,'IP') 
      dis.set_column(2,'diag.')      
#      dis.join_preceding_column(2)
#      dis.join_preceding_column(3)
      dis.set_column(3,'site')        
      dis.set_column(4,'vol')          
      dis.set_column(5,'exp')        
      dis.set_column(6,'block')
      dis.set_column(7,'layout')       
      dis.set_column(8,'nb')
      dis.set_column(9,'status')        
      dis.set_column(10,'age') 
      dis.new_center_line()
      dis.set_column(1,'address') 
      dis.set_column(2,'port')
      dis.set_column(4,'id')       
      dis.set_column(5,'id') 
      dis.set_column(6,'size')
      dis.set_column(8,'stc')
      dis.set_column(10,'(sec)')          
      dis.end_separator()
    dis.new_line()
    dis.set_column(1,self.host) 
    dis.set_column(2,self.port)
    dis.set_column(3,self.site) 
    dis.set_column(4,self.vid) 
    dis.set_column(5,self.eid) 
    dis.set_column(6,self.bsize)
    dis.set_column(7,self.layout)
    dis.set_column(8,self.nbstorcli)
    dis.set_column(9,self.status)  
    dis.set_column(10,self.age)      

  @staticmethod   
  def output():
    global dis
    dis.display()
    dis = None
    
#_______________________________________________
class export_slave(rozofs_module):
   
  def __init__(self,host,number,master):
    rozofs_module.__init__(self,host,'export:%s'%(number),"export_slave")
    self.master = master          

    # Create its eid
    res = self.rozodiag("vfstat_exp")
    if res == None:
      self.ERROR("do not respond to rozodiag") 
      raise ValueError() 
    for line in res:
      words=line.split()
      if len(words) < 15: continue
      try:
        eid=int(words[0])
	vid=words[2]
	path=words[14]
	self.master.add_eid(eid,vid,path)
      except:
      	continue    

  def check(self):
    global fulldiagnostic
    # Check storage status
    res = self.rozodiag("vfstat_stor")
    for v in self.master.volumes:
      up=0
      for line in res:
	words = line.split()
	if len(words) < 6: continue
	try:   vol=int(words[0])
	except:continue
	if int(v.vid) == int(words[0]):
          status = words[6]
	  if status != 'UP': self.CRITICAL("sees storage cid%s/sid%s of volume %s down"%(words[2],words[4],v.vid),"vfstat_stor")
	  else:              up=up+1
      if up == 0: self.CRITICAL("sees all storages of volume %s down"%(v.vid),"vfstat_stor")    
    if fulldiagnostic == True:
      self.check_version()  
      self.check_trx()
      self.check_buffer()
      self.check_rpc_resources() 
      self.check_uptime()
      #self.check_tcp_info()
      self.check_core_files()
                       
#_______________________________________________
class exportd(rozofs_module):
   
  def __init__(self,host):
    global fulldiagnostic
    
    rozofs_module.__init__(self,host,'exportd','exportd')          

    # Check it responds to ping
    if self.activeAddr == None: raise ValueError()
   
    # Get exportd version number
    if self.get_version() == None: raise valueError()    
    self.tag = self.get_git_tag()
    
    self.volumes = []  
    self.clients = []
    self.eids    = []    

    # Check it responds to rozodiag
    res = self.rozodiag("vfstat_vol")
    if res == None:
      raise ValueError()   

    # add it to the list of exportd
    global exportds  
    exportds.append(self)
    
    # Create its volumes
    for line in res:
      if len(line.split()) < 12: continue
      free = line.split()[11]
      freeSz=line.split()[9]
      InitSz=line.split()[7]
      v    = self.add_volume(line.split()[1],free,freeSz,InitSz)

      # Check free space
      if int(free) < 3:    self.CRITICAL("volume %s on export %s has only %s%c of free space"%(v.vid,self.host,free,'%'),"vfstat_vol")
      elif int(free) < 6:  self.ERROR("volume %s on export %s has only %s%c of free space"%(v.vid,self.host,free,'%'),"vfstat_vol")
      elif int(free) < 9:  self.WARNING("volume %s on export %s has only %s%c of free space"%(v.vid,self.host,free,'%'),"vfstat_vol")    

    
    # Create storages        
    res = self.rozodiag("vstor") 
    for line in res:
      words = line.split()
      if len(words) < 9: continue
      v = self.get_volume(words[2])
      v.add_storage(words[0],words[4],words[6],words[8])	    
    
    # Check storage status
    res = self.rozodiag("vfstat_stor")
    for v in self.volumes:
      up=0
      for line in res:
	words = line.split()
	if len(words) < 6: continue
	try:   vol=int(words[0])
	except:continue
	if int(v.vid) == int(words[0]):
          status = words[6]
	  if status != 'UP': self.CRITICAL("sees storage cid%s/sid%s of volume %s down"%(words[2],words[4],v.vid),"vfstat_stor")
	  else:              up=up+1
      if up == 0: self.CRITICAL("sees all storages of volume %s down"%(v.vid),"vfstat_stor")

  def get_version(self):
    """Get the exportd version
    """    
    global version
    res = self.rozodiag("version")
    if res == None: return None

    for line in res:
      words = line.split()
      if len(words) < 3: continue
      if words[0] == "version":
	version = words[2]
	return version
    return None  

  def check_drbd(self):
    res = self.rozodiag("synchro drbd")
    if res == None: return None
    status = None
    for  line in res:
      words = line.split()
      if len(words)<int(4):continue
      if words[0] ==  "0:" or words[0] ==  ":1":
        if status == None: status = "Success"
        if words[1] != "cs:Connected" or words[2] != "ro:Primary/Secondary" or words[3] != "ds:UpToDate/UpToDate":
	  status = "Failed"
    if status == None       : self.WARNING("No DRBD synchronization","synchro")  
    elif status == "Failed" : self.CRITICAL("DRBD not synchronized","synchro drbd")

  def check_pacemaker(self):
    res = self.rozodiag("synchro crm")
    if res == None: return None
    for  line in res:
      if "Failed" in line:
	self.CRITICAL("CRM failure","synchro crm")
  
      
  def check_export(self):
    global fulldiagnostic
    self.check_drbd()
    self.check_pacemaker()
    if fulldiagnostic == True:
      self.check_trx()
      self.check_buffer()	  
      self.check_rpc_resources()    
      #self.check_tcp_info()
      self.check_uptime()
      self.check_core_files() 
    for v in self.volumes:
      for c in (v.clusters_0+v.clusters_1):
	for s in c.sids:
	  if s.activeAddr == None: continue
	  if s.check_storaged() == 0: s.check_storios()
    self.check_clients()
    
  def add_eid(self,eid,vid,path):
    e = export_id(int(eid),vid,path)
    self.eids.append(e)
    return 0    
    
  def get_eid(self,eid):
    for e in self.eids:
      if int(e.eid) == int(eid): return e
    return None	

  def get_volume(self,vid):
    for v in self.volumes:
      if int(v.vid) == int(vid): return v
    return None		
    
  def add_volume(self,vid,free,freeSz,InitSz):
    v = self.get_volume(vid)
    if v != None: return v	
    v = volume (vid,free,freeSz,InitSz)
    self.volumes.append(v)
    return v   

  def display(self):
    print "\nexportd: %s"%(self.host)
    print " %s"%(self.tag)
    for v in sorted(self.volumes,key=lambda volume:volume.vid): 
      v.display()
      print"    export id :"
      for e in sorted(self.eids,key=lambda export_id:export_id.vid): 
        if e.vid != v.vid: continue
        print "        . %3s : %s"%(e.eid,e.path)
    if len(self.clients) != 0:
      for cl in sorted(self.clients,key=lambda client:client.host): cl.display()
      client.output()
                     
  def get_clients(self):
    for idx in range (1,9):
      slave = export_slave(self.host,idx,self)
      slave.check()
      res = slave.rozodiag("client")
      if res == None: continue  
      for line in res:
	words = line.split()
	if len(words) != 10: continue
	ip   = words[7].split(':')[0]
	port = words[7].split(':')[1]
	c = client(ip,port,words[3])
	e = self.get_eid(c.eid)
	if e != None:
	  c.add_vid(e.vid)
        self.clients.append(c)
	    	  
  def check_clients(self):
    self.get_clients()
    for cl in self.clients: cl.check()
#_______________________________________________
def display_unit(val,precision=1):

  try:
    num=float(val)
  except:
    return val

  for x in ['','K','M','G','T','P']:
    if int(num) < 1000:
       format="%3."+("%d"%(precision))+"f%s"
       return format % (num, x)
    num /= 1000
    for cl in self.clients: cl.check()
#_______________________________________________
def local_check():
    
  # Check local storage configured in storage.conf
  
  data = None
  if os.path.exists('/etc/rozofs/storage.conf'):
    data = open('/etc/rozofs/storage.conf', 'r').read()
  elif os.path.exists('/usr/local/etc/rozofs/storage.conf'): 
    data = open('usr/local/etc/rozofs/storage.conf', 'r').read()
  else:
    s = storage(0,0,0,"127.0.0.1") 
    s.ERROR("No storage.conf")
  if data != None:
    for (cid,sid) in re.findall(r".*cid\s*=\s*(\d*)\s*;\s*\n?.*sid\s*=\s*(\d*)\s*;", data):
      s = storage(0,cid,sid,"127.0.0.1")
      if s.check_storaged() == 0: s.check_storios()

  # check local clients from /etc/fstab

  c = None
  with open("/etc/fstab") as f:
    for line in f:
      if len(line.split()) < 3: continue
      if line.split()[2] == "rozofs":
	instance="0"
	if "instance" in line:
	  i=0
	  string=line.split("instance=")[1]
	  while string[i] != ',' and string[i] != ' ': 
	    instance=instance+string[i]
	    i=i+1
        c = client("127.0.0.1","mount:%d"%(int(instance)),"0")
        c.check()
        c.display()
  if c == None:
    c = client("127.0.0.1","mount:?","0") 
    c.ERROR("No RozoFS client in /etc/fsatb")	
    
    
  do_exit()	

#_______________________________________________
def total_check(exports):
  
  # Create every export responding to rozodiag
  for host in exports.split('/'):
    try:    e=exportd(host)
    except: pass
    
  if len(exportds) == 0:
    e = rozofs_module(exports,'exportd','exportd')
    e.ERROR("No export available")     
  else:  
    # Loop on exports 
    for e in exportds: e.check_export()
  do_exit()
 

#_______________________________________________
def syntax(string):
  """Displays error message as well as command syntex
  """
    
  if string != None: print "!!! %s !!!\n"%(string)
 
  
  print " -e <export1>[/<export2>...] "
  print "    This is a \'/\' separated list of export IP addresses or host names."
  
  exit(2) 
  
#_______________________________________________
def do_exit():
  global fulldiagnostic
  
  if fulldiagnostic: 
    rozofs_module.display_error_table() 
    try: client.output()
    except: pass	 	
  elif error_counter != 0 or warning_counter != 0: 
    rozofs_module.display_error_table()

  if error_counter   != 0: exit(2)
  if warning_counter != 0: exit(1)    
  exit(0)	   

###############################################
#
#                  M A I N 
#
###############################################
base=os.path.basename(sys.argv[0])
base=base.split("_status")[0]

parser = OptionParser()
parser.add_option("-e","--export", action="store",type="string", dest="exports", help="A \'/\' separated list of IP addresses or host names of the export nodes.")
parser.add_option("-d","--diagnostic", action="store_true",default=False, dest="diagnostic", help="Display the error list.")
parser.add_option("-f","--full", action="store_true",default=False, dest="full", help="Process to a full diagnostic of the system.")
parser.add_option("-g","--debug", action="store_true",default=False, dest="debug", help="Debug trace.")

(options, args) = parser.parse_args()


time_limit_minutes = 2

# Is it a quiet or a verbose mode
if options.diagnostic == True: quiet=False
else:                          quiet=True

if options.full == True: 
  fulldiagnostic=True   
  quiet=False
else:
  fulldiagnostic=False  

if options.debug == True: 
  debug=True
else:
  debug=False


# Find out rozodiag path
ROZODIAG=None
for path in ["./rozodiag","/usr/bin/rozodiag","/usr/local/bin/rozodiag","./build/src/rozodiag/rozodiag"]:
 if os.path.isfile(path):
   ROZODIAG=path
   break
if ROZODIAG == None:
  print("")
  print("FATAL: Can not find rozodiag !!!")
  exit(2)    
 
   
  
# Export is given
if options.exports != None: total_check(options.exports)

# No export address given. Ask pace maker where is export is
try:
  parsed = ["crm", "resource","status","exportd-rozofs"]
  cmd = subprocess.Popen(parsed, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  output, error = cmd.communicate()

# No Pace maker and no export address given => just local heck
except: local_check()


# Pace maker can not tell us about exportd 
# => just process local check
if error != '': local_check()


# Pace maker tells where the export is running  
if "resource exportd-rozofs is running on:" in output: total_check(output.split()[-1])


