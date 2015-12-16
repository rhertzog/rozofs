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

#include <pthread.h>
#include <inttypes.h>
#include <assert.h>
#include <semaphore.h>
#include <mntent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/resource.h>
#include <getopt.h>

#include <rozofs/rozofs_timer_conf.h>
#include <rozofs/core/rozofs_timer_conf_dbg.h>
#include <rozofs/core/rozofs_ip_utilities.h>
#include <rozofs/core/af_unix_socket_generic.h>
#include <rozofs/common/rozofs_site.h>
#include <rozofs/common/common_config.h>
#include <rozofs/core/rozofs_host_list.h>
#include <rozofs/core/rozo_launcher.h>
#include <rozofs/core/rozofs_core_files.h>

#include "rozofsmount.h"
#include "rozofs_sharedmem.h"
#include "rozofs_rw_load_balancing.h"


#define hash_xor8(n)    (((n) ^ ((n)>>8) ^ ((n)>>16) ^ ((n)>>24)) & 0xff)
#define INODE_HSIZE 8192
#define PATH_HSIZE  8192

#define CACHE_TIMEOUT 10.0

#define ROZOFSMOUNT_MAX_TX 32

#define CONNECTION_THREAD_TIMESPEC  2


//static SVCXPRT *rozofsmount_profile_svc = 0;
int rozofs_rotation_read_modulo = 0;
static char *mountpoint = NULL;
    
//DEFINE_PROFILING(mpp_profiler_t) = {0};

sem_t *semForEver; /**< semaphore used for stopping rozofsmount: typically on umount */

/*
** Exportd id free byte count for quota management
*/

int rozofs_stat_start(void *args);



uint64_t   rozofs_client_hash=0;
/**
* fuse request/reponse trace parameters
*/

int rozofs_site_number;  /**< site number for geo-replication */

/**______________________________________________________________________________
*/
/**
*  Compute the client hash

  @param h: hostname string
  @param instance: instance id
  
  @retval hash value
*/
static inline uint64_t rozofs_client_hash_compute(char * hostname, int instance) {
    unsigned char *d = (unsigned char *) hostname;
    uint64_t h;

    h = 2166136261U;
    /*
     ** hash on hostname
     */
    for (; *d != 0; d++) {
        h = (h * 16777619)^ *d;
    }
    /*
     ** hash on instance id
     */
    h = (h * 16777619)^ instance;
    return h;
}     
/*
 *________________________________________________________
 */

/*
 ** API to be called for stopping rozofsmount

 @param none
 @retval none
 */
void rozofs_exit() {
    if (semForEver != NULL) {
        sem_post(semForEver);
        for (;;) {
            severe("rozofsmount exit required.");
            sleep(10);
        }
        fatal("semForEver is not initialized.");
    }
}

extern void rozofsmount_profile_program_1(struct svc_req *rqstp, SVCXPRT *ctl_svc);

static void usage() {
    fprintf(stderr, "ROZOFS options:\n");
    fprintf(stderr, "    -H EXPORT_HOST\t\tdefine address (or dns name) where exportd daemon is running (default: rozofsexport) equivalent to '-o exporthost=EXPORT_HOST'\n");
    fprintf(stderr, "    -E EXPORT_PATH\t\tdefine path of an export see exportd (default: /srv/rozofs/exports/export) equivalent to '-o exportpath=EXPORT_PATH'\n");
    fprintf(stderr, "    -P EXPORT_PASSWD\t\tdefine passwd used for an export see exportd (default: none) equivalent to '-o exportpasswd=EXPORT_PASSWD'\n");
    fprintf(stderr, "    -o rozofsexporttimeout=N\tdefine timeout (s) for exportd requests (default: 25)\n");
    fprintf(stderr, "    -o rozofsstoragetimeout=N\tdefine timeout (s) for IO storaged requests (default: 4)\n");
    fprintf(stderr, "    -o rozofsstorclitimeout=N\tdefine timeout (s) for IO storcli requests (default: 15)\n");
    fprintf(stderr, "    -o debug_port=N\t\tdefine the base debug port for rozofsmount (default: none)\n");
    fprintf(stderr, "    -o instance=N\t\tdefine instance number (default: 0)\n");
    fprintf(stderr, "    -o rozofsnbstorcli=N\tdefine the number of storcli process(es) to use (default: 1)\n");
    fprintf(stderr, "    -o site=N\t\t\tsite number for geo-replication purpose (default:1)\n");
}

static rozofsmnt_conf_t conf;

/**< structure associated to exportd, needed for communication */
exportclt_t exportclt; 

#if 0
static SVCXPRT *rozofsmount_create_rpc_service(int port) {
    int sock;
    int one = 1;
    struct sockaddr_in sin;

    /* Give the socket a name. */
    sin.sin_family = AF_INET;
    sin.sin_port = htons(port);
    sin.sin_addr.s_addr = INADDR_ANY;

    /* Create the socket. */
    sock = socket(PF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        severe("Can't create socket: %s.", strerror(errno));
        return NULL;
    }

    /* Set socket options */
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (char *) &one, sizeof (int));
    setsockopt(sock, SOL_TCP, TCP_DEFER_ACCEPT, (char *) &one, sizeof (int));
    setsockopt(sock, SOL_TCP, TCP_NODELAY, (char *) &one, sizeof (int));

    /* Bind the socket */
    if (bind(sock, (struct sockaddr *) &sin, sizeof (struct sockaddr)) < 0) {
        severe("Couldn't bind to tcp port %d", port);
        return NULL;
    }

    /* Creates a TCP/IP-based RPC service transport */
    return svctcp_create(sock, ROZOFS_RPC_BUFFER_SIZE, ROZOFS_RPC_BUFFER_SIZE);

}
#endif

#define DISPLAY_UINT32_CONFIG(field)   pChar += sprintf(pChar,"%-25s = %u\n",#field, conf.field); 
#define DISPLAY_INT32_CONFIG(field)   pChar += sprintf(pChar,"%-25s = %d\n",#field, conf.field); 
#define DISPLAY_STRING_CONFIG(field) \
  if (conf.field == NULL) pChar += sprintf(pChar,"%-25s = NULL\n",#field);\
  else                    pChar += sprintf(pChar,"%-25s = %s\n",#field,conf.field); 
  
void show_start_config(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();
  
  DISPLAY_STRING_CONFIG(host);
  DISPLAY_STRING_CONFIG(export);
  DISPLAY_STRING_CONFIG(passwd);  
  DISPLAY_UINT32_CONFIG(buf_size);
  DISPLAY_UINT32_CONFIG(min_read_size);
  DISPLAY_UINT32_CONFIG(max_write_pending);
  DISPLAY_UINT32_CONFIG(nbstorcli);
  DISPLAY_UINT32_CONFIG(max_retry);
  DISPLAY_UINT32_CONFIG(dbg_port);
  DISPLAY_UINT32_CONFIG(instance);  
  DISPLAY_UINT32_CONFIG(export_timeout);
  DISPLAY_UINT32_CONFIG(storcli_timeout);
  DISPLAY_UINT32_CONFIG(storage_timeout);
  DISPLAY_UINT32_CONFIG(fs_mode); 
  DISPLAY_UINT32_CONFIG(cache_mode);  
  DISPLAY_UINT32_CONFIG(attr_timeout);
  DISPLAY_UINT32_CONFIG(entry_timeout);
  DISPLAY_UINT32_CONFIG(rotate);  
  DISPLAY_UINT32_CONFIG(posix_file_lock);  
  DISPLAY_UINT32_CONFIG(bsd_file_lock);  
  DISPLAY_UINT32_CONFIG(noXattr);  
  DISPLAY_INT32_CONFIG(site); 
  DISPLAY_INT32_CONFIG(conf_site_file); 
  DISPLAY_UINT32_CONFIG(running_site);  
  uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());
}  
/*__________________________________________________________________________
*/  
static char * show_rotate_modulo_help(char * pChar) {
  pChar += sprintf(pChar,"usage:\n");
  pChar += sprintf(pChar,"rotateModulo set <value> : set new rotate modulo value\n");
  pChar += sprintf(pChar,"rotateModulo             : display rotate modulo value\n");  
  return pChar; 
}
void show_rotate_modulo(char * argv[], uint32_t tcpRef, void *bufRef) {
  char *pChar = uma_dbg_get_buffer();
  int   new_val;
   
   if (argv[1] !=NULL)
   {
    if (strcmp(argv[1],"set")==0) {
      errno = 0;       
      new_val = (int) strtol(argv[2], (char **) NULL, 10);   
      if (errno != 0) {
        pChar += sprintf(pChar, "bad value %s\n",argv[2]);
	pChar = show_rotate_modulo_help(pChar);
        uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
	return;     
      } 
      rozofs_rotation_read_modulo = new_val;        
      uma_dbg_send_format(tcpRef, bufRef, TRUE, "New rotate modulo set to %d\n",rozofs_rotation_read_modulo);    
      return;     
   }
    /*
    ** Help
    */
    pChar = show_rotate_modulo_help(pChar);
    uma_dbg_send(tcpRef, bufRef, TRUE, uma_dbg_get_buffer());   
    return;   
  }
  uma_dbg_send_format(tcpRef, bufRef, TRUE, "rotation modulo is %d\n",rozofs_rotation_read_modulo);    
  return;     
}   


void rozofs_kill_one_storcli(int instance) {
    char pid_file[64];

    sprintf(pid_file,"/var/run/launcher_geocli_%d_storcli_%d.pid", conf.instance, instance);
    rozo_launcher_stop(pid_file);
}


void rozofs_start_one_storcli(int instance) {
    char cmd[1024];
    uint16_t debug_port_value;
    int site;
     char pid_file[128];
     
    char *cmd_p = &cmd[0];
    cmd_p += sprintf(cmd_p, "%s ", "storcli");
    cmd_p += sprintf(cmd_p, "-i %d ", instance);
    cmd_p += sprintf(cmd_p, "-H %s ", conf.host);
    cmd_p += sprintf(cmd_p, "-o %s ", "geocli");
    cmd_p += sprintf(cmd_p, "-E %s ", conf.export);
    cmd_p += sprintf(cmd_p, "-M %s ", mountpoint);
    
    /* Try to get debug port from /etc/services */
    debug_port_value = conf.dbg_port + instance;
//    sprintf(debug_port_name,"rozo_storcli%d_%d_dbg",conf.instance,instance);
//    debug_port_value = get_service_port(debug_port_name,NULL,debug_port_value);
          
    cmd_p += sprintf(cmd_p, "-D %d ", debug_port_value);
    cmd_p += sprintf(cmd_p, "-R %d ", conf.instance);
    cmd_p += sprintf(cmd_p, "--shaper 0 ");
    cmd_p += sprintf(cmd_p, "-s %d ", ROZOFS_TMR_GET(TMR_STORAGE_PROGRAM));
#if 1
    if (instance == 1) 
    {
      site = (rozofs_site_number+1)&(ROZOFS_GEOREP_MAX_SITE-1);
    }
    else
    {
      site =rozofs_site_number;
    }
    cmd_p += sprintf(cmd_p, "-g %d ", site);
#endif
    /*
    ** check if there is a share mem key
    */
    if (rozofs_storcli_shared_mem[instance-1].key != 0)
    {
      cmd_p += sprintf(cmd_p, "-k %d ",rozofs_storcli_shared_mem[SHAREMEM_IDX_READ].key);       
      cmd_p += sprintf(cmd_p, "-l %d ",rozofs_storcli_shared_mem[SHAREMEM_IDX_READ].buf_sz);       
      cmd_p += sprintf(cmd_p, "-c %d ",rozofs_storcli_shared_mem[SHAREMEM_IDX_READ].buf_count);       
    }
    
    sprintf(pid_file,"/var/run/launcher_geocli_%d_storcli_%d.pid", conf.instance, instance);
    rozo_launcher_start(pid_file,cmd);
  
    info("start storcli (instance: %d, export host: %s, export path: %s, mountpoint: %s,"
            " profile port: %d, rozofs instance: %d, storage timeout: %d).",
            instance, conf.host, conf.export, mountpoint,
            debug_port_value, conf.instance,
            ROZOFS_TMR_GET(TMR_STORAGE_PROGRAM));

}
void rozofs_kill_storcli() {
    int i;

    for (i = 1; i <= STORCLI_PER_FSMOUNT; i++) {
      rozofs_kill_one_storcli(i);
    }
}
void rozofs_start_storcli() {
	int i;

	i = stclbg_get_storcli_number();
	while (i) {
		rozofs_start_one_storcli(i);
		i--;
	}
}
/*
 *_______________________________________________________________________
 */

/**
 *  Crash call back

 */
static void rozofs_crash_cbk(int signal) {
  rozofs_kill_storcli();
}

int fuseloop(/*struct fuse_args *args,*/ int fg) {
    int i = 0;
    int ret;
    int err=0;
    int retry_count;
    int export_index=0;
    char * pHost;

    uma_dbg_record_syslog_name("geocli");

    struct timeval timeout_mproto;
    timeout_mproto.tv_sec = 1;//rozofs_tmr_get(TMR_EXPORT_PROGRAM);
    timeout_mproto.tv_usec = 0;

    if (rozofs_host_list_parse(conf.host,'/') == 0) {
      severe("rozofs_host_list_parse(%s)",conf.host);
    }

    init_rpcctl_ctx(&exportclt.rpcclt);
    
    for (retry_count = 15; retry_count > 0; retry_count--) {


        for (export_index=0; export_index < ROZOFS_HOST_LIST_MAX_HOST; export_index++) { 
	
	    pHost = rozofs_host_list_get_host(export_index);
	    if (pHost == NULL) break;
	    
            /* Initiate the connection to the export and get information
             * about exported filesystem */
            if (exportclt_initialize(
                    &exportclt,
                    pHost,
                    conf.export,
		    rozofs_site_number,
                    conf.passwd,
                    conf.buf_size * 1024,
                    conf.min_read_size * 1024,
                    conf.max_retry,
                    timeout_mproto) == 0) break;
        }
	
	/* Connected to one of the given addresses */
	if (pHost != NULL) break;
	
        sleep(2);
	timeout_mproto.tv_sec++;
	
    }

    if (retry_count == 0) {

        fprintf(stderr,
                "rozofsmount failed for:\n" "export directory: %s\n"
                "export hostname: %s\n" "local mountpoint: %s\n" "error: %s\n"
                "See log for more information\n", conf.export, conf.host,
                mountpoint, strerror(errno));
        return 1;
    }

    /* Creates one debug thread */

    pthread_t thread;
    rozofs_fuse_conf_t rozofs_fuse_conf;

    semForEver = malloc(sizeof (sem_t)); /* semaphore for blocking the main thread doing nothing */
    //    int ret;

    if (sem_init(semForEver, 0, 0) == -1) {
        severe("main() sem_init() problem : %s", strerror(errno));
    }

    /*
     ** Register these topics before start the rozofs_stat_start that will
     ** register other topic. Topic registration is not safe in multi-thread
     ** case
     */
    uma_dbg_addTopic("stclbg", show_stclbg);
    uma_dbg_addTopic("shared_mem", rozofs_shared_mem_display);
    uma_dbg_addTopic("start_config", show_start_config);
    uma_dbg_addTopic("rotateModulo", show_rotate_modulo);

    /*
    ** declare timer debug functions
    */
    rozofs_timer_conf_dbg_init();
    /*
    ** Check if the base port of rozodebug has been provided, if there is no value, set it to default
    */
    if (conf.dbg_port == 0) 
    {
      conf.dbg_port = rozofs_get_service_port_geocli_diag(conf.instance);    
    } 
       
    rozofs_fuse_conf.instance = (uint16_t) conf.instance;
    rozofs_fuse_conf.debug_port = conf.dbg_port;

    rozofs_fuse_conf.exportclt = (void*) &exportclt;
    rozofs_fuse_conf.max_transactions = ROZOFSMOUNT_MAX_TX;

    if ((errno = pthread_create(&thread, NULL, (void*) rozofs_stat_start, &rozofs_fuse_conf)) != 0) {
        severe("can't create debug thread: %s", strerror(errno));
        return err;
    }

    /*
    ** create the shared memory used by the storcli's
    */
    for (i = 0; i < SHAREMEM_PER_FSMOUNT; i++) {
       /*
       ** the size of the buffer is retrieved from the configuration. 1K is added for the management part of
       ** the RPC protocol. The key_instance of the shared memory is the concatenantion of the rozofsmount instance and
       ** storcli instance: (rozofsmount<<1 | storcli_instance) (assuming of max of 2 storclis per rozofsmount)
       */
       int key_instance = conf.instance<<SHAREMEM_PER_FSMOUNT_POWER2 | i;
       ret = rozofs_create_shared_memory(key_instance,i,rozofs_fuse_conf.max_transactions,(conf.buf_size*1024)+1024);
       if (ret < 0)
       {
         severe("Cannot create the shared memory for storcli %d\n",i);
       }
    }   
    
    /*
    ** Declare a signal handler and attach a crash callback
    */
    rozofs_signals_declare("geocli", common_config.nb_core_file);
    rozofs_attach_crash_cbk(rozofs_crash_cbk);
    
    /*
    ** start the storcli processes
    */ 
    rozofs_start_storcli();
    
    for (;;) {
        int ret;
        ret = sem_wait(semForEver);
        if (ret != 0) {
            severe("sem_wait: %s", strerror(errno));
            continue;

        }
        break;
    }

    rozofs_kill_storcli();

    exportclt_release(&exportclt);
    rozofs_layout_release();
    if (conf.export != NULL)
        free(conf.export);
    if (conf.host != NULL)
        free(conf.host);
    if (conf.passwd != NULL)
        free(conf.passwd);
    return err ? 1 : 0;
}



int main(int argc, char *argv[]) {
//    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    int fg = 0;
    int c;
    int val;
    int res;
    struct rlimit core_limit;
    static struct option long_options[] = {
        { "help", no_argument, 0, 'h'},
        { "host", required_argument, 0, 'H'},
        { "path", required_argument, 0, 'E'},
        { "pwd", required_argument, 0, 'P'},
        { "dbg", required_argument, 0, 'D'},
        { "mount", required_argument, 0, 'M'},
        { "instance", required_argument, 0, 'i'},
        { "rozo_instance", required_argument, 0, 'R'},
        { "storagetmr", required_argument, 0, 's'},
        { "georeptmr", required_argument, 0, 'g'},
        { "storclitmr", required_argument, 0, 'c'},
        { "site", required_argument, 0, 'G'},
        { 0, 0, 0, 0}
    };
    
    /*
    ** Change local directory to "/"
    */
    if (chdir("/")!= 0) {}
        
    rozofs_layout_initialize();

    memset(&conf, 0, sizeof (conf));
    /*
    ** init of the shared memory data structure
    */
    rozofs_init_shared_memory();
    /*
    ** init of the timer configuration
    */
    rozofs_tmr_init_configuration();

    conf.max_retry = 50;
    conf.buf_size = 0;
    conf.min_read_size = 0;
    conf.max_write_pending = 4; /*  */ 
    conf.attr_timeout = 10;
    conf.entry_timeout = 10;
    conf.nbstorcli = 0;
    conf.shaper = 0; // Default traffic shaper value
    conf.rotate = 0;
    conf.site = -1;
    conf.conf_site_file = -1; /* no site file  */ 
    conf.dbg_port = 0;   
    while (1) {

        int option_index = 0;
        c = getopt_long(argc, argv, "hH:E:P:i:D:M:R:s:k:c:l:S:G:", long_options, &option_index);

        if (c == -1)
            break;

        switch (c) {
            case 'h':
                usage();
                exit(EXIT_SUCCESS);
                break;
            case 'H':
                conf.host = strdup(optarg);
                break;
            case 'E':
                conf.export = strdup(optarg);
                break;
            case 'P':
                conf.passwd = strdup(optarg);
                break;


            case 'M':
                mountpoint = strdup(optarg);
                break;


            case 'i':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                conf.instance = val;
                break;

            case 'G':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
		if (val >= ROZOFS_GEOREP_MAX_SITE) {
		   errno = EINVAL;
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }                
		conf.site = val;
                break;

            case 'D':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                conf.dbg_port = val;
                break;
	
/*
            case 'R':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                conf.rozofsmount_instance = val;
                break;
*/     
	   case 's':
                errno = 0;
                val = (int) strtol(optarg, (char **) NULL, 10);
                if (errno != 0) {
                    strerror(errno);
                    usage();
                    exit(EXIT_FAILURE);
                }
                rozofs_tmr_configure(TMR_STORAGE_PROGRAM,val);
                break;

            case '?':
                usage();
                exit(EXIT_SUCCESS);
                break;
            default:
	        printf("unknown option %c\n",c);
                usage();
                exit(EXIT_FAILURE);
                break;
        }
    }       
    /*
    ** init of the site number for that rozofs client
    */
    conf.conf_site_file = rozofs_get_local_site();
    while (1)
    {
      if ((conf.site == 0) || (conf.site == 1))
      {
	rozofs_site_number = conf.site;
	break;
      }
      if (conf.conf_site_file < 0)
      {
	rozofs_site_number = 0;
	break;
      }
      rozofs_site_number = conf.conf_site_file;
      break;
    }
    conf.running_site = rozofs_site_number;

    if (conf.host == NULL) {
        conf.host = strdup("rozofsexport");
    }

    if (strlen(conf.host) >= ROZOFS_HOSTNAME_MAX) {
        fprintf(stderr,
                "The length of export host must be lower than %d\n",
                ROZOFS_HOSTNAME_MAX);
    }

    if (conf.export == NULL) {
        conf.export = strdup("/srv/rozofs/exports/export");
    }

    if (conf.passwd == NULL) {
        conf.passwd = strdup("none");
    }

    if (conf.buf_size == 0) {
        conf.buf_size = 256;
    }

    if (conf.buf_size < 128) {
        fprintf(stderr,
                "write cache size too low (%u KiB) - increased to 128 KiB\n",
                conf.buf_size);
        conf.buf_size = 128;
    }

    if (conf.buf_size > 256) {
        fprintf(stderr,
                "write cache size too big (%u KiB) - decreased to 256 KiB\n",
                conf.buf_size);
        conf.buf_size = 256;
    }

    
    if (conf.min_read_size == 0) {
        conf.min_read_size = 4;
    }

    if (conf.min_read_size > conf.buf_size) {
        conf.min_read_size = conf.buf_size;
    }
    
    /*
    ** read common config file
    */
    common_config_read(NULL);        

    /*
    ** always 2 storcli for geo-replication
    */
    stclbg_set_storcli_number(2);
 
    /* Initialize the rotation modulo on distribution for read request */
    rozofs_rotation_read_modulo = conf.rotate;    
    /*
    ** Compute the identifier of the client from host and instance id 
    */
    {
        char hostName[256];
        hostName[0] = 0;
        gethostname(hostName, 256);
        rozofs_client_hash = rozofs_client_hash_compute(hostName, conf.instance);
    }
    // Set timeout for exportd requests
    if (conf.export_timeout != 0) {
        if (rozofs_tmr_configure(TMR_EXPORT_PROGRAM,conf.export_timeout)< 0)
        {
          fprintf(stderr,
                "timeout for exportd requests is out of range: revert to default setting");
        }
    }

    if (conf.storage_timeout != 0) {
        if (rozofs_tmr_configure(TMR_STORAGE_PROGRAM,conf.storage_timeout)< 0)
        {
          fprintf(stderr,
                "timeout for storaged requests is out of range: revert to default setting");
        }
    }

    if (conf.storcli_timeout != 0) {
        if (rozofs_tmr_configure(TMR_STORCLI_PROGRAM,conf.storcli_timeout)< 0)
        {
          fprintf(stderr,
                "timeout for storcli requests is out of range: revert to default setting");
        }
    }
    
    if (!mountpoint) {
        fprintf(stderr, "no mount point\nsee: %s -h for help\n", argv[0]);
        return 1;
    }

    
    // Change the value of maximum size of core file
    core_limit.rlim_cur = RLIM_INFINITY;
    core_limit.rlim_max = RLIM_INFINITY;
    if (setrlimit(RLIMIT_CORE, &core_limit) < 0) {
        warning("Failed to change maximum size of core file: %s",
                strerror(errno));
    }

    // Change AF_UNIX datagram socket length
    af_unix_socket_set_datagram_socket_len(128);

    res = fuseloop(/*&args,*/ fg);

    return res;
}
