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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <rozofs/rozofs.h>
#include <rozofs/core/rozofs_core_files.h>

#include "config.h"
#include "log.h"
#include "daemon.h"

/* pid file */
static char pid_file[255];

/* manage only one daemon at a time */
//static void (*daemon_on_stop) (void) = NULL;

//static void (*daemon_on_hup) (void) = NULL;

static int write_pid(const char *name) {
    int status = -1;
    int lfp = -1;
    char pidf[255] = DAEMON_PID_DIRECTORY;
    char str[10];
    DEBUG_FUNCTION;

    strcat(pidf, name);
    if ((lfp = open(pidf, O_RDWR | O_CREAT, 0640)) < 0)
        goto out;
    if (lockf(lfp, F_TLOCK, 0) < 0)
        goto out;
    sprintf(str, "%d\n", getpid());
    if (write(lfp, str, strlen(str)) != strlen(str))
        goto out;

    status = 0;
out:
    if (lfp > 0)
        close(lfp);
    return status;
}

static int read_pid(const char *name, int *pid) {
    int status = -1;
    int lfp = -1;
    char pidf[255] = DAEMON_PID_DIRECTORY;
    char str[10];
    DEBUG_FUNCTION;

    strcat(pidf, name);
    lfp = open(pidf, O_RDONLY);
    if (lfp < 0)
        goto out;
    if (read(lfp, str, 10) == -1)
        goto out;
    *pid = strtol(str, NULL, 10);

    status = 0;
out:
    return status;
}

#if 0
static void daemon_handle_signal(int sig) {
    DEBUG_FUNCTION;

    switch (sig) {
        case SIGTERM:
            if (daemon_on_stop) {
                daemon_on_stop();
                unlink(pid_file);
                signal(SIGTERM, SIG_DFL);
                raise(SIGTERM);
            }
            break;
        case SIGKILL:
            if (daemon_on_stop) {
                daemon_on_stop();
                unlink(pid_file);
                signal(SIGKILL, SIG_DFL);
                raise(SIGKILL);
            }
            break;
        case SIGHUP:
            if (daemon_on_hup)
                daemon_on_hup();
            break;
    }
}
#endif
void remove_pid_file(int sig) {
  unlink(pid_file);
}
void daemon_start(char * path, int nbCoreFiles, const char *name, void (*on_start) (void),
        void (*on_stop) (void), void (*on_hup) (void)) {
    int pid;
    DEBUG_FUNCTION;

    sprintf(pid_file, "%s%s", DAEMON_PID_DIRECTORY, name);

    // check if running
    if (read_pid(name, &pid) == 0 && kill(pid, 0) == 0) {
        fprintf(stderr, "%s already running as pid: %d\n", pid_file, pid);
        return;
    }
    if (daemon(0, 0) != 0) {
        fprintf(stderr, "daemon failed");
        return;
    }
    
    rozofs_signals_declare (path,nbCoreFiles); 

    if (write_pid(name) != 0) {
        fatal("write_pid failed: %s", strerror(errno));
        return;
    }

    if (on_stop) rozofs_attach_crash_cbk((rozofs_attach_crash_cbk_t)on_stop);
    if (on_hup)  rozofs_attach_hgup_cbk((rozofs_attach_crash_cbk_t)on_hup);
    
    rozofs_attach_crash_cbk(remove_pid_file);

    on_start();
}


void no_daemon_start(char * path, int nbCoreFiles, const char *name, void (*on_start) (void),
        void (*on_stop) (void), void (*on_hup) (void)) {
    int pid;
    DEBUG_FUNCTION;

    sprintf(pid_file, "%s%s", DAEMON_PID_DIRECTORY, name);

    // check if running
    if (read_pid(name, &pid) == 0 && kill(pid, 0) == 0) {
        fprintf(stderr, "%s already running as pid: %d\n", pid_file, pid);
        return;
    }
    
    rozofs_signals_declare (path,nbCoreFiles); 

    if (write_pid(name) != 0) {
        fatal("write_pid failed: %s", strerror(errno));
        return;
    }    
    rozofs_attach_crash_cbk(remove_pid_file);
          
    if (on_stop) rozofs_attach_crash_cbk((rozofs_attach_crash_cbk_t)on_stop);
    if (on_hup)  rozofs_attach_hgup_cbk((rozofs_attach_crash_cbk_t)on_hup);
    
    on_start();
}
