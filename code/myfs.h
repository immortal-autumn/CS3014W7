//#include "fs.h"
#include <uuid/uuid.h>
#include <unqlite.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <fuse.h>

#define MY_MAX_PATH 255
#define MY_MAX_FILE_SIZE 1000
#define MY_MAX_INDIRECT 15
#define MY_MAX_DIRECT 13
#define MY_MAX_FREE 14


// This is a starting File Control Block for the 
// simplistic implementation provided.
//
// It combines the information for the root directory "/"
// and one single file inside this directory. This is why there
// is a one file limit for this filesystem
//
// Obviously, you will need to be change this into a
// more sensible FCB to implement a proper filesystem
//directory access block

typedef struct _indirected {
    uuid_t indirect[MY_MAX_INDIRECT];
} ind;

//file control block:
//inode structure: first 13 with direct access, next three with single indirect access, double indirect access and triple indirect access
//In basic I will only implement the direct access
typedef struct _myfcb {    
    // see 'man 2 stat' and 'man 2 chmod'
    //meta-data for the 'file'
    uid_t  uid;                     /* user */
    gid_t  gid;                     /* group */
    mode_t mode;                    /* protection */
    time_t mtime;                   /* time of last modification */
    time_t ctime;                   /* time of last change to meta-data (status) */
    nlink_t nlink;                  /* Number of hard link associate with it */
    off_t size;                     /* size */
    uuid_t direct[MY_MAX_DIRECT];   /* Direct access */
    uuid_t single_indirect;         /* Single indirect access */
    uuid_t double_indirect;         /* Double indirect access */
    uuid_t triple_indirect;         /* Triple indirect access */
} myfcb;

typedef struct _entry {
    uuid_t fcb_id;
    char name[MY_MAX_PATH];
} myent;

typedef struct _file_data {
    size_t size;
    char data[MY_MAX_FILE_SIZE];
} myfile;

typedef struct _free_list {
    uuid_t free_node[MY_MAX_FREE];
    uuid_t next;
} myfree;

// Some other useful definitions we might need

extern unqlite_int64 root_object_size_value;

// We need to use a well-known value as a key for the root object.
#define ROOT_OBJECT_KEY "root"
#define ROOT_OBJECT_KEY_SIZE 4

// This is the size of a regular key used to fetch things from the 
// database. We use uuids as keys, so 16 bytes each
#define KEY_SIZE 16

// The name of the file which will hold our filesystem
// If things get corrupted, unmount it and delete the file
// to start over with a fresh filesystem
#define DATABASE_NAME "myfs.db"

extern unqlite *pDb;

extern void error_handler(int);
void print_id(uuid_t *);

extern FILE* init_log_file();
extern void write_log(const char *, ...);

extern uuid_t zero_uuid;

// We can use the fs_state struct to pass information to fuse, which our handler functions can
// then access. In this case, we use it to pass a file handle for the file used for logging
struct myfs_state {
    FILE *logfile;
};
#define NEWFS_PRIVATE_DATA ((struct myfs_state *) fuse_get_context()->private_data)




// Some helper functions for logging etc.

// In order to log actions while running through FUSE, we have to give
// it a file handle to use. We define a couple of helper functions to do
// logging. No need to change this if you don't see a need
//

FILE *logfile;

// Open a file for writing so we can obtain a handle
FILE *init_log_file(){
    //Open logfile.
    logfile = fopen("myfs.log", "w");
    if (logfile == NULL) {
		perror("Unable to open log file. Life is not worth living.");
		exit(EXIT_FAILURE);
    }
    //Use line buffering
    setvbuf(logfile, NULL, _IOLBF, 0);
    return logfile;
}

// Write to the provided handle
void write_log(const char *format, ...){
    va_list ap;
    va_start(ap, format);
    vfprintf(NEWFS_PRIVATE_DATA->logfile, format, ap);
}

// Simple error handler which cleans up and quits
void error_handler(int rc){
	if( rc != UNQLITE_OK ){
		const char *zBuf;
		int iLen;
		unqlite_config(pDb,UNQLITE_CONFIG_ERR_LOG,&zBuf,&iLen);
		if( iLen > 0 ){
			perror("error_handler: ");
			perror(zBuf);
		}
		if( rc != UNQLITE_BUSY && rc != UNQLITE_NOTIMPLEMENTED ){
			/* Rollback */
			unqlite_rollback(pDb);
		}
		exit(rc);
	}
}

void print_id(uuid_t *id){
 	size_t i; 
    for (i = 0; i < sizeof *id; i ++) {
        printf("%02x ", (*id)[i]);
    }
}

