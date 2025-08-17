
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include "tbl.h"
#include "codec.h"
#include "../pflayer/pf.h"

#define SLOT_COUNT_OFFSET 2
// --Added--
#define HEADER_SIZE 4
// --Added--
#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

int  getLen(int slot, byte *pageBuf);
int  getNumSlots(byte *pageBuf);
void setNumSlots(byte *pageBuf, int nslots);
int  getNthSlotOffset(int slot, char* pageBuf);

// --Added--
static short readShort(byte *ptr){
    short v;
    memcpy(&v, ptr, sizeof(short));
    return v;
}
static void writeShort(byte *ptr, short v){
    memcpy(ptr, &v, sizeof(short));
}
// --Added--


/**
   Opens a paged file, creating one if it doesn't exist, and optionally
   overwriting it.
   Returns 0 on success and a negative error code otherwise.
   If successful, it returns an initialized Table*.
 */
int
Table_Open(char *dbname, Schema *schema, bool overwrite, Table **ptable)
{
    // Initialize PF, create PF file,
    // allocate Table structure  and initialize and return via ptable
    // The Table structure only stores the schema. The current functionality
    // does not really need the schema, because we are only concentrating
    // on record storage. 
    // --Added--
    int err = 0;
    PF_Init();
    if(overwrite){
        if(access(dbname, F_OK)==0){
            err = PF_DestroyFile(dbname);
            checkerr(err);
        }
        err = PF_CreateFile(dbname);
        checkerr(err);
    }
    int fd = PF_OpenFile(dbname);
    checkerr(fd);
    Table *tbl = (Table *)malloc(sizeof(Table));
    if(!tbl) return -1;
    tbl->schema = schema;
    tbl->fd = fd;
    *ptable = tbl;
    return err;
    // --Added--
}

void
Table_Close(Table *tbl) {
    // Unfix any dirty pages, close file.
    // --Added--
    if(!tbl) return;
    int err = PF_CloseFile(tbl->fd);
    checkerr(err);
    free(tbl);
    // --Added--
}


int
Table_Insert(Table *tbl, byte *record, int len, RecId *rid) {
    // Allocate a fresh page if len is not enough for remaining space
    // Get the next free slot on page, and copy record in the free
    // space
    // Update slot and free space index information on top of page.
    // --Added--
    int fd = tbl->fd;
    int pnum;
    char *pageBuff;
    int err = 0;
    err = PF_GetFirstPage(fd, &pnum, &pageBuff);
    if(err==PFE_EOF){
        err = PF_AllocPage(fd, &pnum, &pageBuff);
        checkerr(err);
        memset(pageBuff, 0, PF_PAGE_SIZE);
        writeShort((byte *)pageBuff, (short)PF_PAGE_SIZE);
        setNumSlots((byte *)pageBuff, 0);
    }
    else{
        checkerr(err);
        while(1){
            short freePtr = readShort((byte *)pageBuff);
            int slots = getNumSlots((byte *)pageBuff);
            int headerNeeded = HEADER_SIZE + (slots+1)*2;
            int avail = freePtr-headerNeeded;
            if(avail >= len){
                break;
            }
            err = PF_UnfixPage(fd, pnum, FALSE);
            checkerr(err);
            err = PF_GetNextPage(fd, &pnum, &pageBuff);
            if(err==PFE_EOF){
                err = PF_AllocPage(fd, &pnum, &pageBuff);
                checkerr(err);
                memset(pageBuff, 0, PF_PAGE_SIZE);
                writeShort((byte *)pageBuff, (short)PF_PAGE_SIZE);
                setNumSlots((byte *)pageBuff, 0);
                break;
            }
            checkerr(err);
        }
    }
    short freePtr = readShort((byte *)pageBuff);
    int slots = getNumSlots((byte *)pageBuff);
    int newStart = freePtr-len;
    memcpy(pageBuff+newStart, record, len);
    int slotPos = HEADER_SIZE+(slots*2);
    writeShort((byte *)(pageBuff+slotPos), (short)newStart);
    setNumSlots((byte *)pageBuff, slots+1);
    writeShort((byte *)pageBuff, (short)newStart);
    err = PF_UnfixPage(fd, pnum, TRUE);
    checkerr(err);
    *rid = (pnum<<16) | (slots&0xFFFF);
    return err;
    // --Added--
}

#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

/*
  Given an rid, fill in the record (but at most maxlen bytes).
  Returns the number of bytes copied.
 */
int
Table_Get(Table *tbl, RecId rid, byte *record, int maxlen) {
    int slot = rid & 0xFFFF;
    int pageNum = rid >> 16;

    // PF_GetThisPage(pageNum)
    // In the page get the slot offset of the record, and
    // memcpy bytes into the record supplied.
    // Unfix the page
    // --Added--
    char *pageBuff;
    int err = PF_GetThisPage(tbl->fd, pageNum, &pageBuff);
    checkerr(err);
    int offset = getNthSlotOffset(slot, pageBuff);
    int len = getLen(slot, (byte *)pageBuff);
    if(len > maxlen) len = maxlen;
    if(len > 0) memcpy(record, pageBuff+offset, len);
    err = PF_UnfixPage(tbl->fd, pageNum, FALSE);
    checkerr(err);
    // --Added--
    return len; // return size of record
}

void
Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn) {
    // For each page obtained using PF_GetFirstPage and PF_GetNextPage
    //    for each record in that page,
    //          callbackfn(callbackObj, rid, record, recordLen)
    // --Added--
    int fd = tbl->fd;
    int pnum;
    char *pageBuff;
    int err = PF_GetFirstPage(fd, &pnum, &pageBuff);
    if(err==PFE_EOF) return;
    checkerr(err);
    while(1){
        int slots = getNumSlots((byte *)pageBuff);
        for(int i=0; i<slots; i++){
            int offset = getNthSlotOffset(i, pageBuff);
            int len = getLen(i, (byte *)pageBuff);
            RecId rid = (pnum<<16) | (i&0xFFFF);
            callbackfn(callbackObj, rid, (byte *)(pageBuff+offset), len);
        }
        err = PF_UnfixPage(fd, pnum, false);
        checkerr(err);
        err = PF_GetNextPage(fd, &pnum, &pageBuff);
        if(err==PFE_EOF) break;
        checkerr(err);
    }
    // --Added--
}

// --Added--
int getNumSlots(byte *pageBuff){
    return (int)readShort(pageBuff+SLOT_COUNT_OFFSET);
}

void setNumSlots(byte *pageBuff, int nslots){
    writeShort(pageBuff+SLOT_COUNT_OFFSET, (short)nslots);
}

int getNthSlotOffset(int slot, char *pageBuff){
    int pos = HEADER_SIZE+slot*2;
    return (int)readShort((byte *)(pageBuff+pos));
}

int getLen(int slot, byte *pageBuff){
    int n = getNumSlots(pageBuff);
    if(slot<0||slot>=n) return 0;
    int start = getNthSlotOffset(slot, (char *)pageBuff);
    if (start <= 0) return 0;
    int minGreater = PF_PAGE_SIZE;
    for(int i=0; i<n; i++){
        if(i==slot) continue;
        int off = getNthSlotOffset(i, (char *)pageBuff);
        if(off>start&&off<minGreater) minGreater = off;
    }
    return minGreater-start;
}
// --Added--
