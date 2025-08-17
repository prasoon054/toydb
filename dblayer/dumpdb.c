#include <stdio.h>
#include <stdlib.h>
#include "codec.h"
#include "tbl.h"
#include "util.h"
#include "../pflayer/pf.h"
#include "../amlayer/am.h"
#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(1);}}


void
printRow(void *callbackObj, RecId rid, byte *row, int len) {
    Schema *schema = (Schema *) callbackObj;
    byte *cursor = row;

    // --Added--
    for(int i=0; i<schema->numColumns; i++){
        ColumnDesc *col = schema->columns[i];
        if(col->type==VARCHAR){
            char tmp[1024];
            int slen = DecodeCString(cursor, tmp, sizeof(tmp));
            printf("%s", tmp);
            cursor += slen+2;
        }
        else if(col->type==INT){
            int v = DecodeInt(cursor);
            printf("%d", v);
            cursor += 4;
        }
        else if(col->type==LONG){
            long long lv = DecodeLong(cursor);
            printf("%lld", lv);
            cursor += 8;
        }
        if(i<schema->numColumns-1) putchar(',');
    }
    putchar('\n');
    // --Added--
}

#define DB_NAME "data.db"
#define INDEX_NAME "data.db.0"

void
index_scan(Table *tbl, Schema *schema, int indexFD, int op, int value) {
    /*
    Open index ...
    while (true)
        find next entry in index
        fetch rid from table
        printRow(...)
        }
        close index ...
        */
    // --Added--
    int scanDesc = AM_OpenIndexScan(indexFD, 'i', 4, op, (char *)&value);
    if(scanDesc < 0){
        AM_PrintError("AM_OpenIndexScan failed");
        return;
    }
    while(1){
        int res = AM_FindNextEntry(scanDesc);
        if(res==AME_EOF || res<0) break;
        RecId rid = res;
        byte buff[1000];
        int recLen = Table_Get(tbl, rid, buff, sizeof(buff));
        if(recLen > 0){
            printRow(schema, rid, buff, recLen);
        }
    }
    AM_CloseIndexScan(scanDesc);
    // --Added--
}

int
main(int argc, char **argv) {
    char *schemaTxt = "Country:varchar,Capital:varchar,Population:int";
    Schema *schema = parseSchema(schemaTxt);
    Table *tbl;

    // --Added--
    int err = Table_Open(DB_NAME, schema, false, &tbl);
    checkerr(err);
    // --Added--
    if (argc == 2 && *(argv[1]) == 's') {
        // invoke Table_Scan with printRow, which will be invoked for each row in the table.
        // --Added--
        Table_Scan(tbl, schema, printRow);
        // --Added--
    } else {
	// index scan by default
	int indexFD = PF_OpenFile(INDEX_NAME);
	checkerr(indexFD);

	// Ask for populations less than 100000, then more than 100000. Together they should
	// yield the complete database.
	index_scan(tbl, schema, indexFD, LESS_THAN_EQUAL, 100000);
	index_scan(tbl, schema, indexFD, GREATER_THAN, 100000);
    }
    Table_Close(tbl);
}
