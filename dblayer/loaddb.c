#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>
#include "codec.h"
#include "../pflayer/pf.h"
#include "../amlayer/am.h"
#include "tbl.h"
#include "util.h"

#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(1);}}

#define MAX_PAGE_SIZE 4000


#define DB_NAME "data.db"
#define INDEX_NAME "data.db.0"
#define CSV_NAME "data.csv"


/*
Takes a schema, and an array of strings (fields), and uses the functionality
in codec.c to convert strings into compact binary representations
 */
int
encode(Schema *sch, char **fields, byte *record, int spaceLeft) {
    // for each field
    //    switch corresponding schema type is
    //        VARCHAR : EncodeCString
    //        INT : EncodeInt
    //        LONG: EncodeLong
    // return the total number of bytes encoded into record
    // --Added--
    int used = 0;
    for(int i=0; i<sch->numColumns; i++){
        ColumnDesc *col = sch->columns[i];
        if(col->type==VARCHAR){
            int w = EncodeCString(fields[i], record+used, spaceLeft-used);
            used += w;
        }
        else if(col->type==INT){
            int val = atoi(fields[i]);
            int w = EncodeInt(val, record+used);
            used += w;
        }
        else if(col->type==LONG){
            long long lval = atoll(fields[i]);
            int w = EncodeLong(lval, record+used);
            used += w;
        }
        else{}
    }
    return used;
    // --Added--
}

Schema *
loadCSV() {
    // Open csv file, parse schema
    FILE *fp = fopen(CSV_NAME, "r");
    if (!fp) {
	perror("data.csv could not be opened");
        exit(EXIT_FAILURE);
    }

    char buf[MAX_LINE_LEN];
    char *line = fgets(buf, MAX_LINE_LEN, fp);
    if (line == NULL) {
	fprintf(stderr, "Unable to read data.csv\n");
	exit(EXIT_FAILURE);
    }

    // Open main db file
    Schema *sch = parseSchema(line);
    Table *tbl;

    // --Added--
    int err;
    int indexFD;
    PF_Init();
    err = Table_Open(DB_NAME, sch, true, &tbl);
    checkerr(err);
    err = AM_CreateIndex(DB_NAME, 0, 'i', 4);
    checkerr(err);
    indexFD = PF_OpenFile(INDEX_NAME);
    checkerr(indexFD);
    // --Added--

    char *tokens[MAX_TOKENS];
    char record[MAX_PAGE_SIZE];

    while ((line = fgets(buf, MAX_LINE_LEN, fp)) != NULL) {
        int n = split(line, ",", tokens);
        assert (n == sch->numColumns);
        int len = encode(sch, tokens, record, sizeof(record));
        RecId rid;

        // --Added--
        err = Table_Insert(tbl, (byte *)record, len, &rid);
        checkerr(err);
        // --Added--

        printf("%d %s\n", rid, tokens[0]);

        // Indexing on the population column 
        int population = atoi(tokens[2]);

        // Use the population field as the field to index on
        // --Added--
        err = AM_InsertEntry(indexFD, 'i', 4, (char *)&population, rid);
        // --Added--
            
        checkerr(err);
    }
    fclose(fp);
    Table_Close(tbl);
    err = PF_CloseFile(indexFD);
    checkerr(err);
    return sch;
}

int
main() {
    loadCSV();
}
