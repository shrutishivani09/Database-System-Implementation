#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <fstream>
#include "BigQ.h"

typedef enum {heap, sorted, tree} fType;
struct Info{OrderMaker *o; int l; fType f;};

class DBFile {
private:
	File *file;
	Page *currPage;
	int currRecordIndex;

	Record *record;
	int currPageIndex;
	Page *pageIterator;	

	OrderMaker *orderMaker;//The sort order
	BigQ *bigQ;//To sort the records in input pipe
	Pipe *inputPipe,*outputPipe;

	bool write;//Reading mode or Writing mode
	Info info;// struct containing information about ordermaker, runlength, fType

	/*
	This function will take the instance of record and add it into the page.
	If the page is full, page is added to file and and new page is created.
	*/
	void AddIntoTheFile(Record &rec);

	/*
	Thid function will get all the records from output pipe in sorted order.
	Extracts all the records from file which are already in sorted order and merges these 2 records
	in sorted fashion and adds these records to a new file instance.
	*/
	void GetRecordsFromPipeAndFile();

	/*
	The params are 2 sorted list of records and this function returns the list which merges 
	the given lists in sorted fashion
	*/
	vector<Record*> merge(vector<Record*> r1, vector<Record*> r2);

public:
	DBFile (); 

	//Write to <fpath>.meta file the metadata which is in the form of Info structure
	int Create (const char *fpath, fType file_type, void *startup);

	//Read from <fpath>.meta file and get the metadata which is in the form of Info structure
	int Open (const char *fpath);

	int Close ();
	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
