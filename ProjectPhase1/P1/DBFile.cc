#include "TwoWayList.cc"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <fstream>
#include <iostream>
#include <string.h>

using namespace std;
// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
	file = new File(); 
	currPage = new Page();
	currPageIndex = -1;
    	pageIterator = new Page();
	record = new Record();
	o.open("lo.txt",std::ios_base::app);
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    //Intialization
	file = new File();

    //const char* to char;
    char* p =  strdup(f_path);
    //Check file and return appropriately 
	int status = file->Open(0,p);
	if(status < 0){// Opening failed 
        file = NULL;
        return 0; 
    } 
    return 1;
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
	Schema mySchema = f_schema;
	FILE *tempFile = fopen (loadpath, "r");
        Record record;	
	if(tempFile!=NULL){
		//cout << "tempFile not null"<<endl ;
		while(record.SuckNextRecord(&mySchema, tempFile)!=0){
			Add(record);
		}
	}
	else{
		cout << "error message" ;
	}
	//cout<<file->GetLength();

}

int DBFile::Open (const char *f_path) {
    char *p = strdup(f_path);
    //cout<<p<<endl;
    listOfRecords = new TwoWayList<Record>;
    int status = file->Open(1,p); //Existing file(First parm 1)
    if(status < 0){// Opening failed
        file = NULL;
        return 0; 
    }
    //cout<<"Length:"<<file->GetLength()<<endl;
    file->GetPage(pageIterator,-1);
    return 1;
}

void DBFile::MoveFirst () {
    currPageIndex = -1;
    file->GetPage(pageIterator,currPageIndex);	
}


int DBFile::Close () {
	int whichPage = file->GetLength()-1;
        //I think I need to make a copy of the pointer.

        file->AddPage(currPage,whichPage);
	//cout<<file->GetLength()<<endl;
	delete file;
	delete currPage;
    return 0;
}

void DBFile::Add (Record &rec) {
    Record *record = &rec;
    int status = currPage->Append(record);
    record->GetBits();
    if(status == 0){//Page is full  
        int whichPage = file->GetLength()-1;

        //I think I need to make a copy of the pointer.

        file->AddPage(currPage,whichPage);
        currPage = new Page();
        currPage->Append(record);
    }
}

int DBFile::GetNext (Record &fetchme) {
int status = pageIterator->GetFirst(&fetchme);
    if(currPageIndex == file->GetLength()-1)return 0;
    if(status == 0){
        // Page is done
        currPageIndex++;
	//cout<<currPageIndex<<" "<<file->GetLength();
	if(currPageIndex == file->GetLength()-1)return 0;
        file->GetPage(pageIterator,currPageIndex);
        if(pageIterator->GetFirst(&fetchme)==0)return 0;;
    }
    return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comp;
	int count = 0; 
      while(GetNext(fetchme)){
	      if(comp.Compare(&fetchme,&literal, &cnf))
			      return 1;
      } 
       	return 0;
}
