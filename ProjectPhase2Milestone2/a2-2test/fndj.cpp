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
#include <vector>

using namespace std;
// stub file .. replace it with your own DBFile.cc

struct SortedInfo{OrderMaker *o; int l;};


DBFile::DBFile () {
	file = new File(); 
	currPage = new Page();
	currPageIndex = -1;
    pageIterator = new Page();
	record = new Record();
	orderMaker = NULL;
	bigQ = NULL;
	inputPipe = new Pipe(100);
	outputPipe = new Pipe(100);
	write = true;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    //Intialization
	string metaData_fname(f_path);
	metaData_fname += ".meta";

	info.f = f_type;

	if(f_type == sorted) {
		struct SortedInfo *sortInfo = NULL;
		sortInfo = (struct SortedInfo *)startup;
		orderMaker = sortInfo->o;
		info.o = sortInfo->o;
		info.l = sortInfo->l;
	}
	else if(f_type == heap) {
		info.o = NULL;
		info.l = 0;
	}
    
    cout<<"hello"<<endl;
	ofstream stream;
	stream.open(metaData_fname,ios::app);
	stream.write((char*)&info,sizeof(info));
    cout<<"hello"<<endl;
	file = new File();
    //const char* to char;
    char* p =  strdup(f_path);

    //Check file and return appropriately 
	file->Open(0,p); 
	write=false;
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
	write = true;
}

int DBFile::Open (const char *f_path) {
	string metaData_fname(f_path);
	metaData_fname += ".meta";

	ifstream stream;
	stream.open(metaData_fname,ios::in);

	stream.read((char*)&info, sizeof(info));
	orderMaker = info.o;

    char *p = strdup(f_path);
    //cout<<p<<endl;
    file->Open(1,p); //Existing file(First parm 1)
    file->GetPage(pageIterator,-1);
	write = false;
    return 1;
}

void DBFile::MoveFirst () {
	if(write && info.f == sorted)GetRecordsFromPipeAndFile();
    currPageIndex = -1;
    file->GetPage(pageIterator,currPageIndex);	
	write = false;
}


int DBFile::Close () {
	int whichPage = file->GetLength()-1;
        //I think I need to make a copy of the pointer.s
    file->AddPage(currPage,whichPage);
	//cout<<file->GetLength()<<endl;
	delete file;
	delete currPage;
    return 0;
}
// NEW ONE
void DBFile::AddIntoTheFile(Record &rec) {
	Record *record = &rec;
	int status = currPage->Append(record);
	if(status == 0){//Page is full  
		int whichPage = file->GetLength()-1;
			//I think I need to make a copy of the pointer.

		file->AddPage(currPage,whichPage);
		currPage = new Page();
		currPage->Append(record);
	}
}
vector<Record*> DBFile :: merge(vector<Record*> r1, vector<Record*> r2) {
	vector<Record*> records;
	ComparisonEngine comparisionEngine;
	int i=0,j=0;
	while(i < r1.size() && j < r2.size()) {
		if(comparisionEngine.Compare(r1.at(i),r2.at(j),orderMaker)<=0) {
			records.push_back(r1.at(i));
			i++;
		}
		else {
			records.push_back(r2.at(j));
			j++;
		}
	}
	while(i < r1.size())records.push_back(r1.at(i++));
	while(j < r2.size())records.push_back(r2.at(j++));
	return records;
}
void DBFile::GetRecordsFromPipeAndFile() {
    cout<<"Calling this shit"<<endl;
	bigQ = new BigQ(*inputPipe,*outputPipe,*orderMaker,info.l);
	Record *rec = new Record();
	vector<Record*> r1;
	while(outputPipe->Remove(rec)==1) {
		r1.push_back(rec);
		rec = new Record();
	}
	vector<Record*> r2;
	int a = -1;
	while(true) {
		Page *page = NULL;
		file->GetPage(page,a);
		if(page == NULL)break;
		Record *record = new Record();
		while(page->GetFirst(record)==1){
			r2.push_back(record);
			record = new Record();
		}
		a++;
	}
	vector<Record*> records = merge(r1,r2);
	for(int i=0;i<records.size();i++) {
		AddIntoTheFile(*records[i]);
	}
	
}


void DBFile::Add (Record &rec) {
	if(info.f == sorted) 
		inputPipe->Insert(&rec);

	else if(info.f == heap){
		AddIntoTheFile(rec);
	}
	write = true;
}

int DBFile::GetNext (Record &fetchme) {
	if(write && info.f == sorted)GetRecordsFromPipeAndFile();
	int status = pageIterator->GetFirst(&fetchme);
    cout<<file->GetLength()-1;
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
	if(write && info.f == sorted)GetRecordsFromPipeAndFile();
	ComparisonEngine comp;
	int count = 0; 
      while(GetNext(fetchme)){
	      if(comp.Compare(&fetchme,&literal, &cnf))
			      return 1;
      } 
       	return 0;
}