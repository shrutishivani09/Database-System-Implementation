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
	inputPipe = new Pipe(10000000);
	outputPipe = new Pipe(10000000);
	write = false;
	count = 0;
	flagLinear = false;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    //Intialization
	 string metaData_fname(f_path);
	 metaData_fname += ".meta";

//	string metaData_fname = "koushik1.txt";
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
    
	ofstream stream;
	stream.open(metaData_fname,ios::out);
	stream.write((char*)&info,sizeof(info));
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
	//string metaData_fname = "koushik1.txt";
	ifstream stream;
	stream.open(metaData_fname,ios::in);

	stream.read((char*)&info, sizeof(info));
	orderMaker = info.o;
    char *p = strdup(f_path);
    // cout<<p<<endl;
    file->Open(1,p); //Existing file(First parm 1)
    if(file->GetLength() > 0)file->GetPage(pageIterator,-1);
	// cout<<"hello"<<file->GetLength()<<endl;
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
	if(write && info.f == sorted)GetRecordsFromPipeAndFile();
	int whichPage = file->GetLength()-1;
        //I think I need to make a copy of the pointer.s
    if(info.f == heap)file->AddPage(currPage,whichPage);
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
	inputPipe->ShutDown();
	bigQ = new BigQ(*inputPipe,*outputPipe,*orderMaker,info.l);
	ComparisonEngine comparisionEngine;
	Record *rec = new Record();
	vector<Record*> r1;
	while(outputPipe->Remove(rec)==1) {
		r1.push_back(rec);
		rec = new Record();
	}
	for(int i=1;i<r1.size();i++) {
		cout << "comparing "<<comparisionEngine.Compare(r1.at(i),r1.at(i-1),orderMaker) << endl;
	}
	vector<Record*> r2;
	int a = -1;
	while(a < file->GetLength()-1) {
		Page *page = new Page();
		file->GetPage(page,a);
		// cout<<a<<" "<<file->GetLength()<<endl;
		if(page == NULL)break;
		Record *record = new Record();
		while(page->GetFirst(record)==1){
			r2.push_back(record);
			record = new Record();
		}
		a++;
	}
	vector<Record*> records = merge(r1,r2);
	ComparisonEngine comp;
	cout<<"****"<<r1.size()<<"****"<<r2.size()<<endl;
	for(int i=0;i<records.size();i++) {
		if(i >= 1) {
			// cout << comp.Compare(records.at(i),records.at(i-1),orderMaker)<<endl;
		}
		AddIntoTheFile(*records[i]);
	}
	int whichPage = file->GetLength()-1;
    file->AddPage(currPage,whichPage);
	
}


void DBFile::Add (Record &rec) {
	if(info.f == sorted) {
		char *b = rec.GetBits();
		if(count < info.l * PAGE_SIZE)inputPipe->Insert (&rec);
		//char *b = temp.GetBits();
		//cout<<"Koushik:"<<((int *) b)[0]<<endl;
		count += ((int *) b)[0];
	}

	else if(info.f == heap){
		AddIntoTheFile(rec);
	}
	write = true;
}

int DBFile::GetNext (Record &fetchme) {
	if(write && info.f == sorted)GetRecordsFromPipeAndFile();
	int status = pageIterator->GetFirst(&fetchme);
    // cout<<file->GetLength()-1<<endl;
    if(currPageIndex == file->GetLength()-1)return 0;
    if(status == 0){
        // Page is done
        currPageIndex++;
	//cout<<currPageIndex<<" "<<file->GetLength();
	if(currPageIndex == file->GetLength()-1)return 0;
        file->GetPage(pageIterator,currPageIndex);
        if(pageIterator->GetFirst(&fetchme)==0)return 0;
    }
    return 1;
}

int DBFile::GetNextLinearScan(Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comp;
      while(GetNext(fetchme)){
	      if(comp.Compare(&fetchme,&literal, &cnf))
			      return 1;
      } 
       	return 0;
}
int DBFile::GetNextBinarySearch(OrderMaker &orderMaker, Record &literal) {
	int l = currPageIndex, r = file->GetLength()-2;
	ComparisonEngine cmpE;
	while (l < r) {
		int m = (l + r) / 2;
		Page *page = new Page();
		Record *record = new Record();

		file->GetPage(page,m);
		page->GetFirst(record);

		int x = cmpE.Compare(record,&literal,&orderMaker);
		if(x < 0)l = m + 1;
		if(x == 0)return m;
		if(x > 0)r = m - 1;
	}
	return l;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	if(write && info.f == sorted)GetRecordsFromPipeAndFile();
	OrderMaker sortedAtt,dummy;
	if(cnf.GetSortOrders(sortedAtt,dummy) == 0){
		flagLinear = true;
		//cout<<"Hello"<<flagLinear<<"\n";
		return GetNextLinearScan(fetchme,cnf,literal);
	}
	if(info.f == heap){
		flagLinear = true;
		return GetNextLinearScan(fetchme,cnf,literal);
	}
	else if(info.f == sorted) {
		// GetNextLinearScan(fetchme,cnf,literal)
		//cout<<"koisji"<<"\n";
		int x = GetNextBinarySearch(sortedAtt,literal);
		if(x != currPageIndex) {
		//	cout<<"koisji"<<"\n";
			currPageIndex = x;
			file->GetPage(currPage,currPageIndex);
		}
		ComparisonEngine comp;
	    while(GetNext(fetchme)){
	      if(comp.Compare(&fetchme,&literal, &cnf))
			      return 1; 
		}
	}
       	return 0;
	
}
