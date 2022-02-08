
#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include <stdlib.h>
#include <string.h>
using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

int main () {

	// // try to parse the CNF
	 cout << "Enter in your CNF: ";
  	 if (yyparse() != 0) {
	 	cout << "Can't parse your CNF.\n";
	 	exit (1);
	 }

	// // suck up the schema from the file
	 Schema lineitem ("catalog", "lineitem");

	// // grow the CNF expression from the parse tree 
	 CNF myComparison;
	 Record literal;
	 myComparison.GrowFromParseTree (final, &lineitem, literal);
	
	 // // print out the comparison to the screen
	 myComparison.Print ();

	// // now open up the text file and start procesing it
//	cout<<"Hello WOrld"<<endl;
        FILE *tableFile = fopen ("/cise/homes/sshivani/git/tpch-dbgen/lineitem.tbl", "r");
	cout<<"1"<<endl;
        Record temp;
	cout<<"temp Record created"<<endl;
        //Schema *mySchema = new Schema("catalog", "lineitem");
	Schema mySchema ("catalog", "lineitem");
	cout<<"hi"<<endl;

	temp.SuckNextRecord(&mySchema,tableFile);
	// //char *bits = literal.GetBits ();
	// //cout << " numbytes in rec " << ((int *) bits)[0] << endl;
	// //literal.Print (&supplier);

    //     // read in all of the records from the text file and see if they match
	// // the CNF expression that was typed in
	 int counter = 0;
	ComparisonEngine comp;
        while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
		counter++;
		if (counter % 10000 == 0) {
			cerr << counter << "\n";
		}

		if (comp.Compare (&temp, &literal, &myComparison))
                	temp.Print (&mySchema);

        }
		string fileName = "/cise/homes/sshivani/P1/lineitem.bin";
		string filePath = "/cise/homes/sshivani/git/tpch-dbgen/lineitem.tbl";
		char* cPath = strcpy(new char[filePath.length()+1],filePath.c_str());
                char* c = strcpy(new char[fileName.length()+1],fileName.c_str());
		File *file = new File();
		DBFile db;
	//	Record r;
		db.Open(c);
		db.GetNext(r);
//		file->Open(1,c);
//		cout<<file->GetLength()<<endl;
		for(int i=0;i<file->GetLength()-1;i++){
			Page *page = new Page();
			Record *r = new Record();
			cout<<"hiii"<<endl;
			file->GetPage(page,i);
			while(page->GetFirst(r)==1)cout<<"hi"<<endl;
		}

		
	//	DBFile *dbfile = new DBFile();
//               cout<<"hi"<<endl;
  //              cout<<"Creating"<<dbfile->Create(c,heap,NULL)<<endl;
//		cout<<"Load"<<endl;
		//dbfile->Add(*temp);
		//dbfile->Load(*mySchema,cPath);
		//while (temp->SuckNextRecord (mySchema, tableFile) == 1) {
			/* code */
		//	Record &record = *temp;
		//	cout<<"Adding"<<endl;
		//	dbfile->Add(record);
		//	cout<<endl;
		//}
		

        // file->Open(0,c);
        // //Record *record = new Record();
        // //cout <<"Record object success"<< temp->GetBits();
        // Page *page = new Page();
        // cout <<"Page object success"<< endl;
        // cout <<page->Append(temp) << endl;
        // //cout <<"page append success"<< endl;
        // //file->AddPage(page,1);

        // cout<<"hello"<<file->GetLength()<< endl;
        // file->AddPage(page,-1);
        // cout<<"hello"<<file->GetLength()<< endl;
}


