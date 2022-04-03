#include "RelOp.h"
#include "BigQ.h"
#include "Schema.h"
#include <map>

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	Record record;
	ComparisonEngine cmp;
	int c = 0;
	while(inFile.GetNext(record) == 1) {
		if(cmp.Compare(&record,&literal,&selOp) == 1) {
			outPipe.Insert(&record);
		}
	}
	outPipe.ShutDown();
}

void SelectFile::WaitUntilDone () {
	// pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {

}


void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) { 
	Record record;
	ComparisonEngine cmp;
	while(inPipe.Remove(&record) == 1) {
		if(cmp.Compare(&record,&literal,&selOp) == 1) {
			outPipe.Insert(&record);
		}
	}
	outPipe.ShutDown();
}

void DuplicateRemoval:: Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) { 
	OrderMaker *orderMaker = new OrderMaker(&mySchema);
	Pipe o(1000000);
	BigQ *bq = new BigQ(inPipe, o, *orderMaker,runlen);
	ComparisonEngine cmp;

	Record *record_i = new Record();
	Record *record_j = new Record();
	o.Remove(record_i);
	while(o.Remove(record_j) == 1) {
		if(cmp.Compare(record_i,record_j,orderMaker)!=0){
			Record *r = new Record();
			r->Consume(record_i);
			outPipe.Insert(r);
			
			record_i = record_j;
		}
		record_j = new Record();
	}
	outPipe.Insert(record_i);
	outPipe.ShutDown();
}

void Project :: Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) { 
	Record *record = new Record();	
	int cnt = 0;
	while(inPipe.Remove(record) == 1) {
		record->Project(keepMe,numAttsOutput,numAttsInput);
		outPipe.Insert(record);
		cnt++;
		record = new Record();
	}
	outPipe.ShutDown();
}
void Sum:: Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { 
	Record record;
	double sum = 0;
	
	while(inPipe.Remove(&record)) {
		int result_int = 0;
		double result_double = 0;
		computeMe.Apply(record,result_int,result_double);
		sum += result_int + result_double;
	}

	
	string s = to_string(sum);
	s += "|";
	FILE *file = fopen("sum.tbl","w");
	fputs(s.c_str(),file);
	fclose(file);

	FILE *f = fopen("sum.tbl","r");
	Attribute DA = {"double", Double};
	Schema out_sch ("out_sch", 1, &DA);

	Record r;
	r.SuckNextRecord(&out_sch,f);
	outPipe.Insert(&r);
	outPipe.ShutDown();
}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
		OrderMaker orderMaker1,orderMaker2;
		Pipe o1(1000000),o2(1000000);

		selOp.GetSortOrders(orderMaker1,orderMaker2);
		BigQ *bq1 = new BigQ(inPipeL, o1, orderMaker1,runlen);
		BigQ *bq2 = new BigQ(inPipeR, o2, orderMaker2,runlen);
		ComparisonEngine cmp;
		vector<Record*> v1,v2;
		Record *record = new Record();
		while(o1.Remove(record)){
			v1.push_back(record);
			record = new Record();
		}
		while(o2.Remove(record)){
			v2.push_back(record);
			record = new Record();
		}
		int cnt = 0;
		int *a = new int[12];
		int k = 0;
		for(int i=0;i<7;i++) a[k++] = i;
		for(int i=0;i<5;i++) a[k++] = i;
		int right = 7;


		int i = 0;
		int j = 0;
		while(i < v1.size() && j < v2.size()) {
		// if(L.at(i) <= R.at(j))
		int status = cmp.Compare(v1[i],&orderMaker1,v2.at(j),&orderMaker2);
		if(status == 0){
			Record r;
			r.MergeRecords(v1[i],v2[j],7, 5, a, 12, 7);
			cnt++;
			outPipe.Insert(&r);
		}
		if(status < 0)i++;
		if(status >= 0) j++;
	}

	// 	for(int i=0;i<v1.size();i++) {
	// 		for(int j=0;j<v2.size();j++) {
	// 			if(cmp.Compare(v1[i],&orderMaker1,v2[j],&orderMaker2) == 0) {
	// // 				Attribute ps_supplycost = {"ps_supplycost", Double};
	// // 				Attribute joinatt[] = {IA,SA,SA,IA,SA,DA,SA, IA,IA,IA,ps_supplycost,SA};
	// // 				Schema join_sch ("join_sch", outAtts, joinatt);

	// 				cout<<v1.size()<<" "<<i<<" "<<j<<endl;
					
	// 			}
	// 		}
	// 	}
	// 	cout<<cnt<<endl;
		outPipe.ShutDown();
}
void WriteOut :: Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { 
	string s = "";
	Record record;
	int cnt = 0;
	while(inPipe.Remove(&record) == 1) {
		s += record.getText(&mySchema);
		cnt++;
	}
	cout<<cnt<<endl;
	fputs(s.c_str(),outFile);
	fclose(outFile);
}
void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){
	int sum = 0;
	Record *record = new Record();	
	ComparisonEngine cmpEng;
	map<Record*, double> umap;
	vector<Record*> v1,v2;
	while(inPipe.Remove(record)) {
		v1.push_back(record);
		record = new Record();
	}
	
	groupAtts.Print();

	Attribute IA = {"int", Int};
	Attribute SA = {"string", String};
	Attribute DA = {"double", Double};

	int sAtts = 7, psAtts = 5;
	int outAtts = sAtts + psAtts;
	Attribute s_nationkey = {"s_nationkey", Int};
	Attribute ps_supplycost = {"ps_supplycost", Double};
	Attribute joinatt[] = {IA,SA,SA,s_nationkey,SA,DA,SA,IA,IA,IA,ps_supplycost,SA};
	Schema join_sch ("join_sch", outAtts, joinatt);


	int i = 0;
	int j = 0;
	map<Record*,double> m;



	for(Record *r:v1) {
		int result_int = 0;
		double result_double = 0;
		computeMe.Apply(*r,result_int,result_double);
		
		bool flag = true;
		for(auto i:m) {
			Record *r1 = i.first;
			
			int status = cmpEng.Compare(r,r1, &groupAtts);
			if(status == 0){
				// r1->Print(&join_sch);
				// r->Print(&join_sch);
				// cout<<"\n";
				m[r1] += result_int + result_double;
				flag = false;
				break;
			}
		}
		if(flag)m[r] = result_int + result_double;
		// cout<<m.size()<<endl;
	}
	// for(auto i:m) {
	// 	Record *r1 = i.first;
	// 	r1->Print(&join_sch);
	// 	cout<<"\n";
	// }
	cout<<"Size:"<<m.size()<<endl;
	int cnt = 0;
	// while(inPipe.Remove(record)) {
	
	// 	else {
	// 		bool flag = false;
	// 		for(std::map<Record*,double> :: iterator i = umap.begin();i != umap.end(); i++){
	// 			Record *r = i->first;
	// 			int status = cmpEng.Compare(r,record, &groupAtts);
	// 			cout<<status<<endl;
	// 			if(status == 0){
	// 				cout<<"Puck"<<endl;
	// 				flag = true;
	// 				umap[r] += result_int + result_double;
	// 				break;
	// 			}
	// 		}
	// 		if(!flag)umap[record] = result_int + result_double;
	// 	}
	// 	record = new Record();
	// }
	string s = "";
	for(auto i:m) {
		double sum = i.second;
		s += to_string(sum) + "|";
		s += "\n";
	}
	// cout<< s;
	FILE *file = fopen("sum1.tbl","w");
	fputs(s.c_str(),file);
	fclose(file);

	FILE *f = fopen("sum1.tbl","r");
	Schema sum_sch ("sum_sch", 1, &DA);

	Record r;
	while(r.SuckNextRecord(&sum_sch,f) == 1) {
		outPipe.Insert(&r);
	}

	outPipe.ShutDown();
}