#include "BigQ.h"
#include "Defs.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	runlen *= PAGE_SIZE;
	//cout<<"hello"<<endl;
	Record *record = new Record();
	//cout<<&in<<endl;
	while(in.Remove(record)==1){
		//cout<<record<<endl;
		records.push_back(record);
		char *b = record->GetBits();
		record = new Record();
	}
	//cout<<"<3"<<endl;
	tpmms(0,records.size()-1,sortorder);
	int i = 0;
	//cout<<"Hello"<<endl;
	while(i < records.size())out.Insert(records[i++]);

	// read data from in pipe sort them into runlen pages

    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	out.ShutDown ();
}
void BigQ :: tpmms(int l,int r,OrderMaker &sortorder) {
	if(l >= r)return;
	int m = (l + r) / 2;
	tpmms(l,m,sortorder);tpmms(m+1,l, sortorder);
	merge(l, m, r, sortorder);
}
void BigQ :: merge(int l,int m,int r,OrderMaker &sortorder) {
	int n1 = m-l+1;
	int n2 = r - m;

	vector<Record*> L,R;

	for(int i=0;i<n1;i++)L.push_back(records.at(l+i));
	for(int i=0;i<n2;i++)R.push_back(records.at(m + 1 +i));

	int i = 0,j = 0,k = l;
	while(i < n1 && j < n2) {
		// if(L.at(i) <= R.at(j))

		if(comparisionEngine.Compare(L.at(i),R.at(j),&sortorder) <= 0)
			records[k++] = L.at(i++);
		else records[k++] = R.at(j++);
	}
	while(i < n1)records[k++] = L.at(i++);
	while(j < n2)records[k++] = R.at(j++);

}
BigQ::~BigQ () {
}
