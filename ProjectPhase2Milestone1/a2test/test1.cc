#include "test1.h"
#include "BigQ.h"
#include "Defs.h"
#include <pthread.h>
bool flag = true;
int rL = 0;
void *producer (void *arg) {

	Pipe *myPipe = (Pipe *) arg;

	Record temp;
	int counter = 0;
	cout<<myPipe<<"producer"<<endl;
	DBFile dbfile;
	dbfile.Open (rel->path ());
	cout << " producer: opened DBFile " << rel->path () << endl;
	dbfile.MoveFirst ();
	rL *= PAGE_SIZE;
	//cout<<"runlen"<<rL<<endl;
	int count = 0;
	while (dbfile.GetNext (temp) == 1 && count<rL) {
		counter += 1;
	//	cout<<count<<endl;
		if (counter%100000 == 0) {
			 cout  << " producer: " << counter << endl;
		 	if(flag)flag = false;	 
		}
		char *b = temp.GetBits();
		myPipe->Insert (&temp);
		//char *b = temp.GetBits();
		//cout<<"Koushik:"<<((int *) b)[0]<<endl;
		count += ((int *) b)[0];
		//cout<<"koushik"<<endl;
	
	}
	//cout<<"Close()"<<endl;
	dbfile.Close ();
	//cout<<"CLosedd()"<<endl;
	myPipe->ShutDown ();
	cout << " producer: inserted " << counter << " recs into the pipe\n";
	return nullptr;
}

void *consumer (void *arg) {
//	cout<<"howdy, how do u do?"<<endl;	
	testutil *t = (testutil *) arg;

	ComparisonEngine ceng;

	DBFile dbfile;
	char outfile[100];

	if (t->write) {
		sprintf (outfile, "%s.bigq", rel->path ());
		dbfile.Create (outfile, heap, NULL);
	}

	int err = 0;
	int i = 0;

	Record rec[2];
	Record *last = NULL, *prev = NULL;
//	cout<<t->pipe<<" consumerr"<<endl;
	while (t->pipe->Remove (&rec[i%2])) {
		prev = last;
		last = &rec[i%2];
		//cout<<"<3"<<endl;
		if (prev && last) {
			if (ceng.Compare (prev, last, t->order) == 1) {
				err++;
			}
			if (t->write) {
				dbfile.Add (*prev);
			}
		}
		if (t->print) {
			last->Print (rel->schema ());
		}
		i++;
	}

	cout << " consumer: removed " << i << " recs from the pipe\n";

	if (t->write) {
		if (last) {
			dbfile.Add (*last);
		}
		cerr << " consumer: recs removed written out as heap DBFile at " << outfile << endl;
		dbfile.Close ();
	}
	cerr << " consumer: " << (i - err) << " recs out of " << i << " recs in sorted order \n";
//	cout<<"IDK\n";
	if (err) {
		if(flag)flag = false;
		cerr << " consumer: " <<  err << " recs failed sorted order test \n" << endl;
	}
	//pthread_join (thread1, NULL);
	return nullptr;
}


void test1 (int option, int runlen) {
	rL = runlen;
	// sort order for records
	OrderMaker sortorder;
	rel->get_sort_order (sortorder);
	//boolean flag = true;
	int buffsz = 100; // pipe cache size
	Pipe input (buffsz);
	Pipe output (buffsz);

	// thread to dump data into the input pipe (for BigQ's consumption)
	pthread_t thread1;
	pthread_create (&thread1, NULL, producer, (void *)&input);

	// thread to read sorted data from output pipe (dumped by BigQ)
	pthread_t thread2;
	testutil tutil = {&output, &sortorder, false, false};
	if (option == 2) {
		tutil.print = true;
	}
	else if (option == 3) {
		tutil.write = true;
	}
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);

//	cout<<"Right before BigQ"<<endl;
	BigQ bq (input, output, sortorder, runlen);
//	cout<<"ENd of BigQ"<<endl;
	pthread_join (thread2, NULL);
	//cout<<"Love me like you do"<<endl;
	pthread_join (thread1, NULL);
	cout<<flag;
}

int main (int argc, char *argv[]) {

	setup ();
//	bool flag = true;
	relation *rel_ptr[] = {n, r, c, p, ps, o, li};

	int tindx = 0;
	while (tindx < 1 || tindx > 3) {
		cout << " select test option: \n";
		cout << " \t 1. sort \n";
		cout << " \t 2. sort + display \n";
		cout << " \t 3. sort + write \n\t ";
		cin >> tindx;
	}

	int findx = 0;
	while (findx < 1 || findx > 7) {
		cout << "\n select dbfile to use: \n";
		cout << "\t 1. nation \n";
		cout << "\t 2. region \n";
		cout << "\t 3. customer \n";
		cout << "\t 4. part \n";
		cout << "\t 5. partsupp \n";
		cout << "\t 6. orders \n";
		cout << "\t 7. lineitem \n \t ";
		cin >> findx;
	}
	rel = rel_ptr [findx - 1];

	int runlen;
	cout << "\t\n specify runlength:\n\t ";
	cin >> runlen;
	
	test1 (tindx, runlen);

	cleanup ();
}
