#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <vector> 

using namespace std;


class BigQ {
private:
vector<Record*> records;
ComparisonEngine comparisionEngine;

void tpmms(int l,int r, OrderMaker &sortorder);
void merge(int l, int mid, int h, OrderMaker &sortorder);
public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif