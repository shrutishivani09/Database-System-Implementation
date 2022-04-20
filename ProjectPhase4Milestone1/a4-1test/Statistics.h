#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <iostream>
#include <vector> 
#include <map>
#include <string>
#include <string.h>
using namespace std;
class Statistics {
private:
	struct RelStructure {
		map<string,int> attributes;
		int n;
	};
	struct Node {
		string value;
		vector<string> children;
	};
	
	map<string,struct RelStructure> relMapping;	
	vector<struct Node> nodes;
	map<string,string> whatRelation;

public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

	double estimateOperation(ComparisonOp *compOp);

};

#endif