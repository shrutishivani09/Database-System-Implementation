#include "Statistics.h"
#include <fstream>
#include <algorithm>
#include <float.h>
#include <iostream>
#include <vector>

using namespace std;

Statistics::Statistics()
{
}
Statistics::Statistics(Statistics &copyMe)
{
}
Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples)
{
    struct RelStructure rel;
    rel.n = numTuples;
    relMapping[relName] = rel;
    //  for (auto const& i : relMapping) {
    //      cout<<i.first<<" write "<<i.second.n<<endl;
    //  }
}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    relMapping[relName].attributes[attName] = numDistincts;
    whatRelation[attName] = relName;
}
void Statistics::CopyRel(char *oldName, char *newName)
{
    relMapping[newName] = relMapping[oldName];
}
vector<string> split(string s, string delimiter)
{
    vector<string> v;
    char array[s.length() + 1];
    strcpy(array, s.c_str());
    char *pt = strtok(array, delimiter.c_str());
    while (pt != NULL)
    {
        v.push_back(pt);
        pt = strtok(NULL, delimiter.c_str());
    }
    return v;
}
void Statistics::Read(char *fromWhere)
{
    FILE *fp;
    fp = fopen(fromWhere, "r");
    string s = "";
    while (1)
    {
        char c = fgetc(fp);
        if (feof(fp))
            break;
        s += c;
    }
    // cout<<"Reading "<<s<<endl;
    fclose(fp);

    vector<string> lines = split(s, "\n");
    for (string line : lines)
    {
        vector<string> v1 = split(line, ":");
        char *relName = const_cast<char *>(v1[0].c_str());

        vector<string> key_values = split(v1[1], " ");
        int n = stoi(key_values[key_values.size() - 1]);
        // cout<<"Relation Name is"<<relName<<endl;
        AddRel(relName, n);
        for (int i = 0; i < key_values.size() - 1; i++)
        {
            vector<string> v2 = split(key_values[i], "-");
            AddAtt(relName, const_cast<char *>(v2[0].c_str()), stoi(v2[1]));
        }
    }
    // cout<<"Size is "<<t.m.size()<<endl;
    // relMapping = t.m;
    // stream.close();
}
void Statistics::Write(char *fromWhere)
{
    FILE *fp;
    fp = fopen(fromWhere, "w+");
    string s = "";
    //    cout<<"Writing...."<<endl;
    for (auto const &i : relMapping)
    {
        if (i.first.length() == 0)
            continue;
        string x = i.first + ":";
        for (auto const &j : i.second.attributes)
        {
            x += "" + j.first + "-" + to_string(j.second) + " ";
            // cout<<i.first<<" "<<j.first<<" hi "<<j.second<<endl;
        }

        x += to_string(i.second.n);
        s += x + "\n";
    }
    // cout<<s<<endl;
    fputs(s.c_str(), fp);
    fclose(fp);
}

double Statistics::estimateOperation(ComparisonOp *compOp)
{
    char *left = compOp->left->value;
    char *right = compOp->right->value;
    int code = compOp->code;
    double gamma = 1.0;

    if (whatRelation[left].length() == 0)
        return 0;

    // Join
    if (compOp->right->code == NAME)
    {
        // Code for Join
        string rel1 = whatRelation[left];
        string rel2 = whatRelation[right];

        // for (auto const& i : relMapping) {
        //     for(auto const & j: i.second.attributes) {
        //         cout<<i.first<<" "<<j.first<<" hi "<<j.second<<endl;
        //     }
        // }

        // cout<<left<<" "<<right<<endl;

        double n1 = (double)relMapping[rel1].attributes[left];
        double n2 = (double)relMapping[rel2].attributes[right];

        double total1 = relMapping[rel1].n;
        double total2 = relMapping[rel2].n;

        // cout << "n's " <<n1<<" "<<n2<<endl;

        // cout<<"Totals "<<total1<<" "<<total2<<endl;
        int m = max(n1, n2);

        // if(n1 == n2)return max(total1,total2);

        return 1.0 / (double)m;
    }

    // Less Than
    //  else if(code == LESS_THAN || code == GREATER_THAN) {
    else if (code == EQUALS)
    {

        // for (auto const& i : relMapping) {
        //     for(auto const & j: i.second.attributes) {
        //         cout<<j.first<<" hi "<<j.second<<endl;
        //     }
        // }

        // gamma = (-0.1904833 + (1.0/(double)(relMapping[whatRelation[left]].attributes[left])));
        gamma = 1.0 / (double)(relMapping[whatRelation[left]].attributes[left]);
    }

    // Greater Than
    else
    {
        gamma = 1.0 / 3.0;
    }
    //     cout<<"Gamme is "<<gamma<<endl;
    // cout<<left<<" "<<whatRelation[left]<<endl;
    // cout<<relMapping[whatRelation[left]].n<<endl;
    // cout<<relMapping[whatRelation[left]].n<<" "<<relMapping[whatRelation[right]].n<<endl;
    return gamma;
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    double estimated_result = Estimate(parseTree, relNames, numToJoin);
    // if(estimated_result == 0)return;
    for (int i = 0; i < numToJoin; i++) {
        relMapping[relNames[i]].n = (int)estimated_result;
        // cout<<relNames[i]<<" "<<estimated_result<<endl;
    }
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    double factor = 1;
    string prev = "";
    double total1 = 1, total2 = 1;
    AndList *currAndList = parseTree;
    bool diff = false, first=false;

    while (currAndList != NULL)
    {
        OrList *currOrList = currAndList->left;
        double currResult = 1;
        string prev = "";

        // cout<<"love;y"<<endl;
        while (currOrList != NULL)
        {
            ComparisonOp *compOp = currOrList->left;

            char *left = compOp->left->value;
            char *right = compOp->right->value;
            int code = compOp->code;

            string rel1 = whatRelation[left];
            string rel2 = whatRelation[right];

            // if(relMapping[rel2].n > 0 )
            cout<<rel1<<" "<<rel2<<relMapping[rel2].n <<endl;
            string xs = left;
            if(strcmp(prev.c_str(),xs.c_str()) == 0)diff = true;

            if (relMapping[rel1].n > 0)
            total1 = relMapping[rel1].n;

            if (relMapping[rel2].n > 0 && rel2.length() > 0)
                total2 = relMapping[rel2].n;

            double gamma = estimateOperation(compOp);
            if(gamma == 0)gamma = 1/3;
            // if(compOp->right->code == NAME)currResult *= (1.0 - gamma);
            // else currResult += gamma;
            if(diff){
                if(!first)
                {currResult = 1 - currResult;first = true;}
                // currResult += gamma;
            }

            // else 
            if(diff)currResult += gamma;
            else currResult *= (1 - gamma);
            
            // gamma * (relMapping[whatRelation[left]].n

            currOrList = currOrList->rightOr;
            prev = left;
        }
        if(!diff)currResult = 1 - currResult;
        
        if(currResult == 0.0){
            currResult = 1.0/3.0;
            // cout<<"koushik"<<currResult<<endl;
        }
        factor *= currResult;
        cout<<"Factoe is "<<factor<<endl;
        diff = false;first = false;
        currAndList = currAndList->rightAnd;
        // break;
    }
    // factor -= 0.01;
    cout << "The factor is " << factor << " " << total1 << " " << total2 << endl;
    double estimatedResult = factor * total1 * total2;
    // cout << "Estimated Result is " << estimatedResult << endl;
    return estimatedResult;
}
