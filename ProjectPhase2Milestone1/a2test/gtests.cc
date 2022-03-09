#include "gtest/gtest.h"
#include<string>
#include<iostream>
#include<fstream>
using namespace std;
string getStatus(string filename) {
    ifstream ReadF(filename);
    string data,x;
    while(getline(ReadF,data))x = data;
    //cout<<"gdnjkgnfsigfngnsofs"<<data<<endl;
    return x;
}
TEST(RegionRecordSort,Sorting0Pages) {
       string data = getStatus("test1.txt");
       EXPECT_EQ(data,"1");
}
TEST(RegionRecordSort,Sorting10Pages) {
       string data = getStatus("test2.txt");
       EXPECT_EQ(data,"1");
}

TEST(LinitemRecordSort,Sorting1Page) {
       string data = getStatus("test3.txt");
       EXPECT_EQ(data,"1");
}

TEST(PartsupSort,SortingRecordsWithDifferntAttribute) {
       string data = getStatus("test4.txt");
       EXPECT_EQ(data,"1");
}
int main(int argc,char **argv) {
        testing::InitGoogleTest(&argc, argv);
        return RUN_ALL_TESTS();

}
