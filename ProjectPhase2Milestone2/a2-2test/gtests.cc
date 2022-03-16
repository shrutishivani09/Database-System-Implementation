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
TEST(Supplier,HeapFileLinearSearch) {
       string data = getStatus("output1.txt");
       EXPECT_EQ(data,"1");
}
TEST(Lineitem,CNFDidntMatchLinearSearch) {
       string data = getStatus("output2.txt");
       EXPECT_EQ(data,"1");
}

TEST(Customer,BinarySearch) {
       string data = getStatus("output3.txt");
       EXPECT_EQ(data,"0");
}

int main(int argc,char **argv) {
        testing::InitGoogleTest(&argc, argv);
        return RUN_ALL_TESTS();

}
