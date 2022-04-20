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
TEST(Lineitem,Query1) {
       string data = getStatus("test1.txt");
       EXPECT_EQ(data,"1");
}
TEST(part,Query2) {
       string data = getStatus("test2.txt");
       EXPECT_EQ(data,"1");
}

TEST(Supplier,Query5) {
       string data = getStatus("test3.txt");
       EXPECT_EQ(data,"1");
}

TEST(SupplierPartsup,Query10) {
       string data = getStatus("test4.txt");
       EXPECT_EQ(data,"1");
}


int main(int argc,char **argv) {
        testing::InitGoogleTest(&argc, argv);
        return RUN_ALL_TESTS();

}
