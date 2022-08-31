#include <iostream>

using namespace std;

struct test
{
    static const size_t kNum = 32;
    uint64_t a[kNum];
}__attribute__((aligned(256)));


int main() 
{
    cout << sizeof(test) << endl;
}






