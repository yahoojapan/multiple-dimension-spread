# What is logical data size?

In MDS, data size is counted as statistics.
MDS counts three data sizes.
One is the data size when binary, the second is the data size when binary is compressed, and the third is the logical data size.
The data size defined here is the logical data size.

# Why define the data size?

MDS balances processing performance and compression efficiency.
Therefore, it is impossible to accurately measure the performance with the binary data size which varies depending on the implementation.
Define the logical data size to make it possible to measure the data amount of file, block, spread irrespective of binary state.
By using this statistic, we measure processing performance and compression efficiency to improve MDS performance.

Other than that, we are assuming use at data rake.
In Data Lake, it is often via a mechanism to gather event data occurring in real time.
If you know the logical data size when saving the data in MDS, you can also measure how much data was processed for the resource to be saved in the data rake.

# Definition of data size

| Data type | byte size |
|:-----------|:------------|
| NULL  | 0byte |
| Boolean | 1byte |
| Byte  | 1byte |
| Short | 2byte |
| Int   | 4byte |
| Long  | 8byte |
| Float | 4byte |
| Double| 8byte |
| String| 4byte + Byte * length |
| Binary| 4byte + Byte * length |
| Array | 8byte |
| Map,Struct| 0 byte |
