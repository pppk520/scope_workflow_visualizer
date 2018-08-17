[![CircleCI](https://circleci.com/gh/pppk520/BackendProjectParser.svg?style=svg)](https://circleci.com/gh/pppk520/BackendProjectParser)

# SCOPE & DWC Workflow Visualizer 
This project works on a simplified Microsoft [SCOPE](http://www.vldb.org/pvldb/1/1454166.pdf) language parser to automatically generate logic graph from scripts. 

Implemented with:
* [pyparsing](http://infohost.nmt.edu/tcc/help/pubs/pyparsing/web/index.html)
* [graphviz](https://www.graphviz.org/)

### Example
The following SCOPE script 

```scope
#DECLARE MyFile1	string = "/path/to/myfile1.ss";
#DECLARE MyFile2	string = "/path/to/myfile2.ss";

#DECLARE MyOutputFile	string = "/path/to/output.ss";
#DECLARE MyOutputFile1	string = "/path/to/output1.ss";

Data1 =
SELECT RGUID,
    ListingId AS OrderItemId,
	HourNum
FROM (SSTREAM @MyFile1);

Data2 =
SELECT RGUID,
    ListingId AS OrderItemId,
	HourNum
FROM (SSTREAM @MyFile2);

Data = SELECT
            RGUID,
            OrderItemId
       FROM Data1
       JOIN Data2 ON Data1.RGUID == Data2.RGUID
       WHERE OrderItemId != 0;

OUTPUT TO SSTREAM @MyOutputFile;
OUTPUT Data1 TO SSTREAM @MyOutputFile1;

``` 
 
The output will be like this: 
 
![Output](https://user-images.githubusercontent.com/6903521/44243157-289ede00-a200-11e8-8af1-379c2871cdc1.png)


