import os
import logging
from myparser.script_parser import ScriptParser

if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)

    s = '''
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
    '''

    tmp_filepath = "d:/tmp/test.script"

    with open(tmp_filepath, 'w') as fw:
        fw.write(s)

    sp = ScriptParser(b_add_sstream_size=False, b_add_sstream_link=False)

    sp.parse_file(tmp_filepath, external_params={}, dest_filepath=tmp_filepath)
    print("find output graph file at [{}.dot.pdf]".format(tmp_filepath))

#    os.unlink(tmp_filepath)

