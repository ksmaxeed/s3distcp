s3distcp
========



I forked from original repository, and I added two new options. It is to be specified together with the GroupBy option.

1. The option to insert a new line when merging files.  
2. The option to remove the directory char "/" from the file path where the merged files will be saved.

The options useful the specific case.

#### Example 
It's useful the case of combine daily data files into a monthly file. 

```
2021/
  ┣ 04/
  ┃  ┣ 16/
  ┃  ┃  ┣ 001.file
  ┃  ┃  ┣ 002.file
  ┃  ┃  ┗ 003.file
  ┃  ┗ 17/
  ┃     ┣ 001.file
  ┃     ┣ 002.file
  ┃     ┗ 003.file
  ┗ 05/
     ┣ 20/
     ┃  ┣ 001.file
     ┃  ┣ 002.file
     ┃  ┗ 003.file
     ┗ 21/
        ┣ 001.file
        ┣ 002.file
        ┗ 003.file
```
To merge files with adding new lines, use the option "--groupWithNewLine=true".
In EMR's Step Custom Jar, specify the Jar file that you built.
Then set the options as follows.

```
 --s3Endpoint=s3-ap-northeast-1.amazonaws.com 
 --src=<SRC_PATH>
 --dest=<DEST_PATH>
 --groupBy=(.+/[0-9]+/[0-9]+/)[0-9]+/.+file
 --outputCodec=gzip
 --groupWithNewLine=true
```

After this step complete, the destination folder contents is like this,

```
2021/
  ┣ 04/
  ┃  ┗ 16/
  ┃     ┗ <MERGED_FILE>.gz
  ┗ 05/
     ┗ 20/
        ┗ <MERGED_FILE>.gz
```

Further more the day folder is not necessary. So add the option "--numberDeletePartition=1".

```
 --s3Endpoint=<REGION> 
 --src=<SRC_PATH>
 --dest=<DEST_PATH>
 --groupBy=(.+/[0-9]+/[0-9]+/)[0-9]+/.+file
 --outputCodec=gzip
 --groupWithNewLine=true
 --numberDeletePartition=1
```

the destination folder contents is like this,

```
2021/
  ┣ 04/
  ┃  ┗ <MERGED_FILE>.gz
  ┗ 05/
     ┗ <MERGED_FILE>.gz
```

#### Information

If you use the MacOS and download the gz files which made with these options, may be it's cannot uncompress file. But it's not the broken gz file. In this case please use gunzip command in the terminal.

--------

reference:  
https://stackoverflow.com/questions/31393706/how-to-get-s3distcp-to-merge-with-newlines
