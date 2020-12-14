proc sql;
	connect using etl_sys;
	execute by etl_sys (
		truncate etl_sys.ETL_LEVEL cascade;
	);

quit;

data work.WUN7X2S; 
   infile "&ETL_FILE_STATIC_ROOT/etl_sys/ETL_LEVEL.csv"
          lrecl = 256
          delimiter = ';'
          dsd
          missover
          firstobs = 2
          encoding = "utf8"; 
   ; 
   attrib LEVEL_CD length = $2
      format = $2.
      informat = $2.; 
   attrib LEVEL_DESC length = $100
      format = $100.
      informat = $100.; 
   attrib LEVEL_WGT length = 8
      format = 3.
      informat = 3.; 
   
   input LEVEL_CD LEVEL_DESC LEVEL_WGT; 
   
run; 

proc sql;
	insert into etl_sys.ETL_LEVEL
	select * from work.WUN7X2S
	;
quit;

data work.W2D4AKGO; 
   infile "&ETL_FILE_STATIC_ROOT/etl_sys/ETL_EVENT_TYPE.csv"
          lrecl = 256
          delimiter = ';'
          dsd
          missover
          firstobs = 2
          encoding = "utf8"; 
   ; 
   attrib EVENT_TYPE_ID length = 8
      format = 21.
      informat = 21.; 
   attrib EVENT_TYPE_CD length = $32
      format = $32.
      informat = $32.; 
   attrib EVENT_TYPE_DESC length = $100
      format = $100.
      informat = $100.; 
   attrib LEVEL_CD length = $2
      format = $2.
      informat = $2.; 
   
   input EVENT_TYPE_ID EVENT_TYPE_CD EVENT_TYPE_DESC LEVEL_CD; 
   
run; 

proc sql;
	insert into etl_sys.ETL_EVENT_TYPE
	select * from work.W2D4AKGO
	;
quit;


data work.W2E7LOZW; 
   infile "&ETL_FILE_STATIC_ROOT/etl_sys/ETL_FORMAT.csv"
          lrecl = 1024
          delimiter = ';'
          dsd
          missover
          firstobs = 2
          encoding = "utf8"; 
   ; 
   attrib FORMAT_NM length = $32
      format = $32.
      informat = $32.; 
   attrib FORMAT_GROUP_CD length = $32
      format = $32.
      informat = $32.; 
   attrib FORMAT_TYPE_CD length = $1
      format = $1.
      informat = $1.; 
   attrib LIBRARY_NM length = $8
      format = $8.
      informat = $8.; 
   attrib TABLE_NM length = $32
      format = $32.
      informat = $32.; 
   attrib START_COL_NM length = $32
      format = $32.
      informat = $32.; 
   attrib END_COL_NM length = $32
      format = $32.
      informat = $32.; 
   attrib LABEL_COL_NM length = $32
      format = $32.
      informat = $32.; 
   attrib OTHER_VALUE_TXT length = $100
      format = $100.
      informat = $100.; 
   attrib HLO_CD length = $1
      format = $1.
      informat = $1.; 
   attrib WHERE_TXT length = $100
      format = $100.
      informat = $100.; 
   
   input FORMAT_NM FORMAT_GROUP_CD FORMAT_TYPE_CD LIBRARY_NM TABLE_NM 
         START_COL_NM END_COL_NM LABEL_COL_NM OTHER_VALUE_TXT HLO_CD WHERE_TXT; 
   
run; 

proc sql;
	connect using etl_sys;
	execute by etl_sys (
		truncate table etl_sys.ETL_FORMAT
	);

	insert into etl_sys.ETL_FORMAT
	select * from work.W2E7LOZW
	;
quit;

%let tpFmtGroup = 001_100_Setup_Events;

%macro transform_format_group_gen;
   %if &tpFmtGroup ne _ALL_VALUES_ %then %do;
      %format_gen (mpFmtGroup=&tpFmtGroup);
   %end;
   %else %do;
      %format_gen;
   %end;
%mend transform_format_group_gen;

%transform_format_group_gen;