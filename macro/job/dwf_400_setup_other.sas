%macro dwf_400_setup_other;
	%let etls_jobName = 001_400_Setup_Other;
	%etl_job_start;

	proc sql;
		connect using etl_sys;
		execute by etl_sys (
			truncate etl_sys.ORA_FUNCTION_MAP;
		);

	quit;

	data work.ORA_FUNCTION_MAP; 
	   infile "&ETL_FILE_STATIC_ROOT/etl_sys/ORA_FUNCTION_MAP.csv"
			  lrecl = 256
			  delimiter = ';'
			  dsd
			  missover
			  firstobs = 2
			  encoding = "utf8"; 
	   ; 
	   attrib SASFUNCNAME length = $32
		  format = $32.
		  informat = $32.; 
	   attrib SASFUNCNAMELEN length = 8; 
	   attrib DBMSFUNCNAME length = $50
		  format = $50.
		  informat = $50.; 
	   attrib DBMSFUNCNAMELEN length = 8; 
	   attrib FUNCTION_CATEGORY length = $20
		  format = $20.
		  informat = $20.; 
	   attrib FUNC_USAGE_CONTEXT length = $20
		  format = $20.
		  informat = $20.; 
	   attrib FUNCTION_RETURNTYP length = $20
		  format = $20.
		  informat = $20.; 
	   attrib FUNCTION_NUM_ARGS length = 8; 
	   attrib CONVERT_ARGS length = 8; 
	   attrib ENGINEINDEX length = 8; 
	   
	   input SASFUNCNAME SASFUNCNAMELEN DBMSFUNCNAME DBMSFUNCNAMELEN 
			 FUNCTION_CATEGORY FUNC_USAGE_CONTEXT FUNCTION_RETURNTYP 
			 FUNCTION_NUM_ARGS CONVERT_ARGS ENGINEINDEX; 
	   
	run;

	proc sql;
		insert into etl_sys.ORA_FUNCTION_MAP
		select * from work.ORA_FUNCTION_MAP
		;
	quit;

	proc sql;
		connect using etl_sys;
		execute by etl_sys (
			truncate etl_sys.ETL_RESOURCE_TYPE_LOAD;
		);

	quit;

	data work.ETL_RESOURCE_TYPE_LOAD; 
	   infile "&ETL_FILE_STATIC_ROOT/etl_sys/ETL_RESOURCE_TYPE_LOAD.csv"
			  lrecl = 256
			  delimiter = ';'
			  dsd
			  missover
			  firstobs = 2
			  encoding = "utf8"; 
	   ; 
	   attrib RESOURCE_CD length = $32
		  format = $32.
		  informat = $32.; 
	   attrib RESOURCE_TYPE_LOAD length = $32
		  format = $32.
		  informat = $32.; 
	   attrib FIELD_TIME_FRAME length = $32
		  format = $32.
		  informat = $32.; 
	   attrib TIME_FRAME_VALUE length = 8;  
	   
	   input RESOURCE_CD RESOURCE_TYPE_LOAD FIELD_TIME_FRAME TIME_FRAME_VALUE; 
	   
	run;

	proc sql;
		insert into etl_sys.ETL_RESOURCE_TYPE_LOAD
		select * from work.ETL_RESOURCE_TYPE_LOAD
		;
	quit;
%mend dwf_400_setup_other;