%macro dwf_200_setup_scheduler;
	%let etls_jobName=001_200_Setup_Scheduler;
	%etl_job_start;

	proc sql;
		connect using etl_sys;
		execute by etl_sys (
			truncate etl_sys.ETL_MODULE cascade;
			truncate etl_sys.ETL_RESOURCE_GROUP cascade;
		);

	quit;

	data work.W20RS9JN; 
	   infile "&ETL_FILE_STATIC_ROOT/etl_sys/ETL_MODULE.csv"
			  lrecl = 256
			  delimiter = ';'
			  dsd
			  missover
			  firstobs = 2
			  encoding = "utf8"; 
	   ; 
	   attrib MODULE_ID length = 8
		  format = 21.
		  informat = 21.; 
	   attrib MODULE_DESC length = $200
		  format = $200.
		  informat = $200.; 
	   attrib MODULE_TYPE_CD length = $3
		  format = $3.
		  informat = $3.; 
	   attrib MODULE_TXT length = $200
		  format = $200.
		  informat = $200.; 
	   
	   input MODULE_ID MODULE_DESC MODULE_TYPE_CD MODULE_TXT; 
	   
	run; 

	proc sql;
		insert into etl_sys.ETL_MODULE
		select * from work.W20RS9JN
		;
	quit;

	data work.WT2NJWRA; 
	   infile "&ETL_FILE_STATIC_ROOT/etl_sys/ETL_RESOURCE_GROUP.csv"
			  lrecl = 256
			  delimiter = ';'
			  dsd
			  missover
			  firstobs = 2
			  encoding = "utf8"; 
	   ; 
	   attrib RESOURCE_GROUP_CD length = $32
		  format = $32.
		  informat = $32.; 
	   attrib RELOAD_POLICY_CD length = $3
		  format = $3.
		  informat = $3.; 
	   attrib COMPLETE_EXTRACT_FLG length = $1
		  format = $1.
		  informat = $1.; 
	   attrib LOAD_MODULE_ID length = 8; 
	   
	   input RESOURCE_GROUP_CD RELOAD_POLICY_CD COMPLETE_EXTRACT_FLG LOAD_MODULE_ID; 
	   
	run; 

	proc sql;
		insert into etl_sys.ETL_RESOURCE_GROUP
		select * from work.WT2NJWRA
		;
	quit;

	data work.W370U0IA; 
	   infile "&ETL_FILE_STATIC_ROOT/etl_sys/ETL_RESOURCE.csv"
			  lrecl = 256
			  delimiter = ';'
			  dsd
			  missover
			  firstobs = 2
			  encoding = "utf8"; 
	   ; 
	   attrib RESOURCE_ID length = 8; 
	   attrib RESOURCE_CD length = $32
		  format = $32.
		  informat = $32.; 
	   attrib RESOURCE_GROUP_CD length = $32
		  format = $32.
		  informat = $32.; 
	   attrib RESOURCE_DESC length = $100
		  format = $100.
		  informat = $100.; 
	   
	   input RESOURCE_ID RESOURCE_CD RESOURCE_GROUP_CD RESOURCE_DESC; 
	   
	run; 

	proc sql;
		insert into etl_sys.ETL_RESOURCE
		select * from work.W370U0IA
		;
	quit;

	%format_gen (mpFmtName=res_cd_id);

	data work.ETL_RESOURCE_X_SOURCE; 
	   infile "&ETL_FILE_STATIC_ROOT/etl_sys/ETL_RESOURCE_X_SOURCE.csv"
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
	   attrib SOURCE_TYPE_CD length = $10
		  format = $10.
		  informat = $10.; 
	   attrib SOURCE_ROLE_CD length = $10
		  format = $10.
		  informat = $10.; 
	   attrib LIBREF_CD length = $8
		  format = $8.
		  informat = $8.; 
	   attrib TABLE_NM length = $32
		  format = $32.
		  informat = $32.; 
	   attrib FILE_NM length = $100
		  format = $100.
		  informat = $100.; 
	   attrib STORED_PROC_NM length = $100
		  format = $100.
		  informat = $100.; 
	   
	   input RESOURCE_CD SOURCE_TYPE_CD SOURCE_ROLE_CD LIBREF_CD TABLE_NM FILE_NM 
			 STORED_PROC_NM; 
	   
	run; 

	proc sql;
	   create view work.W56WGU32 as
		  select
			 (input(RESOURCE_CD, res_cd_id.)) as RESOURCE_ID length = 8
				label = 'RESOURCE_ID',
			 SOURCE_TYPE_CD,
			 SOURCE_ROLE_CD,
			 LIBREF_CD,
			 TABLE_NM,
			 FILE_NM,
			 STORED_PROC_NM
	   from work.ETL_RESOURCE_X_SOURCE
	   ;
	quit;

	proc sql;
		insert into etl_sys.ETL_RESOURCE_X_SOURCE
		select * from work.W56WGU32
		;
	quit;

	data work.ETL_RESOURCE_X_ARCH; 
	   infile "&ETL_FILE_STATIC_ROOT/etl_sys/ETL_RESOURCE_X_ARCH.csv"
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
	   attrib ARCH_NM length = $32
		  format = $32.
		  informat = $32.; 
	   attrib ARCH_ROLE_CD length = $10
		  format = $10.
		  informat = $10.; 
	   
	   input RESOURCE_CD ARCH_NM ARCH_ROLE_CD; 
	   
	run; 

	proc sql;
	   create view work.W56WGU32 as
		  select
			 (input(RESOURCE_CD, res_cd_id.)) as RESOURCE_ID length = 8
				label = 'RESOURCE_ID',
			 ARCH_NM,
			 ARCH_ROLE_CD
	   from work.ETL_RESOURCE_X_ARCH
	   ;
	quit;

	proc sql;
		insert into etl_sys.ETL_RESOURCE_X_ARCH
		select * from work.W56WGU32
		;
	quit;	

	proc sql;
		connect using etl_sys;
		execute by etl_sys (
			truncate etl_sys.ETL_SCHEDULE cascade;
		);

	quit;

	data work.ETL_SCHEDULE; 
	   infile "&ETL_FILE_STATIC_ROOT/etl_sys/ETL_SCHEDULE.csv"
			  lrecl = 256
			  delimiter = ';'
			  dsd
			  missover
			  firstobs = 2
			  encoding = "utf8"; 
	   ; 
	   attrib SCHEDULE_CD length = $20
		  format = $20.
		  informat = $20.; 
	   attrib SCHEDULE_DESC length = $100
		  format = $100.
		  informat = $100.; 
	   
	   input SCHEDULE_CD SCHEDULE_DESC; 
	   
	run; 

	proc sql;
	   create view work.WOJO3HP3 as
		  select
			 SCHEDULE_CD,
			 SCHEDULE_DESC
	   from work.ETL_SCHEDULE
		  where not missing(SCHEDULE_CD)
	   ;
	quit;

	proc sql;
		insert into etl_sys.ETL_SCHEDULE
		select * from work.WOJO3HP3
		;
	quit;


	%let ETL_DWH_INIT_DT = %sysfunc(putn('01APR2020'd, best.));
	%let ETL_MAX_DT = %sysfunc(putn('31DEC2021'd, best.));

	/* DAY - Дневной график */
	data work.w_002_050_DAY;
	   length schedule_cd $20 curr_dt open_dttm close_dttm 8;
	   format open_dttm close_dttm datetime20. curr_dt date9.;

	   do curr_dt = &ETL_DWH_INIT_DT to &ETL_MAX_DT;
		  open_dttm   = dhms (curr_dt,   0,  0, 0);
		  close_dttm  = dhms (curr_dt,  23, 59, 0);
		  schedule_cd = "DAY";
		  output;
	   end;
	run;

	/* WEEK_MON - Недельный график, начиная с понедельника */
	data work.w_002_050_WEEK_MON;
	   length schedule_cd $20 curr_dt open_dttm close_dttm 8;
	   format open_dttm close_dttm datetime20. curr_dt date9.;

	   do curr_dt = intnx('week.2', &ETL_DWH_INIT_DT., 0, 'b') to intnx('week.2', &ETL_MAX_DT, 0, 'e') by 7;
		  open_dttm   = dhms (curr_dt,   0,  0, 0);
		  close_dttm  = dhms (curr_dt+6,  23, 59, 0);
		  schedule_cd = "WEEK_MON";
		  output;
	   end;
	run;

	/* WEEK_TUE - Недельный график, начиная со вторника */
	data work.w_002_050_WEEK_TUE;
	   length schedule_cd $20 curr_dt open_dttm close_dttm 8;
	   format open_dttm close_dttm datetime20. curr_dt date9.;

	   do curr_dt = intnx('week.3', &ETL_DWH_INIT_DT., 0, 'b') to intnx('week.3', &ETL_MAX_DT, 0, 'e') by 7;
		  open_dttm   = dhms (curr_dt,   0,  0, 0);
		  close_dttm  = dhms (curr_dt+6,  23, 59, 0);
		  schedule_cd = "WEEK_TUE";
		  output;
	   end;
	run;

	/* STD - Стандартные часовые графики (по 1, 2 и 4 часа через каждые 30 минут) */
	data work.w_002_050_STDH;
	   length schedule_cd $20 curr_dt curr_time_dt open_dttm close_dttm 8;
	   format open_dttm close_dttm datetime20. curr_dt date9.;
	   drop curr_time_dt;

	   do curr_dt = &ETL_DWH_INIT_DT to &ETL_MAX_DT;
		  do curr_time_dt = dhms (curr_dt,  0, 00, 0) to dhms (curr_dt,  23, 30, 0) by hms (0, 30, 0);
			 open_dttm  = curr_time_dt;
			 close_dttm = curr_time_dt + hms (1, 0, 0);
			 schedule_cd = cat("STD", "_", put(timepart(open_dttm), tod5.), "_", put(timepart(close_dttm), tod5.));
			 output;
			 close_dttm = curr_time_dt + hms (2, 0, 0);
			 schedule_cd = cat("STD", "_", put(timepart(open_dttm), tod5.), "_", put(timepart(close_dttm), tod5.));
			 output;
			 close_dttm = curr_time_dt + hms (4, 0, 0);
			 schedule_cd = cat("STD", "_", put(timepart(open_dttm), tod5.), "_", put(timepart(close_dttm), tod5.));
			 output;
		  end;
	   end;
	run;

	/* STD - Стандартный постоянный график (по 10 минут каждые 10 минут с 10-00 по 13-00) */
	data work.w_002_050_STD10M;
	   length schedule_cd $20 curr_dt curr_time_dt open_dttm close_dttm 8;
	   format open_dttm close_dttm datetime20. curr_dt date9.;
	   drop curr_time_dt;

	   do curr_dt = &ETL_DWH_INIT_DT to &ETL_MAX_DT;
		  do curr_time_dt = dhms (curr_dt, 10, 00, 0) to dhms (curr_dt,  12, 50, 0) by hms (0, 10, 0);
			 open_dttm  = curr_time_dt;
			 close_dttm = curr_time_dt + hms (0, 10, 0);
			 schedule_cd = cat("STD", "_", put(timepart(open_dttm), tod5.), "_", put(timepart(close_dttm), tod5.));
			 output;
		  end;
	   end;
	run;

	/* объединяем все графики */
	data work.w_002_050;
	   set work.w_002_050_:;
	run;

	/* защита от дураков */
	proc sort data=work.w_002_050;
	   by schedule_cd open_dttm;
	run;

	data
	   work.x_002_050_valid
	   WORK.NOT_VALID
	;
	   set work.w_002_050;
	   by schedule_cd open_dttm;

	   retain last_close_dttm;
	   if first.schedule_cd then
		  last_close_dttm = &ETL_MIN_DTTM;

	   if
		  (open_dttm ge close_dttm) or
		  (not first.schedule_cd and (open_dttm lt last_close_dttm))
	   then
		  output WORK.NOT_VALID;
	   else do;
		  output work.x_002_050_valid;
		  last_close_dttm   =  close_dttm;
	   end;

	   keep schedule_cd open_dttm close_dttm;
	run;

	%macro sm_002_050_validate;
	   %local lmvObs;
	   %let lmvObs = %member_obs(mpData=WORK.NOT_VALID);
	   %if &lmvObs le 0 %then %return;

	   %job_event_reg (
		  mpEventTypeCode=  DATA_VALIDATION_FAILED,
		  mpEventValues=    %bquote(Некорректные интервалы (&lmvObs) отброшены в таблицу WORK.NOT_VALID)
	   );
	%mend sm_002_050_validate;
	%sm_002_050_validate;


	/* расчет ид-а окна */
	data work.W2DRTP6G;
	   set work.x_002_050_valid;
	   keep frame_id schedule_cd open_dttm close_dttm;

	   by schedule_cd;
	   retain frame_id;

	   /* Ни в коем случае нельзя менять указанные ниже константы! */
	   /* В противном случае все идентификаторы окон изменятся и всю историю загрузок придется стирать!! */
	   if first.schedule_cd then do;
		  select(schedule_cd);
			 /* Стандартные графики по 1 часу */
			 when("STD_00:00_01:00")    frame_id = 10000000;
			 when("STD_01:00_02:00")    frame_id = 10100000;
			 when("STD_02:00_03:00")    frame_id = 10200000;
			 when("STD_03:00_04:00")    frame_id = 10300000;
			 when("STD_04:00_05:00")    frame_id = 10400000;
			 when("STD_05:00_06:00")    frame_id = 10500000;
			 when("STD_06:00_07:00")    frame_id = 10600000;
			 when("STD_07:00_08:00")    frame_id = 10700000;
			 when("STD_08:00_09:00")    frame_id = 10800000;
			 when("STD_09:00_10:00")    frame_id = 10900000;
			 when("STD_10:00_11:00")    frame_id = 11000000;
			 when("STD_11:00_12:00")    frame_id = 11100000;
			 when("STD_12:00_13:00")    frame_id = 11200000;
			 when("STD_13:00_14:00")    frame_id = 11300000;
			 when("STD_14:00_15:00")    frame_id = 11400000;
			 when("STD_15:00_16:00")    frame_id = 11500000;
			 when("STD_16:00_17:00")    frame_id = 11600000;
			 when("STD_17:00_18:00")    frame_id = 11700000;
			 when("STD_18:00_19:00")    frame_id = 11800000;
			 when("STD_19:00_20:00")    frame_id = 11900000;
			 when("STD_20:00_21:00")    frame_id = 12000000;
			 when("STD_21:00_22:00")    frame_id = 12100000;
			 when("STD_22:00_23:00")    frame_id = 12200000;
			 when("STD_23:00_00:00")    frame_id = 12300000;

			 when("STD_00:30_01:30")    frame_id = 10050000;
			 when("STD_01:30_02:30")    frame_id = 10150000;
			 when("STD_02:30_03:30")    frame_id = 10250000;
			 when("STD_03:30_04:30")    frame_id = 10350000;
			 when("STD_04:30_05:30")    frame_id = 10450000;
			 when("STD_05:30_06:30")    frame_id = 10550000;
			 when("STD_06:30_07:30")    frame_id = 10650000;
			 when("STD_07:30_08:30")    frame_id = 10750000;
			 when("STD_08:30_09:30")    frame_id = 10850000;
			 when("STD_09:30_10:30")    frame_id = 10950000;
			 when("STD_10:30_11:30")    frame_id = 11050000;
			 when("STD_11:30_12:30")    frame_id = 11150000;
			 when("STD_12:30_13:30")    frame_id = 11250000;
			 when("STD_13:30_14:30")    frame_id = 11350000;
			 when("STD_14:30_15:30")    frame_id = 11450000;
			 when("STD_15:30_16:30")    frame_id = 11550000;
			 when("STD_16:30_17:30")    frame_id = 11650000;
			 when("STD_17:30_18:30")    frame_id = 11750000;
			 when("STD_18:30_19:30")    frame_id = 11850000;
			 when("STD_19:30_20:30")    frame_id = 11950000;
			 when("STD_20:30_21:30")    frame_id = 12050000;
			 when("STD_21:30_22:30")    frame_id = 12150000;
			 when("STD_22:30_23:30")    frame_id = 12250000;
			 when("STD_23:30_00:30")    frame_id = 12350000;

			 /* Стандартные графики по 2 часа */
			 when("STD_00:00_02:00")    frame_id = 20000000;
			 when("STD_01:00_03:00")    frame_id = 20100000;
			 when("STD_02:00_04:00")    frame_id = 20200000;
			 when("STD_03:00_05:00")    frame_id = 20300000;
			 when("STD_04:00_06:00")    frame_id = 20400000;
			 when("STD_05:00_07:00")    frame_id = 20500000;
			 when("STD_06:00_08:00")    frame_id = 20600000;
			 when("STD_07:00_09:00")    frame_id = 20700000;
			 when("STD_08:00_10:00")    frame_id = 20800000;
			 when("STD_09:00_11:00")    frame_id = 20900000;
			 when("STD_10:00_12:00")    frame_id = 21000000;
			 when("STD_11:00_13:00")    frame_id = 21100000;
			 when("STD_12:00_14:00")    frame_id = 21200000;
			 when("STD_13:00_15:00")    frame_id = 21300000;
			 when("STD_14:00_16:00")    frame_id = 21400000;
			 when("STD_15:00_17:00")    frame_id = 21500000;
			 when("STD_16:00_18:00")    frame_id = 21600000;
			 when("STD_17:00_19:00")    frame_id = 21700000;
			 when("STD_18:00_20:00")    frame_id = 21800000;
			 when("STD_19:00_21:00")    frame_id = 21900000;
			 when("STD_20:00_22:00")    frame_id = 22000000;
			 when("STD_21:00_23:00")    frame_id = 22100000;
			 when("STD_22:00_00:00")    frame_id = 22200000;
			 when("STD_23:00_01:00")    frame_id = 22300000;

			 when("STD_00:30_02:30")    frame_id = 20050000;
			 when("STD_01:30_03:30")    frame_id = 20150000;
			 when("STD_02:30_04:30")    frame_id = 20250000;
			 when("STD_03:30_05:30")    frame_id = 20350000;
			 when("STD_04:30_06:30")    frame_id = 20450000;
			 when("STD_05:30_07:30")    frame_id = 20550000;
			 when("STD_06:30_08:30")    frame_id = 20650000;
			 when("STD_07:30_09:30")    frame_id = 20750000;
			 when("STD_08:30_10:30")    frame_id = 20850000;
			 when("STD_09:30_11:30")    frame_id = 20950000;
			 when("STD_10:30_12:30")    frame_id = 21050000;
			 when("STD_11:30_13:30")    frame_id = 21150000;
			 when("STD_12:30_14:30")    frame_id = 21250000;
			 when("STD_13:30_15:30")    frame_id = 21350000;
			 when("STD_14:30_16:30")    frame_id = 21450000;
			 when("STD_15:30_17:30")    frame_id = 21550000;
			 when("STD_16:30_18:30")    frame_id = 21650000;
			 when("STD_17:30_19:30")    frame_id = 21750000;
			 when("STD_18:30_20:30")    frame_id = 21850000;
			 when("STD_19:30_21:30")    frame_id = 21950000;
			 when("STD_20:30_22:30")    frame_id = 22050000;
			 when("STD_21:30_23:30")    frame_id = 22150000;
			 when("STD_22:30_00:30")    frame_id = 22250000;
			 when("STD_23:30_01:30")    frame_id = 22350000;

			 /* Стандартные постоянные графики по 10 минут */
			 when("STD_10:00_10:10")    frame_id = 34700000;
			 when("STD_10:10_10:20")    frame_id = 34800000;
			 when("STD_10:20_10:30")    frame_id = 34900000;
			 when("STD_10:30_10:40")    frame_id = 35000000;
			 when("STD_10:40_10:50")    frame_id = 35100000;
			 when("STD_10:50_11:00")    frame_id = 35200000;
			 when("STD_11:00_11:10")    frame_id = 35300000;
			 when("STD_11:10_11:20")    frame_id = 35400000;
			 when("STD_11:20_11:30")    frame_id = 35500000;
			 when("STD_11:30_11:40")    frame_id = 35600000;
			 when("STD_11:40_11:50")    frame_id = 35700000;
			 when("STD_11:50_12:00")    frame_id = 35800000;
			 when("STD_12:00_12:10")    frame_id = 35900000;
			 when("STD_12:10_12:20")    frame_id = 36000000;
			 when("STD_12:20_12:30")    frame_id = 36100000;
			 when("STD_12:30_12:40")    frame_id = 36200000;
			 when("STD_12:40_12:50")    frame_id = 36300000;
			 when("STD_12:50_13:00")    frame_id = 36400000;

			 /* Стандартные графики по 4 часа */
			 when("STD_00:00_04:00")    frame_id = 40000000;
			 when("STD_01:00_05:00")    frame_id = 40100000;
			 when("STD_02:00_06:00")    frame_id = 40200000;
			 when("STD_03:00_07:00")    frame_id = 40300000;
			 when("STD_04:00_08:00")    frame_id = 40400000;
			 when("STD_05:00_09:00")    frame_id = 40500000;
			 when("STD_06:00_10:00")    frame_id = 40600000;
			 when("STD_07:00_11:00")    frame_id = 40700000;
			 when("STD_08:00_12:00")    frame_id = 40800000;
			 when("STD_09:00_13:00")    frame_id = 40900000;
			 when("STD_10:00_14:00")    frame_id = 41000000;
			 when("STD_11:00_15:00")    frame_id = 41100000;
			 when("STD_12:00_16:00")    frame_id = 41200000;
			 when("STD_13:00_17:00")    frame_id = 41300000;
			 when("STD_14:00_18:00")    frame_id = 41400000;
			 when("STD_15:00_19:00")    frame_id = 41500000;
			 when("STD_16:00_20:00")    frame_id = 41600000;
			 when("STD_17:00_21:00")    frame_id = 41700000;
			 when("STD_18:00_22:00")    frame_id = 41800000;
			 when("STD_19:00_23:00")    frame_id = 41900000;
			 when("STD_20:00_00:00")    frame_id = 42000000;
			 when("STD_21:00_01:00")    frame_id = 42100000;
			 when("STD_22:00_02:00")    frame_id = 42200000;
			 when("STD_23:00_03:00")    frame_id = 42300000;

			 when("STD_00:30_04:30")    frame_id = 40050000;
			 when("STD_01:30_05:30")    frame_id = 40150000;
			 when("STD_02:30_06:30")    frame_id = 40250000;
			 when("STD_03:30_07:30")    frame_id = 40350000;
			 when("STD_04:30_08:30")    frame_id = 40450000;
			 when("STD_05:30_09:30")    frame_id = 40550000;
			 when("STD_06:30_10:30")    frame_id = 40650000;
			 when("STD_07:30_11:30")    frame_id = 40750000;
			 when("STD_08:30_12:30")    frame_id = 40850000;
			 when("STD_09:30_13:30")    frame_id = 40950000;
			 when("STD_10:30_14:30")    frame_id = 41050000;
			 when("STD_11:30_15:30")    frame_id = 41150000;
			 when("STD_12:30_16:30")    frame_id = 41250000;
			 when("STD_13:30_17:30")    frame_id = 41350000;
			 when("STD_14:30_18:30")    frame_id = 41450000;
			 when("STD_15:30_19:30")    frame_id = 41550000;
			 when("STD_16:30_20:30")    frame_id = 41650000;
			 when("STD_17:30_21:30")    frame_id = 41750000;
			 when("STD_18:30_22:30")    frame_id = 41850000;
			 when("STD_19:30_23:30")    frame_id = 41950000;
			 when("STD_20:30_00:30")    frame_id = 42050000;
			 when("STD_21:30_01:30")    frame_id = 42150000;
			 when("STD_22:30_02:30")    frame_id = 42250000;
			 when("STD_23:30_03:30")    frame_id = 42350000;

			 when("DAY")                frame_id = 50000000;

			 when("WEEK_MON")           frame_id = 51000000;
			 when("WEEK_TUE")           frame_id = 52000000;
		  end;
	   end;

	   frame_id + 1;
	run;

	proc sql
	;
	create table work.TMP_SCHEDULE_FRAME as
	select
	   W2DRTP6G.FRAME_ID length = 8   
	      label = 'FRAME_ID',
	   W2DRTP6G.SCHEDULE_CD length = 20   
	      format = $20.
	      informat = $20.
	      label = 'SCHEDULE_CD',
	   W2DRTP6G.OPEN_DTTM length = 8   
	      format = DATETIME20.
	      informat = DATETIME20.
	      label = 'OPEN_DTTM',
	   W2DRTP6G.CLOSE_DTTM length = 8   
	      format = DATETIME20.
	      informat = DATETIME20.
	      label = 'CLOSE_DTTM'
	from
	   work.W2DRTP6G as W2DRTP6G
	;
	quit;
	
	proc sql;
		insert into etl_sys.ETL_SCHEDULE_FRAME
		select * from work.TMP_SCHEDULE_FRAME
		;
	quit;
	
	data work.ETL_MODULE_X_RULE; 
	   infile "&ETL_FILE_STATIC_ROOT/etl_sys/ETL_MODULE_X_RULE.csv"
	          lrecl = 1024
	          delimiter = ';'
	          dsd
	          missover
	          firstobs = 2
	          encoding = "utf8"; 
	   ; 
	   attrib MODULE_ID length = 8; 
	   attrib STATE_CD length = $3
	      format = $3.
	      informat = $3.; 
	   attrib FILTER_BY_GROUP length = $100
	      format = $100.
	      informat = $100.; 
	   attrib FILTER_SCHEDULE_CD length = $20
	      format = $20.
	      informat = $20.; 
	   attrib FILTER_RESOURCE_GROUP_CD length = $32
	      format = $32.
	      informat = $32.; 
	   attrib FILTER_RESOURCE_CD length = $32
	      format = $32.
	      informat = $32.; 
	   attrib FILTER_STATUS_CD length = $32
	      format = $32.
	      informat = $32.; 
	   attrib FILTER_EXTRA_TXT length = $100
	      format = $100.
	      informat = $100.; 
	   attrib NEXT_STATE_CD length = $3
	      format = $3.
	      informat = $3.; 
	   attrib ELSE_STATE_CD length = $3
	      format = $3.
	      informat = $3.; 
	   
	   input MODULE_ID STATE_CD FILTER_BY_GROUP FILTER_SCHEDULE_CD 
	         FILTER_RESOURCE_GROUP_CD FILTER_RESOURCE_CD FILTER_STATUS_CD 
	         FILTER_EXTRA_TXT NEXT_STATE_CD ELSE_STATE_CD; 
	   
	run; 
	
	proc sql;
	   create view work.WEGRABT as
	      select
	         MODULE_ID,
	         STATE_CD,
	         FILTER_BY_GROUP,
	         FILTER_SCHEDULE_CD,
	         FILTER_RESOURCE_GROUP_CD,
	         FILTER_RESOURCE_CD,
	         FILTER_STATUS_CD,
	         FILTER_EXTRA_TXT length = 200   
	            format = $200.
	            informat = $200.,
	         NEXT_STATE_CD,
	         ELSE_STATE_CD
	   from work.ETL_MODULE_X_RULE
	   ;
	quit;
	
	proc sql;
	   create view work.WEGT7ML as
	      select
	         (monotonic()) as RULE_ID length = 8
	            label = 'RULE_ID',
	         MODULE_ID,
	         STATE_CD,
	         FILTER_BY_GROUP,
	         FILTER_SCHEDULE_CD,
	         FILTER_RESOURCE_GROUP_CD,
	         FILTER_RESOURCE_CD,
	         FILTER_STATUS_CD,
	         FILTER_EXTRA_TXT,
	         NEXT_STATE_CD,
	         (coalesce(ELSE_STATE_CD, "R")) as ELSE_STATE_CD length = 3
	            format = $3.
	            informat = $3.
	            label = 'ELSE_STATE_CD'
	   from WORK.WEGRABT
	      where not missing(MODULE_ID)
	   ;
	quit;
	
	proc sql;
		insert into etl_sys.ETL_MODULE_X_RULE
		select * from work.WEGT7ML
		;
	quit;

	%let tpFmtGroup = 001_200_Setup_Scheduler;
	%let refDesc = ;
	%macro transform_format_group_gen;
	   %if &tpFmtGroup ne _ALL_VALUES_ %then %do;
		  %format_gen (mpFmtGroup=&tpFmtGroup);
	   %end;
	   %else %do;
		  %format_gen;
	   %end;
	%mend transform_format_group_gen;

	%transform_format_group_gen;
%mend dwf_200_setup_scheduler;
