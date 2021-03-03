%macro dp_export_create_full_fcst(mpPlanAreaNmKomp=COMP_SALE_MONTH
								,mpPlanAreaNmNonkomp=ACT_QNT_MONTH_NONKOMP
								,mpOutTable=max_casl.dp_out); 


	%local lmvPlanAreaNmKomp
			lmvPlanAreaNmNonkomp
			lmvOutTable
			lmvMode
			lmvPath
			lmvOutLibrefNm
			lmvOutTabNameNm
			;
	
	%let lmvPlanAreaNmKomp = &mpPlanAreaNmKomp.;
	%let lmvPlanAreaNmNonkomp = &mpPlanAreaNmNonkomp.;
	%let lmvOutTable = &mpOutTable.;
	
	%member_names (mpTable=&lmvOutTable, mpLibrefNameKey=lmvOutLibrefNm, mpMemberNameKey=lmvOutTabNameNm);

	%dp_export_pa(mpPlanAreaNm=&lmvPlanAreaNmNonkomp.
						,mpOutTable=casuser.dp_out_fcst_nonkomp
						,mpMode=caslib
						,mpPath =/data/dm_rep/); 
						
	%dp_export_pa(mpPlanAreaNm=&lmvPlanAreaNmKomp.
						,mpOutTable=casuser.dp_out_fcst_komp
						,mpMode=caslib
						,mpPath =/data/dm_rep/); 
						
	proc casutil;
		droptable casdata="&lmvOutTabNameNm." incaslib="&lmvOutLibrefNm." quiet;
	quit;
	
	data casuser.full_fcst(replace=yes);
		set casuser.dp_out_fcst_nonkomp;
	run;
	
	data casuser.full_fcst(append=yes);
		set casuser.dp_out_fcst_komp;
	run;
	
	data &lmvOutLibrefNm..&lmvOutTabNameNm. (promote=yes);
		set casuser.full_fcst;
	run;
	
	proc casutil;
		droptable casdata="full_fcst" incaslib="casuser" quiet;
        save incaslib="&lmvOutLibrefNm." outcaslib="&lmvOutLibrefNm." casdata="&lmvOutTabNameNm." casout="&lmvOutTabNameNm..sashdat" replace;
    quit;

%mend dp_export_create_full_fcst;