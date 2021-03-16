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
	
	/*
	data casuser.full_fcst(replace=yes);
		set casuser.dp_out_fcst_nonkomp;
	run;
	
	data casuser.full_fcst(append=yes);
		set casuser.dp_out_fcst_komp;
	run;
	
	data &lmvOutLibrefNm..&lmvOutTabNameNm. (promote=yes);
		set casuser.full_fcst;
	run;
	*/
	
	proc contents data= CASUSER.DP_OUT_FCST_KOMP out=work.komp_struct;
	run;

	proc contents data= CASUSER.DP_OUT_FCST_nonKOMP out=work.nonkomp_struct;
	run;

	proc sql noprint;
			select komp.name into :lmvValidVarList separated by ", "
			from work.komp_struct komp
			inner join work.nonkomp_struct nonkomp
			on komp.name = nonkomp.name
			;
	quit;
	%put &=lmvValidVarList;
	proc fedsql sessref=casauto;
		create table casuser.full_fcst {options replace=true} as
			select &lmvValidVarList. 
			from CASUSER.DP_OUT_FCST_KOMP
			union 
			select &lmvValidVarList. 
			from CASUSER.DP_OUT_FCST_nonKOMP
		;
	quit;
	proc casutil;
		promote casdata="full_fcst" incaslib="casuser" outcaslib="&lmvOutLibrefNm." casout="&lmvOutTabNameNm.";
		droptable casdata="full_fcst" incaslib="casuser" quiet;
        save incaslib="&lmvOutLibrefNm." outcaslib="&lmvOutLibrefNm." casdata="&lmvOutTabNameNm." casout="&lmvOutTabNameNm..sashdat" replace;
    quit;

%mend dp_export_create_full_fcst;