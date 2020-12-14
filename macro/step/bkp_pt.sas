%macro bkp_pt;
	libname PT_BKP "/data/PT_BKP";

	%local lmvCnt;

	data work.pt_mems_list;
		set sashelp.vstable(where=(libname='PT'));
	run;

	proc sql noprint;
		select count(*) as cnt into :lmvCnt
		from work.pt_mems_list
		;
	quit;

	%do i=1 %to &lmvCnt.;
		data _NULL_;
			set work.pt_mems_list(firstobs=&I. obs=&I.);
			call symputx('MemName', memname);
		run;
		
		%let lmvMemName = %sysfunc(substrn(&MemName._bkp,1,32));
		
		data PT_BKP.&lmvMemName.;
			set PT.&MemName.;
		run;
	%end;
%mend bkp_pt;
