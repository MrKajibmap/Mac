/* libname PT_BKP "/data/PT_BKP"; */
%macro restore_bkp_pt;
	data work.pt_mems_list;
		set sashelp.vstable(where=(libname='PT_BKP' and (scan(memname, -1, '_') = '1BKP' or scan(memname, -1, '_') = '1')));
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
			%let lmvTabNmIn = %sysfunc(substr(&MemName., 1, %eval(%length(&MemName)-%length(%scan(&MemName,-1,'_')) -1))) ;
			PROC SQL NOPRINT;	
				CONNECT TO POSTGRES AS CONN (server="10.252.151.3" port=5452 user=pt password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=pt defer=yes readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=8192");
					/* truncate target table in PT PG schema */
					EXECUTE BY CONN
						(
							TRUNCATE TABLE public.&lmvTabNmIn.
						)
					;
					DISCONNECT FROM CONN;
			QUIT;

			proc append base=pt.&lmvTabNmIn. data=pt_bkp.&MemName. force; 
			run; 
	
	%end;
%mend restore_bkp_pt;