%MACRO M_ETL_REDIRECT_LOG(mvMODE, mvJOB_NAME, mvAREA);

	%LET mvMODE=%upcase(&mvMODE);
	%LET mvAREA=&mvAREA;
	%LET mvJOB_NAME = &mvJOB_NAME.;
	
	%M_ETL_GENERATE_DATE;
		
	%LET mvDATETIME=&mvDATETIME;
	
	DATA _NULL_;
		CALL SYMPUT("mvAREA", STRIP("&mvAREA"));
	RUN;
		
	%IF %LENGTH(&mvAREA.)>0 %THEN %DO;
		%LET mvOPT_AREA=/data/logs;
		%LET mvOPT_LOGPATH=&mvAREA./&mvJOB_NAME._&mvDATETIME..LOG;
	%END;

	%IF &mvMODE=START %THEN %DO;
		
		PROC PRINTTO LOG="&mvOPT_AREA./&mvOPT_LOGPATH." NEW;
		RUN;

	%END;
	%ELSE %IF &mvMODE=END %THEN %DO;

		PROC PRINTTO;
		RUN;
		
	%END;

%MEND M_ETL_REDIRECT_LOG;