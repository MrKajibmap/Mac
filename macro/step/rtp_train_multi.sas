%macro rtp_train_multi(mpThreadCnt=10,
						mpModelTable=PMIX_MODEL_TABLE,
						mpId = product_id pbo_location_id sales_dt,
						mpTarget =sum_qty,
						mpAbt = dm_abt.all_ml_train,
						mpPrefix = FOREST,
						mpStart = 1);
		
	%if %sysfunc(SESSFOUND(casauto)) = 0 %then %do; 
		cas casauto;
		caslib _all_ assign;
	%end;
	
	data _null_;
		set models.&mpModelTable. nobs=nobs;
		call symputx('length', nobs, 'G');
		stop;
	run;

	%let mpEnd=&length.;
	%let mvTHREAD_CNT=&mpThreadCnt.;
	
	proc casutil;
		DROPTABLE CASDATA="buffer_table" INCASLIB="casuser" QUIET;
	run;
	
	/* CREATE MAIN BUFFER TABLE WITH PARAMETERS FOR THREADS */
	data casuser.buffer_table(replace=yes);
		do i = &mpStart. to &mpEnd.;
			output;
		end;
	run;
	
	/*
	data casuser.buffer_table(replace=yes);
		do i = &mpStart. to &mpEnd.;
			if i in (10 ,11, 19, 20 ,21 ,22, 32, 33 ,42, 43, 44 ,49 ,50, 51, 52, 53, 54, 55);
			output;
		end;
	run;
	*/
	/* CREATE BUFFERS TABLES WITH PARAMETERS FOR EACH THREAD */
	%MACRO mGENERATE_BATCHES;
		%GLOBAL mvBUFFER_ROW_CNT
				mvROWS_FOR_THREAD
				;
		PROC SQL NOPRINT;
				SELECT COUNT(*) AS CNT, ceil(COUNT(*)/&mvTHREAD_CNT.) AS ROWS_FOR_THREAD INTO :mvBUFFER_ROW_CNT, :mvROWS_FOR_THREAD
				FROM casuser.BUFFER_TABLE
				;
		QUIT;
	
		%PUT &=mvBUFFER_ROW_CNT;
		%PUT &=mvROWS_FOR_THREAD;
	
		/* CALC COUNT OF ROWS WITH PARAMETERS FOR SENDING TO THREADS */
		%DO I = 1 %TO &mvTHREAD_CNT.;
			%IF &I. = 1 %THEN %DO;
				%LET mvFIRST_OBS =1;
			%END;
			%ELSE %DO;
				%LET mvFIRST_OBS =%SYSEVALF(&I. * &mvROWS_FOR_THREAD. - &mvROWS_FOR_THREAD. +1);
			%END;
			/* DELETE BATCH TABLE FOR THREAD*/
		    proc casutil; 
				DROPTABLE CASDATA="BUFFER_TABLE_&I." INCASLIB="casuser" QUIET;
			run;
				/* DELETE BATCH TABLE FOR THREAD*/
		    proc casutil; 
				DROPTABLE CASDATA="BUFFER_TABLE_&I." INCASLIB="public" QUIET;
			run;
			
			/*CREATE BATCH_TABLE FOR THREADS */
			DATA public.BUFFER_TABLE_&I.(promote=yes);
				SET casuser.BUFFER_TABLE(FIRSTOBS = &mvFIRST_OBS. OBS = %SYSEVALF(&mvFIRST_OBS.+&mvROWS_FOR_THREAD. -1));
			RUN;
		%END;
		
	%MEND mGENERATE_BATCHES;
	
	%mGENERATE_BATCHES;

	%put &=mvBUFFER_ROW_CNT;
	%put &=mvROWS_FOR_THREAD;	
	
	%macro signon;
		
		/* SIGNON THREAD_SESSIONS AND PUT PARAMETERS INTO */
		%DO mvTHREAD_NUM = 1 %TO &mvTHREAD_CNT.;
			SIGNON T_&mvTHREAD_NUM. sascmd="!sascmd" WAIT = YES;
			/* ПЕРЕДАЧА ПАРАМЕТРОВ В КАЖДУЮ СЕССИЮ */
			%SYSLPUT mvTHREAD_NUM = &mvTHREAD_NUM. / REMOTE = T_&mvTHREAD_NUM.;
			%SYSLPUT mpTarget = &mpTarget. / REMOTE = T_&mvTHREAD_NUM.;
			%SYSLPUT mpId = &mpId. / REMOTE = T_&mvTHREAD_NUM.;
			%SYSLPUT mpAbt = &mpAbt. / REMOTE = T_&mvTHREAD_NUM.;
			%SYSLPUT mpModelTable = &mpModelTable. / REMOTE = T_&mvTHREAD_NUM.;
			%SYSLPUT mpPrefix = &mpPrefix. / REMOTE = T_&mvTHREAD_NUM.;		
		%END;

	%mend signon;
	%signon;
	
	%macro main_run;
	
	/* CALC SCORING WITH THREADS */
	%DO mvTHREAD_NUM = 1 %TO &mvTHREAD_CNT.;
		RSUBMIT T_&mvTHREAD_NUM. WAIT=NO CMACVAR=T_&mvTHREAD_NUM.;
			options notes symbolgen mlogic mprint;
			PROC PRINTTO LOG="/data/logs/log_of_thread_&mvTHREAD_NUM..txt" NEW;
			RUN;
	
			CAS T_&mvTHREAD_NUM. HOST="sasdevinf.ru-central1.internal" PORT=5570;
			caslib _all_ assign;
			%include "/opt/sas/mcd_config/config/initialize_global.sas";
			
			PROC SQL NOPRINT;
				SELECT COUNT(*) AS CNT INTO :mvROW_CNT 
				FROM public.BUFFER_TABLE_&mvTHREAD_NUM.
				;
			QUIT;

		/* MAIN CODE FOR EACH THREAD */
		%MACRO THREAD_MAIN;
		
			%DO ITER = 1 %TO &mvROW_CNT.;
					/* GET PARAMETERS FROM BUFFER TABLE */
					DATA WORK.GET_PARAMS_FOR_THREAD_ITER;
						SET public.BUFFER_TABLE_&mvTHREAD_NUM.(FIRSTOBS = &ITER. OBS = &ITER.);
						CALL SYMPUTX("i", i);
					RUN;
					%PUT &=i;

					/* %let mvIter = %sysfunc(catx(_, &mvHorNum. , .)); */
				
					 /* %macro m_rtp_train(mpTarget=&mpTarget., mpId=&mpId., mpAbt=&mpAbt., mpModelTable=&mpModelTable., mpPrefix=&mpPrefix.); */
				
						%local lmvTabNmAbt lmvLibrefAbt;
						%member_names (mpTable=&mpAbt, mpLibrefNameKey=lmvLibrefAbt, mpMemberNameKey=lmvTabNmAbt);
	
						data _null_;
							set models.&mpModelTable.(where=(n=&i.));
							call symputx('filter', filter);
							call symputx('model', model);
							call symputx('params', params);
							call symputx('interval', interval);
							call symputx('nominal', nominal);
							call symputx('train', train);
						run;
						
						%if &train. %then %do;
							proc casutil incaslib="Models" outcaslib="Models";
								droptable casdata="&mpPrefix._&i." quiet;
							run;
							
							proc forest data=&lmvLibrefAbt..&lmvTabNmAbt.(where=(&filter.))
							  &params.;
							
							  target &mpTarget. / level=interval;
							
							  input &interval. / level=interval;
							  input &nominal. / level=nominal;
							  grow VARIANCE;
							  id &mpId.;
							  savestate rstore=models.&model.;
							run;
							proc casutil incaslib="Models" outcaslib="Models";
								promote casdata="&mpPrefix._&i.";
							run;
						%end;

				/*	%mend m_rtp_train;
					%m_rtp_train; */
				%END;

				PROC PRINTTO;
				RUN;

			%MEND THREAD_MAIN;

			%THREAD_MAIN;

			ENDRSUBMIT;
			
		%END;	

	%mend main_run;
	%main_run;

	%macro end_thread;

		/* CHECKING THREAD COMPLETION */
		%LET mvIN_PROCESS = 1;
		%DO %UNTIL (&mvIN_PROCESS=0);
			%DO CHECK_ITER = 1 %TO &mvTHREAD_CNT.;
				%IF &CHECK_ITER. = 1 %THEN %DO;
					%LET mvIN_PROCESS = &&T_&CHECK_ITER.;
				%END;
				%ELSE %DO;
					%LET mvIN_PROCESS = %SYSEVALF(&mvIN_PROCESS. + &&T_&CHECK_ITER.);
					%LET mvSLEEP=%SYSFUNC(SLEEP(10, 1));
					%PUT TRYING TO GO NEXT STEP;
				%END;
			%END;
		%END;
		
		/*SIGNOFF THREADS AND DROP BATCH TABLES*/
		%DO mvTHREAD_NUM = 1 %TO &mvTHREAD_CNT.;
			SIGNOFF T_&mvTHREAD_NUM. WAIT=yes;
			%PUT SIGNOFF OF THREAD T_&mvTHREAD_NUM. ;

			proc casutil; 
				DROPTABLE CASDATA="BUFFER_TABLE_&mvTHREAD_NUM." INCASLIB="public" QUIET;
			run;
		%END;
	%mend end_thread;
	%end_thread;	

%mend rtp_train_multi;