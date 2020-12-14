%macro rtp_score_multi(mpThreadCnt=10,
							mpModelTable=PMIX_MODEL_TABLE,
							mpId = product_id pbo_location_id sales_dt,
							mpTarget =sum_qty,
							mpAbt = dm_abt.all_ml_scoring,
							mpPrefix = FOREST,
							mpStart = 1,
							mpOut = casuser.pmix_score);
		
		%if %sysfunc(SESSFOUND(casauto)) = 0 %then %do; 
			cas casauto;
			caslib _all_ assign;
		%end;

		data _null_;
			set models.PMIX_MODEL_TABLE nobs=nobs;
			call symputx('length', nobs, 'G');
			stop;
		run;
		%put &=length;
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
				%SYSLPUT mpId = &mpId. / REMOTE = T_&mvTHREAD_NUM.;
				%SYSLPUT mpAbt = &mpAbt. / REMOTE = T_&mvTHREAD_NUM.;
				%SYSLPUT mpModelTable = &mpModelTable. / REMOTE = T_&mvTHREAD_NUM.;
				%SYSLPUT mpOut = &mpOut. / REMOTE = T_&mvTHREAD_NUM.;		
			%END;

		%mend signon;
		%signon;
		
		%macro main_run;
		
			/* CALC SCORING WITH THREADS */
			%DO mvTHREAD_NUM = 1 %TO &mvTHREAD_CNT.;
				RSUBMIT T_&mvTHREAD_NUM. WAIT=NO CMACVAR=T_&mvTHREAD_NUM.;
					options notes symbolgen mlogic mprint;

					PROC PRINTTO LOG="/data/logs/log_score_thread_&mvTHREAD_NUM..txt" NEW;
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
				
							%local lmvTabNmAbt lmvLibrefAbt lmvLibrefOut lmvTabNmOut;
							%member_names (mpTable=&mpAbt, mpLibrefNameKey=lmvLibrefAbt, mpMemberNameKey=lmvTabNmAbt);

							data _null_;
								set models.&mpModelTable.(where=(n=&i.));
								call symputx('filter', filter);
								call symputx('model', model);
								call symputx('score', score);
							run;
							
							%if &score. %then %do;
								data casuser.scoring;
									set &lmvLibrefAbt..&lmvTabNmAbt.(where=(&filter.));
								run;
								
								proc astore;
								  score data=casuser.scoring
								  rstore=models.&model.
								  out=casuser.mltfc_&lmvTabNmAbt.&i. copyvars=(channel_cd prod_lvl4_id &mpId.);
								quit;
								
								/* promote to public for additional calcs */
								proc casutil;  
									promote casdata="mltfc_&lmvTabNmAbt.&i." casout="mltfc_&lmvTabNmAbt&i." incaslib="casuser" outcaslib="public";
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

		%macro end_thread/*(mpOut=&mpOut.)*/;
		
			%local lmvTabNmAbt lmvLibrefAbt lmvLibrefOut lmvTabNmOut;
			%member_names (mpTable=&mpAbt, mpLibrefNameKey=lmvLibrefAbt, mpMemberNameKey=lmvTabNmAbt);
			%member_names (mpTable=&mpOut, mpLibrefNameKey=lmvLibrefOut, mpMemberNameKey=lmvTabNmOut);
			
			/* CHECKING THREAD COMPLETION */
			%LET mvIN_PROCESS = 1;
			%DO %UNTIL (&mvIN_PROCESS=0);
				%DO CHECK_ITER = 1 %TO &mvTHREAD_CNT.;
					%IF &CHECK_ITER. = 1 %THEN %DO;
						%LET mvIN_PROCESS = &&T_&CHECK_ITER.;
					%END;
					%ELSE %DO;
						%LET mvIN_PROCESS = %SYSEVALF(&mvIN_PROCESS. + &&T_&CHECK_ITER.);
						%LET mvSLEEP=%SYSFUNC(SLEEP(100, 1));
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
			
			/* delete target table */	
			proc casutil ;
				droptable casdata="&lmvTabNmOut." incaslib="&lmvLibrefOut." quiet;
			run;
			/* collect tables from threads */
			data casuser.score_res(replace=yes) ;
				set public.mltfc_&lmvTabNmAbt.:;
			run;

			proc casutil;  
				promote casdata="score_res" casout="&lmvTabNmOut." incaslib="casuser" outcaslib="&lmvLibrefOut.";
				save incaslib="&lmvLibrefOut." outcaslib="&lmvLibrefOut." casdata="&lmvTabNmOut." casout="&lmvTabNmOut..sashdat" replace;
			run;
			/* delete threads output multiforecast tables from public*/
			%do i=1 %to &mpEnd.;
				proc casutil ;
					droptable casdata="mltfc_&lmvTabNmAbt.&i." incaslib="public" quiet;
				run;
			%end;
			
		%mend end_thread;
		%end_thread/*(mpOut=&mpOut.)*/;	
%mend rtp_score_multi;