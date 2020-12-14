%MACRO M_LOG_EVENT(mpMODE, mpPROCESS_NM);
	%GLOBAL
		mvPROCESS_NM
		mvPROCESS_ID
	;
	%LET mvMODE = %upcase(&mpMODE.);
	%let mvPROCESS_NM = &mpPROCESS_NM.;
	%LET mvPREV_SAS_STATUS=0;
	
	/* ETL */
	%LET mvEVENT_TYPE=ETL;
	
	%LET mvMETA_USERNAME=&SYSUSERID.;
		
	%LET mvPROCESS_ID=&SYSJOBID.; 
	
	%IF &mvMODE=START %THEN %DO;
		
		DATA _NULL_;
			PROCESS_NM="&mvPROCESS_NM.";
			PROCESS_ID="&mvPROCESS_ID."; 
			PROCESS_STATUS="Job started";
			CALL SYMPUTX("mvPROCESS_NM",STRIP(PROCESS_NM));
			CALL SYMPUTX("mvPROCESS_ID",PROCESS_ID); 
			CALL SYMPUTX("mvPROCESS_STATUS",STRIP(PROCESS_STATUS));
		RUN;
		
		PROC SQL NOPRINT;
			CONNECT TO POSTGRES AS CONN (server="10.252.151.3" port=5452 user=etl_cfg password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=etl defer=yes readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=8192");
			EXECUTE BY CONN
				(
					INSERT INTO etl_cfg.cfg_log_event
					VALUES
						(
							DEFAULT,
							%STR(%')&mvPROCESS_NM.%STR(%'),
							&mvPROCESS_ID,
							%STR(%')&mvPROCESS_STATUS.%STR(%'),
							'',
							NOW(),
							NULL,
							NULL,
							'',
							NULL, /*(select max(batch_id) from etl_cfg.cfg_batch_etl_cycle), */
							NULL,
							NULL,
							%STR(%')&mvEVENT_TYPE.%STR(%'),
							%STR(%')&mvMETA_USERNAME.%STR(%')
						)
				);
			DISCONNECT FROM CONN;
		QUIT;
		
		%GLOBAL
			mvPROCESS_NM
			mvPROCESS_ID
			mvLOG_EVENT_ID
			mvPRNT_EVENT_ID
		;
		
		%IF &SYSCC>4 %THEN %DO;
			%LET mvPREV_SAS_STATUS=1;
			OPTIONS NOSYNTAXCHECK OBS=MAX;
		%END;
	
		PROC SQL NOPRINT;
			CONNECT TO POSTGRES AS CONN (server="10.252.151.3" port=5452 user=etl_cfg password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=etl defer=yes readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=8192");
			SELECT EVENT_ID INTO :mvLOG_EVENT_ID SEPARATED BY ''
			FROM CONNECTION TO CONN
				(
					SELECT EVENT_ID
					FROM ETL_CFG.CFG_LOG_EVENT
					WHERE
						PROCESS_NM=%STR(%')&mvPROCESS_NM.%STR(%') AND
						PROCESS_ID=&mvPROCESS_ID. AND
						END_DTTM ISNULL
				);
			DISCONNECT FROM CONN;
			;
		QUIT;
		
		%IF &mvPREV_SAS_STATUS=1 %THEN %DO;
			OPTIONS SYNTAXCHECK OBS=0;
		%END;
		
		%LET mvPRNT_EVENT_ID=&mvLOG_EVENT_ID.;
	
	%END;
	%ELSE %IF &mvMODE.=END %THEN %DO;
		
		%IF &SYSCC>4 %THEN %DO;
			OPTIONS NOSYNTAXCHECK OBS=MAX;
		%END;
	
		%IF &SYSCC>4 %THEN %DO;
				%LET mvPROCESS_STATUS=Job finished with ERROR;
				%LET mvERR_TEXT=%SYSFUNC(COMPRESS(%SYSFUNC(TRANWRD(ERROR: %NRQUOTE(&SYSERRORTEXT.), %STR(,), %STR(:))), %STR(''"")));
				%IF %LENGTH(&mvERR_TEXT)>100 %THEN %DO;
					%LET mvERR_LEN=100;
				%END;
				%ELSE %DO;
					%LET mvERR_LEN=%LENGTH(&mvERR_TEXT);
				%END;
				%LET mvSTATUS_DESCRIPTION=%SUBSTR(%STR(&mvERR_TEXT), 1, &mvERR_LEN);
				%LET mvSTATUS_CD=1;
		%END;
		%ELSE %DO;
			%LET mvPROCESS_STATUS=Job finished SUCCESSFULLY;
			%LET mvSTATUS_DESCRIPTION=;
			%LET mvSTATUS_CD=0;
		%END;
	
		PROC SQL NOPRINT;
			CONNECT TO POSTGRES AS CONN (server="10.252.151.3" port=5452 user=etl_cfg password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=etl defer=yes readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=8192");
			EXECUTE BY CONN
				(
					UPDATE etl_cfg.cfg_log_event
					SET
						process_status=%STR(%')&mvPROCESS_STATUS.%STR(%'),
						status_description=%STR(%')&mvSTATUS_DESCRIPTION.%STR(%'),
						status_cd=&mvSTATUS_CD.,
						end_dttm=now(),
						processed_dt=current_date
					WHERE
						process_nm=%STR(%')&mvPROCESS_NM.%STR(%') and
						process_id=&mvPROCESS_ID. and
						end_dttm isnull AND
						event_id=&mvLOG_EVENT_ID.
				);
			DISCONNECT FROM CONN;
		QUIT;
		
		%IF &SYSCC>4 %THEN %DO;
			
			OPTIONS SYNTAXCHECK OBS=0;
			
		%END;
			
	%END;

%MEND M_LOG_EVENT;