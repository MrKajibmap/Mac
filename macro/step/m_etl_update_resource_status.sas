%MACRO M_ETL_UPDATE_RESOURCE_STATUS(mvSTATUS, mvRESOURCE);

	%LET mvSTATUS=%sysfunc(upcase(&mvSTATUS.));
	%LET mvRESOURCE=%sysfunc(upcase(&mvRESOURCE.));
	%LET mvPREV_SYSCC=0;
	
	%IF &SYSCC>4 %THEN %DO;
		/* Если произошла ошибка - помечаем статус ресурса E */
		%LET mvSTATUS=E;
		
		%LET mvPREV_SYSCC=&SYSCC.;
		%LET SYSCC=0;
		
		/**/
		OPTIONS NOSYNTAXCHECK OBS=MAX;
		
	%END;
	
	PROC SQL NOPRINT;
		UPDATE etl_cfg.cfg_status_table
		SET
			STATUS_CD="&mvSTATUS."
		WHERE
				/*(UPCASE(STRIP(RESOURCE_NM))=STRIP("&mvRESOURCE.") AND BATCH_CYCLE_ID=(select max(batch_id) from etl_cfg.cfg_batch_etl_cycle)) 
				or*/
				(UPCASE(STRIP(RESOURCE_NM))=STRIP("&mvRESOURCE.") and BATCH_CYCLE_ID = . AND UPCASE(STRIP(STATUS_CD)) ^="C")
		;
	QUIT;
	
	%IF &mvPREV_SYSCC.>0 %THEN %DO;
	
		%LET SYSCC=&mvPREV_SYSCC.;
		
		OPTIONS SYNTAXCHECK OBS=0;
		
	%END;
		
%MEND M_ETL_UPDATE_RESOURCE_STATUS;