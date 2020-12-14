/*
Пример использования макроса для архивирования.
Режим работы 1 - с использованием таблицы ETL_CFGS.CFG_LOGS_ARCH
%M_ETL_LOG_ARCH();
Режим работы 2 - архивирование заданной директории (PAR)
%M_ETL_LOG_ARCH(/ddpo/SAS_DATA/ETL_LOGS/TEST); 
*/

%MACRO M_ETL_LOG_ARCH(PAR, mvMODE);

	%LET mvMODE=&mvMODE.;
	
	%IF &mvMODE=1 %THEN %DO;
		/* 1.BEGIN. ВЫБОР ЗАПИСЕЙ ИЗ КОНФИГ ТАБЛИЦЫ ПО УСЛОВИЮ */		
		PROC SQL NOPRINT;
			SELECT COUNT(*) INTO :mvCOUNT_GEN
			FROM ETL_CFGS.CFG_LOGS_ARCH
			;
		QUIT;
		%put &=mvcount_gen;
		/* 1.END. */
	
		/* 2.BEGIN. ОБРАБОТКА ЦИКЛОМ ЗАПИСЕЙ ИЗ CFG ТАБЛИЦЫ (ВЫБРАННЫЕ БЛОКИ) */
		%DO J=1 %TO &mvCOUNT_GEN;
			/*2.1.BEGIN. СОЗДАНИЕ МАКРОПЕРЕМЕННЫХ ДЛЯ ВЫБРАННОЙ СТРОКИ ИЗ CFG */
			DATA _NULL_;
				SET ETL_CFGS.CFG_LOGS_ARCH (FIRSTOBS=&J OBS=&J);
				/* CALL SYMPUTX("mvBLOCK_CD", UPCASE(STRIP(BLOCK_CD))); TEST */
				CALL SYMPUTX("mvPATH", PATH);
			RUN;
			/*2.1. END */

			/*2.3.END*/

			/*2.4.BEGIN. ПЕРЕХОД ПО ВЫБРАНОЙ ДИРЕКТОРИИ И ФИЛЬТРАЦИЯ ПО ПРЕФИКСУ mvBLOCK_CD */
			%PUT &=mvPATH.;
			FILENAME MAIN PIPE "ls -dlt &mvPATH.%STR(/)%STR(*)";
			
			DATA FILTER_DS(KEEP=DEPLOY_DIR LOG_NAME LOG_NAME_FULL DATE_CREATED);
				INFILE MAIN;
				FORMAT DATE_CREATED  DATE9.;
				LENGTH
					VAR1 $256
					VAR2  VAR3 VAR4 VAR5 MONTH DAY TIME $32
					DEPLOY_DIR $256
				;
				INPUT VAR1 $ VAR2 $ VAR3 $ VAR4 $ VAR5 $ MONTH $ DAY $ TIME $ DEPLOY_DIR $;
				LOG_NAME=SCAN(SCAN(DEPLOY_DIR, -1, "/"),1,".");
				LOG_NAME_FULL=SCAN(DEPLOY_DIR, -1, "/");
				DATE_NEW=SCAN(SCAN(LOG_NAME,1,"."), -5,"_");
				YEAR = SUBSTR(DATE_NEW,1,4);
				DATE_CREATED = INPUT(STRIP(DAY)||STRIP(MONTH)||STRIP(YEAR), DATE9.);
				/* IF FIND(UPCASE(STRIP(LOG_NAME_FULL)),"&mvBLOCK_CD.") THEN OUTPUT; TEST */
			RUN;
			/*2.4.END*/
		
			/*2.5.BEGIN. ПОДСЧЕТ КОЛИЧЕСТВО СТРОК В ОТФИЛЬТРОВАННОМ НАБОРЕ И ЗАПИСЬ В ПЕРЕМЕННУЮ mvCOUNT */
			PROC SQL NOPRINT;
				SELECT COUNT(*) INTO :mvCOUNT
				FROM WORK.FILTER_DS
				;
			QUIT;
			/*2.5.END*/

			
			%LET mvDIR_TMP = %SCAN(&mvPATH., -1,%STR(/));
			/*%LET mvDIR_GEN = &mvDIR_TMP._&mvBLOCK_CD._&SYSDATE9.; */ /* TEST */
			%LET mvDIR_GEN = &mvDIR_TMP._&SYSDATE9.;
			%PUT &=mvDIR_GEN;
	
			DATA _NULL_;
				CALL SYSTEM("cd &mvPATH.");
				CALL SYSTEM("mkdir &mvDIR_GEN.");
			RUN;

			/*2.6.BEGIN. ОБРАБОТКА ЦИКЛОМ КАЖДОЙ СТРОКИ ИЗ ОТФИЛЬТРОВАННОГО НАБОРА В 2.4. */
			%DO I=1 %TO &mvCOUNT;
				DATA _NULL_;
					SET WORK.FILTER_DS (FIRSTOBS=&I OBS=&I);
					CALL SYMPUTX("mvLOG_NAME", LOG_NAME);
					CALL SYMPUTX("mvLOG_NAME_FULL", LOG_NAME_FULL);
				RUN;
				
				%IF %SYSFUNC(FILEEXIST("&mvPATH./&mvLOG_NAME_FULL.")) %THEN %DO;	
				%PUT &=MVLOG_NAME &=MVBLOCK_CD;
					DATA _NULL_;
							CALL SYSTEM("cd &mvPATH./");
							CALL SYSTEM("mv &mvLOG_NAME_FULL. &mvPATH./&mvDIR_GEN.");
					RUN;
				%END;
				%ELSE %DO;
					%PUT WARNING: FILE "&mvPATH./&mvLOG_NAME." NOT EXIST;
				%END;
			%END;
		
			DATA _NULL_;
				CALL SYSTEM("cd &mvPATH./&mvDIR_GEN.");
				CALL SYSTEM("tar -czvf &mvDIR_GEN..tar.gz *.log"); /* NEW */
				CALL SYSTEM("rm *.log"); /* NEW */
			RUN;
			
			%M_ETL_CLEAR_LIB(WORK,FILTER_DS);
			
			/*2.6.END*/
		%END;
		/*2.END*/
	%END;
	/*END OF mvMODE=0 (BLOCK_CD)*/

	%IF &mvMODE=2 %THEN %DO;
			%LET mvDIRECTORY = &PAR.;
			FILENAME MAIN PIPE "ls -dlt &mvDIRECTORY.%STR(/)%STR(*)";
			
			DATA FILTER_DS(KEEP=LOG_NAME LOG_NAME_FULL DATE_CREATED);
				INFILE MAIN truncover;
				FORMAT DATE_CREATED DATE9.;
				LENGTH
					VAR1 $256
					VAR2  VAR3 VAR4 VAR5 MONTH DAY TIME $32
					DEPLOY_DIR $500
				;
				INPUT VAR1 $ VAR2 $ VAR3 $ VAR4 $ VAR5 $ MONTH $ DAY $ TIME $ DEPLOY_DIR $1-250;
				LOG_NAME=SCAN(SCAN(DEPLOY_DIR, -1, "/"),1,".");
				LOG_NAME_FULL=SCAN(DEPLOY_DIR, -1, "/");
				IF SCAN(LOG_NAME, 1, "_") = "MTLOAD" THEN DO;
						DATE_NEW=SCAN(LOG_NAME, -4,"_");
						YEAR = SUBSTR(DATE_NEW,1,4);
						MONTH = SUBSTR(DATE_NEW,5,2);
						DAY = SUBSTR(DATE_NEW,7,2);
						DATE_CREATED = INPUT(STRIP(DAY)|| '/' || STRIP(MONTH)|| '/' || STRIP(YEAR), DDMMYY10.);

				END;
				ELSE DO;
					DATE_NEW=SCAN(SCAN(LOG_NAME_FULL,1,"."), -3,"_");
					YEAR = SUBSTR(DATE_NEW,1,4);
						MONTH = SUBSTR(DATE_NEW,5,2);
						DAY = SUBSTR(DATE_NEW,7,2);
						DATE_CREATED = INPUT(STRIP(DAY)|| '/' || STRIP(MONTH)|| '/' || STRIP(YEAR), DDMMYY10.);
				END;
				IF (&GL_ETL_TODAY. - DATE_CREATED) > &GL_LOGS_ARCH_THRESHOLD_DAYS. THEN OUTPUT;
				
			RUN;

	/*2.5.BEGIN. ПОДСЧЕТ КОЛИЧЕСТВО СТРОК В ОТФИЛЬТРОВАННОМ НАБОРЕ И ЗАПИСЬ В ПЕРЕМЕННУЮ mvCOUNT */
			PROC SQL NOPRINT;
				SELECT COUNT(*) INTO :mvCOUNT trimmed
				FROM WORK.FILTER_DS
				;
			QUIT;

			/*2.5.END*/

		/* Имя папки */
		/*	%LET mvPATH =/DDPO/sas_data/etl_logs/test;
			%LET mvBLOCK_CD=ETL_FAW_IOW;*/
			%LET mvPATH_TST = &mvDIRECTORY.%STR(/);
			%LET mvDIR_TMP = %SCAN("&mvPATH_TST.", %sysfunc(COUNTC("&mvPATH_TST.",%STR(/))),%STR(/));
			%LET CORR_TIME = %SYSFUNC(DEQUOTE(%SYSFUNC(TRANWRD("&SYSTIME", %STR(:), %STR(_)))));
			%LET mvDIR_GEN = &mvDIR_TMP._&SYSDATE9._&CORR_TIME.;
			%PUT &=mvDIR_GEN &=mvDIR_TMP &=mvPATH_TST;
	
			DATA _NULL_;
				CALL SYSTEM("cd &mvDIRECTORY.");
				CALL SYSTEM("mkdir &mvDIR_GEN.");
			RUN; 
			
/*			2.6.BEGIN. ОБРАБОТКА ЦИКЛОМ КАЖДОЙ СТРОКИ ИЗ ОТФИЛЬТРОВАННОГО НАБОРА В 2.4. */
			%DO I=1 %TO &mvCOUNT;
				DATA _NULL_;
					SET WORK.FILTER_DS (FIRSTOBS=&I OBS=&I);
					CALL SYMPUTX("mvLOG_NAME_FULL", LOG_NAME_FULL);
				RUN;
				
				%IF %SYSFUNC(FILEEXIST("&mvDIRECTORY./&mvLOG_NAME_FULL.")) %THEN %DO;	
					/*
						%LET mvLOG_NAME_FULL = %SYSFUNC(QUOTE(%SYSFUNC(STRIP(&mvLOG_NAME_FULL.))));
						
						%LET mvCOMMAND = mv%str( )&mvLOG_NAME_FULL.%str( )&mvDIRECTORY./&mvDIR_GEN.;
						%put &=mvCOMMAND;
					*/
					DATA _NULL_;
							CALL SYSTEM("cd &mvDIRECTORY./");
							CALL SYSTEM("mv &mvLOG_NAME_FULL. &mvDIRECTORY./&mvDIR_GEN.");
					RUN;
				%END;
				%ELSE %DO;
					%PUT WARNING: FILE "&mvDIRECTORY./&mvLOG_NAME_FULL." NOT EXIST;
				%END;
			%END;
		
			DATA _NULL_;
				CALL SYSTEM("cd &mvDIRECTORY./&mvDIR_GEN.");
				CALL SYSTEM("tar -czvf &mvDIR_GEN..tar.gz *"); 
				CALL SYSTEM("rm *.LOG *.log");  
				CALL SYSTEM("mv &mvDIRECTORY./&mvDIR_GEN. &ETL_LOGS_BKP.");
			RUN;
	%END;
%MEND M_ETL_LOG_ARCH;