/*****************************************************************
*  ВЕРСИЯ:
*     $Id: e7763f2a73c01a40ca400d69780c6052a13b9632 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     На шаге PROC SQL осуществляет сбор статистики на таблице в БД Oracle.
*
*  ПАРАМЕТРЫ:
*     mpLoginSet              +  имя набора параметров подключения к БД (ETL_SYS, ETL_STG и т.д.)
*     mpIn                    +  имя таблицы
*
******************************************************************
*  Использует:
*     %error_check
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     нет
*
******************************************************************
*  ПРИМЕР ИСПОЛЬЗОВАНИЯ:
*     %oracle_gather_stats(mpLoginSet=RDB_BASE, mpIn=A01_ACCOUNT)
*
******************************************************************
*  19-01-2017  Сазонов     Начальное кодирование
******************************************************************/

%macro oracle_gather_stats (
   mpLoginSet                 =  ,
   mpIn                       =  ,
   mpCopyStats                = N,
   mpPartitionTo			  =  
);

    %local lmvPOut;

    %if &mpCopyStats = Y %then %do;
		/* получаем имя последней партиции по которой заполнена статистика */
		%&ETL_DBMS._get_last_anlzd_partition(mpLoginSet = &mpLoginSet, mpIn = &mpIn, mpOutPartition = lmvPOut);
    %end;

    %if &mpCopyStats = Y and not %is_blank(lmvPOut) %then %do;
        /* Копируем статистику из другой партиции */
		%&ETL_DBMS._copy_partition_statistics( mpLoginSet = &mpLoginSet, mpIn = &mpIn, mpPartitionFrom = &lmvPOut, mpPartitionTo = &mpPartitionTo );
    %end;
    %else %do;

        %local lmvSchema;
        %if %symexist(&mpLoginSet._CONNECT_SCHEMA) %then %do;
            %let lmvSchema = &&&mpLoginSet._CONNECT_SCHEMA;
        %end;
        %else %do;
            %let lmvSchema = &mpLoginSet;
        %end;

        proc sql noprint feedback;
            %oracle_connect(mpLoginSet=&mpLoginSet);
            
            execute (
             alter session set ddl_lock_timeout = %rand_between(200,300)
            ) by oracle;
            
             execute (
                 BEGIN
                  SYS.DBMS_STATS.SET_TABLE_PREFS (
                    %&ETL_DBMS._string(&lmvSchema),
                    %&ETL_DBMS._string(&mpIn),
                    'INCREMENTAL',
                    'TRUE'
                    );
                END;
             ) by oracle;
            
            execute (
                 BEGIN
                  SYS.DBMS_STATS.GATHER_TABLE_STATS (
                    %&ETL_DBMS._string(&lmvSchema),
                    %&ETL_DBMS._string(&mpIn)
                    );
                END;
             ) by oracle;
            %if &SQLXRC = -8176 %then %do;
                %let SQLXRC=0;
                %let SQLRC=0;
            %end;
            %error_check (mpStepType=SQL_PASS_THROUGH);
        quit;
    %end;

%mend oracle_gather_stats;
