/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 14b5b0bd616dd39fa377f2d36320483caae89b1e $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Получает ранее созданную песочницу для ETL потока.
*
*  ПАРАМЕТРЫ:
*
******************************************************************
*  Использует:
*     %oracle_connect
*     %etl_get_stream_id
*    %_get_sandbox из конфига расчета (initialize_ifrs_calc.sas)
*
*  Устанавливает макропеременные:
*
******************************************************************
*  Пример использования:
*     %etl_get_sandbox;
*
******************************************************************
*  15-01-2019  Задояный    Начальное кодирование
******************************************************************/

%macro etl_get_sandbox;

   /* Инициализация */
   %local lmvSandboxFolder lmvIFRSStreamID;

   /* Получаем ID потока (TODO: Сделать параметр с job_id для задач отладки) */
   %etl_get_stream_id(mpJobID=&JOB_ID., mpOutStreamID=lmvIFRSStreamID);
   %put &=lmvIFRSStreamID;

   /* Получаем путь к песочнице */
   proc sql noprint;

      /* Соединяемся через другое подключение, чтобы не мешать внешнему коду, в т.ч. рекурсивному */
      %local lmvConnection;
      %let lmvConnection = etlbox%util_recursion;
      %oracle_connect (mpLoginSet=ETL_SYS, mpAlias=&lmvConnection);

      select
         SANDBOX_PATH into :lmvSandboxFolder
         from connection to &lmvConnection
         (
            select SANDBOX_PATH from etl_sys.etl_flow_sandbox where STREAM_ID = %&ETL_DBMS._number(&lmvIFRSStreamID.)
         )
      ;
      disconnect from &lmvConnection
      ;
   quit;
   %error_check;

   %if %is_blank(lmvSandboxFolder) %then %do;
      %error_check;
   %end;
   %else %do;
      %let lmvSandboxFolder = %sysfunc(strip(&lmvSandboxFolder));
      %put &=lmvSandboxFolder;

      /* Инициализация песочницы */
      %_get_sandbox;

   %end;

%mend etl_get_sandbox;