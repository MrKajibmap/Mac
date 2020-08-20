/*****************************************************************
*  ВЕРСИЯ:
*     $Id: b0c3d3cdeb3334cc278af4105ed6ea742c168402 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Регистрирует начало модуля STP.
*
*  ПАРАМЕТРЫ:
*     нет
*
******************************************************************
*  Использует:
*     %job_start
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     В трансформации transform_job_start.sas
*
******************************************************************
*  22-02-2017  Хорошко     Начальное кодирование
******************************************************************/

%macro stp_job_start;
   /* Получаем имя модуля */
   %local lmvJobName;
   %let lmvJobName=%substr(&_PROGRAM,%length(&_METAFOLDER)+1);
   %let ETL_CURRENT_JOB_ID=;

   proc sql noprint;
      select JOB_ID into :ETL_CURRENT_JOB_ID
      from ETL_SYS.ETL_JOB where
      JOB_NM = "&_SESSIONID";
   quit;

   %log4sas_debug (dwf.macro.stp_job_start, ETL_CURRENT_JOB_ID=&ETL_CURRENT_JOB_ID);
   %log4sas_debug (dwf.macro.stp_job_start, %is_blank(ETL_CURRENT_JOB_ID));

   /* Проверяем, есть ли родительский модуль */
   %if %is_blank(ETL_CURRENT_JOB_ID) %then %do;
      %local lmvStreamName lmvContext;
      %etl_stream_name (mpOutNameKey=lmvStreamName, mpOutContextKey=lmvContext);
      %job_start(mpJobType=STREAM, mpParentJobId=, mpJobName=&lmvStreamName.);
   %end;

   /* Начинаем модуль */
   %job_start(mpJobType=JOB, mpParentJobId=&ETL_CURRENT_JOB_ID, mpJobName=&lmvJobName);
%mend stp_job_start;
