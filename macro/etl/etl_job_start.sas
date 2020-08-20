/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 0ef6302dfa55c7f1f46df59ae9334c366183f616 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Регистрирует начало модуля ETL.
*
*  ПАРАМЕТРЫ:
*     нет
*
******************************************************************
*  Использует:
*     %etl_job_name
*     %etl_stream_name
*     %is_blank
*     %job_continue
*     %job_event_reg
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
*  11-08-2014  Нестерёнок     Начальное кодирование
*  18-06-2015  Нестерёнок     Поддержка исполнения из-под Loop
******************************************************************/

%macro etl_job_start;
   /* Получаем имя модуля */
   %local lmvJobName;
   %etl_job_name (mpOutNameKey=lmvJobName);

   /* Проверяем исполнение из-под Loop */
   %if %symexist(handleName) %then %do;
      /* Продолжаем внешний модуль - должен быть передан параметр ETL_CURRENT_JOB_ID */
      %if %is_blank(ETL_CURRENT_JOB_ID) %then %do;
         %job_event_reg (mpEventTypeCode=JOB_CONTUNUE_FAILED,
                         mpEventValues= %bquote(job_type_cd="LOOP_JOB" job_nm="&lmvJobName"));
      %end;
   %end;

   /* Проверяем, есть ли родительский модуль */
   %if %is_blank(ETL_CURRENT_JOB_ID) %then %do;
      /* Продолжаем поток (или создаем новый) */
      %local lmvStreamName lmvContext;
      %etl_stream_name (mpOutNameKey=lmvStreamName, mpOutContextKey=lmvContext);

      %job_continue(mpJobType=STREAM, mpPostEvent=N, mpForcedStart=Y);

      /* Для DIS, начинаем поток заново, если старый был начат не сегодня */
      %if (&lmvContext eq DIS or &lmvContext eq STP) and &STREAM_START_DT ne %sysfunc(today()) %then %do;
         %job_start(mpJobType=STREAM, mpParentJobId=, mpJobName=&lmvStreamName.);
      %end;
   %end;

   /* Начинаем модуль */
   %job_start(mpJobType=JOB, mpParentJobId=&ETL_CURRENT_JOB_ID, mpJobName=&lmvJobName);
%mend etl_job_start;
