/*****************************************************************
*  ВЕРСИЯ:
*     $Id: cd9e40122651084b4050aff039d9b36b76b03ef0 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Регистрирует продолжение ETL-процесса.
*
*  ПАРАМЕТРЫ:
*     mpJobType               +  тип процесса.  Может принимать значения:
*                                * STREAM (процесс загрузки связанных сущностей из одного источника)
*                                * JOB (DI job)
*     mpPostEvent             -  регистрировать ли событие (ошибки) продолжения процесса.
*                                Для внутренних нужд.
*     mpForcedStart           -  запускать ли процесс, если продолжить не удалось.
*                                По умолчанию N.
*
******************************************************************
*  Использует:
*     %job_event_reg
*     %error_check
*
*  Устанавливает макропеременные:
*     <mpJobType>_ID, <mpJobType>_PARENT_ID,
*     <mpJobType>_NAME, <mpJobType>_START_DT[TM]
*     ETL_CURRENT_JOB_ID
*
******************************************************************
*  Пример использования:
*     * продолжить процесс STREAM;
*     %job_continue(mpJobType=STREAM);
*     ... <job 1> ...
*
******************************************************************
*  26-12-2011  Нестерёнок     Начальное кодирование
*  03-04-2012  Нестерёнок     Переход на 2-уровневую схему (STREAM-JOB)
*  16-02-2017  Нестерёнок     Поддержка параллельных одноименных джобов
******************************************************************/

%macro job_continue (
   mpJobType            =  ,
   mpPostEvent          =  Y,
   mpForcedStart        =  N
);
   /* Определение необходимых макропеременных */
   %local lmvJobIdKey lmvJobParentIdKey lmvJobNameKey lmvJobStartDateKey lmvJobStartDttmKey;
   %let lmvJobIdKey        = &mpJobType._ID;
   %let lmvJobParentIdKey  = &mpJobType._PARENT_ID;
   %let lmvJobNameKey      = &mpJobType._NAME;
   %let lmvJobStartDateKey = &mpJobType._START_DT;
   %let lmvJobStartDttmKey = &mpJobType._START_DTTM;

   %global &lmvJobIdKey &lmvJobParentIdKey &lmvJobNameKey &lmvJobStartDateKey &lmvJobStartDttmKey;
   %let &lmvJobIdKey        = ;
   %let &lmvJobParentIdKey  = ;
   %let &lmvJobNameKey      = ;
   %let &lmvJobStartDateKey = ;
   %let &lmvJobStartDttmKey = ;

   /* Получение ID ранее начатого процесса */
   %local lmvJobName lmvContextId lmvContext;
   %etl_stream_name (mpOutNameKey=lmvJobName, mpOutIdKey=lmvContextId, mpOutContextKey=lmvContext);
   %if &mpJobType = JOB %then %do;
	  %if &lmvContext = STP %then %do;
		%let lmvJobName=%substr(&_PROGRAM,%length(&_METAFOLDER)+1);
	  %end;
	  %else %do;
		%etl_job_name (mpOutNameKey=lmvJobName);
   %end;     
   %end;
   
   proc sql noprint;
      select
         cats(j.job_id),
         cats(j.parent_job_id),
         j.job_nm,
         cats(datepart(j.start_dttm)),
         cats(j.start_dttm)
      into
         :&lmvJobIdKey,
         :&lmvJobParentIdKey,
         :&lmvJobNameKey,
         :&lmvJobStartDateKey,
         :&lmvJobStartDttmKey
      from
         ETL_SYS.ETL_JOB j left join ETL_SYS.LSF_JOB l 
            on j.lsf_job_id = l.lsf_job_id
      where
         j.job_type_cd = "&mpJobType"
         and missing(j.end_dttm)
         and j.job_nm = "&lmvJobName"
%if &lmvContext = LSF %then %do;
         and l.parent_flow_id = &lmvContextId
%end;
%else %do;
         and j.host_parent_process_id = &lmvContextId
         /*and j.host_process_id = &lmvContextId*/
%end;
      order by
         start_dttm desc
      ;
   quit;
   /* Проверка системных ошибок */
   %error_check (mpStepType=SQL);

   /* Событие продолжения или ошибки продолжения */
   %if not %is_blank(&lmvJobIdKey) %then %do;
      /* Меняем текущий процесс */
      %let ETL_CURRENT_JOB_ID = &&&lmvJobIdKey;

      %if &mpPostEvent = Y %then %do;
         %job_event_reg (mpEventTypeCode=&mpJobType._CONTINUED);
      %end;
   %end;
   %else %do;
      %if &mpPostEvent = Y %then %do;
         %job_event_reg (mpEventTypeCode=&mpJobType._CONTUNUE_FAILED,
                         mpEventValues= %bquote(job_type_cd="&mpJobType" job_nm="&lmvJobName"));
      %end;

      %if &mpForcedStart eq Y %then %do;
         /* Открываем новый процесс */
         %job_start (mpJobType=&mpJobType, mpParentJobId=, mpJobName=&lmvJobName);
      %end;
   %end;
%mend job_continue;