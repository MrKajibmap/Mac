/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 272d19172e3d6deaff52956c0dd4409b02d29642 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Регистрирует конец ETL-процесса.
*
*  ПАРАМЕТРЫ:
*     mpJobType               -  тип процесса.  Должен быть равен тому, который использовался при старте процесса
*
******************************************************************
*  Использует:
*     %error_check
*     %error_recovery
*     %ETL_DBMS_*
*     %job_event_reg
*
*  Устанавливает макропеременные:
*     ETL_CURRENT_JOB_ID
*     ETL_MODULE_RC
*     SYSCC
*
******************************************************************
*  Пример использования:
*     * начать новый процесс STREAM;
*     %job_start(mpJobType=STREAM, mpParentJobId=, mpJobName=FINANCIAL_ACCOUNT);
*     ...
*     * завершить процесс STREAM;
*     %job_finish(mpJobType=STREAM);
*
******************************************************************
*  27-12-2011  Нестерёнок     Начальное кодирование
*  07-07-2014  Нестерёнок     Убран mpJobRc
*  09-02-2015  Сазонов        Иерархический запрос переписан на рекурсивный для db2
******************************************************************/

%macro job_finish (
   mpJobType            =
);
   /* Восстанавливаемся, если была ошибка */
   %error_recovery;

   /* Проверка системных ошибок */
   %error_check (mpStepType=DATA);

   /* Определение необходимых макропеременных */
   %local lmvJobIdKey lmvJobParentIdKey lmvJobStartDttmKey lmvJobEndDttmKey;
   %let lmvJobIdKey        = &mpJobType._ID;
   %let lmvJobParentIdKey  = &mpJobType._PARENT_ID;
   %let lmvJobStartDttmKey = &mpJobType._START_DTTM;
   %let lmvJobEndDttmKey   = &mpJobType._END_DTTM;

   %let &lmvJobEndDttmKey  = %sysfunc(datetime());

   /* Обработка событий процесса */
   %local lmvMaxErrorLevel lmvJobStatusInd lmvJobErrorText;
   proc sql noprint;
      %&ETL_DBMS._connect(mpLoginSet=ETL_SYS);

      %if &ETL_FIX_ORPHANED_EVENTS %then %do;
         /* Для событий, не отнесенных ни к какому процессу, но произошедших в течение
            времени его выполнения проставляем этот процесс */
         execute (
            update ETL_JOB_EVENT set
               job_id  = &&&lmvJobIdKey
            where
               job_id is null
               and event_dttm between %&ETL_DBMS._timestamp (&&&lmvJobStartDttmKey)
                              and %&ETL_DBMS._timestamp (&&&lmvJobEndDttmKey)
         ) by &ETL_DBMS
         ;
      %end;

      /* Ищем уровень наихудшего события для этого процесса или его подчиненных */
      select
         level_cd,
         event_type_desc
      into
         :lmvMaxErrorLevel,
         :lmvJobErrorText
      from connection to &ETL_DBMS (
         with
%if &ETL_DBMS = postgres %then %do;
            recursive
%end;
         all_runs (job_id, parent_job_id, job_nm, start_dttm) as (
            select job_id, parent_job_id, job_nm, start_dttm from ETL_JOB
            where job_id = &&&lmvJobIdKey
            union all
            select a.job_id, a.parent_job_id, a.job_nm, a.start_dttm
            from ETL_JOB a, all_runs r
            where a.parent_job_id = r.job_id
         )
         select
            evt.level_cd,
            evtt.event_type_desc
         from
            (
               select j1.* from all_runs j1
               /* исключаем перезапускавшиеся джобы */
               inner join
                  (select job_nm, max(start_dttm) start_dttm from all_runs group by job_nm) j2
               on j1.job_nm = j2.job_nm and j1.start_dttm = j2.start_dttm
            ) job,
            ETL_JOB_EVENT evt,
            ETL_LEVEL lvl,
            ETL_EVENT_TYPE evtt
         where
            evt.job_id = job.job_id and
            evt.level_cd = lvl.level_cd and
            evt.event_type_id = evtt.event_type_id
         order by
            level_wgt desc, event_dttm
      );
      %if %is_blank(lmvMaxErrorLevel) %then %do;
         %let lmvMaxErrorLevel = I;
      %end;

      /* Если нет ошибок, считаем успех */
      %if not %etl_level_ge (mpLevel1=&lmvMaxErrorLevel, mpLevel2=E) %then %do;
         %let lmvJobStatusInd = Y;
         %let lmvJobErrorText = NO ERROR;
      %end;
      %else %do;
         %let lmvJobStatusInd = N;

         %if %is_blank(lmvJobErrorText) %then %do;
           %let lmvJobErrorText = Unknown error;
         %end;
      %end;

      /* Дописываем в ETL_JOB */
      execute (
         update ETL_JOB set
            success_flg = %&ETL_DBMS._string (&lmvJobStatusInd),
            error_desc  = %&ETL_DBMS._string (%superq(lmvJobErrorText)),
            end_dttm    = %&ETL_DBMS._timestamp (&&&lmvJobEndDttmKey)
         where
            job_id = &&&lmvJobIdKey
      ) by &ETL_DBMS
      ;
      execute (commit) by &ETL_DBMS
      ;
      disconnect from &ETL_DBMS
      ;
   quit;

   /* Устанавливаем код возврата в ОС */
   %if &lmvJobStatusInd eq Y and &ETL_MODULE_RC eq 0 %then %do;
      %let SYSCC = 0;
   %end;
   %else %do;
      %let SYSCC = 255;
      %if &ETL_MODULE_RC eq 0 %then
         %let ETL_MODULE_RC = 255;;
   %end;

   /* Событие завершения процесса */
   %if &lmvJobStatusInd eq Y %then %do;
      %job_event_reg (mpEventTypeCode=&mpJobType._FINISHED, mpJobID=&&&lmvJobIdKey);
   %end;
   %else %do;
      %job_event_reg (mpEventTypeCode=&mpJobType._FINISH_FAILED, mpJobID=&&&lmvJobIdKey);
   %end;

   /* Меняем текущий процесс */
   %let ETL_CURRENT_JOB_ID = &&&lmvJobParentIdKey;

%mend job_finish;
