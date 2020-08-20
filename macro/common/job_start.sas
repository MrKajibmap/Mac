/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 770a289c7f1c39bf8dfec8be9ea8b59a508ce87d $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Регистрирует начало ETL-процесса.
*
*  ПАРАМЕТРЫ:
*     mpJobType               +  тип процесса.  Может принимать значения:
*                                * STREAM (процесс загрузки связанных сущностей из одного источника)
*                                * JOB (DI job)
*     mpParentJobId           -  ID объемлющего процесса, в контексте которого запускается данный
*                                Для процессов типа STREAM пустой, для JOB - родительский процесс (в т.ч. STREAM)
*     mpJobName               -  имя процесса.  Может быть неуникальным.
*
******************************************************************
*  Использует:
*     sequence ETL_JOB_SEQ
*     %ETL_DBMS_*
*     %error_check
*     %job_event_reg
*     %unique_id
*
*  Устанавливает макропеременные:
*     <mpJobType>_ID, <mpJobType>_PARENT_ID,
*     <mpJobType>_NAME, <mpJobType>_START_DT[TM]
*     ETL_CURRENT_JOB_ID
*
******************************************************************
*  Пример использования:
*     * начать новый процесс STREAM;
*     %job_start(mpJobType=STREAM, mpParentJobId=, mpJobName=FINANCIAL_ACCOUNT);
*     ... <job 1> ...
*     или
*     * начать новый модуль;
*     %job_start(mpJobType=JOB, mpParentJobId=&ETL_CURRENT_JOB_ID, mpJobName=CHECK_FRP_READY);
*
******************************************************************
*  26-12-2011  Нестерёнок     Начальное кодирование
*  03-04-2012  Нестерёнок     Переход на 2-уровневую схему (STREAM-JOB)
*  30-06-2014  Нестерёнок     Переход на многоуровневую схему
******************************************************************/

%macro job_start (mpJobType=, mpParentJobId=, mpJobName=);
   /* Определение необходимых макропеременных */
   %local lmvJobIdKey lmvJobParentIdKey lmvJobNameKey lmvJobStartDateKey lmvJobStartDttmKey;
   %let lmvJobIdKey        = &mpJobType._ID;
   %let lmvJobParentIdKey  = &mpJobType._PARENT_ID;
   %let lmvJobNameKey      = &mpJobType._NAME;
   %let lmvJobStartDateKey = &mpJobType._START_DT;
   %let lmvJobStartDttmKey = &mpJobType._START_DTTM;

   %global &lmvJobIdKey &lmvJobParentIdKey &lmvJobNameKey &lmvJobStartDateKey &lmvJobStartDttmKey;
   %let &lmvJobIdKey        = ;
   %let &lmvJobParentIdKey  = &mpParentJobId;
   %let &lmvJobNameKey      = &mpJobName;
   %let &lmvJobStartDateKey = ;
   %let &lmvJobStartDttmKey = ;

   %local lmvUserNm;
   %if %symexist(_USER) %then %do;
      %let lmvUserNm = &_USER;
   %end;
   %else %if %symexist(_METAUSER) %then %do;
      %let lmvUserNm = &_METAUSER;
   %end;
   %else %do;
      %let lmvUserNm = &SYSUSERID;
   %end;

   /* Получение ID нового процесса */
   %unique_id (mpOutKey=&lmvJobIdKey, mpSequenceName=ETL_JOB_SEQ);

   /* Запись даты-времени и имени процесса в глобальные макропеременные */
   %let &lmvJobStartDttmKey = %sysfunc(datetime());
   %let &lmvJobStartDateKey = %sysfunc(datepart(&&&lmvJobStartDttmKey));

   /* Получаем информацию о родительском PID (для режима OSS) */
   %local lmvBashProcessID;
   %if %sysfunc(sysexist(BASH_PROCESS_ID)) %then %do;
      %let lmvBashProcessID = %sysfunc(sysget(BASH_PROCESS_ID));
      %if %sysfunc(notdigit(&lmvBashProcessID)) %then
         %let lmvBashProcessID = ;
   %end;   
   
   /* получаем информацию LSF */
   %local lmvLsfFlowId lmvLsfJobId;
   %let lmvLsfFlowId = ;
   %let lmvLsfJobId  = ;
   %let lmvNewLog  = ;
   %if %sysfunc(sysexist(LSB_JOBNAME)) %then %do;
      %let lmvLsfFlowId = %scan(%sysfunc(sysget(LSB_JOBNAME)), 1, :);
      %if %sysfunc(notdigit(&lmvLsfFlowId)) %then
         %let lmvLsfFlowId = ;
   %end;
   %if %sysfunc(sysexist(LSB_JOBID)) %then
      %let lmvLsfJobId = %sysfunc(sysget(LSB_JOBID));

   /* получаем имя лога */
   /* TODO: получить имя лога в случае запуска из-под WSS */
   %local lmvLogPath;
   %let lmvLogPath = %log_location;
   %if %is_blank(lmvLogPath) and %sysfunc(getoption(objectserver)) = OBJECTSERVER %then %do;
      %let lmvLogPath = &SYSPROCESSMODE;
   %end;

   proc sql noprint;
      %&ETL_DBMS._connect(mpLoginSet=ETL_SYS);

      /* Делаем записи о начале процесса */
      %if not %is_blank(lmvLsfJobId) %then %do;
         execute (

%if &ETL_DBMS = postgres %then %do;
            with s as (select
               %&ETL_DBMS._number(&lmvLsfJobId)     lsf_job_id,
               %&ETL_DBMS._number(&lmvLsfFlowId)    parent_flow_id,
               %&ETL_DBMS._string(&LSF_UNKNOWN)     lsf_status_cd
                     )
             insert into LSF_JOB as t (lsf_job_id, parent_flow_id, lsf_status_cd)
            select lsf_job_id, parent_flow_id, lsf_status_cd from s
            on conflict (lsf_job_id) do update set
               t.parent_flow_id  =  excluded.parent_flow_id,
               t.lsf_status_cd   =  excluded.lsf_status_cd
%end;
%else %do;
            merge into LSF_JOB t
            using (select
               %&ETL_DBMS._number(&lmvLsfJobId)     lsf_job_id,
               %&ETL_DBMS._number(&lmvLsfFlowId)    parent_flow_id,
               %&ETL_DBMS._string(&LSF_UNKNOWN)     lsf_status_cd
               from dual
            ) s on (
               t.lsf_job_id = s.lsf_job_id
            )
            when matched then update set
               t.parent_flow_id  =  s.parent_flow_id,
               t.lsf_status_cd   =  s.lsf_status_cd
            when not matched then
                insert (lsf_job_id,    parent_flow_id,      lsf_status_cd)
                values (s.lsf_job_id,  s.parent_flow_id,    s.lsf_status_cd)
%end;
         ) by &ETL_DBMS
         ;
         %error_check (mpStepType=SQL_PASS_THROUGH)
         ;
      %end;
      execute (
         insert into ETL_JOB (job_id, parent_job_id, job_nm, success_flg,
                              start_dttm, job_type_cd, lsf_job_id,
                              host_nm, host_process_id, log_path, user_nm,
                              host_parent_process_id)
         values (
            %&ETL_DBMS._number(&&&lmvJobIdKey), %&ETL_DBMS._number(&&&lmvJobParentIdKey), %&ETL_DBMS._string(&&&lmvJobNameKey), %&ETL_DBMS._string(),
            %&ETL_DBMS._timestamp (&&&lmvJobStartDttmKey), %&ETL_DBMS._string(&mpJobType), %&ETL_DBMS._number(&lmvLsfJobId),
            %&ETL_DBMS._string(&SYSHOSTNAME), %&ETL_DBMS._number(&SYSJOBID), %&ETL_DBMS._string(&lmvLogPath), %&ETL_DBMS._string(&lmvUserNm),
            coalesce(%&ETL_DBMS._number(&lmvBashProcessID), %&ETL_DBMS._number(&SYSJOBID))
         )
      ) by &ETL_DBMS
      ;
      %error_check (mpStepType=SQL_PASS_THROUGH)
      ;
      disconnect from &ETL_DBMS;
   quit;

   /* Проверка системных ошибок */
   %error_check (mpStepType=SQL);

   /* Событие старта процесса или ошибки старта */
   %if not %is_blank(&lmvJobIdKey) %then %do;
      /* Меняем текущий процесс */
      %let ETL_CURRENT_JOB_ID = &&&lmvJobIdKey;

      %job_event_reg (mpEventTypeCode=&mpJobType._STARTED);
   %end;
   %else %do;
      %job_event_reg (mpEventTypeCode=&mpJobType._START_FAILED,
                      mpEventValues= job_type_cd="&mpJobType" job_nm="&mpJobName");
   %end;
%mend job_start;