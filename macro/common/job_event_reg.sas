/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 48972561b80ece8f38e5801f31b1c517a8f3c1f0 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Регистрирует событие.
*     Если событие - фатальная ошибка, то немедленно завершает процесс.
*     Работает в глобальном режиме, внутри PROC SQL, или внутри DATA STEP.
*
*  ПАРАМЕТРЫ:
*     mpEventTypeCode         -  мнемокод типа события
*                                по умолчанию UNKNOWN_EVENT
*     mpJobId                 -  код процесса, в котором произошло событие,
*                                по умолчанию &ETL_CURRENT_JOB_ID
*     mpLevel                 -  уровень события
*                                по умолчанию соответствует событию
*     mpEventDesc             -  описание события
*                                по умолчанию соответствует событию
*     mpEventValues           -  значения, связанные с событием (например, значение нарушенного FK)
*
******************************************************************
*  Использует:
*     ETL_MODULE_RC
*     %ETL_DBMS_*
*     %etl_stop
*     %unique_id
*     sequence ETL_EVENT_SEQ
*
*  Устанавливает макропеременные:
*     ETL_MODULE_RC (в случае регистрации ошибки)
*
*  Ограничения:
*     В режиме внутри DATA STEP событие протоколируется только в лог.
*
******************************************************************
*  Пример использования:
   * в режиме global statement;
      * событие - начат новый процесс загрузки;
      %job_event_reg (mpEventTypeCode=ETL_STARTED);
   * в режиме внутри SQL;
      proc sql;
         insert into my_table values (1, 2, 3);
         %job_event_reg (mpEventTypeCode=TABLE_UPDATED, mpEventValues=my_table);
      quit;
   * в режиме внутри DATA STEP;
      data my_table;
         if a > b then do;
            %job_event_reg (mpEventTypeCode=TABLE_UPDATED, mpEventValues=my_table);
         end;
      run;
*
******************************************************************
*  17-01-2011  Нестерёнок     Начальное кодирование
*  31-08-2012  Нестерёнок     Рефактор mpMode
*  15-04-2014  Нестерёнок     Добавлен режим внутри DATA STEP
*  23-07-2014  Нестерёнок     В случае регистрации ошибки устанавливает ETL_MODULE_RC
******************************************************************/

%macro job_event_reg (mpEventTypeCode=, mpJobId=&ETL_CURRENT_JOB_ID, mpLevel=, mpEventDesc=, mpEventValues=);
   /* Проверка среды */
   %local lmvIsNotSQL lmvIsDataStep;
   %let lmvIsNotSQL   = %eval (&SYSPROCNAME ne SQL);
   %let lmvIsDataStep = %eval (&SYSPROCNAME eq DATASTEP);

   /* Разбор параметров */
   %local lmvEventId lmvJobId lmvEventTypeId lmvLevel lmvEventDttm lmvEventDesc;

   /* Получение имен переменных */
   %if &lmvIsDataStep %then %do;
      %local lmvUID;
      %unique_id (mpOutKey=lmvUID);

      %local lmvEventIdField lmvJobIdField lmvEventTypeIdField lmvLevelField lmvEventDttmField lmvEventDescField lmvLogLevel;
      %let lmvEventIdField       =  job_event_id_&lmvUID;
      %let lmvJobIdField         =  job_id_&lmvUID;
      %let lmvEventTypeIdField   =  event_type_id_&lmvUID;
      %let lmvLevelField         =  level_cd_&lmvUID;
      %let lmvEventDttmField     =  event_dttm_&lmvUID;
      %let lmvEventDescField     =  event_desc_&lmvUID;
      %let lmvLogLevel           =  log_level_&lmvUID;

      drop &lmvEventIdField &lmvJobIdField &lmvEventTypeIdField &lmvLevelField &lmvEventDttmField &lmvEventDescField &lmvLogLevel;
   %end;

   /* Получение времени события */
   %if &lmvIsDataStep %then %do;
      &lmvEventDttmField = datetime();
   %end;
   %else %do;
      %let lmvEventDttm = %sysfunc(datetime());
   %end;

   /* Получение ID типа события */
   %if &lmvIsDataStep %then %do;
      &lmvEventTypeIdField = input (&mpEventTypeCode, ?evtt_cd_id.);
      if missing(&lmvEventTypeIdField) then &lmvEventTypeIdField = -1;
   %end;
   %else %do;
      %if %is_blank(mpEventTypeCode) %then
         %let mpEventTypeCode = UNKNOWN_EVENT;
      %let lmvEventTypeId = %sysfunc (inputn (&mpEventTypeCode, evtt_cd_id.));
      %if &lmvEventTypeId eq . %then
         %let lmvEventTypeId = -1;
   %end;

   /* Получение уровня события */
   /* по умолчанию берется уровень типа события */
   %let lmvLevel = &mpLevel;
   %if &lmvIsDataStep %then %do;
      %if not %is_blank(lmvLevel) %then %do;
         if not missing(&mpLevel) then
            &lmvLevelField = &mpLevel;
         else
      %end;
            &lmvLevelField = put (&lmvEventTypeIdField, evtt_id_level.);
   %end;
   %else %do;
      %if %is_blank(lmvLevel) %then %do;
         %let lmvLevel = %sysfunc (putn (&lmvEventTypeId, evtt_id_level.));
      %end;

      /* Если событие - отладочная информация, то оно создается только в режиме отладки */
      %if &lmvLevel eq D and not &ETL_DEBUG %then %return;
   %end;

   /* Получение текста события */
   %let lmvEventDesc = &mpEventDesc;
   %if &lmvIsDataStep %then %do;
      %if not %is_blank(lmvEventDesc) %then %do;
         if not missing(&mpEventDesc) then
            &lmvEventDescField = &mpEventDesc;
         else
      %end;
            &lmvEventDescField = put (&lmvEventTypeIdField, evtt_id_desc.);
   %end;
   %else %do;
      %if %is_blank(lmvEventDesc) %then %do;
         %let lmvEventDesc = %sysfunc (putn (&lmvEventTypeId, evtt_id_desc.));
      %end;
   %end;

   /* Получение ID события */
   %let lmvEventId =;
   %if &lmvIsDataStep %then %do;
      %unique_id(mpOutKey=lmvEventId);
      retain &lmvEventIdField &lmvEventId;
      &lmvEventIdField + 1;
   %end;
   %else %do;
      %unique_id(mpOutKey=lmvEventId, mpSequenceName=ETL_EVENT_SEQ, mpLoginSet=ETL_SYS);
      %if %is_blank(lmvEventId) %then %do;
         %log4sas_error (cwf.macro.job_event_reg, Cannot get new event id.);
         %return;
      %end;
   %end;

   /* Получение ID процесса */
   %let lmvJobId = &mpJobId;
   %if &lmvIsDataStep %then %do;
      %if not %is_blank(lmvJobId) %then %do;
         &lmvJobIdField = &lmvJobId;
      %end;
      %else %do;
         call missing (&lmvJobIdField);
      %end;
   %end;

   /* Делаем запись о событии */
   %if &lmvIsDataStep %then %do;
      /* Если событие - отладочная информация, то оно создается только в режиме отладки */
      if &lmvLevelField ne "D" then do;
         /* Регистрация в логе */
         select (&lmvLevelField);
            when ("F")
               &lmvLogLevel = "FATAL";
            when ("E")
               &lmvLogLevel = "ERROR";
            when ("W")
               &lmvLogLevel = "WARN";
            when ("D")
               &lmvLogLevel = "DEBUG";
            otherwise
               &lmvLogLevel = "INFO";
         end;

         rc = log4sas_logevent ("cwf.macro.job_event_reg", &lmvLogLevel, cat ("job_event_id       =  ", &lmvEventIdField) );
         rc = log4sas_logevent ("cwf.macro.job_event_reg", &lmvLogLevel, cat ("job_id             =  ", &lmvJobIdField) );
         rc = log4sas_logevent ("cwf.macro.job_event_reg", &lmvLogLevel, cat ("event_type_id      =  ", &lmvEventTypeIdField) );
         rc = log4sas_logevent ("cwf.macro.job_event_reg", &lmvLogLevel, cat ("level_cd           =  ", &lmvLevelField) );
         rc = log4sas_logevent ("cwf.macro.job_event_reg", &lmvLogLevel, cat ("event_dttm         =  ", put (&lmvEventDttmField, datetime19.)) );
         rc = log4sas_logevent ("cwf.macro.job_event_reg", &lmvLogLevel, cat ("event_desc         =  ", &lmvEventDescField) );
         rc = log4sas_logevent ("cwf.macro.job_event_reg", &lmvLogLevel, cat ("event_values_txt   =  ", &mpEventValues) );

         /* Если событие - ошибка, то устанавливаем ETL_MODULE_RC */
%if &ETL_MODULE_RC = 0 %then %do;
         if &lmvLevelField in ("E", "F") then do;
            call symputx ("ETL_MODULE_RC", input (&lmvLevelField, lvl_cd_wgt.));
         end;
%end;
         /* Если событие - фатальная ошибка, то процесс завершается немедленно (не для STP) */
         if &lmvLevelField = "F" then do;
            %etl_stop;
         end;
      end;
   %end;
   %else %do;
      /* Регистрация в таблице событий */
      /* Открываем proc sql, если он еще не открыт */
      %if &lmvIsNotSQL %then %do;
         proc sql;
      %end;
         /* Соединяемся через другое подключение, чтобы не мешать внешнему коду */
         %&ETL_DBMS._connect(mpLoginSet=ETL_SYS, mpAlias=etlevt);

         execute (
            insert into ETL_JOB_EVENT (job_event_id, job_id, event_type_id, level_cd,
                                 event_dttm, event_desc, event_values_txt)
            values (
               %&ETL_DBMS._number(&lmvEventId), %&ETL_DBMS._number(&lmvJobId), %&ETL_DBMS._number(&lmvEventTypeId), %&ETL_DBMS._string(&lmvLevel),
               %&ETL_DBMS._timestamp (&lmvEventDttm), %&ETL_DBMS._string(&lmvEventDesc), %&ETL_DBMS._string(&mpEventValues)
            )
         ) by etlevt
         ;
         execute (commit) by etlevt
         ;
         disconnect from etlevt
         ;

      %if &lmvIsNotSQL %then %do;
         quit;
      %end;

      /* Регистрация в логе */
      %local lmvLogLevel;
      %if &lmvLevel eq F %then
         %let lmvLogLevel = FATAL;
      %else %if &lmvLevel eq E %then
         %let lmvLogLevel = ERROR;
      %else %if &lmvLevel eq W %then
         %let lmvLogLevel = WARN;
      %else %if &lmvLevel eq D %then
         %let lmvLogLevel = DEBUG;
      %else
         %let lmvLogLevel = INFO;
      ;

      %log4sas_logevent (cwf.macro.job_event_reg, &lmvLogLevel, %bquote(job_event_id      =  &lmvEventId)      );
      %log4sas_logevent (cwf.macro.job_event_reg, &lmvLogLevel, %bquote(job_id            =  &lmvJobId)        );
      %log4sas_logevent (cwf.macro.job_event_reg, &lmvLogLevel, %bquote(event_type_id     =  &lmvEventTypeId)  );
      %log4sas_logevent (cwf.macro.job_event_reg, &lmvLogLevel, %bquote(level_cd          =  &lmvLevel)        );
      %log4sas_logevent (cwf.macro.job_event_reg, &lmvLogLevel, %bquote(event_dttm        =  %sysfunc(putn(&lmvEventDttm, datetime19.))) );
      %log4sas_logevent (cwf.macro.job_event_reg, &lmvLogLevel, %bquote(event_desc        =  &lmvEventDesc)    );
      %log4sas_logevent (cwf.macro.job_event_reg, &lmvLogLevel, %bquote(event_values_txt  =  &mpEventValues)   );

      /* Если событие - ошибка, то устанавливаем ETL_MODULE_RC */
      %if (&ETL_MODULE_RC = 0) and %etl_level_ge (mpLevel1=&lmvLevel, mpLevel2=E) %then
         %let ETL_MODULE_RC = %sysfunc (inputn (&lmvLevel, lvl_cd_wgt.));

      /* Если событие - фатальная ошибка, то процесс завершается немедленно (не для STP) */
      %if &lmvLevel eq F %then %etl_stop;
   %end;
%mend job_event_reg;
