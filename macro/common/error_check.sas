/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 96b1eb74cd6598df095b8d61ae558a8e6de97b62 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Проверяет системные переменные на случай регистрации ими ошибки.
*     Если ошибка обнаружена, инициирует событие и выводит сообщение в лог.
*
*  ПАРАМЕТРЫ:
*     mpStepType           -  тип предыдущей операции, после которой проверяется наличие ошибки:
*                             DATA (по умолчанию) - data step / non-SQL proc step
*                             SQL - proc sql step/stmt w/o pass-through
*                             SQL_PASS_THROUGH - proc sql step/stmt with pass-through
*     mpJobId              -  код процесса, в котором произошло событие,
*                             по умолчанию &ETL_CURRENT_JOB_ID
*     mpEventTypeCode      -  код события, которое будет создано, если обнаружена ошибка
*                             по умолчанию ERROR_CHECK
*
******************************************************************
*  Использует:
*     %error_recovery
*     %job_event_reg
*     %util_recursion
*
*  Устанавливает макропеременные:
*     STEP_RC        - 0, если ошибок не было, или код ошибки (всегда > 0)
*     STEP_MESSAGE   - пусто, если ошибок не было, или текст ошибки
*     ETL_MODULE_RC      - 0, если ошибок (в течение сессии SAS) не было, или код ошибки
*
*  Ограничения:
*     1. В случае использования внутри DIS Loop могут быть проблемы с проверкой SYSRC.
*        При необходимости измените проверку "SYSRC ne 0" на "SYSRC gt 0".
*
******************************************************************
*  Пример использования:
*     * в режиме global statement;
*     data ...
*     run;
*     %error_check;
*     %if &STEP_RC ne 0 %then %do; ...
*
*     * в режиме внутри SQL;
*     proc sql;
*        insert into ...;
*        %error_check (mpStepType=SQL);
*        execute (...) by &ETL_DBMS;
*        %error_check (mpStepType=SQL_PASS_THROUGH);
*     quit;
*
******************************************************************
*  17-01-2012  Нестерёнок     Начальное кодирование
*  29-03-2012  Нестерёнок     Поддержка рекурсивного вызова
*  26-07-2012  Нестерёнок     Защита от бесконечной рекурсии
*  31-08-2012  Нестерёнок     Рефактор mpMode
*  20-09-2012  Нестерёнок     Добавлен mpEventTypeCode
*  22-11-2012  Нестерёнок     KB3595:  Коды SYSDBRC, SQLXRC могут содержать не числа
*  18-11-2013  Нестерёнок     Добавлен SYSRC
*  27-03-2014  Нестерёнок     SQLRC, JOB_RC(9.4) = 4/255 - не ошибки
******************************************************************/

%macro error_check (
   mpStepType        =  DATA,
   mpJobId           =  &ETL_CURRENT_JOB_ID,
   mpEventTypeCode   =  ERROR_CHECK
);
   /* Защита от бесконечной рекурсии */
   %if %util_recursion gt 2 %then %return;

   /* Проверка кода события */
   %local lmvEventTypeId;
   %let lmvEventTypeId = %sysfunc (inputn (&mpEventTypeCode, evtt_cd_id.));
   %if &lmvEventTypeId eq . %then %do;
      %let lmvEventTypeId = -1;
      %job_event_reg (mpEventTypeCode=ILLEGAL_ARGUMENT, mpJobID=&mpJobID, mpLevel=E,
                      mpEventValues= %bquote(mpEventTypeCode="&mpEventTypeCode"));
   %end;

   /* Получение уровня события */
   %local lmvLevel;
   %let lmvLevel = %sysfunc (putn (&lmvEventTypeId, evtt_id_level.));

   /* Макросы проверок */
   %macro syslibrc_set;
      %if %symexist(SYSLIBRC) ne 0 %then %do;
         %if (&SYSLIBRC ne 0) and (%is_blank(STEP_RC)) %then %do;
            %let STEP_RC      = &SYSLIBRC;
            %let STEP_MESSAGE = %sysfunc(sysmsg());
         %end;
      %end;
   %mend syslibrc_set;

   %macro sqlxrc_set;
      %if %symexist(SQLRC) ne 0 %then %do;
        %if (&SQLRC eq 0) %then %return;
     %end;

      %if %symexist(SQLXRC) ne 0 %then %do;
         %if ("&SQLXRC" ne "0") and (%is_blank(STEP_RC)) %then %do;
            %if (%sysfunc(verify(&SQLXRC, -0123456789)) = 0) %then
               %let STEP_RC      = &SQLXRC;
            %else %if %symexist(SQLRC) ne 0 %then
               %let STEP_RC      = &SQLRC;
            %let STEP_MESSAGE = &SQLXMSG;
         %end;
      %end;
   %mend sqlxrc_set;

   %macro sqlrc_set;
      %if %symexist(SQLRC) ne 0 %then %do;
         %if (&SQLRC ne 0) and (&SQLRC ne 4) and (%is_blank(STEP_RC)) %then %do;
            %let STEP_RC      = &SQLRC;
            %let STEP_MESSAGE = &SYSERRORTEXT;
         %end;
      %end;
   %mend sqlrc_set;

   %macro sysdbrc_set;
      %if %symexist(SQLRC) ne 0 %then %do;
        %if (&SQLRC eq 0) %then %return;
     %end;

      %if %symexist(SYSDBRC) ne 0 %then %do;
         %if ("&SYSDBRC" ne "0") and (%is_blank(STEP_RC)) %then %do;
            %if (%sysfunc(verify(&SYSDBRC, -0123456789)) = 0) %then
               %let STEP_RC      = &SYSDBRC;
            %else %if %symexist(SQLRC) ne 0 %then
               %let STEP_RC      = &SQLRC;
            %let STEP_MESSAGE = &SYSDBMSG;
         %end;
      %end;
   %mend sysdbrc_set;

   %macro syserr_set;
      %if (&SYSERR ne 0) and (&SYSERR ne 4) and (%is_blank(STEP_RC)) %then %do;
         %let STEP_RC      = &SYSERR;
         %let STEP_MESSAGE = %sysfunc(sysmsg());
      %end;
   %mend syserr_set;

   %macro sysrc_set;
      /* 1. В случае использования внутри DIS Loop могут быть проблемы с проверкой SYSRC.
       * При необходимости измените проверку "SYSRC ne 0" на "SYSRC gt 0".
      */
      %if (&SYSRC ne 0) and (%is_blank(STEP_RC)) %then %do;
         %let STEP_RC      = &SYSRC;
         %let STEP_MESSAGE = %sysfunc(sysmsg());
      %end;
   %mend sysrc_set;

   %macro diserr_set;
      %if %symexist(JOB_RC) ne 0 %then %do;
         %if (&JOB_RC ne 0) and (&JOB_RC ne 4) and (%is_blank(STEP_RC))
            and not (%util_sasver_ge (mpMajor=9, mpMinor=4) and (&JOB_RC = 255))
         %then %do;
            %let STEP_RC      = &JOB_RC;
            %let STEP_MESSAGE = DIS generated code encountered an error;
         %end;
      %end;
   %mend diserr_set;

   %macro default_set;
      %if (%is_blank(STEP_RC)) %then %do;
         %let STEP_RC      = 0;
         %let STEP_MESSAGE = ;
      %end;
      %else %do;
         /* Для DIS код ошибки д.б. больше 0 */
         %let STEP_RC      = %sysfunc(abs(&STEP_RC));
      %end;
   %mend default_set;

   %global STEP_RC STEP_MESSAGE;

   /* Обнуляем переменные */
   %let STEP_RC = ;
   %let STEP_MESSAGE = ;

   %syslibrc_set;

   %if &mpStepType eq SQL %then %do;
      %sysdbrc_set;
      %sqlrc_set;
   %end;

   %if &mpStepType eq SQL_PASS_THROUGH %then %do;
      %sqlxrc_set;
      %sysdbrc_set;
      %sqlrc_set;
   %end;

   %syserr_set;
   %sysrc_set;
   %diserr_set;

   %default_set;

   /* Обнуляем системные переменные (кроме SYSERR, т.к. R/O) */
   %let SYSLIBRC  = 0;
   %let SYSDBRC   = 0;
   %let SQLRC     = 0;
   %let SQLXRC    = 0;
   %let JOB_RC    = 0;
   %let SYSRC     = 0;

   %if &STEP_RC ne 0 %then %do;
      /* Сохраняем результаты, т.к. job_event_reg может вызвать error_check рекурсивно */
      %local lmvStepRc lmvStepMsg;
      %let lmvStepRc  = &STEP_RC;
      %let lmvStepMsg = &STEP_MESSAGE;

      %if (%etl_level_ge (mpLevel1=&lmvLevel, mpLevel2=E)) and (&STEP_RC gt &ETL_MODULE_RC) %then %do;
         %let ETL_MODULE_RC = &STEP_RC;
         %etl_rcSet(&ETL_MODULE_RC);
      %end;

      /* Восстанавливаемся, если была ошибка */
      %error_recovery;

      /* Создаем событие */
      %log4sas_error (cwf.macro.error_check, &STEP_MESSAGE);
      %job_event_reg (mpEventTypeCode=&mpEventTypeCode, mpJobID=&mpJobID,
                      mpEventValues= %bquote(RC=&STEP_RC MSG=&STEP_MESSAGE));

      /* Восстанавливаем результаты */
      %let STEP_RC      = &lmvStepRc;
      %let STEP_MESSAGE = &lmvStepMsg;
   %end;
%mend error_check;