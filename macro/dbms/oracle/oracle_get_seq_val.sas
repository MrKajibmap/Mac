/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 04cb9b75ff2ca5923a6812969b65c76d636911fe $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Получает значение из сиквенса Oracle и возвращает его в макропеременную
*     Работает в глобальном режиме, внутри PROC SQL.
*
*  ПАРАМЕТРЫ:
*     mpOutKey          +  имя макропеременной, в которую возвращается идентификатор
*     mpSequenceName    +  имя ETL_DBMS sequence для получения очередного идентификатора
*     mpLoginSet        +  По умолчанию ETL_SYS
*
******************************************************************
*  Использует:
*     %oracle_connect
*     mpSequenceName
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  30-01-2015  Сазонов        Начальное кодирование
******************************************************************/

%macro oracle_get_seq_val (
   mpOutKey                   =  ,
   mpSequenceName             =  ,
   mpLoginSet                 =  ETL_SYS
);
   /* Проверяем корректность параметров */
   %if %is_blank(mpOutKey) %then %do;
     %log4sas_error (dwf.macro.oracle_get_seq_val, Incorrect parameter mpOutKey value.);
     %return;
   %end;

   %if %is_blank(mpSequenceName) %then %do;
     %log4sas_error (dwf.macro.oracle_get_seq_val, Incorrect parameter mpSequenceName value.);
     %return;
   %end;

   /* Проверка среды */
   %local lmvIsNotSQL;
   %let lmvIsNotSQL   = %eval (&SYSPROCNAME ne SQL);

   /* Открываем proc sql, если он еще не открыт */
   %if &lmvIsNotSQL %then %do;
       proc sql noprint;
   %end;
   %else %do;
      reset noprint;
   %end;

   /* Соединяемся через другое подключение, чтобы не мешать внешнему коду, в т.ч. рекурсивному */
   %local lmvConnection;
   %let lmvConnection = oraseq%util_recursion;
   %oracle_connect (mpLoginSet=&mpLoginSet, mpAlias=&lmvConnection);

   select
    OBJECT_ID into :&mpOutKey
   from connection to &lmvConnection
   (
    select &mpSequenceName..nextval as OBJECT_ID from dual
   )
   ;

   /* Сбор ошибок */
    %error_check (mpStepType=SQL_PASS_THROUGH);

   disconnect from &lmvConnection
   ;

   %if &lmvIsNotSQL %then %do;
    quit;
   %end;

   /* kinda trim */
   %let &mpOutKey = &&&mpOutKey;
%mend oracle_get_seq_val;