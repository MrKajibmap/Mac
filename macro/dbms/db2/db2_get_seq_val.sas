/*****************************************************************
*  ВЕРСИЯ:
*     $Id: b3e7d5bf9fbffe0a9ef0a29690b76e7e0a9f6e13 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Получает значение из сиквенса DB2 и возвращает его в макропеременную
*     Работает в глобальном режиме, внутри PROC SQL.
*
*  ПАРАМЕТРЫ:
*     mpOutKey          +  имя макропеременной, в которую возвращается идентификатор
*     mpSequenceName    +  имя ETL_DBMS sequence для получения очередного идентификатора
*     mpLoginSet        +  По умолчанию ETL_SYS
*
******************************************************************
*  Использует:
*     %db2_connect
*     mpSequenceName
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  30-01-2015  Сазонов        Начальное кодирование
******************************************************************/

%macro db2_get_seq_val (
   mpOutKey                   =  ,
   mpSequenceName             =  ,
   mpLoginSet                 =  ETL_SYS
);
   /* Проверяем корректность параметров */
   %if %is_blank(mpOutKey) %then %do;
     %log4sas_error (dwf.macro.db2_get_seq_val, Incorrect parameter mpOutKey value.);
     %return;
   %end;

   %if %is_blank(mpSequenceName) %then %do;
     %log4sas_error (dwf.macro.db2_get_seq_val, Incorrect parameter mpSequenceName value.);
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
   %let lmvConnection = db2seq%util_recursion;
   %db2_connect (mpLoginSet=&mpLoginSet, mpAlias=&lmvConnection);

   select
    OBJECT_ID into :&mpOutKey
   from connection to &lmvConnection
   (
    select nextval for &mpSequenceName as OBJECT_ID from SYSIBM.SYSDUMMY1
   )
   ;
   disconnect from &lmvConnection
   ;

   %if &lmvIsNotSQL %then %do;
    quit;
   %end;

   /* kinda trim */
   %let &mpOutKey = &&&mpOutKey;
%mend db2_get_seq_val;