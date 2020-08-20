/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 5a4abc43c36060ec91d41c441220f3e21893c342 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Получает значение из сиквенса Postgres и возвращает его в макропеременную
*     Работает в глобальном режиме, внутри PROC SQL.
*
*  ПАРАМЕТРЫ:
*     mpOutKey          +  имя макропеременной, в которую возвращается идентификатор
*     mpSequenceName    +  имя ETL_DBMS sequence для получения очередного идентификатора
*     mpLoginSet        +  По умолчанию ETL_SYS
*
******************************************************************
*  Использует:
*     %postgres_connect
*     %postgres_string
*     mpSequenceName
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  31-03-2017  Задояный       Начальное кодирование
******************************************************************/

%macro postgres_get_seq_val (
   mpOutKey                   =  ,
   mpSequenceName             =  ,
   mpLoginSet                 =  ETL_SYS
);
   /* Проверяем корректность параметров */
   %if %is_blank(mpOutKey) %then %do;
     %log4sas_error (dwf.macro.postgres_get_seq_val, Incorrect parameter mpOutKey value.);
     %return;
   %end;

   %if %is_blank(mpSequenceName) %then %do;
     %log4sas_error (dwf.macro.postgres_get_seq_val, Incorrect parameter mpSequenceName value.);
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
   %let lmvConnection = pgseq%util_recursion;
   %postgres_connect (mpLoginSet=&mpLoginSet, mpAlias=&lmvConnection);

   select
      OBJECT_ID into :&mpOutKey
   from connection to &lmvConnection
   (
      select nextval(%postgres_string(&mpLoginSet..&mpSequenceName.)) as OBJECT_ID
   )
   ;
   disconnect from &lmvConnection
   ;

   %if &lmvIsNotSQL %then %do;
    quit;
   %end;

   /* kinda trim */
   %let &mpOutKey = &&&mpOutKey;
%mend postgres_get_seq_val;
