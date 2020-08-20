/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 01900c55510231f57d70ea7f749ad4005751483d $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Генерирует mpIdCount уникальных числовых значений (идентификаторов), и сохраняет их в таблицу mpOut.
*     Идентификаторы берутся из одноименного ETL_DBMS sequence Oracle.
*     Если mpOut существует, он перезаписывается.
*     Выходная таблица mpOut содержит следующие поля:
*        OBJECT_ID (INTEGER) - уникальный идентификатор
*        ORDER_NO (INTEGER) - порядковый номер идентификатора, начиная с 1
*     и отсортирована по возрастанию ORDER_NO.
*     Работает в глобальном режиме или внутри PROC SQL.
*
*  ПАРАМЕТРЫ:
*     mpIdCount               +  требуемое кол-во идентификаторов, должно быть целым и больше 0
*     mpOut                   +  имя выходной таблицы
*     mpSequenceName          +  имя ETL_DBMS sequence для получения очередного идентификатора
*     mpLoginSet              +  По умолчанию ETL_SYS
*
******************************************************************
*  Использует:
*     %oracle_connect
*     указанный sequence
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  30-01-2015  Сазонов        Начальное кодирование
******************************************************************/

%macro oracle_get_seq_vals (
   mpIdCount         =  ,
   mpOut             =  ,
   mpSequenceName    =  ,
   mpLoginSet        =  ETL_SYS
);
   /* Проверяем корректность параметров */
   %if %is_blank(mpOut) %then %do;
      %log4sas_error (dwf.macro.oracle_get_seq_vals, Incorrect parameter mpOut value.);
      %return;
   %end;
   %if %is_blank(mpSequenceName) %then %do;
      %log4sas_error (dwf.macro.oracle_get_seq_vals, Incorrect parameter mpSequenceName value.);
      %return;
   %end;
   %if %is_blank(mpIdCount) %then %do;
      %log4sas_error (dwf.macro.oracle_get_seq_vals, Incorrect parameter mpIdCount value.);
      %return;
   %end;
   %if &mpIdCount le 0 %then %do;
      %log4sas_error (dwf.macro.oracle_get_seq_vals, Specify positive id count.);
      %return;
   %end;

   /* Открываем proc sql, если он еще не открыт */
   %local lmvIsNotSQL;
   %let lmvIsNotSQL = %eval (&SYSPROCNAME ne SQL);
   %if &lmvIsNotSQL %then %do;
      proc sql noprint;
   %end;
   %else %do;
         reset noprint;
   %end;

   /* Соединяемся через другое подключение, чтобы не мешать внешнему коду, в т.ч. рекурсивному */
   %local lmvConnection;
   %let lmvConnection = oraseqs%util_recursion;
   %oracle_connect (mpLoginSet=&mpLoginSet, mpAlias=&lmvConnection);

   create table &mpOut as
      select
         OBJECT_ID,
         ORDER_NO
      from connection to &lmvConnection
      (
         with rec_cte (row_num) as (
            select 1 as row_num from dual
            union all
            select row_num+1 from rec_cte
            where row_num < &mpIdCount
         )
         select &mpSequenceName..nextval as OBJECT_ID, row_num as ORDER_NO
         from rec_cte
      )
      order by ORDER_NO
   ;
   disconnect from &lmvConnection
   ;

   %if &lmvIsNotSQL %then %do;
      quit;
   %end;
%mend oracle_get_seq_vals;
