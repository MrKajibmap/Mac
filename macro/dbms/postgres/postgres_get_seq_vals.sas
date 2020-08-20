/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 8cc123f2c7af89045e2ac9f47a3399994734b783 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Генерирует mpIdCount уникальных числовых значений (идентификаторов), и сохраняет их в таблицу mpOut.
*     Идентификаторы берутся из одноименного ETL_DBMS sequence Postgres.
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
*     %postgres_connect
*     %postgres_string
*     указанный sequence
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  02-11-2017  Задояный       Начальное кодирование
******************************************************************/

%macro postgres_get_seq_vals (
   mpIdCount         =  ,
   mpOut             =  ,
   mpSequenceName    =  ,
   mpLoginSet        =  ETL_SYS
);
  /* Проверяем корректность параметров */
   %if %is_blank(mpOut) %then %do;
      %log4sas_error (dwf.macro.postgres_get_seq_vals, Incorrect parameter mpOut value.);
      %return;
   %end;
   %if %is_blank(mpSequenceName) %then %do;
      %log4sas_error (dwf.macro.postgres_get_seq_vals, Incorrect parameter mpSequenceName value.);
      %return;
   %end;
   %if %is_blank(mpIdCount) %then %do;
      %log4sas_error (dwf.macro.postgres_get_seq_vals, Incorrect parameter mpIdCount value.);
      %return;
   %end;
   %if &mpIdCount le 0 %then %do;
      %log4sas_error (dwf.macro.postgres_get_seq_vals, Specify positive id count.);
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
    %let lmvConnection = pgseqs%util_recursion;
    %postgres_connect (mpLoginSet=&mpLoginSet, mpAlias=&lmvConnection);

    create table &mpOut as
      select
         OBJECT_ID,
         ORDER_NO
      from connection to &lmvConnection
      (
         select
            nextval(%postgres_string(&mpLoginSet..&mpSequenceName.)) as OBJECT_ID,
            s.ORDER_NO
            from generate_series(1, &mpIdCount.) as s(ORDER_NO)
      )
      order by ORDER_NO
    ;
    disconnect from &lmvConnection
    ;

  %if &lmvIsNotSQL %then %do;
    quit;
  %end;
%mend postgres_get_seq_vals;
