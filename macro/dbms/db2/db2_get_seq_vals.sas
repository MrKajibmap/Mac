/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 557454b70e5351e22c4bdff0015c30b6a5777971 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Генерирует mpIdCount уникальных числовых значений (идентификаторов), и сохраняет их в таблицу mpOut.
*     Идентификаторы берутся из одноименного ETL_DBMS sequence DB2.
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
*     %db2_connect
*     указанный sequence
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  30-01-2015  Сазонов			Начальное кодирование
******************************************************************/

%macro db2_get_seq_vals (
   mpIdCount         =  ,
   mpOut             =  ,
   mpSequenceName    =  ,
   mpLoginSet        =  ETL_SYS
);
  /* Проверяем корректность параметров */
   %if %is_blank(mpOut) %then %do;
      %log4sas_error (dwf.macro.db2_get_seq_vals, Incorrect parameter mpOut value.);
      %return;
   %end;
   %if %is_blank(mpSequenceName) %then %do;
      %log4sas_error (dwf.macro.db2_get_seq_vals, Incorrect parameter mpSequenceName value.);
      %return;
   %end;
   %if %is_blank(mpIdCount) %then %do;
      %log4sas_error (dwf.macro.db2_get_seq_vals, Incorrect parameter mpIdCount value.);
      %return;
   %end;
   %if &mpIdCount le 0 %then %do;
      %log4sas_error (dwf.macro.db2_get_seq_vals, Specify positive id count.);
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
     %let lmvConnection = db2seqs%util_recursion;
     %db2_connect (mpLoginSet=&mpLoginSet, mpAlias=&lmvConnection);

	 create table &mpOut as
		select
		   OBJECT_ID,
		   ORDER_NO
        from connection to &lmvConnection
		(
		   with generated (keyval) as
			(select 1 from sysibm.sysdummy1
			  union all
			 select keyval + 1 from generated WHERE keyval < &mpIdCount)       
			select nextval for &mpSequenceName as OBJECT_ID, keyval AS ORDER_NO
			  from generated
		)
		order by ORDER_NO
	 ;
     disconnect from &lmvConnection
	 ;

  %if &lmvIsNotSQL %then %do;
	 quit;
  %end;   
%mend db2_get_seq_vals;