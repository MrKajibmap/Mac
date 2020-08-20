/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для генерации партиций в PG для таблицы dm_rep.va_datamart
*
*  ПАРАМЕТРЫ:
*     mpPromoCalculationRk - promo_calculation_rk из модели данных Promo Tool
*
******************************************************************
*  Использует: 
*	  нет
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*    %partitions_processing(mpPromoCalculationRk=110, mpTableName=dm_rep.va_datamart);
*
****************************************************************************
*  18-06-2020  Борзунов     Начальное кодирование
*  30-06-2020  Борзунов		Добавление имени таблицы в параметр
*  30-06-2020  Михайлова    Добавлено использование lmvTableName при создании партиции
*  02-07-2020  Борзунов		Добавлено квотирование диапазона значений партиции
*							(для символьных и числовых переменных партиционирования)
****************************************************************************/

%macro partitions_processing(mpPromoCalculationRk=, mpTableName=);
	%local lmvPromoCalculationRk lmvTableName lmvNM lmvLIB lmvCNT;
	%let lmvPromoCalculationRk=&mpPromoCalculationRk.;
	%let lmvTableName = &mpTableName.;
	%let lmvNM = %sysfunc(lowcase(%scan(&lmvTableName.,-1,%str(.))));
	%let lmvLIB = %sysfunc(lowcase(%scan(&lmvTableName.,1,%str(.))));
	
	/* проверка входного параметра (схема в pg) */
	proc sql noprint;
		connect to postgres as &lmvLIB.(&dm_rep_connect_options);
		select cnt into :lmvCNT
		from 
		connection to &lmvLIB.
			(
					select count(t1.*) as cnt 
					from 
					(SELECT distinct schema_name FROM information_schema.schemata
					where lower(schema_name) = %str(%')&lmvLIB.%str(%')) as t1
			);
		disconnect from &lmvLIB.;
	quit;
	%put &=lmvCNT >>>>>>>>>>> &=lmvCNT;
	%if &lmvCNT. = 0 %then %do;
		%put ERROR: Invalid libref = "&lmvLIB.";
		%abort;
	%end;
	
	/* получение списка всех партиций */
	proc sql noprint;
		connect to postgres as &lmvLIB.(&&&lmvLIB._connect_options);
		create table work.pg_cfg_partitions_list as
		select * from 
		connection to &lmvLIB.
			(
				SELECT
					nmsp_parent.nspname AS parent_schema,
					parent.relname      AS parent,
					nmsp_child.nspname  AS child_schema,
					child.relname       AS child
				FROM pg_inherits
					JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
					JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
					JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
					JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
				WHERE lower(parent.relname)=%str(%')&lmvNM.%str(%')
			);
		disconnect from &lmvLIB.;
	quit;
	
	/*создание по входному параметру партиций */
	proc sql noprint;
		select count(*) as cnt into :mvCNT
		from work.pg_cfg_partitions_list
		where child = "&lmvNM._&lmvPromoCalculationRk."
		;
	quit;
	
	/*Если такой партиции нет - создаем по новой */
	%if &mvCNT. = 0 %then %do;
		proc sql noprint;
			connect to postgres as &lmvLIB. (&&&lmvLIB._connect_options);
			execute by &lmvLIB.
				(
					CREATE TABLE IF NOT EXISTS &lmvLIB..&lmvNM._&lmvPromoCalculationRk. PARTITION OF &lmvTableName
					FOR VALUES IN (%str(%')&lmvPromoCalculationRk.%str(%'))
				);
			disconnect from &lmvLIB.;
		quit;
	%end;
	/* Иначе очищаем партицию */
	%else %do;
		proc sql noprint;
			connect to postgres as &lmvLIB. (&&&lmvLIB._connect_options);
			execute by &lmvLIB.
				(
					TRUNCATE &lmvLIB..&lmvNM._&lmvPromoCalculationRk.
				);
			disconnect from &lmvLIB.;
		quit;
	%end;
%mend partitions_processing;