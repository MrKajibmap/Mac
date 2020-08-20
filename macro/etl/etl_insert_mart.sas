/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 34a51e956e161cf630de7eb7719ddc063b2480cc $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Наполняет целевую таблицу MART при помощи набора (insert).
*     Работает в режиме SQL.
*
*  ПАРАМЕТРЫ:
*     mpIn                    +  имя входного набора
*     mpOut                   +  имя выходного набора, обновляемой таблицы STG/IA/DDS
*
******************************************************************
*  Использует:
*     ETL_TXN_CONNECTION
*     ETL_TXN_LOGIN_SET
*     ETL_MODULE_RC
*     %error_check
*     %ETL_DBMS_table_name
*     %member_vars_*
*     %unique_id
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     нет
*
******************************************************************
*  Пример использования:
*     в трансформе transform_insert_mart.sas
*
******************************************************************
*  20-04-2015  Сазонов   Начальное кодирование
*  09-09-2015  Сазонов   Сделал очистку через delete в транзакции (раньше сначала делался truncate вне транзакции)
******************************************************************/

%macro etl_insert_mart(mpIn=,mpOut=);

%local lmvOutDbms lmvOutSchema lmvOutLoginSet;
%&ETL_DBMS._table_name (mpSASTable=&mpOut, mpOutFullNameKey=lmvOutDbms,
mpOutSchemaKey=lmvOutSchema, mpOutLoginSetKey=lmvOutLoginSet);

%local lmvInsertVars;
/* переменные обновляемой таблицы */
%member_vars_get(&mpIn, lmvInsertVars);
/* Получаем уникальный идентификатор */
%local lmvUID;
%unique_id (mpOutKey=lmvUID);


/* Создаем временную таблицу с обновляющими записями */
%local lmvUpdateTable;
%let lmvUpdateTable    = etl_update_&lmvUID;

/* Соединяемся через другое подключение, чтобы не мешать текущей транзакции */
%&ETL_DBMS._connect(mpLoginSet=&ETL_TXN_LOGIN_SET, mpAlias=updel);

execute by updel (
 create table &ETL_TXN_LOGIN_SET..&lmvUpdateTable as
 (select %member_vars_expand(&lmvInsertVars, {})
 from &lmvOutDbms) WITH NO DATA
);
%error_check (mpStepType=SQL_PASS_THROUGH);

disconnect from updel;
%error_check (mpStepType=SQL);

/* Выполняем обновление */
execute by &ETL_TXN_CONNECTION (
  delete from &lmvOutDbms where 1=1
);
%error_check (mpStepType=SQL_PASS_THROUGH);

insert into &ETL_TXN_LOGIN_SET..&lmvUpdateTable
  (
     %member_vars_expand(&lmvInsertVars, {})
  )
  select
     %member_vars_expand(&lmvInsertVars, source.{})
  from &mpIn source;
%error_check (mpStepType=SQL);

execute by &ETL_TXN_CONNECTION (
  insert into &lmvOutDbms
  (
     %member_vars_expand(&lmvInsertVars, {})
  )
  select
     %member_vars_expand(&lmvInsertVars, {})
  from &ETL_TXN_LOGIN_SET..&lmvUpdateTable
);
%error_check (mpStepType=SQL_PASS_THROUGH);

/* Удаляем временную таблицу */
execute by &ETL_TXN_CONNECTION (
 drop table &ETL_TXN_LOGIN_SET..&lmvUpdateTable
);
%error_check (mpStepType=SQL_PASS_THROUGH);

%member_vars_clean(&lmvInsertVars);
%mend etl_insert_mart;