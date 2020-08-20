/*****************************************************************
*  ВЕРСИЯ:
*     $Id: f06999d246887eddaeeceb0b57b9023bdd7129bd $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Начинает транзакцию по обновлению DDS от имени указанного пользователя.
*     Последующие шаги должны работать в режиме SQL.
*
*  ПАРАМЕТРЫ:
*     mpLoginSet              +  имя набора параметров подключения к БД
*
******************************************************************
*  Использует:
*     %error_check
*     %ETL_DBMS_connect
*
*  Устанавливает макропеременные:
*     ETL_TXN_CONNECTION
*     ETL_TXN_LOGIN_SET
*
*  Ограничения:
*     1.  Транзакция должна быть завершена явным коммитом, или произойдет rollback.
*         См. %etl_transaction_finish
*
******************************************************************
*  ПРИМЕР ИСПОЛЬЗОВАНИЯ:
*     %etl_transaction_start (mpLoginSet=DWH_DDS);
*        ... sql updates here ...
*     %etl_transaction_finish;
*
******************************************************************
*  02-04-2012  Нестерёнок     Начальное кодирование
*  11-07-2014  Нестерёнок     Добавлены ETL_TXN_CONNECTION, ETL_TXN_LOGIN_SET
*  28-11-2014  Нестерёнок     В случае закрытия транзакции без commit выполняется rollback
*  09-02-2015  Сазонов        Для db2 транзакция открывается implicitly
******************************************************************/

%macro etl_transaction_start (
   mpLoginSet                 =
);
   %global ETL_TXN_CONNECTION ETL_TXN_LOGIN_SET;
   %let ETL_TXN_CONNECTION    =  &ETL_DBMS;
   %let ETL_TXN_LOGIN_SET     =  &mpLoginSet;

   proc sql stimer noprint;
      %&ETL_DBMS._connect (mpLoginSet=&ETL_TXN_LOGIN_SET, mpOptions= dbconterm="rollback")
      ;
%if &ETL_DBMS = oracle %then %do;
      execute by &ETL_TXN_CONNECTION (
         set transaction read write
      );
%end;
%if &ETL_DBMS = postgres %then %do;
      execute by &ETL_TXN_CONNECTION (
         start transaction read write
      );
%end;
      %error_check (mpStepType=SQL_PASS_THROUGH);
%mend etl_transaction_start;
