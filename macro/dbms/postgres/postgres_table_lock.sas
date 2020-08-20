/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 0e3125b150b877d58abee8de19c23f3ad11c3b69 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Открывает блокировку таблицы, с ожиданием или без него.
*     Может вызываться только внутри транзакции.
*
*  ПАРАМЕТРЫ:
*     mpTable                 +  имя таблицы в Oracle
*     mpLockMode              +  режим блокировки
*     mpWait                  -  0 или пусто - без ожидания
*                                N>0 - ожидая до N секунд
*                                INF - ожидая бесконечно
*                                по умолчанию INF
*     mpConnection            -  имя подключения, в контексте которого открыта транзакция
*                                по умолчанию oracle
*
******************************************************************
*  Пример использования:
*     %etl_transaction_start (mpLoginSet=ETL_IA);
*     %postgres_table_lock (mpTable=COUNTERPARTY_BK_RK, mpLockMode=EXCLUSIVE, mpWait=INF);
*     execute by postgres (
*       ...
*
******************************************************************
*  08-08-2018  Задояный     Начальное кодирование
******************************************************************/

%macro postgres_table_lock (
   mpTable                 =  ,
   mpLockMode              =  ,
   mpWait                  =  ,
   mpConnection            =  postgres
);
   execute (
      lock table &mpTable
         in &mpLockMode mode
   ) by &mpConnection;
%mend postgres_table_lock;
