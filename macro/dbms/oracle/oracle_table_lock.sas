/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 5316ed26854d2668d03e3ef124955a40d9012614 $
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
*     %oracle_table_lock (mpTable=COUNTERPARTY_BK_RK, mpLockMode=EXCLUSIVE, mpWait=INF);
*     execute by oracle (
*       ...
*
******************************************************************
*  06-06-2014  Нестерёнок     Начальное кодирование
******************************************************************/

%macro oracle_table_lock (
   mpTable                 =  ,
   mpLockMode              =  ,
   mpWait                  =  INF,
   mpConnection            =  oracle
);
   execute (
      lock table &mpTable
         in &mpLockMode mode
%if "&mpWait" = "0" or "&mpWait" = "" %then %do;
         nowait
%end;
%else %if "&mpWait" ne "INF" %then %do;
         wait &mpWait
%end;
   ) by &mpConnection;
%mend oracle_table_lock;
