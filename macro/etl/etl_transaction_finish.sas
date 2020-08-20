/*****************************************************************
*  ВЕРСИЯ:
*     $Id: ac6c42094c44f11ce22e7ae5394869ef6e0283db $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Завершает транзакцию.
*
*  ПАРАМЕТРЫ:
*     нет
*
******************************************************************
*  Использует:
*     ETL_MODULE_RC
*     %error_check
*     %etl_stop
*
*  Удаляет макропеременные:
*     ETL_TXN_CONNECTION
*     ETL_TXN_LOGIN_SET
*
******************************************************************/

%macro etl_transaction_finish;
   %error_check (mpStepType=SQL_PASS_THROUGH);

   %if (&ETL_MODULE_RC eq 0) %then %do;
         execute by &ETL_TXN_CONNECTION (
            commit
         );
      quit;
   %end;
   %else %do;
         /* Восстанавливаемся, если была ошибка */
         %error_recovery;

         execute by &ETL_TXN_CONNECTION (
            rollback
         );
      quit;

	  %job_event_reg (mpEventTypeCode=TRANSACTION_ROLLBACK);
   %end;

   %symdel ETL_TXN_CONNECTION ETL_TXN_LOGIN_SET;
%mend etl_transaction_finish;

