/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 54bd06956e8b85573c9ebc410c18fb96158a9afa $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Обнуляет указанную таблицу.
*
*  ПАРАМЕТРЫ:
*     mpTable                 +  имя таблицы в SAS, которую требуется обнулить
*
******************************************************************
*  Использует:
*     %error_check
*     %postgres_connect
*     %postgres_table_name
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %oracle_table_truncate (mpTable=ETL_STG.ACCNTAB);
*
******************************************************************
*  02-10-2013  Нестерёнок     Начальное кодирование
******************************************************************/

%macro postgres_table_truncate (mpTable=);
   %local lmvTable lmvLoginSet;
   %postgres_table_name (mpSASTable=&mpTable,  mpOutFullNameKey=lmvTable,  mpOutLoginSetKey=lmvLoginSet);

   proc sql;
      %postgres_connect (mpLoginSet=&lmvLoginSet);
      execute (
         truncate table &lmvTable
      ) by postgres;
   quit;
   %error_check (mpStepType=SQL_PASS_THROUGH);
%mend postgres_table_truncate;
