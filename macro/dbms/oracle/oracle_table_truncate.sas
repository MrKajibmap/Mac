/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 6e1643c2a7dacd5b0c20f6273d32d98fd51a28fc $
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
*     %oracle_connect
*     %oracle_table_name
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

%macro oracle_table_truncate (mpTable=);
   %local lmvTable lmvLoginSet;
   %oracle_table_name (mpSASTable=&mpTable,  mpOutFullNameKey=lmvTable,  mpOutLoginSetKey=lmvLoginSet);

   proc sql;
      %oracle_connect (mpLoginSet=&lmvLoginSet);
      execute (
         truncate table &lmvTable
      ) by oracle;
   quit;
   %error_check (mpStepType=SQL_PASS_THROUGH);
%mend oracle_table_truncate;
