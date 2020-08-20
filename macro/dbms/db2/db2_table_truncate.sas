/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 62e7f2ae9fbbb0c683ec64572346e01302b87f2b $
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
*     %db2_connect
*     %db2_table_name
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %db2_table_truncate (mpTable=ETL_STG.ACCNTAB);
*
******************************************************************
*  02-10-2013  Нестерёнок     Начальное кодирование
******************************************************************/

%macro db2_table_truncate (mpTable=);
   %local lmvTable lmvLoginSet;
   %db2_table_name (mpSASTable=&mpTable,  mpOutFullNameKey=lmvTable,  mpOutLoginSetKey=lmvLoginSet);

   proc sql;
      %db2_connect (mpLoginSet=&lmvLoginSet);
      execute (
         truncate table &lmvTable immediate
      ) by db2;
   quit;
   %error_check (mpStepType=SQL_PASS_THROUGH);
%mend db2_table_truncate;
