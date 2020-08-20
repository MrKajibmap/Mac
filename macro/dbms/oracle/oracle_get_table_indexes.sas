/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 7ca587f34495e11bc3f4fab91032e61aff6a60f6 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Получает список индексов над таблицей.
*
*  ПАРАМЕТРЫ:
*     mpTable                 +  имя таблицы в SAS
*     mpOutIndexList          +  выходная переменная со списком индексов
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
*
******************************************************************
*  09-03-2017  Сазонов     Начальное кодирование
******************************************************************/

%macro oracle_get_table_indexes (mpTable=, mpOutIndexList=);
   %local lmvTable lmvLoginSet;
   %oracle_table_name (mpSASTable=&mpTable,  mpOutNameKey=lmvTable,  mpOutLoginSetKey=lmvLoginSet);

   %local lmvIsNotSQL;
   %let lmvIsNotSQL   = %eval (&SYSPROCNAME ne SQL);

   /* Открываем proc sql, если он еще не открыт */
   %if &lmvIsNotSQL %then %do;
    proc sql noprint;
     %oracle_connect (mpLoginSet=&lmvLoginSet);
   %end;
     reset noprint;
      select index_name into :&mpOutIndexList separated by ' ' from connection to oracle (
         select index_name from user_indexes
         where table_owner=%oracle_string(&lmvLoginSet)
         and table_name=%oracle_string(&lmvTable)
      );
   %if &lmvIsNotSQL %then %do;
     %error_check (mpStepType=SQL_PASS_THROUGH);
     disconnect from oracle;
    quit;
   %end;
%mend oracle_get_table_indexes;
