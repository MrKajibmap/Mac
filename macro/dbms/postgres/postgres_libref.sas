/*****************************************************************
* ВЕРСИЯ:
*   $Id: d143e19e84a1c34eeb6f5b75f46a484365d13881 $
*
******************************************************************
* НАЗНАЧЕНИЕ:
*   Осуществляет назначение библиотеки на БД Postgres, под указанным пользователем
*
* ПАРАМЕТРЫ:
*   mpLoginSet       +  имя набора параметров подключения (ETL_SYS, ETL_STG и т.п.)
*   mpLibref         -  имя назначаемой библиотеки
*                       по умолчанию совпадает с mpLoginSet
*   mpOptions        -  специфичные параметры назначения
*
******************************************************************
* ИСПОЛЬЗУЕТ:
*     %generic_libref
*
* УСТАНАВЛИВАЕТ МАКРОПЕРЕМЕННЫЕ:
*     нет
*
******************************************************************
* ПРИМЕР ИСПОЛЬЗОВАНИЯ:
*    %postgres_libref (mpLoginSet=ETL_STG);
*    data aaa;
*        set etl_stg.bbb;
*        ...
*
******************************************************************
* 31-03-2017   Задояный  Начальное кодирование
******************************************************************/

%macro postgres_libref (mpLoginSet=, mpLibref=&mpLoginSet, mpOptions=);
   %local lmvPostgresOptions;
   %let lmvPostgresOptions = &mpOptions;

   %if %symexist(&mpLoginSet._CONNECT_SCHEMA) %then %do;
      %let lmvPostgresOptions = %trim(&lmvPostgresOptions) schema=&&&mpLoginSet._CONNECT_SCHEMA;
   %end;

   %generic_libref (mpLoginSet=&mpLoginSet, mpLibref=&mpLibref, mpEngine=postgres, mpEngineOptions=&lmvPostgresOptions);
%mend postgres_libref;
