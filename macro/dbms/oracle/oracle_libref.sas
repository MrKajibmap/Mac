/*****************************************************************
* ВЕРСИЯ:
*   $Id: acba5e60852617a7b281e66197d549a3ec2ece55 $
*
******************************************************************
* НАЗНАЧЕНИЕ:
*   Осуществляет назначение библиотеки на БД Oracle, под указанным пользователем
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
*    %oracle_libref (mpLoginSet=ETL_STG);
*    data aaa;
*        set etl_stg.bbb;
*        ...
*
******************************************************************
* 19-12-2011   Нестерёнок  Начальное кодирование
* 21-02-2012   Нестерёнок  Поддержка параметра CONNECT_SCHEMA
******************************************************************/

%macro oracle_libref (mpLoginSet=, mpLibref=&mpLoginSet, mpOptions=);
   %local lmvOracleOptions;
   %let lmvOracleOptions = &mpOptions;

   %if %symexist(&mpLoginSet._CONNECT_SCHEMA) %then %do;
      %let lmvOracleOptions = %trim(&lmvOracleOptions) schema=&&&mpLoginSet._CONNECT_SCHEMA;
   %end;

   %generic_libref (mpLoginSet=&mpLoginSet, mpLibref=&mpLibref, mpEngine=oracle, mpEngineOptions=&lmvOracleOptions);
%mend oracle_libref;
