/*****************************************************************
* НАЗНАЧЕНИЕ:
*   Осуществляет назначение библиотеки на DB2, под указанным пользователем
*
* ПАРАМЕТРЫ:
*   mpLoginSet       +  имя набора параметров подключения (MID и т.п.)
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
*    %db2_libref (mpLoginSet=MID, mpLibref=rmidas);
*    data aaa;
*        set rmidas.bbb;
*        ...
*
******************************************************************
* 18-01-2012   Нестерёнок  Начальное кодирование
* 15-03-2012   Нестерёнок  Поддержка параметра CONNECT_SCHEMA
******************************************************************/

%macro db2_libref (mpLoginSet=, mpLibref=&mpLoginSet, mpOptions=);
   %local lmvDb2Options;
   %let lmvDb2Options = &mpOptions;

   %if %symexist(&mpLoginSet._CONNECT_SCHEMA) %then %do;
      %let lmvDb2Options = %trim(&lmvDb2Options) schema=&&&mpLoginSet._CONNECT_SCHEMA;
   %end;

   %generic_libref (mpLoginSet=&mpLoginSet, mpLibref=&mpLibref, mpEngine=db2, mpEngineOptions=&lmvDb2Options);
%mend db2_libref;
