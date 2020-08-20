/*****************************************************************
* ВЕРСИЯ:
*   $Id: 7c0f409bf81b33f73751d2ed7ac39849d64310e6 $
*
******************************************************************
* НАЗНАЧЕНИЕ:
*   Осуществляет назначение библиотеки на MS SQL, под указанным пользователем
*
* ПАРАМЕТРЫ:
*   mpLoginSet       +  имя набора параметров подключения (RSS и т.п.)
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
*    %sqlsvr_libref (mpLoginSet=RSS, mpLibref=rss);
*    data aaa;
*        set rss.bbb;
*        ...
*
******************************************************************
* 18-01-2012   Нестерёнок  Начальное кодирование
******************************************************************/

%macro sqlsvr_libref (mpLoginSet=, mpLibref=&mpLoginSet, mpOptions=);
   %local lmvSqlsvrOptions;
   %let lmvSqlsvrOptions = &mpOptions;

   %if %symexist(&mpLoginSet._CONNECT_DATABASE) %then %do;
      %let lmvSqlsvrOptions = %trim(&lmvSqlsvrOptions) dbconinit="use database &&&mpLoginSet._CONNECT_DATABASE";
   %end;

   %generic_libref (mpLoginSet=&mpLoginSet, mpLibref=&mpLibref, mpEngine=sqlsvr, mpEngineOptions=&lmvSqlsvrOptions);
%mend sqlsvr_libref;
