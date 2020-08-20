/*****************************************************************
* НАЗНАЧЕНИЕ:
*   Осуществляет назначение библиотеки на любой engine, под указанным пользователем
*
* ПАРАМЕТРЫ:
*   mpLoginSet       +  имя набора параметров подключения (ETL_SYS, MIDAS и т.п.)
*   mpLibref         -  имя назначаемой библиотеки
*                       по умолчанию совпадает с mpLoginSet
*   mpEngine         +  engine библиотеки
*   mpEngineOptions  -  специфичные для engine параметры
*
******************************************************************
* ИСПОЛЬЗУЕТ:
*     %log_*
*     %error_check
*
* УСТАНАВЛИВАЕТ МАКРОПЕРЕМЕННЫЕ:
*     нет
*
******************************************************************
* ПРИМЕР ИСПОЛЬЗОВАНИЯ:
*    Явное использование не предполагается.
*
******************************************************************
* 18-01-2012   Нестерёнок  Начальное кодирование
******************************************************************/

%macro generic_libref (mpLoginSet=, mpLibref=&mpLoginSet, mpEngine=, mpEngineOptions=);
   /* Отключение вывода в лог */
   %log_disable;

   %if (not %symexist(&mpLoginSet._CONNECT_OPTIONS)) %then
   %do;
      %log_enable;
      %log4sas_error (cwf.macro.generic_libref, Login credentials for set &mpLoginSet are not defined);
      %return;
   %end;

   /* Назначение библиотеки */
   libname &mpLibref &mpEngine
      &&&mpLoginSet._CONNECT_OPTIONS
      &mpEngineOptions
   ;

   /* Сбор ошибок */
   %error_check (mpStepType=DATA);

   /* Восстановление вывода в лог */
   %log_enable;
%mend generic_libref;
