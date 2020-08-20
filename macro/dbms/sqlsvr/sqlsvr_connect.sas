/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 4aca14f4f86d20300f697cfa64802f2317e4c7d4 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     На шаге PROC SQL осуществляет подключение к БД MSSQL, под указанным пользователем
*
*  ПАРАМЕТРЫ:
*     mpLoginSet [ALD и т.д.]    +  имя набора параметров подключения к БД
*
******************************************************************
*  Пример использования:
*     proc sql;
*        %sqlsvr_connect (mpLoginSet=ALD);
*        execute (...) by sqlsvr;
*        disconnect from sqlsvr;
*     quit;
*
******************************************************************
*  11-04-2013  Малявин        Начальное кодирование
******************************************************************/

%macro sqlsvr_connect (mpLoginSet=, mpAlias=sqlsvr);
   /* Отключение вывода в лог */
   %log_disable;

   %if (not %symexist(&mpLoginSet._CONNECT_OPTIONS)) %then
   %do;
      %log_enable;
      %log4sas_warn (cwf.macro.sqlsvr_connect, Login credentials for set &mpLoginSet are not defined);
      %return;
   %end;

   connect to sqlsvr as &mpAlias (
      &&&mpLoginSet._CONNECT_OPTIONS
   );

   /* Сбор ошибок */
   %error_check (mpStepType=SQL_PASS_THROUGH);

   /* Установка схемы по умолчанию */
   %if %symexist(&mpLoginSet._CONNECT_SCHEMA) %then
      %if &&&mpLoginSet._CONNECT_SCHEMA ~=  %then %do;
         execute (
            use [&&&mpLoginSet._CONNECT_SCHEMA]
         ) by &mpAlias;
   %end;

   /* Сбор ошибок */
   %error_check (mpStepType=SQL_PASS_THROUGH);

   /* Восстановление вывода в лог */
   %log_enable;
%mend sqlsvr_connect;
