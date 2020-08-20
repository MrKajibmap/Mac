/*****************************************************************
* ВЕРСИЯ:
*     $Id: 4b24fec4cd4302c433aa7efa5c9a2902e55db9d0 $
*
******************************************************************
* НАЗНАЧЕНИЕ:
*     На шаге PROC SQL осуществляет подключение к БД DB2, под указанным пользователем
*
* ПАРАМЕТРЫ:
*     mpLoginSet [MID и т.д.]    +  имя набора параметров подключения к БД
*
******************************************************************
* Пример использования:
*  proc sql;
*    %db2_connect (mpLoginSet=MID);
*    execute (...) by db2;
*    disconnect from db2;
*  quit;
*
******************************************************************
* 15-03-2012   Нестерёнок  Начальное кодирование
* 31-08-2012   Нестерёнок  Рефактор mpMode
* 29-01-2015   Сазонов     Добавлен mpOptions
******************************************************************/

%macro db2_connect (
   mpLoginSet   =   ,
   mpAlias      =   db2,
   mpOptions     =
   );
   /* Отключение вывода в лог */
   %log_disable;

   %if (not %symexist(&mpLoginSet._CONNECT_OPTIONS)) %then
   %do;
      %log_enable;
      %log4sas_warn (cwf.macro.db2_connect, Login credentials for set &mpLoginSet are not defined);
      %return;
   %end;

   connect to db2 as &mpAlias (
      &&&mpLoginSet._CONNECT_OPTIONS
     &mpOptions
   );

   /* Сбор ошибок */
   %error_check (mpStepType=SQL_PASS_THROUGH);

   /* Установка схемы по умолчанию */
   %if %symexist(&mpLoginSet._CONNECT_SCHEMA) %then %do;
      execute (
         set schema &&&mpLoginSet._CONNECT_SCHEMA
      ) by &mpAlias;
   %end;

   /* Сбор ошибок */
   %error_check (mpStepType=SQL_PASS_THROUGH);

   /* Восстановление вывода в лог */
   %log_enable;
%mend db2_connect;
