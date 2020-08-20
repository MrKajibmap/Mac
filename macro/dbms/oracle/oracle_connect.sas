/*****************************************************************
*  ВЕРСИЯ:
*     $Id: a0022c941d3ae806d5176a8fedfee83fbd5c2e3c $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     На шаге PROC SQL осуществляет подключение к БД Oracle.
*     На время назначения подключения вывод в лог не производится.
*
*  ПАРАМЕТРЫ:
*     mpLoginSet              +  имя набора параметров подключения к БД (ETL_SYS, ETL_STG и т.д.)
*     mpAlias                 -  имя подключения
*                                По умолчанию oracle
*     mpOptions               -  дополнительные опции подключения
*                                Если указаны, то имеют приоритет над полученными из mpLoginSet
*
******************************************************************
*  Использует:
*     %error_check
*     %log_disable
*     %log_enable
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     нет
*
******************************************************************
*  ПРИМЕР ИСПОЛЬЗОВАНИЯ:
*     proc sql;
*        %oracle_connect (mpLoginSet=ETL_STG);
*        execute (truncate table FINANCIAL_ACCOUNT) by oracle;
*        disconnect from oracle;
*     quit;
*
******************************************************************
*  19-12-2011  Нестерёнок     Начальное кодирование
*  21-02-2012  Нестерёнок     Поддержка параметра CONNECT_SCHEMA
*  28-11-2014  Нестерёнок     Добавлен mpOptions
******************************************************************/

%macro oracle_connect (
   mpLoginSet                 =  ,
   mpAlias                    =  oracle,
   mpOptions                  =
);
   /* Отключение вывода в лог */
   %log_disable;

   %if (not %symexist(&mpLoginSet._CONNECT_OPTIONS)) %then
   %do;
      %log_enable;
      %log4sas_warn (cwf.macro.oracle_connect, Login credentials for set &mpLoginSet are not defined);
      %return;
   %end;

   connect to oracle as &mpAlias (
      &&&mpLoginSet._CONNECT_OPTIONS
      &mpOptions
   );

   /* Сбор ошибок */
   %error_check (mpStepType=SQL_PASS_THROUGH);

   /* Установка схемы по умолчанию */
   %if %symexist(&mpLoginSet._CONNECT_SCHEMA) %then %do;
      execute (
         alter session set current_schema = &&&mpLoginSet._CONNECT_SCHEMA
      ) by &mpAlias;
   %end;

   /* Сбор ошибок */
   %error_check (mpStepType=SQL_PASS_THROUGH);

   /* Восстановление вывода в лог */
   %log_enable;
%mend oracle_connect;
