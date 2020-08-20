/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 47b4ee3277d88efa0917f5950da82280b6b4d11b $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     На шаге PROC SQL осуществляет подключение к БД Postgres.
*     На время назначения подключения вывод в лог не производится.
*
*  ПАРАМЕТРЫ:
*     mpLoginSet              +  имя набора параметров подключения к БД (ETL_SYS, ETL_STG и т.д.)
*     mpAlias                 -  имя подключения
*                                По умолчанию postgres
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
*        %postgres_connect (mpLoginSet=ETL_STG);
*        execute (truncate table FINANCIAL_ACCOUNT) by postgres;
*        disconnect from postgres;
*     quit;
*
******************************************************************
*  31-03-2017  Задояный     Начальное кодирование
******************************************************************/

%macro postgres_connect (
   mpLoginSet                 =  ,
   mpAlias                    =  postgres,
   mpOptions                  =
);
   /* Отключение вывода в лог */
   %log_disable;

   %if (not %symexist(&mpLoginSet._CONNECT_OPTIONS)) %then
   %do;
      %log_enable;
      %log4sas_warn (cwf.macro.postgres_connect, Login credentials for set &mpLoginSet are not defined);
      %return;
   %end;

   connect to postgres as &mpAlias (
      &&&mpLoginSet._CONNECT_OPTIONS
      &mpOptions
   );

   /* Сбор ошибок */
   %error_check (mpStepType=SQL_PASS_THROUGH);

   /* Установка схемы по умолчанию */
   %if %symexist(&mpLoginSet._CONNECT_SCHEMA) %then %do;
      execute (
         set search_path TO &&&mpLoginSet._CONNECT_SCHEMA;
      ) by &mpAlias;
   %end;

   /* Сбор ошибок */
   %error_check (mpStepType=SQL_PASS_THROUGH);

   /* Восстановление вывода в лог */
   %log_enable;
%mend postgres_connect;
