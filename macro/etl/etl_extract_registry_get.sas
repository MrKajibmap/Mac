/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 6704d40d4dd572ada6bdee658cf86bfdfa723dc6 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Получает список записей реестра для заданного списка ресурсов и версии.
*
*  ПАРАМЕТРЫ:
*     mpInResource            +  имя набора данных, списка ресурсов
*     mpVersion               +  идентификатор версии
*                                MIN - самая ранняя из открытых (A, P)
*     mpOutRegistry           +  имя выходного набора, списка записей в реестре
*
******************************************************************
*  Использует:
*     %error_check
*
******************************************************************
*  Пример использования:
*     В трансформах transform_extract.sas, transform_extract_db2.sas
*
******************************************************************
*  15-05-2014  Нестерёнок     Выделено из transform_extract.sas
*  16-05-2014  Нестерёнок     Введена политика обновления при перезапуске
******************************************************************/

%macro etl_extract_registry_get (
   mpInResource      =  ,
   mpVersion         =  ,
   mpOutRegistry     =
);
   %if &mpVersion = MIN %then %do;
      /* Определяем самую раннюю доступную версию */
      proc sql;
         create table &mpOutRegistry as
         select
            r1.resource_id
          , min(r1.version_id) as version_id
          , min(r1.available_dttm) as available_dttm
         from
            ETL_SYS.ETL_RESOURCE_REGISTRY r1
         inner join
         (  select
               r3.resource_id,
               min(available_dttm) as available_dttm
            from
               ETL_SYS.ETL_RESOURCE_REGISTRY r3,
               &mpInResource r4
            where
               r3.resource_id = r4.resource_id
               and r3.status_cd in ("A", "P")
            group by r3.resource_id
         ) r2
         on
            r1.resource_id = r2.resource_id
            and r1.available_dttm = r2.available_dttm
            and r1.status_cd in ("A", "P")
         group by
            r1.resource_id
         ;
      quit;
      %error_check (mpStepType=SQL);
   %end;
   %else %do;
      proc sql;
         create table &mpOutRegistry as
         select
            r1.resource_id
          , r1.version_id
          , r1.available_dttm
         from
            ETL_SYS.ETL_RESOURCE_REGISTRY r1
         inner join
            &mpInResource r2
         on
            r1.resource_id = r2.resource_id
            and r1.version_id = &mpVersion
         ;
      quit;
      %error_check (mpStepType=SQL);
   %end;
%mend etl_extract_registry_get;
