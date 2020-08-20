/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 547f8f80827ce426a6b19e6b2de6692ce013a391 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Проверяет статус переданного реестра, возвращает его состояние.
*
*  ПАРАМЕТРЫ:
*     mpInRegistry            +  имя входного набора, списка обновляемых записей в реестре
*     mpLogTable              -  имя набора, которое будет указано в тексте событий
*     mpNoOpenAction          -  если = ERR, то статус -1 вызывает ошибку, иначе нет
*     mpStatusKey             +  имя макропеременной, в которую возвращается статус
*                                недопустимы имена lmvStatus, lmvGroupPolicy
*
******************************************************************
*  Использует:
*     %error_check
*     %job_event_reg
*
*  Устанавливает макропеременные:
*     mpStatusKey =  0           успех, ресурс открыт и еще не выгружался
*     mpStatusKey = -1           ошибка, для данного ресурса нет открытых записей в реестре
*     mpStatusKey = -2           ошибка, политика обновления группы NOX
*     mpStatusKey =  1           политика обновления группы ADD, и ресурс уже выгружен
*     mpStatusKey =  2           политика обновления группы NEW, и ресурс уже выгружен
*
******************************************************************
*  Пример использования:
*     В трансформе transform_extract.sas, макро etl_archive_put.sas
*
******************************************************************
*  15-05-2014  Нестерёнок     Выделено из etl_extract_registry.sas
*  11-08-2014  Кузенков       Добавлен mpNoOpenAction
******************************************************************/

%macro etl_extract_registry_check (
   mpInRegistry      =  ,
   mpLogTable        =  ,
   mpNoOpenAction    =  ERR,
   mpStatusKey       =
);
   /* Определяем статус версии */
   %local lmvStatus lmvGroupPolicy;
   proc sql noprint;
      select
         r1.status_cd,
         put (put (r1.resource_id, res_id_grp.), $resgrp_cd_rld.)
      into
         :lmvStatus,
         :lmvGroupPolicy
      from
         ETL_SYS.ETL_RESOURCE_REGISTRY r1
      inner join
         &mpInRegistry r2
      on
         r1.resource_id = r2.resource_id
         and r1.version_id = r2.version_id
         and put (r1.resource_id, res_id_extr_role.) = "SRC"
      ;
   quit;
   %error_check (mpStepType=SQL);

   /* Если для данного ресурса нет открытых записей в реестре, то это ошибка */
   %if %is_blank(lmvStatus) %then %do;

      %if &mpNoOpenAction = ERR %then %do;
        %job_event_reg (mpEventTypeCode= RESOURCE_NOT_FOUND,
                        mpEventDesc=     %bquote(Нет открытых ресурсов, связанных с таблицей &mpLogTable) );
      %end;

      %let &mpStatusKey = -1;
      %return;
   %end;

   /* Если политика обновления группы NOX, то это ошибка */
   %if &lmvGroupPolicy = NOX %then %do;
      %job_event_reg (mpEventTypeCode= ILLEGAL_ARGUMENT,
                      mpEventDesc=     %bquote(Ресурс не подлежит выгрузке) );
      %let &mpStatusKey = -2;
      %return;
   %end;

   /* Если политика обновления группы ADD, и ресурс уже выгружен, то еще раз его выгружать не надо */
   %if (&lmvGroupPolicy = ADD) and (&lmvStatus = P) %then %do;
      %job_event_reg (mpEventTypeCode= EXTRACT_INFO,
                      mpEventDesc=     %bquote(Выгрузка ресурсов таблицы &mpLogTable уже производилась) );
      %let &mpStatusKey = 1;
      %return;
   %end;

   /* Если политика обновления группы NEW, и ресурс уже выгружен, то его надо перезагрузить */
   %if (&lmvGroupPolicy = NEW) and (&lmvStatus = P) %then %do;
      %job_event_reg (mpEventTypeCode= EXTRACT_INFO,
                      mpEventDesc=     %bquote(Требуется повторная выгрузка ресурсов таблицы &mpLogTable) );
      %let &mpStatusKey = 2;
      %return;
   %end;

   /* Возвращаем успех */
   %let &mpStatusKey = 0;
%mend etl_extract_registry_check;
