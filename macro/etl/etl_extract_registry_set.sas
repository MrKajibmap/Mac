/*****************************************************************
*  ВЕРСИЯ:
*     $Id: d7d902eede668fabb30c1ba295fe2808a45329ca $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Переводит ресурсы группы в состояние N, если все ее оригиналы в состоянии P.
*
*  ПАРАМЕТРЫ:
*     mpInRegistry            +  имя входного набора, списка обновляемых записей в реестре
*     mpNoOpenAction          -  Действие при нехватке открытых ресурсов в реестре,
*                                ERR - вызывать ошибку, NOP - ничего не делать,
*                                по умолчанию ERR
*     mpLogTable              -  имя набора, которое будет указано в тексте событий
*
******************************************************************
*  Использует:
*     %error_check
*     %etl_transaction_*
*     %job_event_reg
*     %member_drop
*     %member_obs
*     %resource_update
*     %unique_id
*     %util_loop_data
*
******************************************************************
*  Пример использования:
*     В трансформе transform_archive_put.sas
*
******************************************************************
*  16-05-2014  Нестерёнок     Начальное кодирование
*  11-08-2014  Кузенков       Добавлен mpNoOpenAction
*  12-08-2015  Сазонов        Добавлена проверка на количество ресурсов != количеству в реестре для случая если mpNoOpenAction != ERR
******************************************************************/

%macro etl_extract_registry_set (
   mpInRegistry      =  ,
   mpNoOpenAction    =  ERR,
   mpLogTable        =
);
   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   %local lmvOrigResourceTable lmvResourceTable;
   %let lmvOrigResourceTable  = work.tr_extr_orig_&lmvUID.;
   %let lmvResourceTable      = work.tr_extr_res_&lmvUID.;

   /* В зависимости от типа группы, получаем список ресурсов-оригиналов */
   proc sql;
      create table &lmvOrigResourceTable as
      select
         resource_id,
         resource_group_cd
      from
         ETL_SYS.ETL_RESOURCE
      where
         put (resource_id, res_id_extr_role.) = "SRC"
      ;
      /* для групп, требующих полноты, отбираем все ресурсы */
      create table &lmvResourceTable as
      select
         resource_id
      from
         &lmvOrigResourceTable
      where
         resource_group_cd in (
            select distinct
               r3.resource_group_cd
            from
               &mpInRegistry r2
            inner join
               &lmvOrigResourceTable r3
            on
               r2.resource_id = r3.resource_id
               and put (r3.resource_group_cd, $resgrp_cd_full.) = "Y"
         )
      ;
      /* для групп, не требующих полноты, отбираем выгруженные оригиналы */
      insert into &lmvResourceTable (resource_id)
      select
         r2.resource_id
      from
         &mpInRegistry r2
      inner join
         &lmvOrigResourceTable r3
      on
         r2.resource_id = r3.resource_id
         and put (r3.resource_group_cd, $resgrp_cd_full.) = "N"
      ;
   quit;
   %error_check (mpStepType=SQL);

   %if %member_obs (mpData= &lmvResourceTable) le 0 %then %do;
      %job_event_reg (mpEventTypeCode= RESOURCE_NOT_FOUND,
                      mpEventDesc=     %bquote(Нет открытых ресурсов, связанных с таблицей &mpLogTable) );
      %return;
   %end;

   /* Получаем реестр */
   %local lmvRegistryTable lmvRegistryExtTable;
   %let lmvRegistryTable      = work.tr_extr_reg_&lmvUID.;
   %let lmvRegistryExtTable   = work.tr_extr_regx_&lmvUID.;

   %etl_extract_registry_get (
      mpInResource   =  &lmvResourceTable,
      mpVersion      =  MIN,
      mpOutRegistry  =  &lmvRegistryTable
   );

   %local lmvResourceTableObs lmvRegistryTableObs;
   %let lmvResourceTableObs = %member_obs(mpData=&lmvResourceTable);
   %let lmvRegistryTableObs = %member_obs(mpData=&lmvRegistryTable);
   %member_drop(&lmvOrigResourceTable);
   %member_drop(&lmvResourceTable);

   %if &mpNoOpenAction = ERR %then %do;
      %if &lmvResourceTableObs ne &lmvRegistryTableObs %then %do;
         %job_event_reg (mpEventTypeCode= RESOURCE_NOT_FOUND,
                         mpEventDesc=     %bquote(Не хватает открытых ресурсов в группе таблицы &mpLogTable) );
         %return;
      %end;
   %end;

   /* Находим все записи с той же версией (зеркала) */
   %local lmvNotPVersionCount;
   proc sql noprint;
      create table &lmvRegistryExtTable as select
         resource_id,
         version_id,
         status_cd
      from
         ETL_SYS.ETL_RESOURCE_REGISTRY
      where
         version_id in (
            select distinct version_id from &lmvRegistryTable
         )
      ;
      select count(version_id)
      into :lmvNotPVersionCount
      from &lmvRegistryExtTable
      where status_cd ne "P"
      ;
   quit;
   %error_check (mpStepType=SQL);

   /* Если не все версии в состоянии P, то выход */
   %if (&STEP_RC ne 0) or (&lmvNotPVersionCount gt 0) or (&lmvResourceTableObs ne &lmvRegistryTableObs) %then %return;

   /* Макро для обновления */
%macro _etl_extr_reg_loop;
      %resource_update (
         mpResourceId=&resource_id, mpVersion=&version_id,
         mpDate=NOCHG, mpProcessedBy=&JOB_ID, mpStatus=N,
         mpNotFound=ERR,
         mpConnection=&ETL_DBMS
      );
%mend _etl_extr_reg_loop;

   /* Транзакционно переводим все версии из P в N */
   %etl_transaction_start (mpLoginSet=ETL_SYS);
      %util_loop_data (mpLoopMacro=_etl_extr_reg_loop, mpData=&lmvRegistryExtTable);
   %etl_transaction_finish;

   /* Конец */
   %member_drop (&lmvRegistryTable);
   %member_drop (&lmvRegistryExtTable);
%mend etl_extract_registry_set;
