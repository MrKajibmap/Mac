/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 1aae6c4758c2eb71b39cba14df45df02eff7edff $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Отбирает из архива выгрузок самую старую(-ые) из еще не загруженных.
*     Выбираются записи, соответствующие самой старой дате выгрузки для указанного ресурса(-ов).
*
*  ПАРАМЕТРЫ:
*     mpInArchive             +  имя входной таблицы, архива выгрузок
*     mpOutData               +  имя выходной таблицы, отобранного набора
*     mpOutRegistry           -  имя выходной таблицы, списка записей из реестра
*                                Содержит поля: resource_id, version_id, available_dttm
*     mpRole                  -  роль, которой должен соответствовать ресурс, чтобы быть отобранным из архива
*                                по умолчанию ARCH
*     mpWhere                 -  условие отбора из mpInArchive
*                                по умолчанию отсутствует
*     mpProcessedBy           -  значение, которым обновляется реестр
*                                по умолчанию &STREAM_ID
*     mpFullStage             -  получать полный (L+N) или нет (N) срез из архива
*                                по умолчанию полный (Yes)
*     mpFieldExtractDttm      -  имя поля для даты выгрузки, добавляется в выходную таблицу если указано
*                                Как правило, ETL_AVAILABLE_DTTM
*
******************************************************************
*  Использует:
*     %error_check
*     %job_event_reg
*     %member_names
*     %member_obs
*     %resource_update
*     %util_loop_data
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     в трансформации transform_archive_get.sas
*
******************************************************************
*  15-11-2013  Нестерёнок     Начальное кодирование
*  07-04-2015  Сазонов        Не делать выгрузку если была выгружена та же версия.
******************************************************************/

%macro etl_archive_get (
      mpInArchive             =  ,
      mpOutData               =  ,
      mpOutRegistry           =  ,
      mpRole                  =  ARCH,
      mpWhere                 =  ,
      mpProcessedBy           =  &STREAM_ID,
      mpFullStage             =  Yes,
      mpFieldExtractDttm      =  ,
	  mpDrop                  =
);

   %local lmvFieldExtractId lmvFieldResourceId;
   %let lmvFieldExtractId  = ETL_EXTRACT_ID;
   %let lmvFieldResourceId = ETL_RESOURCE_ID;

   /* Находим базовый список ресурсов для отбора */
   %local lmvLibref lmvMemberName;
   %member_names (mpTable=&mpInArchive, mpLibrefNameKey=lmvLibref, mpMemberNameKey=lmvMemberName);

   %local lmvResourceIdList1;
   %let lmvResourceIdList1 = ;
   proc sql noprint;
      select
         resource_id into :lmvResourceIdList1 separated by ", "
      from
         ETL_SYS.ETL_RESOURCE_X_ARCH
      where
         arch_nm = "%upcase(&lmvMemberName)"
         and arch_role_cd = "%unquote(&mpRole)"
      ;
   quit;
   %error_check (mpStepType=SQL);

   /* Находим расширенный список ресурсов для расчета даты обновления */
   %local lmvResourceIdList2;
   %let lmvResourceIdList2 = ;
   proc sql noprint;
      select resource_id into :lmvResourceIdList2 separated by ", "
      from ETL_SYS.ETL_RESOURCE
      where resource_group_cd = (
         select distinct resource_group_cd
            from ETL_SYS.ETL_RESOURCE
            where resource_id in (&lmvResourceIdList1)
      );
   quit;
   %error_check (mpStepType=SQL);

   /* Находим дату обновления - мин. дату, для которой у этих ресурсов есть новые выгрузки */
   %local lmvExtractDate lmvExtractDttm;
   %let lmvExtractDate   = ;
   %let lmvExtractDttm   = ;
   proc sql noprint;
      select min(available_dttm) format=best20. into :lmvExtractDttm
      from ETL_SYS.ETL_RESOURCE_REGISTRY
      where
         status_cd = "N" and
         resource_id in (&lmvResourceIdList2)
      ;
   quit;
   %error_check (mpStepType=SQL);

   /* Если новых выгрузок нет для всей группы, то это ошибка */
   %if &lmvExtractDttm eq . %then %do;
      %job_event_reg (mpEventTypeCode=DATA_NOT_AVAILABLE,
                      mpEventValues= %bquote(В группах, связанных с таблицей &lmvMemberName, нет новых выгрузок) );
      %return;
   %end;

   %let lmvExtractDate   = %sysfunc(datepart(&lmvExtractDttm));

   /* Находим для базового списка ресурсов мин. версию среди незагруженных или макс. среди загруженных */
   %local lmvTempResTable;
   %let lmvTempResTable    = work.t_arch_get_res;
   proc sql noprint;
      create table &lmvTempResTable as select
         resource_id,
         status_cd,
         case (status_cd) when ("N") then min(version_id) else max(version_id) end as version_id
      from ETL_SYS.ETL_RESOURCE_REGISTRY
      where
         %if &mpFullStage = Yes %then %do;
            status_cd in ("N", "L") and
         %end;
         %else %do;
            status_cd = "N" and
         %end;
         resource_id in (&lmvResourceIdList1) and
         available_dttm = &lmvExtractDttm
      group by
         resource_id, status_cd
      ;
   quit;
   %error_check (mpStepType=SQL);

   /* Получаем номер выгрузки для каждого ресурса */
   %local lmvWhere;
   %let lmvWhere = ;
   %if %is_blank(mpOutRegistry) %then
      %let mpOutRegistry = work.t_arch_get_reg;;

   proc sql noprint;
      create table &mpOutRegistry as select
         resource_id,
         version_id,
         &lmvExtractDttm as available_dttm length=8 format=datetime20.
      from &lmvTempResTable
      group by resource_id
      /* Если у ресурса есть и N, и L версии, то берем N */
      having status_cd = max (status_cd)
      ;
      select
         catt ("(", "&lmvFieldExtractId=", version_id, ")")
         into :lmvWhere separated by " or "
         from &mpOutRegistry
      ;
   quit;
   %error_check (mpStepType=SQL);

   /* Если никаких выгрузок нет для данного ресурса, то это ошибка */
   %if %member_obs(mpData=&mpOutRegistry) le 0 %then %do;
      %job_event_reg (mpEventTypeCode=DATA_NOT_AVAILABLE,
                      mpEventValues= %bquote(У ресурсов, связанных с таблицей &lmvMemberName, нет новых выгрузок) );
      %return;
   %end;

   %local lmvVersion_id;
   %let lmvVersion_id = ;
   proc sql noprint;
      select version_id
         into :lmvVersion_id from &mpOutRegistry;
   quit;
   %error_check (mpStepType=SQL);

%if %member_obs(mpData=&mpOutRegistry) = 1 and %sysfunc(exist(&mpOutData)) %then %do;
   %if &lmvVersion_id eq %member_attr(mpData=&mpOutData, mpAttr=LABEL)  %then %do;
   	%log4sas_info (dwf.macro.etl_archive_get, Table &mpOutData already exists with version &lmvVersion_id);
   %end;
%end;
%else %do;
   /* Создаем выходную таблицу */
   data &mpOutData(label=&lmvVersion_id);
      set &mpInArchive;

      where (&lmvWhere)
      %if not %is_blank(mpWhere) %then %do;
         and (%unquote(&mpWhere))
      %end;
      ;

      %if not %is_blank(mpFieldExtractDttm) %then %do;
         attrib
            &mpFieldExtractDttm length=8 format=datetime20.;
         &mpFieldExtractDttm = &lmvExtractDttm;
      %end;
	  %if not %is_blank(mpDrop) %then %do;
	    drop &mpDrop;
	  %end;
   run;
   %error_check;
%end;

   /* Обновляем записи в реестре */
   %macro _transform_archive_get_res_upd;
      %resource_update (
         mpResourceId=&resource_id, mpVersion=&version_id,
         mpDate=NOCHG, mpProcessedBy=&mpProcessedBy, mpStatus=NOCHG,
         mpNotFound=ERR);
   %mend _transform_archive_get_res_upd;
   %util_loop_data (mpLoopMacro=_transform_archive_get_res_upd, mpData=&mpOutRegistry);

%mend etl_archive_get;
