/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 432170832b0cce92a0696692ad7e2383ded913c0 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Обновляет в реестре доп. атрибуты ресурса указанной версии.
*     Все значения атрибутов обновляются, если переданы пустыми.
*     Для того, чтобы не менять значение атрибута, укажите значение NOCHG (по умолчанию).
*     В режиме ADD для параметров, переданных как NOCHG, устанавливаются значения по умолчанию (пустые).
*
*  ПАРАМЕТРЫ:
*     mpResourceId            *  идентификатор ресурса.  Исключает mpResourceCode, приоритет.
*                                Если указать ALL, будут обновлены все ресурсы указанной версии
*     mpResourceCode          *  мнемокод ресурса
*     mpVersion               +  требуемая версия ресурса
*     mpUpdateDttm            -  атрибут - реальная дата, соответствующая данной версии
*                                по умолчанию NOCHG, т.е. без изменений
*     mpArchRows              -  атрибут - кол-во записей в выгрузке
*                                по умолчанию NOCHG, т.е. без изменений
*     mpArchRowLen            -  атрибут - размер записи в выгрузке
*                                по умолчанию NOCHG, т.е. без изменений
*     mpNotFound              -  если запись не найдена:
*                                NOP - ничего не делать
*                                ERR - сообщить об ошибке
*                                ADD - добавить новую запись
*                                по умолчанию NOP, т.е. ничего не делать
*     mpConnection            -  если указано, использовать это подключение вместо установки нового
*
******************************************************************
*  Использует:
*     %ETL_DBMS_*
*     %job_event_reg
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     mpResourceId=ALL подразумевает, что либо дочерних записей нет, либо они совпадают с родительскими.
*
******************************************************************
*  Пример использования:
*     %resource_attr_update (
*        mpResourceCode=MY_RESOURCE, mpVersion=333,
*        mpArchRowsNo=12345,
*        mpNotFound=ADD);
*
******************************************************************
*  15-11-2013  Нестерёнок     Начальное кодирование
*  02-12-2013  Нестерёнок     Добавлен mpArchRowLen
*  16-05-2014  Нестерёнок     Добавлен mpResourceId=ALL
*  09-09-2015  Сазонов        Убран pass-through при select - db2 bug
******************************************************************/

%macro resource_attr_update (
      mpResourceId            =  ,
      mpResourceCode          =  UNKNOWN_RESOURCE,
      mpVersion               =  ,
      mpUpdateDttm            =  NOCHG,
      mpArchRows              =  NOCHG,
      mpArchRowLen            =  NOCHG,
      mpNotFound              =  NOP,
      mpConnection            =
);
   /* Получение ID ресурса */
   %local lmvResourceId;
   %if not %is_blank(mpResourceId) %then %do;
      %let lmvResourceId = &mpResourceId;
   %end;
   %else %do;
      %if &mpResourceCode = ALL %then
         %let lmvResourceId = ALL;
      %else
         %let lmvResourceId = %sysfunc (inputn (&mpResourceCode, res_cd_id.));;
   %end;

   /* Проверка ID ресурса */
   %if &lmvResourceId le 0 %then %do;
      %job_event_reg (
         mpEventTypeCode=RESOURCE_NOT_FOUND,
         mpEventValues= %bquote(mpResourceCode="&mpResourceCode")
      );
      %return;
   %end;

   /* Проверка версии ресурса */
   %if &mpVersion eq . %then %let mpVersion = ;
   %if %is_blank(mpVersion) %then %do;
      %job_event_reg (
         mpEventTypeCode=ILLEGAL_ARGUMENT,
         mpEventValues= %bquote(mpVersion is NULL)
      );
   %end;

   /* Открываем proc sql, если он еще не открыт */
   %local lmvIsNotSQL;
   %let lmvIsNotSQL = %eval (&SYSPROCNAME ne SQL);
   %if &lmvIsNotSQL %then %do;
      proc sql noprint;
   %end;
   %else %do;
         reset noprint;
   %end;

      /* Устанавливаем соединение, если требуется */
      %local lmvNotConnected;
      %let lmvNotConnected = %eval (&lmvIsNotSQL or %is_blank(mpConnection));
      %if &lmvNotConnected %then %do;
         %let mpConnection = etlraupd;
         %&ETL_DBMS._connect(mpLoginSet=ETL_SYS, mpAlias=&mpConnection);
      %end;

      /* Получаем имена в СУБД */
      %local lmvRegistryDbms lmvRegistryAttrDbms;
      %&ETL_DBMS._table_name (mpSASTable=ETL_SYS.ETL_RESOURCE_REGISTRY,  mpOutFullNameKey=lmvRegistryDbms);
      %&ETL_DBMS._table_name (mpSASTable=ETL_SYS.ETL_RESOURCE_REGISTRY_ATTR,  mpOutFullNameKey=lmvRegistryAttrDbms);

      %local lmvResourceIdList;
        /* Находим родительские записи в реестре */
%if &ETL_DBMS = db2 %then %do;
      select resource_id
      into :lmvResourceIdList separated by " "
           from ETL_SYS.ETL_RESOURCE_REGISTRY
            where
               version_id = &mpVersion
               %if &lmvResourceId ne ALL %then %do;
                  and resource_id = &lmvResourceId
               %end;
      ;
      %error_check (mpStepType=SQL);
%end;
%else %do;
     select resource_id
      into :lmvResourceIdList separated by " "
         from connection to &mpConnection (
            select resource_id
            from &lmvRegistryDbms
            where
               version_id = %&ETL_DBMS._number(&mpVersion)
               %if &lmvResourceId ne ALL %then %do;
                  and resource_id = %&ETL_DBMS._number(&lmvResourceId)
               %end;
         )
      ;
      %error_check (mpStepType=SQL_PASS_THROUGH);
%end;

      /* Если не находим, то ошибка */
      %if %is_blank(lmvResourceIdList) %then %do;
         %job_event_reg (
            mpEventTypeCode=RESOURCE_NOT_FOUND,
            mpEventValues= %bquote(mpResourceId="&lmvResourceId" mpVersion="&mpVersion")
         );
      %end;

   /* Находим запись в реестре атрибутов */
    %let lmvObs = 0;
%if &ETL_DBMS = db2 %then %do;
      select count(*) into :lmvObs
           from ETL_SYS.ETL_RESOURCE_REGISTRY_ATTR
            where
               version_id = &mpVersion
               %if &lmvResourceId ne ALL %then %do;
                  and resource_id = &lmvResourceId
               %end;
      ;
      %error_check (mpStepType=SQL);
%end;
%else %do;
     select cnt into :lmvObs
         from connection to &mpConnection (
            select count(*) cnt
            from &lmvRegistryAttrDbms
            where
               version_id = %&ETL_DBMS._number(&mpVersion)
               %if &lmvResourceId ne ALL %then %do;
                  and resource_id = %&ETL_DBMS._number(&lmvResourceId)
               %end;
         )
      ;
      %error_check (mpStepType=SQL_PASS_THROUGH);
%end;

      /* Если не находим */
      %if &lmvObs eq 0 %then %do;
         %if &mpNotFound eq ERR %then %do;
            %job_event_reg (
               mpEventTypeCode=RESOURCE_NOT_FOUND,
               mpEventValues= %bquote(mpResourceId="&lmvResourceId" mpVersion="&mpVersion")
            );
         %end;
         %else %if (&mpNotFound eq ADD) %then %do;
%macro _etl_res_attr_loop (resource_id);
            %resource_attr_add (
               mpResourceId=&resource_id,
               mpVersion=&mpVersion,
               %if &mpUpdateDttm ne NOCHG %then %do;
                  mpUpdateDttm=&mpUpdateDttm,
               %end;
               %if &mpArchRows ne NOCHG %then %do;
                  mpArchRows=&mpArchRows,
               %end;
               %if &mpArchRowLen ne NOCHG %then %do;
                  mpArchRowLen=&mpArchRowLen,
               %end;
               mpConnection=&mpConnection
            );
%mend _etl_res_attr_loop;
            %util_loop (mpMacroName=_etl_res_attr_loop, mpWith=&lmvResourceIdList);
         %end;
         %goto exit;
      %end;

      /* Если нечего обновлять */
      %if (&mpUpdateDttm eq NOCHG) and (&mpArchRows eq NOCHG) and (&mpArchRowLen eq NOCHG) %then %do;
         %job_event_reg (
            mpEventTypeCode=UNEXPECTED_ARGUMENT,
            mpEventValues= %bquote(Ни один из атрибутов не требуется обновлять (&lmvResourceId-&mpVersion) )
         );
         %goto exit;
      %end;

      /* Обновляем записи в реестре */
      execute (
         update &lmvRegistryAttrDbms set
            %if &mpUpdateDttm ne NOCHG %then %do;
               update_dttm       = %&ETL_DBMS._timestamp(&mpUpdateDttm)
            %end;
            %if &mpUpdateDttm ne NOCHG and &mpArchRows ne NOCHG %then %do;
               ,
            %end;
            %if &mpArchRows ne NOCHG %then %do;
               arch_rows_no      = %&ETL_DBMS._number(&mpArchRows)
            %end;
            %if &mpArchRows ne NOCHG and &mpArchRowLen ne NOCHG %then %do;
               ,
            %end;
            %if &mpArchRowLen ne NOCHG %then %do;
               arch_row_len      = %&ETL_DBMS._number(&mpArchRowLen)
            %end;
         where
            version_id = %&ETL_DBMS._number(&mpVersion)
            %if &lmvResourceId ne ALL %then %do;
               and resource_id = %&ETL_DBMS._number(&lmvResourceId)
            %end;
      ) by &mpConnection
      ;
      %error_check (mpStepType=SQL_PASS_THROUGH);

   %exit:
   /* Закрываем новое соединение */
      %if &lmvNotConnected %then %do;
         disconnect from &mpConnection;
      %end;
   %if &lmvIsNotSQL %then %do;
      quit;
   %end;
%mend resource_attr_update;
