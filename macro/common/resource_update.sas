/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 28ebf74131107b838ba60301e0362c072fb87232 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Обновляет в реестре запись о ресурсе указанной версии.
*
*     Все значения атрибутов обновляются, даже если переданы пустыми.
*     Для того, чтобы не менять значение атрибута, укажите значение NOCHG.
*     Один из пары обязательных атрибутов mpResourceId/mpVersion может задаваться как ALL.
*
*  ПАРАМЕТРЫ:
*     mpResourceCode          *  мнемокод ресурса
*     mpResourceId            *  идентификатор ресурса.  Исключает mpResourceCode, приоритет.
*                                Если указать ALL, будут обновлены все ресурсы указанной версии
*     mpVersion               +  требуемая версия ресурса
*                                Если указать ALL, будут обновлены все версии ресурса
*     mpDate                  -  бизнес-дата, соответствующая данной версии
*                                по умолчанию NOCHG, т.е. без изменений
*     mpProcessedBy           -  код процесса, который добавляет запись,
*                                по умолчанию ETL_CURRENT_JOB_ID
*     mpStatus                -  состояние ресурса:
*                                A - доступен
*                                N - новый, выгружен
*                                L - загружен, обработан
*                                Е - ошибочный, некорректный, недоступен
*                                C - удален
*                                по умолчанию NOCHG, т.е. без изменений
*     mpNotFound              -  если запись не найдена:
*                                NOP - ничего не делать
*                                ERR - сообщить об ошибке
*                                ADD - добавить новую запись
*                                по умолчанию NOP, т.е. ничего не делать
*     mpWhere                 -  дополнительное ограничение, в формате ETL_DBMS
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
*     mpResourceId=ALL подразумевает mpNotFound<>ADD, иначе регистрируется ошибка
*     mpVersion=ALL подразумевает mpNotFound<>ADD, иначе регистрируется ошибка
*     Оба обязательных атрибута mpResourceId/mpVersion не могут задаваться как ALL одновременно.
*
******************************************************************
*  Пример использования:
*     %resource_update (
*        mpResourceCode=MY_RESOURCE, mpVersion=ALL,
*        mpDate=NOCHG, mpProcessedBy=&STREAM_ID, mpStatus=L,
*        mpNotFound=ERR, mpWhere= processed_by_job_id=&STREAM_ID);
*
******************************************************************
*  22-02-2012  Нестерёнок     Начальное кодирование
*  28-04-2012  Нестерёнок     Смена первичного ключа
*  04-10-2012  Нестерёнок     Добавлен mpUpdateDttm
*  23-10-2012  Нестерёнок     Добавлен mpConnection
*  10-07-2013  Нестерёнок     Исключен mpVersionSet
*  18-11-2013  Нестерёнок     mpUpdateDttm перенесен в атрибуты
*  19-03-2015  Сазонов        Убран pass-through при select - db2 bug
*  09-09-2015  Сазонов        Вернул pass-through при select - для не db2
******************************************************************/

%macro resource_update (
   mpResourceCode=UNKNOWN_RESOURCE,
   mpResourceId=,
   mpVersion=,
   mpDate=NOCHG,
   mpProcessedBy=&ETL_CURRENT_JOB_ID,
   mpStatus=NOCHG,
   mpNotFound=NOP,
   mpWhere=,
   mpConnection=
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
      %job_event_reg (mpEventTypeCode=RESOURCE_NOT_FOUND, mpJobId=&mpProcessedBy,
                     mpEventValues= %bquote(mpResourceCode="&mpResourceCode") );
      %return;
   %end;

   /* Получение версии */
   %local lmvVersion;
   %if &mpVersion eq . %then
      %let lmvVersion = ;
   %else
      %let lmvVersion = &mpVersion;;

   /* Проверка версии */
   %if %is_blank(lmvVersion) %then %do;
      %job_event_reg (mpEventTypeCode=ILLEGAL_ARGUMENT, mpJobId=&mpProcessedBy,
                     mpEventValues= %bquote(mpVersion is NULL) );
   %end;

   /* Проверка ID ресурса и версии */
   %if (&lmvResourceId = ALL) and (&lmvVersion = ALL) %then %do;
      %job_event_reg (mpEventTypeCode=ILLEGAL_ARGUMENT, mpJobId=&mpProcessedBy,
                     mpEventValues= %bquote(mpResourceId = mpVersion = ALL) );
   %end;

   /* Проверка обновляющего процесса */
   %if %is_blank(mpProcessedBy) %then %do;
      %let mpProcessedBy = &ETL_CURRENT_JOB_ID;
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
         %let mpConnection = etlrupd;
         %&ETL_DBMS._connect(mpLoginSet=ETL_SYS, mpAlias=&mpConnection);
      %end;

      /* Получаем имена в СУБД */
      %local lmvRegistryDbms;
      %&ETL_DBMS._table_name (mpSASTable=ETL_SYS.ETL_RESOURCE_REGISTRY,  mpOutFullNameKey=lmvRegistryDbms);

      %local lmvObs;
      /* Находим запись(-и) в реестре */
      %let lmvObs = 0;
%if &ETL_DBMS = db2 %then %do;
       select count(*) into :lmvObs
         from ETL_SYS.ETL_RESOURCE_REGISTRY
            where
               1 = 1
               %if &lmvResourceId ne ALL %then %do;
                  and resource_id = &lmvResourceId
               %end;
               %if &lmvVersion ne ALL %then %do;
                  and version_id = &lmvVersion
               %end;
               %if not %is_blank(mpWhere) %then %do;
                  and (%unquote(&mpWhere))
               %end;
      ;
      %error_check (mpStepType=SQL);
%end;
%else %do;
     select cnt into :lmvObs
         from connection to &mpConnection (
            select count(*) cnt
            from &lmvRegistryDbms
            where
               1 = 1
               %if &lmvResourceId ne ALL %then %do;
                  and resource_id = %&ETL_DBMS._number(&lmvResourceId)
               %end;
               %if &lmvVersion ne ALL %then %do;
                  and version_id = %&ETL_DBMS._number(&lmvVersion)
               %end;
               %if not %is_blank(mpWhere) %then %do;
                  and (%unquote(&mpWhere))
               %end;
         )
      ;
     %error_check (mpStepType=SQL_PASS_THROUGH);
%end;

      /* Если не находим */
      %if &lmvObs eq 0 %then %do;
         %if &mpNotFound eq ERR %then %do;
            %job_event_reg (mpEventTypeCode=RESOURCE_NOT_FOUND, mpJobId=&mpProcessedBy,
                           mpEventValues= %bquote(mpResourceId="&lmvResourceId" mpVersion="&lmvVersion") );
         %end;
         %else %if (&mpNotFound eq ADD) %then %do;
            %if (&lmvResourceId ne ALL) and (&lmvVersion ne ALL) and (&mpDate ne NOCHG) and (&mpStatus ne NOCHG) %then %do;
               %resource_add (mpResourceId=&lmvResourceId, mpVersion=&lmvVersion,
                              mpDate=&mpDate, mpProcessedBy=&mpProcessedBy, mpStatus=&mpStatus,
                              mpConnection=&mpConnection
                              );
            %end;
            %else %do;
               %job_event_reg (
                  mpEventTypeCode=ILLEGAL_ARGUMENT, mpJobId=&mpProcessedBy,
                  mpEventValues= %bquote(mpNotFound="ADD" mpResourceId="&lmvResourceId" mpVersion="&lmvVersion" mpDate="&mpDate" mpStatus="&mpStatus") );
            %end;
         %end;
         %goto exit;
      %end;

      /* Обновляем запись в реестре */
      execute (
         update &lmvRegistryDbms set
            processed_by_job_id  = %&ETL_DBMS._number(&mpProcessedBy)
            %if &mpDate ne NOCHG %then %do;
               ,
               available_dttm       = %&ETL_DBMS._timestamp(&mpDate)
            %end;
            %if &mpStatus ne NOCHG %then %do;
               ,
               status_cd            = %&ETL_DBMS._string(&mpStatus)
            %end;
         where
            1 = 1
            %if &lmvResourceId ne ALL %then %do;
               and resource_id = %&ETL_DBMS._number(&lmvResourceId)
            %end;
            %if &lmvVersion ne ALL %then %do;
               and version_id = %&ETL_DBMS._number(&lmvVersion)
            %end;
            %if not %is_blank(mpWhere) %then %do;
               and (%unquote(&mpWhere))
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
%mend resource_update;
