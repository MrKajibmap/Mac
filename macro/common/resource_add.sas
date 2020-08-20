/*****************************************************************
*  ВЕРСИЯ:
*     $Id: e861bbcdfc0c90faaf6ddf5d5c908ea443a7a887 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Добавляет в реестр запись о ресурсе на указанную дату.
*
*  ПАРАМЕТРЫ:
*     mpResourceCode          +  мнемокод ресурса
*     mpResourceId            -  идентификатор ресурса.  Исключает mpResourceCode, приоритет.
*     mpVersion               +  версия ресурса
*                                Может быть задана значением UNIQUE, тогда будет сгенерировано уникальное значение
*     mpDate                  -  бизнес-дата, на которую делается запись
*     mpProcessedBy           -  код процесса, который добавляет запись,
*                                по умолчанию &ETL_CURRENT_JOB_ID
*     mpStatus                -  состояние ресурса:
*                                A - доступен
*                                N - новый, выгружен
*                                L - загружен, обработан
*                                Е - ошибочный, некорректный, недоступен
*                                C - удален
*                                По умолчанию A
*     mpOut                   -  выходная таблица SAS, содержит добавленную строку реестра
*     mpConnection            -  если указано, использовать это подключение вместо установки нового (etlradd)
*
******************************************************************
*  Использует:
*     %ETL_DBMS_*
*     %job_event_reg
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %resource_add (mpResourceCode=MY_RESOURCE, mpVersion=222, mpDate=%sysfunc(datetime()), mpStatus=A);
*
******************************************************************
*  16-02-2012  Нестерёнок     Начальное кодирование
*  04-10-2012  Нестерёнок     Добавлен mpUpdateDttm
*  23-10-2012  Нестерёнок     Добавлен mpConnection
*  18-11-2013  Нестерёнок     mpUpdateDttm перенесен в атрибуты
*  27-02-2014  Нестерёнок     Добавлен mpVersion=UNIQUE
*  27-02-2014  Нестерёнок     Добавлен mpOut
******************************************************************/

%macro resource_add (
   mpResourceCode=UNKNOWN_RESOURCE,
   mpResourceId=,
   mpVersion=UNIQUE,
   mpDate=,
   mpProcessedBy=&ETL_CURRENT_JOB_ID,
   mpStatus=A,
   mpOut=,
   mpConnection=
);
   /* Получение ID ресурса */
   %local lmvResourceId;
   %if not %is_blank(mpResourceId) %then %do;
      %let lmvResourceId = &mpResourceId;
   %end;
   %else %do;
      %let lmvResourceId = %sysfunc (inputn (&mpResourceCode, res_cd_id.));
   %end;

   /* Проверка ID ресурса */
   %if not %is_blank(mpResourceCode) and &lmvResourceId le 0 %then %do;
      %job_event_reg (mpEventTypeCode=RESOURCE_NOT_FOUND, mpJobId=&mpProcessedBy,
                     mpEventValues= %bquote(mpResourceCode="&mpResourceCode") );
      %return;
   %end;

   /* Проверка версии ресурса */
   %if &mpVersion eq . %then %let mpVersion = ;
   %if %is_blank(mpVersion) %then %do;
      %job_event_reg (
         mpEventTypeCode=ILLEGAL_ARGUMENT, mpJobId=&mpProcessedBy,
         mpEventValues= %bquote(mpVersion is NULL)
      );
   %end;
   %if &mpVersion = UNIQUE %then %do;
      %unique_id (mpOutKey=mpVersion, mpSequenceName=ETL_VERSION_SEQ);
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
         %let mpConnection = etlradd;
         %&ETL_DBMS._connect(mpLoginSet=ETL_SYS, mpAlias=&mpConnection);
      %end;

      /* Получаем имена в СУБД */
      %local lmvRegistryDbms;
      %&ETL_DBMS._table_name (mpSASTable=ETL_SYS.ETL_RESOURCE_REGISTRY,  mpOutFullNameKey=lmvRegistryDbms);

      /* Делаем запись в реестре */
      execute (
         insert into &lmvRegistryDbms (
            resource_id, version_id,
            available_dttm, processed_by_job_id, status_cd)
         values (
            %&ETL_DBMS._number(&lmvResourceId), %&ETL_DBMS._number(&mpVersion),
            %&ETL_DBMS._timestamp(&mpDate), %&ETL_DBMS._number(&mpProcessedBy), %&ETL_DBMS._string(&mpStatus)
         )
      ) by &mpConnection
      ;
      %error_check (mpStepType=SQL_PASS_THROUGH);

      /* Создаем выходную таблицу */
      %if not %is_blank(mpOut) %then %do;
         create table &mpOut like ETL_SYS.ETL_RESOURCE_REGISTRY
         ;
         insert into &mpOut (
            resource_id, version_id,
            available_dttm, processed_by_job_id, status_cd)
         values (
            &lmvResourceId, &mpVersion,
            &mpDate, &mpProcessedBy, "&mpStatus"
         );
         %error_check (mpStepType=SQL);
      %end;

   /* Закрываем новое соединение */
      %if &lmvNotConnected %then %do;
         disconnect from &mpConnection;
      %end;
   %if &lmvIsNotSQL %then %do;
      quit;
   %end;
%mend resource_add;
