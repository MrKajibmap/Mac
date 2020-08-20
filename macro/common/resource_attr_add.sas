/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 3490a22446f571b5d45be5d47a5d9ceb14dbfa0a $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Добавляет в реестр запись о доп. атрибутах ресурса указанной версии.
*
*  ПАРАМЕТРЫ:
*     mpResourceId            *  идентификатор ресурса.  Исключает mpResourceCode, приоритет.
*     mpResourceCode          *  мнемокод ресурса
*     mpVersion               +  версия ресурса
*     mpUpdateDttm            -  атрибут - реальная дата, соответствующая данной версии
*     mpArchRows              -  атрибут - кол-во записей в выгрузке
*     mpArchRowLen            -  атрибут - размер записи в выгрузке
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
******************************************************************
*  Пример использования:
*     %resource_add (mpResourceCode=MY_RESOURCE, mpVersion=222, mpArchRowsNo=12345);
*
******************************************************************
*  18-11-2013  Нестерёнок     Начальное кодирование
*  02-12-2013  Нестерёнок     Добавлен mpArchRowLen
******************************************************************/

%macro resource_attr_add (
      mpResourceId            =  ,
      mpResourceCode          =  UNKNOWN_RESOURCE,
      mpVersion               =  ,
      mpUpdateDttm            =  ,
      mpArchRows              =  ,
      mpArchRowLen            =  ,
      mpConnection            =
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
      %job_event_reg (
         mpEventTypeCode=RESOURCE_NOT_FOUND,
         mpEventValues= %bquote(mpResourceCode="&mpResourceCode") );
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
         %let mpConnection = etlraadd;
         %&ETL_DBMS._connect(mpLoginSet=ETL_SYS, mpAlias=&mpConnection);
      %end;

      /* Получаем имена в СУБД */
      %local lmvRegistryAttrDbms;
      %&ETL_DBMS._table_name (mpSASTable=ETL_SYS.ETL_RESOURCE_REGISTRY_ATTR,  mpOutFullNameKey=lmvRegistryAttrDbms);

      /* Делаем запись в реестре */
      execute (
         insert into &lmvRegistryAttrDbms (
            resource_id, version_id,
            update_dttm, arch_rows_no, arch_row_len)
         values (
            %&ETL_DBMS._number(&lmvResourceId), %&ETL_DBMS._number(&mpVersion),
            %&ETL_DBMS._timestamp(&mpUpdateDttm), %&ETL_DBMS._number(&mpArchRows), %&ETL_DBMS._number(&mpArchRowLen)
         )
      ) by &mpConnection
      ;
      %error_check (mpStepType=SQL_PASS_THROUGH);

   /* Закрываем новое соединение */
      %if &lmvNotConnected %then %do;
         disconnect from &mpConnection;
      %end;
   %if &lmvIsNotSQL %then %do;
      quit;
   %end;
%mend resource_attr_add;
