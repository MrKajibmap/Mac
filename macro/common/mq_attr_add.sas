/*****************************************************************
*  ВЕРСИЯ:
*     $Id: d3d55446f4e37d12acba63c0aa3b87006b1f03ef $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Добавляет в протокол запись о доп. атрибутах сообщения MQ.
*
*  ПАРАМЕТРЫ:
*     mpMsgId                 +  идентификатор обновляемого сообщения
*     mpCopyRc                -  атрибут - результат копирования файла из сообщения
*     mpSourceCode            -  атрибут - код источника сообщения
*     mpTargetCode            -  атрибут - код приемника сообщения
*     mpFileName              -  атрибут - имя файла из сообщения
*     mpFTPServerName         -  атрибут - имя сервера FTP из сообщения
*     mpFilePath              -  атрибут - путь к файлу из сообщения на FTP
*     mpConnection            -  если указано, использовать это подключение вместо установки нового
*
******************************************************************
*  Использует:
*     %error_check
*     %ETL_DBMS_*
*     %job_event_reg
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %mq_attr_add (mpMsgId=12345, mpCopyRc=0);
*
******************************************************************
*  08-09-2014  Нестерёнок     Начальное кодирование
******************************************************************/

%macro mq_attr_add (
   mpMsgId                 =  ,
   mpCopyRc                =  ,
   mpSourceCode            =  ,
   mpTargetCode            =  ,
   mpFileName              =  ,
   mpFTPServerName         =  ,
   mpFilePath              =  ,
   mpConnection            =
);
   /* Проверка ID сообщения */
   %if %is_blank(mpMsgId) %then %do;
      %job_event_reg (mpEventTypeCode=ILLEGAL_ARGUMENT,
                     mpEventValues= %bquote(mpMsgId is NULL) );
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
         %let mpConnection = etlmaadd;
         %&ETL_DBMS._connect(mpLoginSet=ETL_SYS, mpAlias=&mpConnection);
      %end;

      /* Получаем имена в СУБД */
      %local lmvMqAttrDbms;
      %&ETL_DBMS._table_name (mpSASTable=ETL_SYS.MQ_MSG_ATTR,  mpOutFullNameKey=lmvMqAttrDbms);

      /* Делаем запись в протоколе */
      execute (
         insert into &lmvMqAttrDbms (
            msg_id,
            copy_rc_no, source_system_cd, target_system_cd, file_nm, ftp_server_nm, ftp_file_path_txt)
         values (
            %&ETL_DBMS._number(&mpMsgId),
            %&ETL_DBMS._number(&mpCopyRc), %&ETL_DBMS._string(&mpSourceCode), %&ETL_DBMS._string(&mpTargetCode),
            %&ETL_DBMS._string(&mpFileName), %&ETL_DBMS._string(&mpFTPServerName), %&ETL_DBMS._string(&mpFilePath)
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
%mend mq_attr_add;
