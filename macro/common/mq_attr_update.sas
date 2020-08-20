/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 50d9c9bbb7b235f09633039408e2657c17a01a31 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Обновляет в протоколе доп. атрибуты сообщения MQ.
*
*     Все значения атрибутов обновляются, если переданы пустыми.
*     Для того, чтобы не менять значение атрибута, укажите значение NOCHG (по умолчанию).
*     В режиме ADD для параметров, переданных как NOCHG, устанавливаются значения по умолчанию (пустые).
*
*  ПАРАМЕТРЫ:
*     mpMsgId                 +  идентификатор обновляемого сообщения
*     mpCopyRc                -  атрибут - результат копирования файла из сообщения
*                                по умолчанию NOCHG, т.е. без изменений
*     mpSourceCode            -  атрибут - код источника сообщения
*                                по умолчанию NOCHG, т.е. без изменений
*     mpTargetCode            -  атрибут - код приемника сообщения
*                                по умолчанию NOCHG, т.е. без изменений
*     mpFileName              -  атрибут - имя файла из сообщения
*                                по умолчанию NOCHG, т.е. без изменений
*     mpFTPServerName         -  атрибут - имя сервера FTP из сообщения
*                                по умолчанию NOCHG, т.е. без изменений
*     mpFilePath              -  атрибут - путь к файлу из сообщения на FTP
*                                по умолчанию NOCHG, т.е. без изменений
*     mpNotFound              -  если запись не найдена:
*                                NOP - ничего не делать
*                                ERR - сообщить об ошибке
*                                ADD - добавить новую запись
*                                по умолчанию NOP, т.е. ничего не делать
*     mpConnection            -  если указано, использовать это подключение вместо установки нового (etlmaupd)
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
*  Ограничения:
*     нет
*
******************************************************************
*  Пример использования:
*     %resource_attr_update (
*        mpResourceCode=MY_RESOURCE, mpVersion=333,
*        mpArchRowsNo=12345,
*        mpNotFound=ADD);
*
******************************************************************
*  08-09-2014  Нестерёнок     Начальное кодирование
******************************************************************/

%macro mq_attr_update (
   mpMsgId                 =  ,
   mpCopyRc                =  NOCHG,
   mpSourceCode            =  NOCHG,
   mpTargetCode            =  NOCHG,
   mpFileName              =  NOCHG,
   mpFTPServerName         =  NOCHG,
   mpFilePath              =  NOCHG,
   mpNotFound              =  NOP,
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
         %let mpConnection = etlmaupd;
         %&ETL_DBMS._connect(mpLoginSet=ETL_SYS, mpAlias=&mpConnection);
      %end;

      /* Получаем имена в СУБД */
      %local lmvMqDbms lmvMqAttrDbms;
      %&ETL_DBMS._table_name (mpSASTable=ETL_SYS.MQ_MSG,  mpOutFullNameKey=lmvMqDbms);
      %&ETL_DBMS._table_name (mpSASTable=ETL_SYS.MQ_MSG_ATTR,  mpOutFullNameKey=lmvMqAttrDbms);

      %local lmvObs;
      /* Находим родительские записи в протоколе */
      %let lmvObs = 0;
      select cnt into :lmvObs
         from connection to &mpConnection (
            select count(*) cnt
            from &lmvMqDbms
            where msg_id = %&ETL_DBMS._number(&mpMsgId)
         )
      ;
      %error_check (mpStepType=SQL_PASS_THROUGH);

      /* Если не находим, то ошибка */
      %if &lmvObs eq 0 %then %do;
         %job_event_reg (
            mpEventTypeCode=MQ_NOT_FOUND,
            mpEventValues= %bquote(mpMsgId="&mpMsgId")
         );
      %end;

      /* Находим запись в протоколе атрибутов */
      %let lmvObs = 0;
      select cnt into :lmvObs
         from connection to &mpConnection (
            select count(*) cnt
            from &lmvMqAttrDbms
            where msg_id = %&ETL_DBMS._number(&mpMsgId)
         )
      ;
      %error_check (mpStepType=SQL_PASS_THROUGH);

      /* Если не находим */
      %if &lmvObs eq 0 %then %do;
         %if &mpNotFound eq ERR %then %do;
            %job_event_reg (
               mpEventTypeCode=MQ_NOT_FOUND,
               mpEventValues= %bquote(mpMsgId="&mpMsgId")
            );
         %end;
         %else %if (&mpNotFound eq ADD) %then %do;
            %mq_attr_add (
               mpMsgId=&mpMsgId,
               %if &mpCopyRc ne NOCHG %then %do;
                  mpCopyRc=&mpCopyRc,
               %end;
               %if &mpSourceCode ne NOCHG %then %do;
                  mpSourceCode=&mpSourceCode,
               %end;
               %if &mpTargetCode ne NOCHG %then %do;
                  mpTargetCode=&mpTargetCode,
               %end;
               %if &mpFileName ne NOCHG %then %do;
                  mpFileName=&mpFileName,
               %end;
               %if &mpFTPServerName ne NOCHG %then %do;
                  mpFTPServerName=&mpFTPServerName,
               %end;
               %if &mpFilePath ne NOCHG %then %do;
                  mpFilePath=&mpFilePath,
               %end;
               mpConnection=&mpConnection
            );
         %end;
         %goto exit;
      %end;

      /* Если нечего обновлять */
      %if (&mpCopyRc eq NOCHG) and (&mpSourceCode eq NOCHG) and (&mpTargetCode eq NOCHG) and
          (&mpFileName eq NOCHG) and (&mpFTPServerName eq NOCHG) and (&mpFilePath eq NOCHG)
      %then %do;
         %job_event_reg (
            mpEventTypeCode=UNEXPECTED_ARGUMENT,
            mpEventValues= %bquote(Ни один из атрибутов не требуется обновлять (mpMsgId=&mpMsgId) )
         );
         %goto exit;
      %end;

      /* Обновляем записи в протоколе */
      execute (
         update &lmvMqAttrDbms set
            %if &mpCopyRc ne NOCHG %then %do;
               copy_rc_no        = %&ETL_DBMS._number(&mpCopyRc)
            %end;
            %if &mpCopyRc ne NOCHG and &mpSourceCode ne NOCHG %then %do;
               ,
            %end;
            %if &mpSourceCode ne NOCHG %then %do;
               source_system_cd  = %&ETL_DBMS._string(&mpSourceCode)
            %end;
            %if &mpSourceCode ne NOCHG and &mpTargetCode ne NOCHG %then %do;
               ,
            %end;
            %if &mpTargetCode ne NOCHG %then %do;
               target_system_cd  = %&ETL_DBMS._string(&mpTargetCode)
            %end;
            %if &mpTargetCode ne NOCHG and &mpFileName ne NOCHG %then %do;
               ,
            %end;
            %if &mpFileName ne NOCHG %then %do;
               file_nm           = %&ETL_DBMS._string(&mpFileName)
            %end;
            %if &mpFileName ne NOCHG and &mpFTPServerName ne NOCHG %then %do;
               ,
            %end;
            %if &mpFTPServerName ne NOCHG %then %do;
               ftp_server_nm     = %&ETL_DBMS._string(&mpFTPServerName)
            %end;
            %if &mpFTPServerName ne NOCHG and &mpFilePath ne NOCHG %then %do;
               ,
            %end;
            %if &mpFilePath ne NOCHG %then %do;
               ftp_file_path_txt = %&ETL_DBMS._string(&mpFilePath)
            %end;
         where
            msg_id = %&ETL_DBMS._number(&mpMsgId)

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
%mend mq_attr_update;
