/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 64b1f8a36abc6660d2de48c19dc36e261c3edf44 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Обновляет в протоколе запись о сообщении MQ.
*
*     Все значения атрибутов обновляются, даже если переданы пустыми.
*     Для того, чтобы не менять значение атрибута, укажите значение NOCHG.
*
*  ПАРАМЕТРЫ:
*     mpMsgId                 +  идентификатор обновляемого сообщения
*     mpMsgCorrect            -  атрибут - флаг корректности формата сообщения (Y/N)
*     mpResourceId            -  атрибут - идентификатор ресурса
*     mpVersion               -  атрибут - версия ресурса
*     mpDate                  -  атрибут - бизнес-дата, соответствующая данной версии
*     mpNotFound              -  если запись не найдена:
*                                NOP - ничего не делать
*                                ERR - сообщить об ошибке
*                                ADD - добавить новую запись
*                                по умолчанию NOP, т.е. ничего не делать
*     mpConnection            -  если указано, использовать это подключение вместо установки нового (etlmupd)
*
******************************************************************
*  Использует:
*     %error_check
*     %ETL_DBMS_*
*     %job_event_reg
*     %mq_add
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     нет
*
******************************************************************
*  Пример использования:
*     %mq_update (
*        mpMsgId=12345,
*        mpMsgCorrect=Y, mpDate=NOCHG, mpResourceId=11210, mpVersion=67890,
*        mpNotFound=ERR);
*
******************************************************************
*  08-09-2014  Нестерёнок     Начальное кодирование
******************************************************************/

%macro mq_update (
   mpMsgId                 =  ,
   mpMsgCorrect            =  NOCHG,
   mpResourceId            =  NOCHG,
   mpVersion               =  NOCHG,
   mpDate                  =  NOCHG,
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
         %let mpConnection = etlmupd;
         %&ETL_DBMS._connect(mpLoginSet=ETL_SYS, mpAlias=&mpConnection);
      %end;

      /* Получаем имена в СУБД */
      %local lmvMqDbms;
      %&ETL_DBMS._table_name (mpSASTable=ETL_SYS.MQ_MSG,  mpOutFullNameKey=lmvMqDbms);

      %local lmvObs;
      /* Находим запись(-и) в протоколе */
      %let lmvObs = 0;
      select cnt into :lmvObs
         from connection to &mpConnection (
            select count(*) cnt
            from &lmvMqDbms
            where msg_id = %&ETL_DBMS._number(&mpMsgId)
         )
      ;
      %error_check (mpStepType=SQL_PASS_THROUGH);

      /* Если не находим */
      %if &lmvObs eq 0 %then %do;
         %if &mpNotFound eq ERR %then %do;
            %job_event_reg (mpEventTypeCode=MQ_NOT_FOUND,
                            mpEventValues= %bquote(mpMsgId="&mpMsgId") );
         %end;
         %else %if (&mpNotFound eq ADD) %then %do;
            %mq_add (mpMsgKey=mpMsgId, mpOutMsgIdKey=mpMsgId);
         %end;
         %goto exit;
      %end;

      /* Обновляем запись в протоколе */
      execute (
         update &lmvMqDbms set
            %if &mpMsgCorrect ne NOCHG %then %do;
               format_correct_flg   = %&ETL_DBMS._string(&mpMsgCorrect)
            %end;
            %if &mpMsgCorrect ne NOCHG and &mpResourceId ne NOCHG %then %do;
               ,
            %end;
            %if &mpResourceId ne NOCHG %then %do;
               resource_id          = %&ETL_DBMS._number(&mpResourceId)
            %end;
            %if &mpResourceId ne NOCHG and &mpVersion ne NOCHG %then %do;
               ,
            %end;
            %if &mpVersion ne NOCHG %then %do;
               version_id           = %&ETL_DBMS._number(&mpVersion)
            %end;
            %if &mpVersion ne NOCHG and &mpDate ne NOCHG %then %do;
               ,
            %end;
            %if &mpDate ne NOCHG %then %do;
               available_dttm       = %&ETL_DBMS._timestamp(&mpDate)
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
%mend mq_update;
