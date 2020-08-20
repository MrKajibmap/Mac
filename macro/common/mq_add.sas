/*****************************************************************
*  ВЕРСИЯ:
*     $Id: d838d3fa160d8a5f70080e6d94ec14b120967e9b $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Добавляет в протокол запись о сообщении MQ.
*     Работает в глобальном режиме или внутри PROC SQL.
*
*  ПАРАМЕТРЫ:
*     mpMsgKey                +  имя макропеременной, содержащей текст сообщения
*     mpJobId                 -  код процесса, который добавляет запись,
*                                по умолчанию &ETL_CURRENT_JOB_ID
*     mpOutMsgIdKey           -  имя макропеременной, в которую возвращается идентификатор сообщения
*
******************************************************************
*  Использует:
*     %error_check
*     %unique_id
*     sequence MQ_MSG_SEQ
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %mq_read (mpOutKey=lmvMqOut);
*     %mq_add (mpMsgKey=lmvMqOut, mpOutMsgIdKey=lmvMsgId);
*
******************************************************************
*  05-09-2014  Нестерёнок     Начальное кодирование
******************************************************************/

%macro mq_add (
   mpMsgKey             =  ,
   mpJobId              =  &ETL_CURRENT_JOB_ID,
   mpOutMsgIdKey        =
);
   /* Получение ID сообщения */
   %if %is_blank(mpOutMsgIdKey) %then %do;
      %local lmvMsgId;
      %let mpOutMsgIdKey = lmvMsgId;
   %end;
   %unique_id (mpOutKey=&mpOutMsgIdKey, mpSequenceName=MQ_MSG_SEQ);

   /* Открываем proc sql, если он еще не открыт */
   %local lmvIsNotSQL;
   %let lmvIsNotSQL = %eval (&SYSPROCNAME ne SQL);
   %if &lmvIsNotSQL %then %do;
      proc sql;
   %end;

      /* Делаем запись в протоколе */
      insert into ETL_SYS.MQ_MSG (
         msg_id,
         read_dttm, msg_txt, read_job_id)
      values(
         &&&mpOutMsgIdKey,
         "%sysfunc(datetime(), datetime.)"dt,
         %sysfunc(quote(%superq(&mpMsgKey))),
         &mpJobId
      );
      %error_check (mpStepType=SQL);

   %if &lmvIsNotSQL %then %do;
      quit;
   %end;
%mend mq_add;
