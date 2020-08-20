/*****************************************************************
* ВЕРСИЯ:
*   $Id: 7945fb797e44c659fa29822d01a5967bc1923972 $
*
******************************************************************
* НАЗНАЧЕНИЕ:
*   Отправляет сообщение в очередь IBM MQ.
*
* ПАРАМЕТРЫ:
*   mpQueueManager      - имя менеджера очереди, по умолчанию &MQ_QUEUE_MANAGER
*   mpQueue             - имя очереди, по умолчанию &MQ_QUEUE
*   mpMsgKey            - имя макропеременной, содержимое которой отправляется
*                         по умолчанию lmvMqMsg
*   mpMqCompletionKey   - имя макропеременной, в которую будет помещен результат исполнения:
*                         0   - успех, сообщение отправлено
*                         <> 0 - ошибка
*   mpMqReasonKey       - имя макропеременной, в которую будет помещен код ошибки
*
******************************************************************
* Использует:
*     error_check
*     rcSetDS
*
* Устанавливает макропеременные:
*     нет
*
******************************************************************
* Пример использования:
   %local lmvMqOut lmvMqCompCode lmvMqReason;
   %mq_write;
   %if &lmvMqCompCode = 0 %then ...
*
******************************************************************
* 23-05-2012   Нестерёнок  Начальное кодирование
******************************************************************/

%macro mq_write (mpQueueManager=&MQ_QUEUE_MANAGER, mpQueue=&MQ_QUEUE, mpMsgKey=lmvMqMsg, mpMqCompletionKey=lmvMqCompCode, mpMqReasonKey=lmvMqReason);
   %macro rcSetDS(error);
      if &error gt input(symget('trans_rc'),12.) then
         call symput('trans_rc',trim(left(put(&error,12.))));
      if &error gt input(symget('job_rc'),12.) then
         call symput('job_rc',trim(left(put(&error,12.))));
   %mend rcSetDS;

   /* MQ Server and the SAS Server are on different machines  */
   %let MQMODEL=CLIENT;

   %let &mpMqCompletionKey = ;
   %let &mpMqReasonKey     = ;

   data _null_;
      /* length for parameters and options  */
      length etls_parms etls_options $ 256;

      /* SAS Variable to hold message to be sent to queue  */
      length etls_read $ 32000;
      etls_read = symget("&mpMsgKey");

      etls_inplength = lengthn(etls_read);

      /* Check for message truncation  */
      if (etls_inplength > 32000) then
      do;
         rc = log4sas_info ("dwf.macro.mq_write", "Message truncated.  Message length input by user is:32000");
         rc = log4sas_info ("dwf.macro.mq_write", catx (" ", "Minimum message length required is:", etls_inplength) );
      end;

      /* Connection handle obtained from MQCONN call  */
      length etls_hconn 8;

      /* Completion code  */
      length etls_compcode 8;

      /* Reason code  */
      length etls_qrc 8;

      /* Object descriptor handle obtained from MQOD call  */
      length etls_hod 8;

      /* Object handle obtained from MQOPEN call  */
      length etls_hobj 8;

      /* Put message options handle obtained from MQPMO call  */
      length etls_hpmo 8;

      /* Length statement for parameters  */
      length etls_MSGTYPE 8;

      /* Message descriptor handle obtained from MQMD call  */
      length etls_hmd 8;

      /* Map descriptor handle  */
      length etls_hmap 8;

      /* Descriptor  */
      length etls_desc $ 20;

      /* Data descriptor handle obtained from MQSETPARMS call  */
      length etls_hdata 8;

      /* Queue manager name  */
      etls_queuemanagername="&mpQueueManager";

      /* MQCONN: Connects base SAS to a MQSeries queue manager  */
      call mqconn(etls_queuemanagername, etls_hconn, etls_compcode, etls_qrc);

      /* MQCONN failure conditions  */
      if etls_compcode ^= 0 then
      do;
         %rcSetDS(8000);

         if etls_qrc=2002 then
         do;
            rc = log4sas_error ("dwf.macro.mq_write", catx (" ",
               "Reason Code 2002. Already connected to queue manager",
               "&mpQueueManager."
            ));
         end;
         else
         if etls_qrc=2018 then
         do;
            rc = log4sas_error ("dwf.macro.mq_write", catx (" ",
               "Reason Code 2018. Connection handle is invalid. A",
               "connection handle that is created by an MQCONN call must be used",
               "within the same DATA step where it was created."
            ));
         end;
         else
         if etls_qrc=2035 then
         do;
            rc = log4sas_error ("dwf.macro.mq_write", catx (" ",
               "Reason Code 2035. User is not authorized to perform",
               "the attempted action. Verify that you are connecting to the correct",
               "queue and queue manager. Verify that you are authorized to connect",
               "to the queue manager. If error is reported to a client connecting",
               "to a queue manager, you might need to set the user ID under the MCA",
               "tab in the server connection channel definition properties to a",
               "user ID that has permission to access the queue manager on the",
               "server machine."
            ));
         end;
         else
         if etls_qrc=2058 then
         do;
            rc = log4sas_error ("dwf.macro.mq_write", catx (" ",
               "Reason Code 2058. Error in Queue Manager Name",
               "&mpQueueManager. Check spelling and case of the queue manager",
               "name that is used in the application and is defined in the queue",
               "manager."
            ));
         end;
         else
         if etls_qrc=2059 then
         do;
            rc = log4sas_error ("dwf.macro.mq_write", catx (" ",
               "Reason Code 2059. Queue Manager &mpQueueManager is",
               "not available. Restart the queue manager."
            ));
         end;
         else
         do;
            rc = log4sas_error ("dwf.macro.mq_write", catx (" ",
               "Please refer to Websphere MQ Application Programming",
               "Reference (http://www-306.ibm.com/software/integration/wmq/library/).",
               etls_qrc
            ));
         end;

         etls_qmessage = sysmsg();
         rc = log4sas_error ("dwf.macro.mq_write", catx (" ", "MQCONN: Error connecting ", etls_qmessage) );
         goto etls_mqexit;
      end;

      /* Generate action  */
      etls_gen_mqod="GEN";
      /* Queue name  */
      etls_queuename="&mpQueue";
      etls_objectname="OBJECTNAME";

      /* Manipulates object descriptor parameters to be used on a subsequent  */
      /*  MQOPEN or MQPUT1 call                                               */
      call mqod(etls_hod, etls_gen_mqod, etls_qrc, etls_objectname, etls_queuename);

      if etls_qrc ^= 0 then
      do;
         %rcSetDS(8000);

         etls_qmessage = sysmsg();
         rc = log4sas_error ("dwf.macro.mq_write", catx (" ", "MQOD: failed ", etls_qmessage) );
         goto etls_mqexit;
      end;

      /* Options  */

      /* Establishes access to an MQSeries object (queue, process definition, or  */
      /*  queue manager)                                                          */
      etls_options="OUTPUT";

      call mqopen(etls_hconn, etls_hod, etls_options, etls_hobj, etls_compcode, etls_qrc);

      /* MQOPEN failure conditions  */
      if etls_compcode ^= 0 then
      do;
         %rcSetDS(8000);

         if etls_qrc=2085 then
         do;
            rc = log4sas_error ("dwf.macro.mq_write", catx (" ",
               "Reason Code 2085. Unknown Object Name. Check spelling",
               "and case of the queue name that is used in the application and is",
               "defined in the queue manager."
            ));
         end;
         else
         do;
            rc = log4sas_error ("dwf.macro.mq_write", catx (" ",
               "Please refer to Websphere MQ Application Programming",
               "Reference (http://www-306.ibm.com/software/integration/wmq/library/).",
               etls_qrc
            ));
         end;

         goto etls_mqexit;
      end;

      /* Generate action  */
      etls_gen_mqpmo="GEN";

      /* Manipulates MQSeries put message options to be used on a subsequent  */
      /*  MQPUT call                                                          */
      call mqpmo(etls_hpmo, etls_gen_mqpmo, etls_qrc);

      if etls_qrc ^= 0 then
      do;
         %rcSetDS(8000);

         etls_qmessage = sysmsg();
         rc = log4sas_error ("dwf.macro.mq_write", catx (" ", "MQPMO: failed ", etls_qmessage) );
         goto etls_mqexit;
      end;

      /* Generate action  */
      etls_gen_mqmd="GEN";
      etls_parms="MSGTYPE";
      etls_MSGTYPE=8;

      /* Manipulates message descriptor parameters to be used on a subsequent  */
      /*  MQPUT, MQPUT1 or MQGET call                                          */
      call mqmd(etls_hmd, etls_gen_mqmd, etls_qrc, etls_parms, etls_MSGTYPE);

      if etls_qrc ^= 0 then
      do;
         %rcSetDS(8000);

         etls_qmessage = sysmsg();
         rc = log4sas_error ("dwf.macro.mq_write", catx (" ", "MQMD: failed with reason code:", etls_qmessage) );
         goto etls_mqexit;
      end;

      etls_desc="CHAR,,32000";

      /* MQMAP: Defines a data map that can be subsequently used on a MQSETPARMS  */
      /*  or MQGETPARMS call                                                      */
      call mqmap(etls_hmap, etls_qrc, etls_desc);

      if etls_qrc ^= 0 then
      do;
         %rcSetDS(8000);

         etls_qmessage = sysmsg();
         rc = log4sas_error ("dwf.macro.mq_write", catx (" ", "MQMAP: failed with reason code:", etls_qmessage) );
         goto etls_mqexit;
      end;

      /* Creates a data descriptor that describes the actual base SAS variables   */
      /* along with an associated data mapping. This data descriptor can then be  */
      /*  used on a subsequent MQPUT or MQPUT1 call                               */
      call mqsetparms(etls_hdata, etls_hmap, etls_qrc, etls_read);

      if etls_qrc ^= 0 then
      do;
         %rcSetDS(8000);

         etls_qmessage = sysmsg();
         rc = log4sas_error ("dwf.macro.mq_write", catx (" ", "MQSETPARMS: failed ", etls_qmessage) );
         goto etls_mqexit;
      end;

      /* Puts a message on a MQSeries queue that has been previously opened  */

      call mqput(etls_hconn, etls_hobj, etls_hmd, etls_hpmo, etls_hdata, etls_compcode, etls_qrc);

      /* MQPUT failed  */
      if etls_compcode ^= 0 then
      do;
         %rcSetDS(8000);

         rc = log4sas_error ("dwf.macro.mq_write", catx (" ", "MQPUT failed with reason code:", etls_qrc) );
         goto etls_mqexit;
      end;

      etls_mqexit:
      call symputx("&mpMqCompletionKey", etls_compcode);
      call symputx("&mpMqReasonKey", etls_qrc);

      /* MQCLOSE: Relinquishes access to an MQSeries object (queue, queue  */
      /*  manager, process definition).                                    */
      /* Options  */
      etls_options="NONE";
      if (etls_hobj) then
      do;

         call mqclose(etls_hconn, etls_hobj, etls_options, etls_compcode, etls_qrc);

         if etls_qrc ^= 0 then
         do;
            %rcSetDS(8000);

            rc = log4sas_error ("dwf.macro.mq_write", catx (" ", "MQCLOSE: failed with reason code :", etls_qrc) );
            etls_qmessage = sysmsg();
            rc = log4sas_error ("dwf.macro.mq_write", catx (" ", "MQCLOSE failed :", etls_qmessage) );
         end;
      end;

      /* MQDISC: Breaks the connection between a MQSeries queue manager and base  */
      /*  SAS                                                                     */
      call mqdisc(etls_hconn, etls_compcode, etls_qrc);

      if etls_qrc ^= 0 then
      do;
         %rcSetDS(8000);

         rc = log4sas_error ("dwf.macro.mq_write", catx (" ", "MQDISC: failed with reason code :", etls_qrc) );
         etls_qmessage = sysmsg();
         rc = log4sas_error ("dwf.macro.mq_write", catx (" ", "MQDISC failed :", etls_qmessage) );
      end;
      if etls_hod ^=0 then
      do;
         /* Free object descriptor handle  */
         CALL MQFREE(etls_hod);
      end;
      if etls_hpmo ^=0 then
      do;
         /* Free message options handle  */
         CALL MQFREE(etls_hpmo);
      end;
      if etls_hmd ^=0 then
      do;
         /* Free message descriptor handle  */
         CALL MQFREE(etls_hmd);
      end;
      if etls_hmap ^=0 then
      do;
         /* Free map descriptor handle  */
         CALL MQFREE(etls_hmap);
      end;
   run;
   %error_check (mpEventTypeCode=MQ_WRITE_FAILED);

%mend mq_write;

