/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 59469d238ae0f0857af69e5bf053c95c089b3f71 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Исполняет системную команду, выводит в лог ее вывод.
*     Работает в глобальном режиме или внутри DATA STEP.
*
*  ПАРАМЕТРЫ:
*     mpCommand               +  текст команды
*     mpResultKey             +  имя (макро)переменной, в которую будет помещен системный код возврата
*     mpGrabLog               -  перенаправлять (Y) или нет (N) вывод команды в лог
*     mpMsgKey                -  имя макропеременной, в которую будет помещена выведенная строка #mpMsgLineNo
*     mpMsgLineNo             -  номер строки лога, которая будет возвращена
*                                по умолчанию 1, т.е. первая выведенная строка
*     mpWaitOption            -  wait (ждать окончания работы команды) или nowait (не ждать)
*                                по умолчанию wait
*
******************************************************************
*  Использует:
*     XCMD
*     job_event_reg
*     file_temp
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     Если mpGrabLog=Y, то не следует использовать многостроковые команды, перенаправление вывода будет некорректно.
*     Если mpGrabLog=N, то
*        -  в Windows вывод команды не показывается в логе
*        -  mpMsgKey не заполняется
*     В режиме внутри DATA STEP mpGrabLog=Y недоступен.
*
******************************************************************
*  Пример использования:
*     %global cmdres cmdmsg;
*     %sys_command (
*        mpCommand=     mv a.sas b.sas,
*        mpResultKey=   cmdres,
*        mpMsgKey=      cmdmsg,
*        mpWaitOption=  nowait
*     );
*     %put cmdres=&cmdres cmdmsg=&cmdmsg;
*
******************************************************************
*  26-08-2013  Нестерёнок     Начальное кодирование
*  10-04-2014  Нестерёнок     Добавлен mpMsgLineNo
*  15-04-2014  Нестерёнок     Добавлен режим внутри DATA STEP
******************************************************************/

%macro sys_command (
   mpCommand=,
   mpResultKey=,
   mpGrabLog=Y,
   mpMsgKey=,
   mpMsgLineNo=1,
   mpWaitOption=wait
);
   /* Проверка среды */
   %local lmvIsDataStep;
   %let lmvIsDataStep = %eval (&SYSPROCNAME eq DATASTEP);

   %if %sysfunc(getoption(XCMD)) = NOXCMD %then %do;
      %if &lmvIsDataStep %then %do;
         %job_event_reg (mpEventTypeCode  =  "XCMD_CALL",
                         mpLevel          =  "E",
                         mpEventDesc      =  "Установлена опция NOXCMD",
                         mpEventValues    =  &mpCommand );
      %end;
      %else %do;
         %job_event_reg (mpEventTypeCode  =  XCMD_CALL,
                         mpLevel          =  E,
                         mpEventDesc      =  %bquote(Установлена опция NOXCMD),
                         mpEventValues    =  %bquote(&mpCommand) );
      %end;
      %return;
   %end;

   /* Проверка параметров */
   %local lmvCommand lmvResultKey lmvMsgKey;
   %let lmvCommand = &mpCommand;
   %if %is_blank(mpResultKey) %then %do;
      %if &lmvIsDataStep %then
         %let mpResultKey = sys_command_rc;
      %else
         %let mpResultKey = lmvResultKey;;
   %end;
   %if %is_blank(mpMsgKey) %then %do;
      %let mpMsgKey = lmvMsgKey;
   %end;
   %if &lmvIsDataStep %then
      %let mpGrabLog = Y;;

   /* Готовим лог */
   %if &mpGrabLog = Y %then %do;
      %file_temp (mpFileRef=cmdlog);
      %local lmvLogName;
      %let lmvLogName = %sysfunc(pathname(cmdlog));

      /* Это POSIX-синтаксис и должен выполняться и в Windows, и в Unix */
      %let lmvCommand = &lmvCommand 1> &lmvLogName 2>&1;
   %end;

   /* Выполняем */
   %if &lmvIsDataStep %then %do;
      &mpResultKey = system (&lmvCommand);
   %end;
   %else %do;
      systask command "%superq(lmvCommand)" &mpWaitOption shell status=&mpResultKey;
   %end;

   /* Получаем первую строку лога */
   %if &mpGrabLog = Y %then %do;
      data _null_;
         infile cmdlog;
         input;
         if _n_ = &mpMsgLineNo then call symputx("&mpMsgKey", _infile_);

         /* выводим лог из файла */
         put _infile_;
      run;

      /* Удаляем файл */
      %local lmvRC;
      %let lmvRC = %sysfunc(fdelete(cmdlog));
   %end;

   /* Протоколируем вызов XCMD */
   %job_event_reg (mpEventTypeCode=XCMD_CALL,
                   mpEventValues= %bquote(&mpCommand (rc=&&&mpResultKey)) );
%mend sys_command;