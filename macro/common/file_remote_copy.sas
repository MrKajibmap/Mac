/*****************************************************************
* ВЕРСИЯ:
*     $Id: 4fbc3c5724d88ffd67217043e9eb45fd3b3ec8f6 $
*
******************************************************************
* НАЗНАЧЕНИЕ:
*     Копирует файл через SCP.
*     Работает в глобальном режиме или внутри DATA STEP.
*
* ПАРАМЕТРЫ:
*     mpInFileName      +  имя входного файла, в формате [[user@]server:][filepath]filename
*     mpOutFileName     +  имя выходного файла, в формате [[user@]server:][filepath]filename
*     mpResultKey       -  имя макропеременной, в которую будет помещен системный код исполнения
*
******************************************************************
* ИСПОЛЬЗУЕТ:
*     %sys_command
*
* УСТАНАВЛИВАЕТ МАКРОПЕРЕМЕННЫЕ:
*     нет
*
******************************************************************
* ПРИМЕР ИСПОЛЬЗОВАНИЯ:
*    %file_remote_copy (mpInFileName="myftpserver:/folder1/folder2/file.txt", mpOutFileName="localfolder/file.txt");
*
******************************************************************
* 23-05-2012   Нестерёнок  Начальное кодирование
* 31-08-2012   Нестерёнок  Рефактор mpMode
******************************************************************/


%macro file_remote_copy (mpInFileName=, mpOutFileName=, mpResultKey=);
   %local lmvIsDataStep;
   %let lmvIsDataStep = %eval (&SYSPROCNAME eq DATASTEP);

   /* Выполняем копирование */
   %if &lmvIsDataStep %then %do;
      %sys_command (
         mpCommand      =  catx (" ", "scp", &mpInFileName, &mpOutFileName),
         mpResultKey    =  &mpResultKey
      );
   %end;
   %else %do;
      %local lmvCopyMsg;
      %sys_command (
         mpCommand      =  %bquote(scp &mpInFileName &mpOutFileName),
         mpResultKey    =  &mpResultKey,
         mpMsgKey       =  lmvCopyMsg
      );
   %end;

   /* Проверяем исполнение */
   %if &&&mpResultKey ne 0 %then %do;
      %if &lmvIsDataStep %then %do;
         %job_event_reg (mpEventTypeCode  =  "XCMD_FAILED",
                         mpEventValues    =  "scp exited with rc=&&&mpResultKey" );
      %end;
      %else %do;
         %job_event_reg (mpEventTypeCode  =  XCMD_FAILED,
                         mpEventValues    =  %bquote(&lmvCopyMsg) );
      %end;
   %end;
%mend file_remote_copy;