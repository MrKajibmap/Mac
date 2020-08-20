/*****************************************************************
* ВЕРСИЯ:
*     $Id: 4440589a9170c8ceb08528155a8edc7d872d74c8 $
*
******************************************************************
* НАЗНАЧЕНИЕ:
*     Удаляет файл.
*     Работает в глобальном режиме или внутри DATA STEP.
*
* ПАРАМЕТРЫ:
*     mpFileName        -  имя входного файла
*                          Если не указано, mpInFileRef должен быть уже назначен
*     mpFileRef         +  fileref входного файла
*                          по умолчанию _fdel
*
******************************************************************
*  Использует:
*     нет
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     нет
*
******************************************************************
*  ПРИМЕР ИСПОЛЬЗОВАНИЯ:
*    %file_delete (mpFileName="source.zip");
*
******************************************************************
*  29-10-2018  Нестерёнок     Начальное кодирование
******************************************************************/


%macro file_delete (
   mpFileName              =  "",
   mpFileRef               =  "_fdel"
);
   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Открываем data step, если он еще не открыт */
   %local lmvIsNotDataStep lmvExitLabel;
   %let lmvIsNotDataStep = %eval (&SYSPROCNAME ne DATASTEP);
   %let lmvExitLabel = exit_&lmvUID;
   %if &lmvIsNotDataStep %then %do;
      data _null_;
   %end;

   /* assign filerefs */
   if lengthn(&mpFileName) gt 0 then do;
      rc = filename(&mpFileRef,  &mpFileName,  "DISK");
      if rc ne 0 then do;
         rc = log4sas_error ("cwf.macro.file_delete", catx (" ", "Cannot assign fileref to file", &mpFileName));
         goto &lmvExitLabel;
      end;
   end;

   /* delete file */
   rc = fdelete(&mpFileRef);
   if rc ne 0 then do;
      if lengthn(&mpFileName) gt 0 then do;
         rc = log4sas_error ("cwf.macro.file_delete", catx (" ", "Cannot delete file", &mpFileName));
      end;
      else do;
         rc = log4sas_error ("cwf.macro.file_delete", catx (" ", "Cannot delete fileref", &mpFileRef));
      end;
      goto &lmvExitLabel;
   end;

   &lmvExitLabel:
   /* free resources */
   if lengthn(&mpFileName) gt 0 then
      rc = filename(&mpFileRef, "");

   /* Закрываем data step, если сами его открыли */
   %if &lmvIsNotDataStep %then %do;
      run;
   %end;
%mend file_delete;
