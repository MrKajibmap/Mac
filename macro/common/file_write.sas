/*****************************************************************
*  ВЕРСИЯ:
*     $Id: dbee9e0c7c9f4b3ad5aabb95975795d13b70d80d $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Записывает содержимое макропеременной в файл.
*     Работает в глобальном режиме или внутри DATA STEP.
*
*  ПАРАМЕТРЫ:
*     mpInKey           +  имя макропеременной, из которой берется содержимое файла
*                          по умолчанию lmvFileContents
*     mpOutFileName     -  имя выходного файла
*                          Если не указано, mpOutFileRef должен быть уже назначен
*     mpOutFileRef      +  fileref выходного файла
*                          по умолчанию _fout
*     mpLrecl           -  максимальная длина строки
*                          по умолчанию 1000
*
******************************************************************
*  Использует:
*     unique_id
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*    %local lmvMyFile;
*    %let lmvMyFile = Bla-bla-blah;
*    %file_write (mpInKey=lmvMyFile, mpOutFileName="file.txt");
*
******************************************************************
* 23-05-2012   Нестерёнок  Начальное кодирование
* 31-08-2012   Нестерёнок  Рефактор mpMode
******************************************************************/


%macro file_write (mpInKey=lmvFileContents, mpOutFileName="", mpOutFileRef="_fout", mpLrecl=1000);
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
   if lengthn(&mpOutFileName) gt 0 then do;
      rc = filename(&mpOutFileRef, &mpOutFileName, "DISK", "lrecl=&mpLrecl");
      if rc ne 0 then do;
         rc = log4sas_error ("cwf.macro.file_write", catx (" ", "Cannot assign fileref to file", &mpOutFileName));
         goto &lmvExitLabel;
      end;
   end;

   /* open files */
   fido = fopen(&mpOutFileRef, "O");
   if fido le 0 then do;
      rc = log4sas_error ("cwf.macro.file_write", catx (" ", "Cannot open fileref", &mpOutFileRef, "for output"));
      goto &lmvExitLabel;
   end;
   drop fido;

   /* write file */
   length lmvLine $&mpLrecl;
   lmvLine = symget("&mpInKey");
   rc = fput(fido, lmvLine);
   rc = fwrite(fido);

   &lmvExitLabel:
   /* close resources */
   rc = fclose(fido);
   if lengthn(&mpOutFileName) gt 0 then
      rc = filename(&mpOutFileRef, "");

   /* Закрываем data step, если сами его открыли */
   %if &lmvIsNotDataStep %then %do;
      run;
   %end;
%mend file_write;