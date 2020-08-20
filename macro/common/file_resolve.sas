/*****************************************************************
*  ВЕРСИЯ:
*     $Id: cecc2f124389ea6d539128dd84bf0c9da426ddf3 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Копирует файл, разрешая встречающиеся макропеременные.
*     Работает в глобальном режиме или внутри DATA STEP.
*
*  ПАРАМЕТРЫ:
*     mpInFileName      -  имя входного файла
*                          Если не указано, mpInFileRef должен быть уже назначен
*     mpInFileRef       +  fileref входного файла
*                          по умолчанию _fin
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
*  Ограничения:
*     1. Макрос предназначен для чтения текстовых файлов.
*        Строки сливаются в одну, отбрасывая CR/LF и другие whitespaces в конце каждой строки.
*
******************************************************************
*  Пример использования:
*     %file_resolve (mpInFileName="unresolved.txt", mpOutFileName="resolved.txt");
*
******************************************************************
* 22-12-2011   Нестерёнок  Начальное кодирование
* 23-05-2012   Нестерёнок  Добавил mpMode
* 31-08-2012   Нестерёнок  Рефактор mpMode
******************************************************************/

%macro file_resolve (mpInFileName="", mpInFileRef="_fin", mpOutFileName="", mpOutFileRef="_fout", mpLrecl=1000);
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
   if lengthn(&mpInFileName) gt 0 then do;
      rc = filename(&mpInFileRef,  &mpInFileName,  "DISK", "lrecl=&mpLrecl");
      if rc ne 0 then do;
         rc = log4sas_error ("cwf.macro.file_resolve", catx (" ", "Cannot assign fileref to file", &mpInFileName));
         goto &lmvExitLabel;
      end;
   end;
   if lengthn(&mpOutFileName) gt 0 then do;
      rc = filename(&mpOutFileRef, &mpOutFileName, "DISK", "lrecl=&mpLrecl");
      if rc ne 0 then do;
         rc = log4sas_error ("cwf.macro.file_resolve", catx (" ", "Cannot assign fileref to file", &mpOutFileName));
         goto &lmvExitLabel;
      end;
   end;

   /* open files */
   fidi = fopen(&mpInFileRef, "I");
   if fidi le 0 then do;
      rc = log4sas_error ("cwf.macro.file_resolve", catx (" ", "Cannot open fileref", &mpInFileRef, "for input"));
      goto &lmvExitLabel;
   end;
   fido = fopen(&mpOutFileRef, "O");
   if fido le 0 then do;
      rc = log4sas_error ("cwf.macro.file_resolve", catx (" ", "Cannot open fileref", &mpOutFileRef, "for output"));
      goto &lmvExitLabel;
   end;
   drop fidi fido;

   /* resolve file */
   length lmvLine $&mpLrecl;
   do while(fread(fidi) = 0);
      rc = fget(fidi, lmvLine, &mpLrecl);
      lmvLine = resolve(lmvLine);
      rc = fput(fido, lmvLine);
      rc = fwrite(fido);
   end;
   drop lmvLine;

   &lmvExitLabel:
   /* close resources */
   rc = fclose(fidi);
   if lengthn(&mpInFileName) gt 0 then
      rc = filename(&mpInFileRef, "");
   rc = fclose(fido);
   if lengthn(&mpOutFileName) gt 0 then
      rc = filename(&mpOutFileRef, "");

   /* Закрываем data step, если сами его открыли */
   %if &lmvIsNotDataStep %then %do;
      run;
   %end;
%mend file_resolve;