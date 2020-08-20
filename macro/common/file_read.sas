/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 6c7ce43f37bda08c314c9174bbe4696010fdf16c $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Читает содержимое файла в макропеременную.
*     Работает в глобальном режиме или внутри DATA STEP.
*
*  ПАРАМЕТРЫ:
*     mpInFileName      -  имя входного файла
*                          Если не указано, mpInFileRef должен быть уже назначен
*     mpInFileRef       +  fileref входного файла
*                          по умолчанию _fin
*     mpOutKey          +  имя макропеременной, в которую в будет помещено содержимое файла
*                          по умолчанию lmvFileContents
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
*    %local lmvMyFile;
*    %file_read (mpInFileName="file.txt", mpOutKey=lmvMyFile);
*    %put &lmvMyFile;
*
******************************************************************
* 23-05-2012   Нестерёнок  Начальное кодирование
* 31-08-2012   Нестерёнок  Рефактор mpMode
******************************************************************/


%macro file_read (mpInFileName="", mpInFileRef="_fin", mpOutKey=lmvFileContents, mpLrecl=1000);
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
         rc = log4sas_error ("cwf.macro.file_read", catx (" ", "Cannot assign fileref to file", &mpInFileName));
         goto &lmvExitLabel;
      end;
   end;

   /* open files */
   fidi = fopen(&mpInFileRef, "I");
   if fidi le 0 then do;
      rc = log4sas_error ("cwf.macro.file_read", catx (" ", "Cannot open fileref", &mpInFileRef, "for input"));
      goto &lmvExitLabel;
   end;
   drop fidi;

   /* read file */
   length lmvLine $&mpLrecl lmvText $32000;
   do while(fread(fidi) = 0);
      rc = fget(fidi, lmvLine, &mpLrecl);
      lmvText = cats (lmvText, lmvLine);
   end;
   drop lmvLine lmvText;

   &lmvExitLabel:
   /* close resources */
   rc = fclose(fidi);
   if lengthn(&mpInFileName) gt 0 then
      rc = filename(&mpInFileRef, "");
   call symput ("&mpOutKey", cats(lmvText));

   /* Закрываем data step, если сами его открыли */
   %if &lmvIsNotDataStep %then %do;
      run;
   %end;
%mend file_read;