/*****************************************************************
* ВЕРСИЯ:
*     $Id: 2469bc042a0dae10dff32c1d80b30c0a89f441bb $
*
******************************************************************
* НАЗНАЧЕНИЕ:
*     Копирует файл путем поблочной перезаписи.
*     Работает в глобальном режиме или внутри DATA STEP.
*
* ПАРАМЕТРЫ:
*     mpInFileName      -  имя входного файла
*                          Если не указано, mpInFileRef должен быть уже назначен
*     mpInFileRef       +  fileref входного файла
*                          по умолчанию _fin
*     mpOutFileName     -  имя выходного файла
*                          Если не указано, mpOutFileRef должен быть уже назначен
*     mpOutFileRef      +  fileref выходного файла
*                          по умолчанию _fout
*     mpBlockSize       -  размер блока в байтах
*                          по умолчанию 1024
*     mpMaxSize         -  копируемый размер в байтах
*                          по умолчанию не ограничен
*     mpStopOnWS        -  ограничить копирование точно побайтно (N) или на границе слова (Y)
*                          по умолчанию N
*
******************************************************************
*  Использует:
*     unique_id
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1. Если макрос получает на вход fileref-ы, то обязанность определения корректного режима чтения из них лежит
*        на вызывающем процессе.
*
******************************************************************
*  ПРИМЕР ИСПОЛЬЗОВАНИЯ:
*    %file_copy (mpInFileName="source.zip", mpOutFileName="target.zip");
*
******************************************************************
*  18-08-2014  Нестерёнок     Начальное кодирование
*  29-10-2018  Нестерёнок     Добавлены mpMaxSize, mpStopOnWS
******************************************************************/


%macro file_copy (
   mpInFileName            =  "",
   mpInFileRef             =  "_fin",
   mpOutFileName           =  "",
   mpOutFileRef            =  "_fout",
   mpBlockSize             =  1024,
   mpMaxSize               =  ,
   mpStopOnWS              =  N
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

   /* Режимы работы */
   %local lmvStopOnSize;
   %let lmvStopOnSize = %eval (not %is_blank(mpMaxSize));

   /* assign filerefs */
   if lengthn(&mpInFileName) gt 0 then do;
      rc = filename(&mpInFileRef,  &mpInFileName,  "DISK");
      if rc ne 0 then do;
         rc = log4sas_error ("cwf.macro.file_copy", catx (" ", "Cannot assign fileref to file", &mpInFileName));
         goto &lmvExitLabel;
      end;
   end;
   if lengthn(&mpOutFileName) gt 0 then do;
      rc = filename(&mpOutFileRef, &mpOutFileName, "DISK");
      if rc ne 0 then do;
         rc = log4sas_error ("cwf.macro.file_copy", catx (" ", "Cannot assign fileref to file", &mpOutFileName));
         goto &lmvExitLabel;
      end;
   end;

   /* open files */
   fidi = fopen(&mpInFileRef, "I", &mpBlockSize, "B");
   if fidi le 0 then do;
      rc = log4sas_error ("cwf.macro.file_copy", catx (" ", "Cannot open fileref", &mpInFileRef, "for input"));
      goto &lmvExitLabel;
   end;
   fido = fopen(&mpOutFileRef, "O", &mpBlockSize, "B");
   if fido le 0 then do;
      rc = log4sas_error ("cwf.macro.file_copy", catx (" ", "Cannot open fileref", &mpOutFileRef, "for output"));
      goto &lmvExitLabel;
   end;
   drop fidi fido;

   /* Временные переменные */
   %local lmvBlock lmvLeftSize lmvRLen lmvPLen;
   %let lmvBlock     =  block_&lmvUID.;
   %let lmvLeftSize  =  left_size_&lmvUID.;
   %let lmvRLen      =  rlen_size_&lmvUID.;
   %let lmvPLen      =  plen_&lmvUID.;

   /* copy file by block */
   length &lmvBlock $&mpBlockSize;
%if &lmvStopOnSize %then %do;
      &lmvLeftSize = &mpMaxSize;
      drop &lmvLeftSize;
%end;
   do while(fread(fidi) = 0);
      &lmvRLen = frlen (fidi);
%if &lmvStopOnSize %then %do;
      if &lmvLeftSize lt &lmvRLen then
         &lmvRLen = &lmvLeftSize;
%end;

      rc = fget(fidi, &lmvBlock, &lmvRLen);
      &lmvPLen = &lmvRLen;
%if &lmvStopOnSize and &mpStopOnWS = Y %then %do;
      if &lmvLeftSize le &lmvRLen then do;
         &lmvPLen = findc(&lmvBlock, "", "S", -&lmvRLen);
         &lmvPLen = findc(&lmvBlock, "", "KS", -&lmvPLen);
         /* could be that last part is WS only */
         if &lmvPLen = 0 then leave;
      end;
%end;
      rc = fput(fido, putc(&lmvBlock, "$varying", &lmvPLen));
      rc = fwrite(fido);
%if &lmvStopOnSize %then %do;
      if &lmvLeftSize lt &lmvRLen then leave;
      &lmvLeftSize = &lmvLeftSize - &lmvRLen;
%end;
   end;
   drop &lmvBlock &lmvRLen &lmvPLen;

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
%mend file_copy;