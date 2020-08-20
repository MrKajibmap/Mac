/*****************************************************************
*  ВЕРСИЯ:
*     $Id: ce21e3f3b2325ad93acacda14ebee882bc1dd7f5 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Назначает указанный fileref на временный файл.
*
*  ПАРАМЕТРЫ:
*     mpFileRef               -  fileref выходного файла
*                                по умолчанию tmp
*     mpTempVar               -  переменная среды, из которой берется путь для создания файла
*                                по умолчанию TMP (Windows) или TMPDIR (Unix)
*                                Если переменная среды не задана, файл создается в папке пользователя
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
*    %file_temp (mpFileRef=tmpFile);
*    data _null_;
*       file tmpFile;
*       ...
*
******************************************************************
*  26-08-2013  Нестерёнок     Начальное кодирование
******************************************************************/

%macro file_temp (
   mpFileRef=tmp,
   mpTempVar=
);
   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Получаем папку, в которой создается файл */
   %if %is_blank(mpTempVar) %then %do;
      %if &SYS_OS_FAMILY = WIN %then %do;
         %let mpTempVar = TMP;
      %end;
      %else %if &SYS_OS_FAMILY = UNIX %then %do;
         %let mpTempVar = TMPDIR;
      %end;
   %end;

   %local lmvTmpFolder;
   %if not %is_blank(mpTempVar) %then %do;
      %if &SYS_OS_FAMILY = WIN %then %do;
         %if %sysfunc(sysexist(&mpTempVar)) %then
            %let lmvTmpFolder = %sysget(&mpTempVar)\;
         %else
            %let lmvTmpFolder = %sysget(USERPROFILE)\;
      %end;
      %else %if &SYS_OS_FAMILY = UNIX %then %do;
         %if %sysfunc(sysexist(&mpTempVar)) %then
            %let lmvTmpFolder = %sysget(&mpTempVar)/;
         %else
            %let lmvTmpFolder = %sysget(HOME)/;
      %end;
   %end;

   /* Назначаем fileref */
   filename &mpFileRef "&lmvTmpFolder.&lmvUID..tmp";
%mend file_temp;