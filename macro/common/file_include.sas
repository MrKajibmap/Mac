/*****************************************************************
*  ВЕРСИЯ:
*     $Id: e5a381b72a66732fa29f0accd6a67af9c43d97e3 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Подключает внешний файл с SAS-кодом.
*
*  ПАРАМЕТРЫ:
*     mpFullPath              +  имя файла в двойных кавычках
*
******************************************************************
*  Пример использования:
*     %file_include("&ETL_DATA_ROOT/EGProjectsCode/TEST.sas");
*
******************************************************************
*  28-12-2017  Михайлова      Начальное кодирование
******************************************************************/
%macro file_include (
   mpFullPath
);
   %if %sysfunc(fileexist(&mpFullPath)) %then %do;
      %log4sas_debug (cwf.macro.file_include, Including %bquote(&mpFullPath));
      %include &mpFullPath;
   %end;
   %else %do;
      %log4sas_error (cwf.macro.file_include, The external file %bquote(&mpFullPath) does not exist);
      %etl_stop;
   %end;
%mend file_include;
