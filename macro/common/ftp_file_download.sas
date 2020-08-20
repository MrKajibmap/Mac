/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 2c8028cb48bbbf3af5fabd7e92870ed879ba537f $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Скачивает указанный файл с FTP-сервера.
*
*  ПАРАМЕТРЫ:
*     mpLoginSet              +  имя набора параметров подключения к FTP (LTS и т.п.)
*     mpInFileName            +  имя входного файла, относительно FTP-сервера
*     mpOutFileName           +  имя выходного файла, относительно локальной ФС
*
******************************************************************
*  Пример использования:
*     %ftp_file_download(mpLoginSet=LTS,
*        mpInFileName=&FTP_FILE_PATH_TXT./&REMOTE_FILE_NM,
*        mpOutFileName=&ETL_FILE_INPUT_ROOT/LTS/&FILE_NM
*     );
*
******************************************************************
*  01-06-2013  Пропирный      Начальное кодирование
*  11-06-2013  Пильчин        Замена параметров FTP на CONNECT_OPTIONS
*  08-11-2013  Нестерёнок     Замена CONNECT_OPTIONS на mpLoginSet
*  26-12-2013  Нестерёнок     Добавлен mpInFileEncoding
******************************************************************/

%macro ftp_file_download (mpLoginSet=, mpInFileName=, mpInFileEncoding=, mpOutFileName=);
   /* Отключение вывода в лог */
   %log_disable;

   %if (not %symexist(&mpLoginSet._CONNECT_OPTIONS)) %then
   %do;
      %log_enable;
      %log4sas_warn (dwf.macro.ftp_file_download, Login credentials for set &mpLoginSet are not defined);
      %return;
   %end;

   /* Подключение к FTP */
   filename ifile ftp "&mpInFileName" %unquote(&&&mpLoginSet._CONNECT_OPTIONS) lrecl=1000
      %if not %is_blank(mpInFileEncoding) %then
         encoding="&mpInFileEncoding";
   ;
   /* Сбор ошибок */
   %error_check;

   /* Восстановление вывода в лог */
   %log_enable;

   /* Передача файла */
   filename ofile "&mpOutFileName" lrecl=1000;

   data _null_;
     length str $1000;
     infile ifile truncover lrecl=1000;
     input str $1000.;
     file ofile lrecl=1000;
     put str;
   run;
   %error_check (mpEventTypeCode=FTP_READ_FAILED);
%mend ftp_file_download;