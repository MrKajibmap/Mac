/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 1cba4788fac8ca0a379606df1c634049c66adadc $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Получает имя файла, являющегося выходом указанного шага трансформации DIS.
*     Если параметры заданы неверно, выходные переменные не обновляются.
*
*  ПАРАМЕТРЫ:
*     mpStepId             -  FQID шага трансформации
*                             по умолчанию &transformID, сгенерированный DIS
*     mpOutFileNameKey     -  имя макропеременной, в которую возвращается имя выходного файла
*     mpOutFileIndex       -  порядковый номер порта выходного файла
*                             по умолчанию 1
*     mpOutConnectionKey   -  имя макропеременной, в которую возвращаются параметры подключения
*
******************************************************************
*  ИСПОЛЬЗУЕТ:
*     нет
*
*  УСТАНАВЛИВАЕТ МАКРОПЕРЕМЕННЫЕ:
*     нет
*
******************************************************************
*  ПРИМЕР ИСПОЛЬЗОВАНИЯ:
*    %local lmvMyFile;
*    %dis_target_file (mpStepId=A5DZGBZE.AO0000ZV, mpOutFileNameKey=lmvMyFile);
*    %put lmvMyFile=&lmvMyFile;
*
******************************************************************
*  17-05-2013  Нестерёнок     Начальное кодирование
*  23-10-2014  Нестерёнок     Добавлен mpOutConnectionKey
******************************************************************/

%macro dis_target_file (
   mpStepId                =  &transformID,
   mpOutFileNameKey        =  ,
   mpOutFileIndex          =  1,
   mpOutConnectionKey      =
);
   data _null_;
      length stepURI transURI extTableURI fileURI name $256 rc 8;
      call missing (stepURI, transURI, extTableURI, fileURI, name);

      /* stepURI указывает на шаг трансформации */
      stepURI = "omsobj:&mpStepId";

      /* получаем связанный файл в fileURI */
      rc = metadata_getnasn(stepURI, "Transformations", 1, transURI);
      link checkRC;
      rc = metadata_getnasn(transURI, "ClassifierTargets", &mpOutFileIndex, extTableURI);
      link checkRC;
      rc = metadata_getnasn(extTableURI, "OwningFile", 1, fileURI);
      link checkRC;

      /* возвращаем результат */
%if not %is_blank(mpOutFileNameKey) %then %do;
      rc = metadata_getattr (fileURI, "FileName", name);
      link checkRC;
      call symput ("&mpOutFileNameKey", cats(name));
%end;
%if not %is_blank(mpOutConnectionKey) %then %do;
      length serverURI connURI domainURI $256 port $8 domain $32;
      call missing (serverURI, connURI, domainURI, port, domain);

      rc = metadata_getnasn(fileURI, "DeployedComponents", 1, serverURI);
      link checkRC;
      rc = metadata_getnasn(serverURI, "SourceConnections", 1, connURI);
      link checkRC;
      rc = metadata_getattr (connURI, "HostName", name);
      link checkRC;
      rc = metadata_getattr (connURI, "Port", port);
      link checkRC;
      rc = metadata_getnasn(connURI, "Domain", 1, domainURI);
      link checkRC;
      rc = metadata_getattr (domainURI, "Name", domain);
      link checkRC;
      call symput ("&mpOutConnectionKey", catt("host=""", name, """ port=", port, " authdomain=""", domain, """") );
%end;
      goto exit;

   checkRC:
      if rc lt 0 then do;
         rc = log4sas_error ("dwf.macro.dis_target_file", catx (" ", sysmsg(), ", rc=", rc) );
         goto exit;
      end;
      else
         return;
   exit:
      stop;
   run;
%mend dis_target_file;