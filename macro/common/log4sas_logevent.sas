/*****************************************************************
*  ВЕРСИЯ:
*     $Id: ad7e3a6f35e283f5fecc1a4be183cfcdb2584eb7 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Логирует сообщение указанным уровнем.
*
*  ПАРАМЕТРЫ:
*     mpName                  +  имя логгера
*     mpLevel                 +  уровень (TRACE, DEBUG, INFO, WARN, ERROR, FATAL)
*     mpMessage               +  сообщение
*
******************************************************************
*  Использует:
*     %is_blank
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     нет
*
******************************************************************
*  Пример использования:
*     %log4sas_logevent (my.logger, WARN, mpName is null);;
*
******************************************************************
*  16-04-2018  Нестерёнок     Начальное заимствование
******************************************************************/

%macro log4sas_logevent(
   mpName,
   mpLevel,
   mpMessage
);
   %if %is_blank(mpName) %then %do;
      %log4sas_error (cwf.macro.log4sas_logevent, mpName is null);
      %return;
   %end;
   %if %is_blank(mpLevel) %then %do;
      %log4sas_error (cwf.macro.log4sas_logevent, mpLevel is null);
      %return;
   %end;
   %if %is_blank(mpMessage) %then %do;
      %log4sas_warn (cwf.macro.log4sas_logevent, mpMessage is null);
      %return;
   %end;

   %local lmvRc;
   %let lmvRc = %sysfunc(log4sas_logevent(&mpName, &mpLevel, &mpMessage));
%mend log4sas_logevent;
