/*****************************************************************
*  ВЕРСИЯ:
*     $Id: f1ed22c45e57ae7e84a14ff4bcaa1637efdcdda6 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Этот макрос может понадобиться только в редком случае необходимости доступа
*     к полю SQL Server с типом datetime.
*     В других СУБД (как и в стандарте) такого типа данных не существует.
*
******************************************************************/

%macro sqlsvr_datetime(mpSASDatetime);
   %if (%is_blank(mpSASDatetime)) or (&mpSASDatetime eq .) %then %do;
      NULL
   %end;
   %else %do;
      convert(datetime, %unquote(%str(%')%sysfunc(strip(%sysfunc(putn(&mpSASDatetime, B8601DT19.3))))%str(%')), 126)
   %end;
%mend sqlsvr_datetime;