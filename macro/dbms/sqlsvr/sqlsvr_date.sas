%macro sqlsvr_date(mpSASDate);
   %if (%is_blank(mpSASDate)) or (&mpSASDate eq .) %then %do;
      NULL
   %end;
   %else %do;
      convert (datetime,%unquote(%str(%')%sysfunc(strip(%sysfunc(putn(&mpSASDate, B8601DN.))))%str(%')), 112)
   %end;
%mend sqlsvr_date;