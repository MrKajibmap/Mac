%macro postgres_date(mpSASDate);
   %if (%is_blank(mpSASDate)) or (&mpSASDate eq .) %then %do;
      NULL
   %end;
   %else %do;
/*  Функция to_date() не проваливается в запрос к партициям, а литерал - да */
   		DATE %unquote(%str(%')%sysfunc(putn(&mpSASDate, e8601da.))%str(%'))
   %end;
%mend;