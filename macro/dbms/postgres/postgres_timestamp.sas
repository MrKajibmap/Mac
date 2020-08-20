%macro postgres_timestamp (mpSASDatetime);
   %if (%is_blank(mpSASDatetime)) or (&mpSASDatetime eq .) %then %do;
      NULL
   %end;
   %else %do;
/*  Функция to_timestamp() не проваливается в запрос к партициям, а литерал - да */
      TIMESTAMP %unquote(%str(%')%sysfunc(putn(&mpSASDatetime, e8601dt.))%str(%'))
   %end;
%mend;