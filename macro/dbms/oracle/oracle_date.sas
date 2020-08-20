%macro oracle_date(mpSASDate);
   %if (%is_blank(mpSASDate)) or (&mpSASDate eq .) %then %do;
      NULL
   %end;
   %else %do;
      to_date(%unquote(%str(%')
         %sysfunc(putn(&mpSASDate, yymmddn8.))
      %str(%'),'YYYYMMDD'))
   %end;
%mend;