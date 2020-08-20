%macro oracle_timestamp (mpSASDatetime);
   %if (%is_blank(mpSASDatetime)) or (&mpSASDatetime eq .) %then %do;
      NULL
   %end;
   %else %do;
      to_timestamp(%unquote(%str(%')%sysfunc(datepart(&mpSASDatetime), ddmmyyn8.) %sysfunc(timepart(&mpSASDatetime), time12.3)%str(%')), 'DDMMYYYY:HH24:MI:SS:FF3')
   %end;
%mend;