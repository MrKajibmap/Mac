%macro postgres_number (mpText);
   %if (%is_blank(mpText)) or (&mpText eq .) %then %do;
      NULL
   %end;
   %else %do;
      &mpText
   %end;
%mend postgres_number;