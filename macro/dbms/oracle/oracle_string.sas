%macro oracle_string(mpText);
  %if %is_blank(mpText) %then %do;
     NULL
  %end;
  %else %do;
     %unquote(%str(%')%qsysfunc(trim(&mpText))%str(%'))
  %end;
%mend oracle_string;