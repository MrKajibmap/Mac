%macro db2_string(mpText);
  %if %is_blank(mpText) %then %do;
     NULL
  %end;
  %else %do;
     %unquote(%str(%')%trim(&mpText)%str(%'))
  %end;
%mend db2_string;