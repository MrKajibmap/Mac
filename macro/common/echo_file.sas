%macro echo_file(mpFileRef);
   data _null_;
      infile &mpFileRef;
      input;
      put _infile_;
   run;
%mend echo_file;