/**
 * Name: mCopyFile SAS macro
 *
 * Назначение:
 * Копирует один файл (или поток) в другой (или поток)
 *
 * Parameters:
 *    mpIn     +  входящий файл (fileref или путь)
 *    mpOut    -  выходной файл (fileref или путь). По умолчанию _WEBOUT
 * 	  mpInEnc  -  кодировка входного файла
 *	  mpOutEnc -  кодировка выходного файла
 *
 * Example:
 *   Ex1: Stream file to _WEBOUT
 *     %mCopyFile(mpIn="C:\Temp\1.html");
 *
 *   Ex2: Append to existing
 *     %mCopyFile(mpIn="C:\Temp\1.dat", mpOut="C:\Temp\2.dat" MOD);
 *
 * Version: 1.0
 * History:
 *   1.0 - 26MAR2014, Andrey Kuzenkov
 *         Initial version
 */

%macro mCopyFile (mpIn=, mpOut=_WEBOUT, mpInEnc=, mpOutEnc=, mpMode=text);
%IF &mpMode=text %THEN
%DO;
   data _null_;
      infile &mpIn
         %IF %kLENGTH(&mpInEnc.) > 0 %THEN %DO;
            encoding="&mpInEnc"
         %END;
         lrecl=16384
      ;
      file &mpOut
         %IF %kLENGTH(&mpOutEnc.) > 0 %THEN %DO;
            encoding="&mpOutEnc"
         %END;
         lrecl=16384
      ;
      input;
      put _infile_ /*$VARYING16384. bytes*/;
   run;
 %END;
 
 %ELSE %IF &mpMode=binary %THEN 
 %DO;
   DATA _NULL_;
      INFILE &mpIn RECFM=F LENGTH=bytes LRECL=16384;
      FILE &mpOut RECFM=N LRECL=16384;
      INPUT;
      PUT _INFILE_ $VARYING16384. bytes;
   RUN;
 %END;
%mend mCopyFile;
