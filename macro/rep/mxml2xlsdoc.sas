/***********************************************************
* DESCRIPTION:
*   mXML2XlsDoc - Макрос предназначен для преобразования файла XML (.xml)
*        в файл Word Document (.doc), Excel Workbook (.xls) или Microsoft Excel WorkSheet (.xlsx)
*
* PARAMS:
*    mpType             -  doc | xls | xlsx - тип преобразования, в .doc, .xls  или в .xlsx
*    mpinFile           -  путь и имя исходного файла xml (в кавычках) или имя объявленного исходного файла xml
*    mpoutFile          -  путь и имя выходного файла (в кавычках) или имя объявленного выходного файла
*                          (имя файла должно обязательно содержать расширение, иначе оно будет автоматически дописано)
*    mpResultVar        -  название макропеременной, в которой будет лежать код результата выполнения макроса
*                          Значение по умолчанию: RESULT
*                          Макропеременная может принимать следующие значения:
*                             0 - преобразование произведено корректно
*                             1 - ошибка при открытии xml (некорректный xml)
*                             2 - ошибка при преобразовании в указанный формат
*                             3 - ошибка при закрытии файла
*
* GLOBAL MACRO VARIABLES:
*  template_path
*
* EXTERNAL MACRO:
*
* Примеры использования:
*   %mXML2XlsDoc(mpType=xls, mpinFile=fileref1, mpoutFile=fileref2);
*  %mXML2XlsDoc(mpType=doc, mpinFile='C:\Data\Temp\Инициирующая_СЗ_tmp.xml', mpoutFile='C:\Data\Temp\output.doc');
*
*****************************************************************
* 27.02.2015, Tischenko - Initial coding
* 05.06.2015, Uryupina - вызов wscript заменен на cscript
* 19.06.2015, Uryupina - добавлено преобразование в формат xlsx, файл xlsx2xls.vbs заменен на xml2xls.vbs
* 31.07.2015, Uryupina - добавлена обработка ошибок и параметр mpResultVar
****************************************************************/
%macro mXML2XlsDoc(mpType=, mpinFile=, mpoutFile=, mpResultVar=RESULT);

%IF not(%symexist(&mpResultVar.)) %THEN %DO;
   %GLOBAL &mpResultVar.;
%END;
%let &mpResultvar.=0;

%let mpVbsWordPath=%str("&TEMPLATE_PATH.\VBS\xml2doc.vbs");
%let mpVbsXLSPath=%str("&TEMPLATE_PATH.\VBS\xml2xls.vbs");
%let mpVbsXLSXPath=%str("&TEMPLATE_PATH.\VBS\xml2xlsx.vbs");

/*Если указан путь в VBS-скрипту, то используем его. */
%if %upcase(&mpType)=XLS %then %do;
  %let mpVbsPath=&mpVbsXLSPath;
%end;
%else %if %upcase(&mpType)=XLSX %then %do;
  %let mpVbsPath=&mpVbsXLSXPath;
%end;
%else %if %upcase(&mpType)=DOC %then %do;
  %let mpVbsPath=&mpVbsWordPath;
%end;
%else %do; /*Если не указан тип*/
  %let mvErrMsg=Не указан тип выходного файла;
  %goto EXIT_MACRO;
%end;

/*Проверка существования &mpVbsPath*/
%if %sysfunc(fileexist(&mpVbsPath))>0 %then %do;
  %let mpVbsPath=%sysfunc(dequote(&mpVbsPath));
%end;
%else %do;
  %let mvErrMsg=%sysfunc(sysmsg());
  %goto EXIT_MACRO;
%end;

/*Проверка, является ли параметр mpinFile путем к файлу или fileref, проверка на существование файла*/
%if %sysfunc(nvalid(&mpinFile))=1 and %length(&mpinFile)<=8 %then %do;
  %if %sysfunc(fileref(&mpinFile))=0 %then %do;
    %let xmlpath=%sysfunc(pathname(&mpinFile));
  %end;
  %else %do;
    %let mvErrMsg=%sysfunc(sysmsg());
    %goto EXIT_MACRO;
  %end;
%end;
%else %do;
  %if %sysfunc(fileexist(&mpinFile))>0 %then %do;
    %let xmlpath=%sysfunc(dequote(&mpinFile));
  %end;
  %else %do;
    %let mvErrMsg=%sysfunc(sysmsg());
    %goto EXIT_MACRO;
  %end;
%end;

/*Проверка, является ли параметр mpoutFile путем к файлу или fileref*/
%if %sysfunc(nvalid(&mpoutFile))=1 and %length(&mpoutFile)<=8 %then %do;
  %if %sysfunc(fileref(&mpoutFile))<=0 %then %do;
    %let outpath=%sysfunc(pathname(&mpoutFile));
  %end;
  %else %do;
    %let mvErrMsg=%sysfunc(sysmsg());
    %goto EXIT_MACRO;
  %end;
%end;
%else %do;
  %let outpath=%sysfunc(dequote(&mpoutFile));
%end;

/*Преобразование XML в DOC или XLS*/
data _null_;
  X " cscript.exe ""&mpVbsPath."" ""&xmlpath."" ""&outpath."" ";
run;

%let &mpResultVar.=&sysrc.;

%IF (&sysrc. > 0) %THEN %DO;
   %IF (&sysrc. = 1) %THEN %DO;
      %let mvErrMsg = %bquote(ОШИБКА: При открытии входного файла произошла ошибка);
   %END;
   %ELSE %IF (&sysrc. = 2) %THEN %DO;
      %let mvErrMsg = %bquote(ОШИБКА: При конвертации файла в указанный формат произошла ошибка);
   %END;
   %ELSE %IF (&sysrc. = 3) %THEN %DO;
      %let mvErrMsg = %bquote(ОШИБКА: При закрытии выходного файла произошла ошибка);
   %END;
   %GOTO EXIT_MACRO;
%END;

%let mvErrMsg=;
%EXIT_MACRO: %put &mvErrMsg;

%mend mXML2XlsDoc;

