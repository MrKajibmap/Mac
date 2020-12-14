/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для очистки библиотеки (sas/cas)
*
*  ПАРАМЕТРЫ:
*     mpCaslibNm 		- Наименование либы для очищения
*	
******************************************************************
*  Использует: 
*	  нет
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*    %tech_clean_lib(mpCaslibNm=casuser);
*
****************************************************************************
*  24-08-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro tech_clean_lib(mpCaslibNm=casuser);
	
	%local lmvCnt lmvCaslibNm lmvMemName;
	%let lmvCaslibNm = %sysfunc(upcase(&mpCaslibNm.));
	
	%if %sysfunc(SESSFOUND(casauto)) = 0 %then %do; 
		cas casauto;
		caslib _all_ assign;
	%end;

	data work.mems_list;
		set sashelp.vstable(where=(libname="&lmvCaslibNm."));
	run;

	proc sql noprint;
		select count(*) as cnt into :lmvCnt
		from work.mems_list
		;
	quit;

	%if &lmvCnt.>0 %then %do;
		%do i=1 %to &lmvCnt.;
			data _NULL_;
				set work.mems_list(firstobs=&I. obs=&I.);
				call symputx('lmvMemName', memname);
			run;
			
			proc casutil;
				droptable casdata="&lmvMemName." incaslib="&lmvCaslibNm." quiet;
			run;
		%end;
	%end;
	%else %do;
		%put "WARNING: Input parameter mpCaslibNm=&lmvCaslibNm. is invalid or caslib "&lmvCaslibNm." is empty.";
		%return;
	%end;

%mend tech_clean_lib;