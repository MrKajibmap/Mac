/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*	Необходимо подключить новую либу Оракл и указать ее имя 
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для исторической загрузки данных в DM_REP.VA_DATAMART_HIST.
*
*  ПАРАМЕТРЫ:
*     mpFileName			Путь до файла с тестовыми данными
*	  mpOutLibref			Наименование тестовой либы в Оракл
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
*     %load_test_cases_oracle(mpFileName=/data/tmp/Test_cases.xlsx, mpOutLibref=ora_tst);
*
****************************************************************************
*  18-06-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro load_test_cases_oracle(mpFileName=, mpOutLibref=);

	%local lmvFileName lmvLibref lmvOutLibref;
	%let lmvLibref = TST_CASE;
	%let lmvFileName = &mpFileName.;
	%let lmvOutLibref = &mpOutLibref.;

	libname lmvLibref XLSX "&lmvFileName.";

	proc sql noprint;
		select memname into :lmvTableList separated by ' '
		from SASHELP.VTABLE
		where upcase(libname) = "&lmvLibref."
		;
		select count(*) as cnt into :lmvCNT
		from SASHELP.VTABLE
		where upcase(libname) = "&lmvLibref."
		;
	quit;
	
	%do i=1 %to &lmvCNT.;
		%let lmvCurrentMem = %scan(&lmvTableList.,&i.,%str( ));
		/* DS case */
		data &lmvOutLibref..&lmvCurrentMem.;
			set &lmvLibref..&lmvCurrentMem.;
		run;
	%end;
	
	libname lmvLibref CLEAR;
	
%mend load_test_cases_oracle;