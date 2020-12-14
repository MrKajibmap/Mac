/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для генерации CSV файлов для DP
*
*  ПАРАМЕТРЫ:
*     mpResourceNm=dm_abt.plan_gc_month
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
*    %dp_generate_csv(mpResourceNm=dm_abt.plan_gc_month);
*
****************************************************************************
*  27-08-2020  Борзунов     Начальное кодирование
****************************************************************************/

%macro dp_generate_csv(mpResourceNm=plan_upt_days,
						mpPath=/data/tmp/);

	%local lmvOutLibref lmvOutTabName;
	%member_names (mpTable=&mpResourceNm, mpLibrefNameKey=lmvOutLibref, mpMemberNameKey=lmvOutTabName);
	%if %sysfunc(exist(&mpResourceNm.)) %then %do;
		proc export data=&lmvOutLibref..&lmvOutTabName.(datalimit=all)
					outfile="&mpPath.&lmvOutTabName..csv"
					dbms=dlm
					replace
					;
					delimiter='|'
					;
		run;
	%end;
	%else %do;
		%put "WARNING: Input table &mpResourceNm. does not exist. Please verify input parameters.";
		%return;
	%end;

%mend dp_generate_csv;