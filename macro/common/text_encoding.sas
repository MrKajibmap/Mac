/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для перекодировки текстовых переменных
*
*  ПАРАМЕТРЫ:
*     mpTable - таблица в которой хотим заненить текстовую переменную
*	  mpVariable - название текстовой переменной
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
*    %text_encoding(mpTable=public.product_dictionary_ml, mpVariable=a_hero);
*
****************************************************************************
*  23-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro text_encoding(mpTable=, mpVariable=);

	proc casutil;
		droptable incaslib="casuser" casdata="encoding_&mpVariable." quiet;
	run;

	proc fedsql sessref=casauto;
		create table casuser.unique{options replace=true} as
		select distinct
		&mpVariable
		from
		&mpTable. 
		;
	quit;

	data work.unique;
		set casuser.unique;
	run;

	data work.encoding_&mpVariable.;
		set work.unique;
		&mpVariable._id = _N_;
	run;

	data casuser.encoding_&mpVariable.;
		set work.encoding_&mpVariable.;
	run;

	proc fedsql sessref = casauto;
		create Table casuser.&mpTable.{options replace=true} as 
		select
		t1.*,
		t2.&mpVariable._id
		from
		&mpTable. as t1
		left join
		casuser.encoding_&mpVariable. as t2
		on
		t1.&mpVariable = t2.&mpVariable
		;
	quit;

	proc casutil;
		promote casdata="encoding_&mpVariable." incaslib="casuser" outcaslib="casuser";
	run;

%mend text_encoding;