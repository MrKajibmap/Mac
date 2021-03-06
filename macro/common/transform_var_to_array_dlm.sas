/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос разворачивает переменную  mpVar в список элементов массива, разделенных запятой 
*
*  ПАРАМЕТРЫ:
*     mpVar - переменная для преобразования в массив
*	  mpIndex - индекс
*	  mpStart - начало обработки в рамках строки 
*	  mpEnd - конец обработки в рамках строки
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
*    %%transform_var_to_array_dlm(mpVar=sum_qty,mpIndex=t,mpStart=%eval(&lag),mpEnd=%eval(&lag+&window-1));
*
****************************************************************************
*  24-07-2020  Борзунов     Начальное кодирование
****************************************************************************/

/* Макрос разворачивает переменную mpVar в список элементов массива, разделенных запятой */
%macro rtp_argt(mpVar,mpIndex,mpStart,mpEnd);
	%do indx=&mpStart. %to &mpEnd.;
		&mpVar.[&mpIndex.-&indx.]
		%if &indx. ne &mpEnd. %then %do;
			,
		%end;
	%end;
%mend rtp_argt;