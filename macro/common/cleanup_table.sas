/*****************************************************************
 *  ВЕРСИЯ:
 *     $Id: ca45a47540ace1faeeb3058e59cf0ae33bdc2d0a $
 *
 ******************************************************************
 *  НАЗНАЧЕНИЕ:
 *     Макрос для удаления устаревших данных из таблиц
 *
 *  ПАРАМЕТРЫ:
 *     mpLib             	+  название библиотеки для очистки
 *     mpTab                +  название таблицы для очистки
 *     mpNewTab             +  название временной таблицы, в которую будут сохраняться очищенные данные
 *     mpType               +  тип удаления: по версиям или по дате
 *     mpVersionCnt         -  количество версий, которые надо сохранить в таблице (пусто для mpType=date)
 *     mpPeriodMin          -  минимальная дата интервала, который сохраняется в таблице (пусто для mpType=version). Тип DATE
 *     mpPeriodMax          -  максимальная дата интервала, который сохраняется в таблице (пусто для mpType=version). Тип DATE
 *
 ******************************************************************
 *  Использует:
 *     %is_blank
 *     %member_exists
 *
 *  Устанавливает макропеременные:
 *     нет
 *
 *  Ограничения:
 *     Во всех таблицах библиотеках должно быть не пустое поле VERSION_ID
 *
 ******************************************************************
 *  Пример использования:
 *     %cleanup_table(mpLib=ETL_IA,mpTab=fctr_promo_mon_aggr,mpNewTab=tmp01,mpType=date,mpColumn=PERIOD_START_DT,mpVersionCnt=,mpPeriodMin=intnx("week.2",&etl_current_date-1,-26),mpPeriodMax=);
 *
 ******************************************************************
 *  06-03-2017 Куликовский, начальная версия
 *  27-03-2017 Михайлова, добавлена таблица параметров
 *  05-04-2017 Михайлова, цикл вынесен во внешний джоб
 *  14-04-2017 Морозов: добавлен mpDirection
 *  19-04-2017 Морозов: добавлена проверка на отсутствие оригинальной таблицы
 *  19-04-2017 Михайлова: заменено использование %error_check на обработку &SYSRC, &SYSCC, &SYSERR
 *                        добавлена обработка &mpDepth функцией %CMPRES в where data option (обход ошибки ERROR 22-322: Missing ')' parenthesis for data set option list)
 *  11-05-2017 Михайлова: исправление механизма отбора версий и изменение проверки ошибочного значения &SYSCC
 *  16-11-2017 Михайлова: разделенны параметры глубины хранения данных для типов version и date. Добавлен механизма удаления данных и по нижней границе периода и по верхней одновременно. Удален параметр mpDirection.
 ******************************************************************/

/*Макрос для очистки одной из таблиц библиотеки mpLib. 
Запускается в цикле по всем таблицам библиотеки mpLib. 
Оставляет mpVersionCnt версиий*/
%macro cleanup_table(mpLib=,mpTab=,mpNewTab=,mpType=,mpColumn=,mpVersionCnt=,mpPeriodMin=,mpPeriodMax=);

	%macro comment;
	%mend comment;
	
	%let mpLib=&mpLib;
	%let mpTab=&mpTab;
	%let mpNewTab=&mpNewTab;
	%let mpType=&mpType;
	%let mpColumn=&mpColumn;
	%let mpVersionCnt=&mpVersionCnt;
	%let mpPeriodMin=&mpPeriodMin;
	%let mpPeriodMax=&mpPeriodMax;
	%local lmvVersionList; /*Список версий*/
	%local lmvMinVersion; /*Минимальная из версий для сохранения*/
	
	/*Удаляем &mpLib..&mpNewTab. если она уже существовала*/
	%if %member_exists (&mpLib..&mpNewTab.) %then %do;
		proc delete data=&mpLib..&mpNewTab.;
		run;
	%end;
	
	/*Проверяем существование таблицы*/
	%if %member_exists (&mpLib..&mpTab.) %then %do;
	
		/*Если таблица версионная, то получаем список версий и минимальную для сохранения*/
		%if &mpType.=version %then %do;			
			proc sql noprint;
				select distinct 
					&mpColumn. format=best32.
				into
					:lmvVersionList separated by ' '
				from 
					&mpLib..&mpTab.
				order by 
					&mpColumn. desc
				;
			quit;
			
			%if %sysfunc(countw(&lmvVersionList))>=&mpVersionCnt. %then %do;
				%let lmvMinVersion=%sysfunc(scan(%quote(&&lmvVersionList),&mpVersionCnt.));
			%end;
			%else %do;
				%let lmvMinVersion=%sysfunc(scan(%quote(&&lmvVersionList),-1));
			%end;
		%end;
	
		/*Сохраняем только нужные записи*/
		proc append 
			base=&mpLib..&mpNewTab.
			data=&mpLib..&mpTab.
			%if &mpType.=version %then %do;
				%if not %is_blank(lmvVersionList) %then %do;
					(where=(&mpColumn. >= &lmvMinVersion.))
				%end;
			%end;
			%if &mpType.=date and (not %is_blank(mpPeriodMin) OR not %is_blank(mpPeriodMax)) %then %do;
			    (where=(
				%if not %is_blank(mpPeriodMin) %then %do;
					&mpColumn. >= %CMPRES(&mpPeriodMin.)
				%end;
				%if not %is_blank(mpPeriodMin) AND not %is_blank(mpPeriodMax) %then %do;
				  and
				%end;
				%if not %is_blank(mpPeriodMax) %then %do;
					&mpColumn. <= %CMPRES(&mpPeriodMax.)
				%end;
				))
			%end;
		;
		run;
	
	  %if &SYSRC ne 0 or &SYSCC > 4 or &SYSERR ne 0 %then %do;
	    %let lmvErrorMsg=Ошибка при отборе сохраняемых строк из &mpLib..&mpTab..;
	    %goto EXIT;
	  %end;

	
		/*Удаляем старую таблицу*/
		proc delete data=&mpLib..&mpTab.;
		run;
	
	  %if &SYSRC ne 0 or &SYSCC > 4 or &SYSERR ne 0 %then %do;
	    %let lmvErrorMsg=Ошибка при удалении старого набора &mpLib..&mpTab.;
	    %goto EXIT;
	  %end;
		
		/*Переименовываем новую таблицу*/
		proc datasets lib=&mpLib. nolist;
			change &mpNewTab.=&mpTab.;
		quit;
	
	  %if &SYSRC ne 0 or &SYSCC  > 4 or &SYSERR ne 0 %then %do;
	    %let lmvErrorMsg=Ошибка при переименовании нового набора &mpLib..&mpNewTab.;
	    %goto EXIT;
	  %end;
	%end;
	%else %do;
		%let lmvErrorMsg=Таблица &mpLib..&mpTab. не существует;
		%goto exit;
	%end;
		
  %return;
	%EXIT:
    %put ERROR: &lmvErrorMsg;
		
%mend cleanup_table;