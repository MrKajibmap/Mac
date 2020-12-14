/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Создание модели недельного профиля
*		Для разбивки GC по дням и переагрегации недель до месяцев
*
*  ПАРАМЕТРЫ:
*     mpOutWpGc		- выходная таблица wp_gc (по умолчанию dm_abt)
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
*     %vf_train_week_profile(mpOutWpGc=dm_abt.wp_gc);
*
****************************************************************************
*  02-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf_train_week_profile(mpOutWpGc=dm_abt.wp_gc);
	/*Создать cas-сессию, если её нет*/
	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;

	%local lmvOutLibrefWpGc lmvOutTabNameWpGc;
	%member_names (mpTable=&mpOutWpGc, mpLibrefNameKey=lmvOutLibrefWpGc, mpMemberNameKey=lmvOutTabNameWpGc);
	
	/*0. Удаление целевых таблиц */
	proc casutil;
		droptable casdata="&lmvOutTabNameWpGc." incaslib="&lmvOutLibrefWpGc." quiet;
	run;
	
	/*пропорции дней по числу заказов в магазине
	за 8 последних недель*/
	proc fedsql sessref=casauto noprint;
		create table casuser.gc_days{options replace=true} as
		select t1.PBO_LOCATION_ID
				,t1.CHANNEL_CD
				,case 
					when weekday(t1.SALES_DT)>1 
					then weekday(t1.SALES_DT)-1 else 7 
				end as weekday
				,sum(t1.receipt_QTY) as prop
		from casuser.pbo_sales t1
		where SALES_DT<=date %tslit(&VF_HIST_END_DT) 
				and sales_dt>=intnx('week.2',date %tslit(&VF_HIST_END_DT),-7)
		group by 1,2,3
		;
	quit;
	
	proc fedsql sessref=casauto;
		create table casuser.gc_days_sum{options replace=true} as
			select t1.PBO_LOCATION_ID
					,t1.CHANNEL_CD
					,sum(prop) as s_prop
			from casuser.gc_days t1
			group by 1,2
		;
		create table casuser.gc_days_prop{options replace=true} as
			select t1.PBO_LOCATION_ID
					,t1.CHANNEL_CD
					,weekday
					,case
						when s_prop>0
						then prop/s_prop 
						else 1/7
					end as pr_wkday
			from casuser.gc_days t1 
			inner join casuser.gc_days_sum t2
				on t1.pbo_location_id=t2.pbo_location_id 
				and t1.channel_cd=t2.channel_cd
		;
	quit;
	proc cas;
	transpose.transpose /
	   table={name="gc_days_prop", caslib="casuser", groupby={"channel_cd","PBO_LOCATION_ID"}} 
	   attributes={{name="channel_cd"},{name="PBO_LOCATION_ID"} }
	   transpose={"pr_wkday"} 
	   prefix="prday_" 
	   id={"weekday"} 
	   casout={name="&lmvOutTabNameWpGc.", caslib="casuser", replace=true};
	quit;
	
	data casuser.&lmvOutTabNameWpGc.(replace=yes);
		length prday_1 prday_2 prday_3 prday_4 prday_5 prday_6 prday_7 8;
		call missing(prday_1, prday_2, prday_3, prday_4, prday_5, prday_6, prday_7);
		set casuser.&lmvOutTabNameWpGc.;
	run;
	
	proc casutil;
	    promote casdata="&lmvOutTabNameWpGc." incaslib="casuser" outcaslib="&lmvOutLibrefWpGc.";
		save incaslib="&lmvOutLibrefWpGc." outcaslib="&lmvOutLibrefWpGc." casdata="&lmvOutTabNameWpGc." casout="&lmvOutTabNameWpGc..sashdat" replace;
	run;
%mend vf_train_week_profile;
 