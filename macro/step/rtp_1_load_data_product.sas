/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки данных в CAS в рамках сквозного процесса для оперпрогноза (продукты)
*
*  ПАРАМЕТРЫ:
*     mpMode 		- Режим работы - S/T/A(Скоринг/Обучение/Обучение+скоринг)
*	  mpOutTrain	- выходная таблица набора для обучения
*	  mpOutScore	- выходная таблица набора для скоринга
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
*    %rtp_1_load_data_product(mpMode=S, mpOutScore=casuser.all_ml_scoring);
*	 %rtp_1_load_data_product(mpMode=T, mpOutTrain=casuser.all_ml_train);
*	 %rtp_1_load_data_product(mpMode=A, mpOutTrain=casuser.all_ml_train, mpOutScore=casuser.all_ml_scoring);
*
****************************************************************************
*  24-07-2020  Борзунов     Начальное кодирование
*  27-08-2020  Борзунов		Заменен источник данных на ETL_IA. Добавлена выгрузка на диск целевых таблиц
*  24-09-2020  Борзунов		Добавлена промо-разметка из ПТ
****************************************************************************/
%macro rtp_1_load_data_product(mpMode=A,
					 mpOutTrain=casshort.all_ml_train,
					 mpOutScore=casshort.all_ml_scoring,
					 mpWorkCaslib=casshort);

	options symbolgen mprint;
	
	%local lmvMode 
			lmvInLib
			lmvReportDttm 
			lmvStartDate 
			lmvEndDate 
			lmvLibrefOutTrain
			lmvTabNmOutTrain
			lmvLibrefOutScore
			lmvTabNmOutScore
			lmvWorkCaslib
			;
			
	%let lmvMode = &mpMode.;
	%let lmvInLib=ETL_IA;
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	%let lmvStartDateScore =%sysfunc(intnx(year,&etl_current_dt.,-1,s));
	%let lmvWorkCaslib = &mpWorkCaslib.;

	%if &lmvMode. = S %then %do;
		/*%let lmvStartDate =%eval(%sysfunc(intnx(year,&etl_current_dt.,-1,s))-91);*/
		%let lmvStartDate = %eval(%sysfunc(intnx(month,&etl_current_dt.,-15,s))-91);
		/* VF_HIST_END_DT_SAS = дата окончания истории = вчерашний день) */
		%let lmvEndDate = &VF_HIST_END_DT_SAS.;
		%let lmvScoreEndDate = %sysfunc(intnx(day,&VF_HIST_END_DT_SAS.,91,s));
	%end;
	%else %if &lmvMode = T or &lmvMode. = A %then %do;
		/*%let lmvStartDate = %eval(%sysfunc(intnx(year,&etl_current_dt.,-3,s))-91);*/
		%let lmvStartDate = %eval(%sysfunc(intnx(month,&etl_current_dt.,-15,s))-91);
		/*%let lmvEndDate = &VF_HIST_END_DT_SAS.;*/
		%let lmvEndDate = %sysfunc(intnx(year, %sysfunc(date()), -1, s));	
		%let lmvScoreEndDate = %sysfunc(intnx(day,&VF_HIST_END_DT_SAS.,91,s));
	%end;
	
	%member_names (mpTable=&mpOutTrain, mpLibrefNameKey=lmvLibrefOutTrain, mpMemberNameKey=lmvTabNmOutTrain);
%member_names (mpTable=&mpOutTrain, mpLibrefNameKey=lmvLibrefOutTrain, mpMemberNameKey=lmvTabNmOutTrain);
	%put &=lmvLibrefOutTrain;
	%put &=lmvTabNmOutTrain;
	
	%member_names (mpTable=&mpOutScore, mpLibrefNameKey=lmvLibrefOutScore, mpMemberNameKey=lmvTabNmOutScore);
	
	/****** 0. Объявление макропеременных ******/
	options mprint nomprintnest nomlogic nomlogicnest nosymbolgen mcompilenote=all mreplace;
	/*Создать cas-сессию, если её нет*/
	%if %sysfunc(SESSFOUND(casauto)) = 0 %then %do; 
		cas casauto;
		caslib _all_ assign;
	%end;
	
	/****** 1. Сбор "каркаса" из pmix ******/
	/* Сначала собираем справочник товаров для того, чтобы создать фильтр */
	proc casutil;
	  %if &lmvMode. = A or &lmvMode. = T %then %do;
		droptable casdata="&lmvTabNmOutTrain." incaslib="&lmvLibrefOutTrain." quiet;
	  %end;
	  %if &lmvMode. = A or &lmvMode. = S %then %do;
		droptable casdata="&lmvTabNmOutScore." incaslib="&lmvLibrefOutScore." quiet;
	  %end;
		droptable casdata="abt1_ml" incaslib="casuser" quiet;
	run;
	
	/* Подготовка таблицы с продажами */
	proc fedsql sessref=casauto; 
			create table casuser.abt1_ml{options replace=true} as
			select 
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty
			from (
				select 
					t1.PBO_LOCATION_ID,
					t1.PRODUCT_ID,
					t1.CHANNEL_CD,
					t1.SALES_Dt,
					(t1.SALES_QTY + t1.SALES_QTY_PROMO) as sum_qty
				from &lmvWorkCaslib..pmix_sales t1)  t1
			left join
				 &lmvWorkCaslib..product_dictionary_ml as t2 
			on
				t1.product_id = t2.product_id
				and t1.SALES_DT >= %str(date%')%sysfunc(putn(&lmvStartDate.,yymmdd10.))%str(%') and
				t1.SALES_DT <= %str(date%')%sysfunc(putn(&lmvScoreEndDate.,yymmdd10.))%str(%')
		;
	quit;

	/****** 2. Добавление цен ******/
	proc casutil;
	  droptable casdata="abt2_ml" incaslib="casuser" quiet;
	run;

	/* Добавляем к продажам цены */
	proc fedsql sessref=casauto; 
		create table casuser.abt2_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				max(t2.GROSS_PRICE_AMT) as GROSS_PRICE_AMT
			from
				casuser.abt1_ml as t1 left join
				&lmvWorkCaslib..price_ml as t2
			on
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.PRODUCT_ID = t2.PRODUCT_ID and
				t1.SALES_DT <= t2.end_dt and   
				t1.SALES_DT >= t2.start_dt
			group by 
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty
			;
	quit;

	proc casutil;
	  droptable casdata="abt1_ml" incaslib="casuser" quiet;
	  droptable casdata="abt3_ml" incaslib="casuser" quiet;
	run;

	/****** 3. Протяжка временных рядов ******/
	%let fc_end=%sysfunc(putn(&lmvScoreEndDate,yymmdd10.));

	proc cas;
	timeData.timeSeries result =r /
		series={
			{name="sum_qty", setmiss="MISSING"},
			{name="GROSS_PRICE_AMT", setmiss="PREV"}
		}
		tEnd= "&fc_end"
		table={
			caslib="casuser",
			name="abt2_ml",
			groupby={"PBO_LOCATION_ID","PRODUCT_ID", "CHANNEL_CD"}
		}
		timeId="SALES_DT"
		trimId="LEFT"
		interval="day"
		casOut={caslib="casuser", name="abt3_ml", replace=True}
		;
	run;
	quit;

	proc casutil;
	  droptable casdata="abt2_ml" incaslib="casuser" quiet;
	  droptable casdata="abt4_ml" incaslib="casuser" quiet;
	run;

	/****** 4. Фильтрация ******/
	/* 4.1 Убираем временные закрытия ПБО */
	/* Удалаем даты закрытия pbo из abt */
	proc fedsql sessref=casauto;
		create table casuser.abt4_ml{options replace=true} as
			select 
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT
			from
				casuser.abt3_ml as t1
			left join
				&lmvWorkCaslib..pbo_closed_ml as t2
			on
				t1.sales_dt >= t2.start_dt and
				t1.sales_dt <= t2.end_dt and
				t1.pbo_location_id = t2.pbo_location_id and
				t1.channel_cd = t2.channel_cd
			where
				t2.pbo_location_id is missing
		;
	quit;

	/* 4.2 Убираем закрытые насовсем магазины */
	/* Удаляем закрытые насовсем магазины  */
	proc fedsql sessref=casauto;
		create table casuser.abt4_ml{options replace = true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT
			from
				casuser.abt4_ml as t1
			left join
				&lmvWorkCaslib..closed_pbo as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt >= t2.OPEN_DATE and
				t1.sales_dt <= t2.CLOSE_DATE
			where
				t2.pbo_location_id is not missing
		;
	quit;

	/* 4.3.2 Оставляем нулевые продажи в периодах ввода товаров из product chain */
	/*
	proc fedsql sessref=casauto;
		create table casuser.abt4_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t2.predecessor_product_id,
				coalesce(t1.sum_qty, case when t2.predecessor_product_id ^= . then 0 else . end) as sum_qty,
				t1.GROSS_PRICE_AMT
			from 
				casuser.abt4_ml as t1
			left join 
				&lmvWorkCaslib..product_chain t2
			on 
				t2.predecessor_product_id = t1.product_id 
				and t2.predecessor_dim2_id = t1.pbo_location_id
				and t1.sales_dt between datepart(t2.successor_start_dt) and datepart(t2.predecessor_end_dt)
				and t2.lifecycle_cd = 'N'
				and t1.sales_dt <= %str(date%')%sysfunc(putn(&lmvEndDate.,yymmdd10.))%str(%')
		;
	quit;
	*/

	/* 4.3 Убираем из истории пропуски в продажах */
	proc fedsql sessref=casauto;
		create table casuser.abt4_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT
			from 
				casuser.abt4_ml as t1
			where 		
			(t1.sum_qty is not missing and t1.SALES_DT <= %str(date%')%sysfunc(putn(&lmvScoreEndDate.,yymmdd10.))%str(%')) or
			(t1.SALES_DT > %str(date%')%sysfunc(putn(&lmvEndDate.,yymmdd10.))%str(%'))
		;
	quit;

	/* 4.4 Пересекаем с ассортиментной матрицей скоринговую витрину */
	proc fedsql sessref=casauto;
		create table casuser.abt4_ml {options replace = true} as	
			select distinct
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT					
			from
				casuser.abt4_ml as t1
			left join
				&lmvWorkCaslib..assort_matrix  t2
			on
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.PRODUCT_ID = t2.PRODUCT_ID and
				t1.SALES_DT <= t2.end_dt and 
				t1.SALES_DT >= t2.start_dt
			where	
			(t1.SALES_DT <= %str(date%')%sysfunc(putn(&lmvScoreEndDate.,yymmdd10.))%str(%')) or 
			(t2.PBO_LOCATION_ID is not missing)
		;
	quit;
	
	proc casutil;
		droptable casdata="abt3_ml" incaslib="casuser" quiet;
	run;

	/****** 5. Подсчет лагов ******/
	proc casutil;
	  droptable casdata='lag_abt1' incaslib='casuser' quiet;
	  droptable casdata='lag_abt2' incaslib='casuser' quiet;
	  droptable casdata='lag_abt3' incaslib='casuser' quiet;
	  droptable casdata='abt5_ml' incaslib='casuser' quiet;
	run;


	/* считаем медиану и среднее арифметическое */
	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name ='abt4_ml',
			caslib = 'casuser', 
			groupBy = {
				{name = 'PRODUCT_ID'},
				{name = 'PBO_LOCATION_ID'},
				{name = 'CHANNEL_CD'}
			}
		},
		series = {{name='sum_qty'}},
		interval='DAY',
		timeId = {name='SALES_DT'},
		trimId = "LEFT", 
		code=
			%unquote(%str(%"))
			%let names=; /*будущий список выходных переменных для proc cas */
			%let minlag=91; /*параметр MinLag*/
			/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
			%let window_list = 7 30 90 180 365;
			%let lag=&minlag;
			%let n_win_list=%sysfunc(countw(&window_list.));
			%do ic=1 %to &n_win_list.;
			  %let window=%scan(&window_list,&ic); /*текущее окно*/
			  %let intnm=%rtp_namet(&window);        /*название интервала окна; 7->week итд */
			  %let intnm=%sysfunc(strip(&intnm.));
			  do t = %eval(&lag+&window) to _length_; /*from=(lag)+(window)*/
				lag_&intnm._avg[t]=mean(%rtp_argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1)));
				lag_&intnm._med[t]=median(%rtp_argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1)));
			  end;
			 %let names={name=%tslit(lag_&intnm._avg)}, &names;
			 %let names={name=%tslit(lag_&intnm._med)}, &names; 

			%end; /* ic over window_list*/
			/*remove last comma from names*/
			%let len=%length(&names);
			%let names=%substr(%quote(&names),1,%eval(&len-1));
			/*-=-=-завершающий код proc cas=-=-=*/
			%unquote(%str(%"))
		,
		arrayOut={
			table={name='lag_abt1', replace=true, caslib='casuser'},
			arrays={&names}
		}
	;
	run;
	quit;

	/* Считаем стандартное отклонение */
	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name ='abt4_ml',
			caslib = 'casuser', 
			groupBy = {
				{name = 'PRODUCT_ID'},
				{name = 'PBO_LOCATION_ID'},
				{name = 'CHANNEL_CD'}
			}
		},
		series = {{name='sum_qty'}},
		interval='DAY',
		timeId = {name='SALES_DT'},
		trimId = "LEFT",
		code=
			%unquote(%str(%"))
			%let names=; /*будущий список выходных переменных для proc cas */
			%let minlag=91; /*параметр MinLag*/
			/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
			%let window_list = 7 30 90 180 365;
			%let lag=&minlag;
			%let n_win_list=%sysfunc(countw(&window_list.));
			%do ic=1 %to &n_win_list.;
			  %let window=%scan(&window_list,&ic); /*текущее окно*/
			  %let intnm=%rtp_namet(&window);        /*название интервала окна; 7->week итд */
			  %let intnm=%sysfunc(strip(&intnm.));
			  do t = %eval(&lag+&window) to _length_; /*from=(lag)+(window)*/
				lag_&intnm._std[t]=std(%rtp_argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1)));
			  end;
			 %let names={name=%tslit(lag_&intnm._std)}, &names;

			%end; /* ic over window_list*/
			/*remove last comma from names*/
			%let len=%length(&names);
			%let names=%substr(%quote(&names),1,%eval(&len-1));
			/*-=-=-завершающий код proc cas=-=-=*/
			%unquote(%str(%"))
		,
		arrayOut={
			table={name='lag_abt2', replace=true, caslib='casuser'},
			arrays={&names}
		}
	;
	run;
	quit;

	/* Считаем процентили */
	proc cas;
	timeData.runTimeCode result=r /
		table = {
			name ='abt4_ml',
			caslib = 'casuser', 
			groupBy = {
				{name = 'PRODUCT_ID'},
				{name = 'PBO_LOCATION_ID'},
				{name = 'CHANNEL_CD'}
			}
		},
		series = {{name='sum_qty'}},
		interval='DAY',
		timeId = {name='SALES_DT'},
		trimId = "LEFT",
		code=
			%unquote(%str(%"))
			%let names=; /*будущий список выходных переменных для proc cas */
			%let minlag=91; /*параметр MinLag*/
			/*-=-=-=-=-= min_lag + окна -=-=-=-=-=-*/
			%let window_list = 7 30 90 180 365;
			%let lag=&minlag;
			%let n_win_list=%sysfunc(countw(&window_list.));
			%do ic=1 %to &n_win_list.;
			  %let window=%scan(&window_list,&ic); /*текущее окно*/
			  %let intnm=%rtp_namet(&window);        /*название интервала окна; 7->week итд */
			  %let intnm=%sysfunc(strip(&intnm.));
			  do t = %eval(&lag+&window) to _length_; /*from=(lag)+(window)*/
				lag_&intnm._pct10[t]=pctl(10,%rtp_argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1))) ;
				lag_&intnm._pct90[t]=pctl(90,%rtp_argt(sum_qty,t,%eval(&lag),%eval(&lag+&window-1))) ;
			  end;
			 %let names={name=%tslit(lag_&intnm._pct10)}, &names;
			 %let names={name=%tslit(lag_&intnm._pct90)}, &names;

			%end; /* ic over window_list*/
			/*remove last comma from names*/
			%let len=%length(&names);
			%let names=%substr(%quote(&names),1,%eval(&len-1));
			/*-=-=-завершающий код proc cas=-=-=*/
			%unquote(%str(%"))
		,
		arrayOut={
			table={name='lag_abt3', replace=true, caslib='casuser'},
			arrays={&names}
		}
	;
	run;
	quit;

	/* соеденим среднее, медиану, стд, процентили вместе, убирая пропуску вр ВР */
	proc fedsql sessref=casauto;
		create table casuser.abt5_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t2.lag_halfyear_avg,
				t2.lag_halfyear_med,
				t2.lag_month_avg,
				t2.lag_month_med,
				t2.lag_qtr_avg,
				t2.lag_qtr_med,
				t2.lag_week_avg,
				t2.lag_week_med,
				t2.lag_year_avg,
				t2.lag_year_med
			from
				casuser.abt4_ml as t1,
				casuser.lag_abt1 as t2
			where
				t1.CHANNEL_CD = t2.CHANNEL_CD and
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.PRODUCT_ID = t2.PRODUCT_ID and
				t1.SALES_DT = t2.SALES_DT
		;
	quit;

	proc casutil;
	  droptable casdata="abt4_ml" incaslib="casuser" quiet;
	  droptable casdata="lag_abt1" incaslib="casuser" quiet;
	run;
	
	proc fedsql sessref=casauto;
		create table casuser.abt5_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t2.lag_halfyear_std,
				t2.lag_month_std,
				t2.lag_qtr_std,
				t2.lag_week_std,
				t2.lag_year_std
			from
				casuser.abt5_ml as t1,
				casuser.lag_abt2 as t2
			where
				t1.CHANNEL_CD = t2.CHANNEL_CD and
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.PRODUCT_ID = t2.PRODUCT_ID and
				t1.SALES_DT = t2.SALES_DT
		;
	quit;
	
	proc casutil;
	  droptable casdata="lag_abt2" incaslib="casuser" quiet;
	run;
	
	proc fedsql sessref=casauto;
		create table casuser.abt5_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t2.lag_halfyear_pct10,		 
				t2.lag_halfyear_pct90,		 
				t2.lag_month_pct10	,
				t2.lag_month_pct90	,
				t2.lag_qtr_pct10,	
				t2.lag_qtr_pct90,	
				t2.lag_week_pct10,	
				t2.lag_week_pct90,	
				t2.lag_year_pct10,	
				t2.lag_year_pct90
			from
				casuser.abt5_ml as t1,
				casuser.lag_abt3 as t2
			where
				t1.CHANNEL_CD = t2.CHANNEL_CD and
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.PRODUCT_ID = t2.PRODUCT_ID and
				t1.SALES_DT = t2.SALES_DT
		;
	quit;

	proc casutil;
	  droptable casdata='lag_abt3' incaslib='casuser' quiet;
	  droptable casdata="abt6_ml" incaslib="casuser" quiet;
	run;
	
	/* Соединяем с витриной */
	proc fedsql sessref = casauto;
		/* Подготоваливаем таблицу для джойна с витриной */
		create table casuser.abt_promo{options replace = true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				max(coalesce(t2.other_promo, 0)) as other_promo,  
				/* 0 as other_promo, */
				max(coalesce(t2.support, 0)) as support,
				max(coalesce(t2.bogo, 0)) as bogo,
				max(coalesce(t2.discount, 0)) as discount,
				max(coalesce(t2.evm_set, 0)) as evm_set,
				max(coalesce(t2.non_product_gift, 0)) as non_product_gift,
				max(coalesce(t2.pairs, 0)) as pairs,
				max(coalesce(t2.product_gift, 0)) as product_gift,
				max(coalesce(t3.side_promo_flag, 0)) as side_promo_flag
			from
				casuser.abt5_ml as t1
			left join
				&lmvWorkCaslib..promo_transposed as t2
			on
				t1.product_id = t2.product_LEAF_ID and
				t1.pbo_location_id = t2.PBO_LEAF_ID and
				t1.SALES_DT <= t2.END_DT and
				t1.SALES_DT >= t2.START_DT
			left join
				&lmvWorkCaslib..promo_ml_main_code as t3
			on
				t1.product_id = t3.product_MAIN_CODE and
				t1.pbo_location_id = t3.PBO_LEAF_ID and
				t1.SALES_DT <= t3.END_DT and
				t1.SALES_DT >= t3.START_DT
			group by
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT
		;
		/* Добавляем промо к витрине */
		create table casuser.abt6_ml{options replace = true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10	,
				t1.lag_month_pct90	,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				t2.other_promo,  
				t2.support,
				t2.bogo,
				t2.discount,
				t2.evm_set,
				t2.non_product_gift,
				t2.pairs,
				t2.product_gift,
				t2.side_promo_flag 
			from
				casuser.abt5_ml as t1
			left join
				casuser.abt_promo as t2
			on
				t1.product_id = t2.product_id and
				t1.pbo_location_id = t2.pbo_location_id and
				t1.SALES_DT = t2.SALES_DT and
				t1.CHANNEL_CD = t2.CHANNEL_CD
		;
	quit;

	proc casutil;
		droptable casdata="abt_promo" incaslib="casuser" quiet;
		droptable casdata="abt5_ml" incaslib="casuser" quiet;
	run;

	/****** 7. Добавляем мароэкономику ******/
	proc casutil;
		droptable casdata="abt7_ml" incaslib="casuser" quiet;
	run;

	/* Соединяем с ABT */
	proc fedsql sessref = casauto;
		create table casuser.abt7_ml{options replace = true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t2.A_CPI,
				t2.A_GPD,
				t2.A_RDI
			from
				casuser.abt6_ml as t1 left join 
				&lmvWorkCaslib..macro_transposed_ml as t2
			on
				t1.sales_dt = t2.period_dt
		;
	quit;

	proc casutil;
	  droptable casdata="abt6_ml" incaslib="casuser" quiet;
	   droptable casdata = "abt8_ml" incaslib = "casuser" quiet;
	run;

	proc fedsql sessref =casauto;
		create table casuser.abt8_ml{options replace = true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t2.TEMPERATURE,
				t2.PRECIPITATION
			from
				casuser.abt7_ml as t1
			left join
				&lmvWorkCaslib..weather as t2
			on 
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = datepart(t2.REPORT_DT)
		;
	quit;

	proc casutil;
	  droptable casdata="abt7_ml" incaslib="casuser" quiet;
	run;


	/***** 9. Добавляем trp конкурентов *****/
	proc casutil;
		droptable casdata="abt9_ml" incaslib="casuser" quiet;
	run;
	/* Соединяем с ABT */
	proc fedsql sessref = casauto;
		create table casuser.abt9_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t2.comp_trp_BK,
				t2.comp_trp_KFC
			from
				casuser.abt8_ml as t1
			left join
				&lmvWorkCaslib..comp_transposed_ml_expand as t2
			on
				t1.sales_dt = t2.REPORT_DT
		;
	quit;

	proc casutil;
	    droptable casdata="abt8_ml" incaslib="casuser" quiet;
	run;

	/***** 10. Добавляем медиаподдержку *****/
	proc casutil;
	  droptable casdata="abt10_ml" incaslib="casuser" quiet;
	run;

	proc fedsql sessref=casauto;
		create table casuser.abt10_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t2.sum_trp
			from
				casuser.abt9_ml as t1
			left join
				&lmvWorkCaslib..sum_trp as t2
			on 
				t1.product_id = t2.PRODUCT_LEAF_ID and
				t1.pbo_location_id = t2.PBO_LEAF_ID and
				t1.sales_dt = t2.report_dt
		;
	quit;

	proc casutil;
		droptable casdata="abt9_ml" incaslib="casuser" quiet;
		 droptable casdata="abt11_ml" incaslib="casuser" quiet;
	run;

	proc fedsql sessref=casauto;
		create table casuser.abt11_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t2.prod_lvl4_id, 
				t2.prod_lvl3_id,
				t2.prod_lvl2_id,
				t2.a_hero_id as hero,
				t2.a_item_size_id as item_size,
				t2.a_offer_type_id as offer_type,
				t2.a_price_tier_id as price_tier
		from
			casuser.abt10_ml as t1
		left join
			&lmvWorkCaslib..product_dictionary_ml as t2
		on
			t1.product_id = t2.product_id
		;
	quit;
	 
	proc casutil;
	  droptable casdata="abt10_ml" incaslib="casuser" quiet;
	   droptable casdata="abt12_ml" incaslib="casuser" quiet;
	run;
	
	proc fedsql sessref=casauto;
		create table casuser.abt12_ml{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
				t2.lvl3_id,
				t2.lvl2_id,
				t2.A_AGREEMENT_TYPE_id as agreement_type,
				t2.A_BREAKFAST_id as breakfast,
				t2.A_BUILDING_TYPE_id as building_type,
				t2.A_COMPANY_id as company,
				t2.A_DELIVERY_id as delivery,
				t2.A_DRIVE_THRU_id as drive_thru,
				t2.A_MCCAFE_TYPE_id as mccafe_type,
				t2.A_PRICE_LEVEL_id as price_level,
				t2.A_WINDOW_TYPE_id as window_type
			from
				casuser.abt11_ml as t1
			left join
				&lmvWorkCaslib..pbo_dictionary_ml as t2
			on
				t1.pbo_location_id = t2.pbo_location_id
		;
	quit;

	proc casutil;
		droptable casdata="abt11_ml" incaslib="casuser" quiet;
		droptable casdata="abt13_ml" incaslib="casuser" quiet;
	run;

	/* Добавляем к витрине */
	proc fedsql sessref = casauto;
		create table casuser.abt13_ml{options replace = true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
				t1.lvl3_id,
				t1.lvl2_id,
				t1.agreement_type,
				t1.breakfast,
				t1.building_type,
				t1.company,
				t1.delivery,
				t1.drive_thru,
				t1.mccafe_type,
				t1.price_level,
				t1.window_type,
				t2.week,
				t2.weekday,
				t2.month,
				t2.weekend_flag
			from
				casuser.abt12_ml as t1
			left join
				&lmvWorkCaslib..cldr_prep_features as t2
			on
				t1.sales_dt = t2.date
		;
	quit;

	/******  14. Добавим события ******/
	proc casutil;
		droptable casdata="abt14_ml" incaslib="casuser" quiet;
		droptable casdata="abt12_ml" incaslib="casuser" quiet;
	run;

	/* добавляем к ваитрине */
	proc fedsql sessref=casauto;
		create table casuser.abt14_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
				t1.lvl3_id,
				t1.lvl2_id,
				t1.agreement_type,
				t1.breakfast,
				t1.building_type,
				t1.company,
				t1.delivery,
				t1.drive_thru,
				t1.mccafe_type,
				t1.price_level,
				t1.window_type,
				t1.week,
				t1.weekday,
				t1.month,
				t1.weekend_flag,
				coalesce(t2.defender_day, 0) as defender_day,
				coalesce(t2.female_day, 0) as female_day,
				coalesce(t2.may_holiday, 0) as may_holiday,
				coalesce(t2.new_year , 0) as new_year,
				coalesce(t2.russia_day, 0) as russia_day,
				coalesce(t2.school_start, 0) as school_start,
				coalesce(t2.student_day, 0) as student_day,
				coalesce(t2.summer_start, 0) as summer_start,
				coalesce(t2.valentine_day, 0) as valentine_day
			from
				casuser.abt13_ml as t1
			left join
				&lmvWorkCaslib..russia_event_t as t2
			on
				t1.sales_dt = t2.date
		;	
	quit;

	proc casutil;
		droptable casdata="abt13_ml" incaslib="casuser" quiet;
	run;

	/******	15. Добавим ценовые ранги ******/
	proc casutil;
		droptable casdata="abt15_ml" incaslib="casuser" quiet;
		droptable casdata="unique_day_price" incaslib="casuser" quiet;
		droptable casdata="sum_count_price" incaslib="casuser" quiet;
		droptable casdata="price_rank" incaslib="casuser" quiet;
		droptable casdata="price_rank2" incaslib="casuser" quiet;
		droptable casdata="price_rank3" incaslib="casuser" quiet;
		droptable casdata="price_feature" incaslib="casuser" quiet;
	run;

	/* уникальные ПБО/день/категория товаров/товар/цена */
	proc fedsql sessref = casauto;
		create table casuser.unique_day_price as 
			select distinct
				t1.pbo_location_id,
				t1.PROD_LVL3_ID,
				t1.sales_dt,
				t1.product_id,
				t1.GROSS_PRICE_AMT
			from
				casuser.abt14_ml as t1
		;
	quit;

	/* Считаем суммарную цену в групе и количество товаров */
	proc fedsql sessref = casauto;
		create table casuser.sum_count_price{options replace = true} as
			select
				t1.pbo_location_id,
				t1.PROD_LVL3_ID,
				t1.sales_dt,
				count(t1.product_id) as count_product,
				sum(t1.GROSS_PRICE_AMT) as sum_gross_price_amt
			from casuser.unique_day_price as t1
			group by
				t1.pbo_location_id,
				t1.PROD_LVL3_ID,
				t1.sales_dt
		;
	quit;

	/* считаем позицию товара в отсортированном списке цен */
	data casuser.price_rank / sessref = casauto;
		set casuser.unique_day_price;
		by pbo_location_id sales_dt PROD_LVL3_ID GROSS_PRICE_AMT ;
		if first.PROD_LVL3_ID then i = 0;
		if GROSS_PRICE_AMT ^= lag(GROSS_PRICE_AMT) then i+1;
	run;

	proc fedsql sessref = casauto;
		create table casuser.price_rank2{options replace=true} as
			select
				t1.pbo_location_id,
				t1.sales_dt,
				t1.PROD_LVL3_ID,
				max(t1.i) as max_i
			from
				casuser.price_rank as t1
			group by
				t1.pbo_location_id,
				t1.sales_dt,
				t1.PROD_LVL3_ID
		; 
	quit;

	/* Соединяем таблицы price_rank, price_rank2 */
	proc fedsql sessref=casauto;
		create table casuser.price_rank3{options replace=true} as
			select
				t1.product_id,
				t1.pbo_location_id,
				t1.PROD_LVL3_ID,
				t1.sales_dt,
				t1.GROSS_PRICE_AMT,
				t1.i,
				t2.max_i
			from
				casuser.price_rank as t1
			left join
				casuser.price_rank2 as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.PROD_LVL3_ID = t2.PROD_LVL3_ID and
				t1.sales_dt = t2.sales_dt
		;
	quit;

	/* Соединяем таблицы price_rank3 и sum_count_price */
	proc fedsql sessref=casauto;
		create table casuser.price_feature{options replace=true} as
			select
				t1.product_id,
				t1.pbo_location_id,
				t1.PROD_LVL3_ID,
				t1.sales_dt,
				t1.GROSS_PRICE_AMT,
				t1.i,
				t1.max_i,
				t2.count_product,
				t2.sum_gross_price_amt,
				divide(t1.i,t1.max_i) as price_rank,
				(
					case
						when t2.sum_gross_price_amt = t1.GROSS_PRICE_AMT then 1
						else divide(t1.GROSS_PRICE_AMT,divide((t2.sum_gross_price_amt - t1.GROSS_PRICE_AMT),(t2.count_product - 1)))
					end
				) as price_index
			from
				casuser.price_rank3 as t1
			left join
				casuser.sum_count_price as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.PROD_LVL3_ID = t2.PROD_LVL3_ID and
				t1.sales_dt = t2.sales_dt
			where GROSS_PRICE_AMT is not null
		;
	quit;

	/* Добавляем в витрину */
	proc fedsql sessref = casauto;
		create table casuser.abt15_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
				t1.lvl3_id,
				t1.lvl2_id,
				t1.agreement_type,
				t1.breakfast,
				t1.building_type,
				t1.company,
				t1.delivery,
				t1.drive_thru,
				t1.mccafe_type,
				t1.price_level,
				t1.window_type,
				t1.week,
				t1.weekday,
				t1.month,
				t1.weekend_flag,
				t1.defender_day,
				t1.female_day,
				t1.may_holiday,
				t1.new_year,
				t1.russia_day,
				t1.school_start,
				t1.student_day,
				t1.summer_start,
				t1.valentine_day, 
				t2.price_rank,
				t2.price_index
			from
				casuser.abt14_ml as t1
			left join
				casuser.price_feature as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.product_id = t2.product_id and
				t1.sales_dt = t2.sales_dt
		;
	quit;
		
	proc casutil;
		droptable casdata="unique_day_price" incaslib="casuser" quiet;
		droptable casdata="sum_count_price" incaslib="casuser" quiet;
		droptable casdata="price_rank" incaslib="casuser" quiet;
		droptable casdata="price_rank2" incaslib="casuser" quiet;
		droptable casdata="price_rank3" incaslib="casuser" quiet;
		droptable casdata="price_feature" incaslib="casuser" quiet;
		droptable casdata="abt14_ml" incaslib="casuser" quiet;
	run;

	/******	16. Перекодируем channel_cd  ******/
	proc casutil;
		droptable casdata="abt16_ml" incaslib="casuser" quiet;
	run;
	
	%text_encoding(mpTable=casuser.abt15_ml, mpVariable=channel_cd);

	/* Заменяем текстовое поле на числовое */
	proc fedsql sessref = casauto;
		create table casuser.abt16_ml{options replace=true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD_id as channel_cd,
				t1.SALES_DT,
				t1.sum_qty,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,		 
				t1.lag_halfyear_pct90,		 
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,	
				t1.lag_qtr_pct90,	
				t1.lag_week_pct10,	
				t1.lag_week_pct90,	
				t1.lag_year_pct10,	
				t1.lag_year_pct90,
				t1.other_promo,  
				t1.support,
				t1.bogo,
				t1.discount,
				t1.evm_set,
				t1.non_product_gift,
				t1.pairs,
				t1.product_gift,
				t1.side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.comp_trp_BK,
				t1.comp_trp_KFC,
				t1.sum_trp,
				t1.prod_lvl4_id, 
				t1.prod_lvl3_id,
				t1.prod_lvl2_id,
				t1.hero,
				t1.item_size,
				t1.offer_type,
				t1.price_tier,
				t1.lvl3_id,
				t1.lvl2_id,
				t1.agreement_type,
				t1.breakfast,
				t1.building_type,
				t1.company,
				t1.delivery,
				t1.drive_thru,
				t1.mccafe_type,
				t1.price_level,
				t1.window_type,
				t1.week,
				t1.weekday,
				t1.month,
				t1.weekend_flag,
				t1.defender_day,
				t1.female_day,
				t1.may_holiday,
				t1.new_year,
				t1.russia_day,
				t1.school_start,
				t1.student_day,
				t1.summer_start,
				t1.valentine_day, 
				t1.price_rank,
				t1.price_index
			from
				casuser.abt15_ml as t1
		;
	quit;
	
	proc casutil;
		droptable casdata="abt15_ml" incaslib="casuser" quiet;
	quit;
	
	proc fedsql sessref=casauto;
	%if &lmvMode. = A or &lmvMode = T %then %do;
		create table casuser.&lmvTabNmOutTrain.{options replace = true} as 
			select *
			from casuser.abt16_ml 
			where sales_dt >= date %str(%')%sysfunc(putn(&lmvEndDate., yymmdd10.))%str(%')
			/* where sales_dt >= date %str(%')%sysfunc(putn(%sysfunc(intnx(year,&lmvStartDate.,1,b)), yymmdd10.))%str(%') */
			;
	%end;
	%if &lmvMode. = A or &lmvMode = S %then %do;
		create table casuser.&lmvTabNmOutScore.{options replace = true} as 
			select * 
			from casuser.abt16_ml 
			where sales_dt > date %str(%')%sysfunc(putn(&lmvStartDateScore., yymmdd10.))%str(%') and
				sales_dt <= date %str(%')%sysfunc(putn(&lmvScoreEndDate., yymmdd10.))%str(%')
		;
	%end;
	quit;

	proc casutil;
		droptable casdata="abt16_ml" incaslib="casuser" quiet;
		
		 %if &lmvMode = T %then %do; 
			save incaslib="&lmvLibrefOutTrain." outcaslib="&lmvLibrefOutTrain." casdata="&lmvTabNmOutTrain." casout="&lmvTabNmOutTrain..sashdat" replace;
		%end;
		%if &lmvMode. = A or &lmvMode = S %then %do;
			promote casdata="&lmvTabNmOutTrain." incaslib="casuser" outcaslib="&lmvLibrefOutTrain.";
			promote casdata="&lmvTabNmOutScore." incaslib="casuser" outcaslib="&lmvLibrefOutScore.";
			*save incaslib="&lmvLibrefOutScore." outcaslib="&lmvLibrefOutScore." casdata="&lmvTabNmOutScore." casout="&lmvTabNmOutScore..sashdat" replace; 
		%end;
		droptable casdata="&lmvTabNmOutScore." incaslib="casuser" quiet;
		droptable casdata="&lmvTabNmOutTrain." incaslib="casuser" quiet;
		droptable casdata="abt16_ml" incaslib="casuser" quiet;
	quit;

%mend rtp_1_load_data_product;
