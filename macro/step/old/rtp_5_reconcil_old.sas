/*
	Схема программы:
	
	0. Объявление макропеременных
	1. Собираем 3 таблицы:
		Для PMIX и UPT:
			* ПБО - факт, прогноз (pmix и upt), независимые переменные
			* Мастеркод - факт, прогноз, реконсилированный прогноз (pmix и upt), 
				независимые переменные
			* Товар - факт, прогноз, два реконсилированных прогноза (pmix и upt), факт
		Даты: декабрь
	2. Создаем макрос по подсчету wape, bias с параметрами:
		* level - название таблицы, с прогнозом и фактом (russca_low/middle/top)
		* fact - название переменной с фактом
		* predict - название переменной с прогнозом
		* category - название категориальной переменной
		* output_name - название выходной таблицы с отчетом
*/

%macro rtp_5_reconcil_old(mpProjectId =8512d42a-d02d-4a7a-8bd7-78a5aa771cf4,
					mpPboFact = public.russca_pbo_pmix_score_full,
					mpMasterCodeTbl = public.RUSAKR_MASTER_DAYS_FULL,
					mpProductTable = public.RUSAKR_PMIX_DAYS_FULL,
					mpGcFact = public.pbo_sales,
					mpGcPredict = public.gc_days_full,
					mpResultTable = DM_ABT.PMIX_RECONCILED_FULL,
					mpScoreStartDt = date '2020-02-01',
					mpScoreEndDt = date '2020-02-29'
					);
	options mprint nomprintnest nomlogic nomlogicnest nosymbolgen mcompilenote=all mreplace;
	/*Создать cas-сессию, если её нет*/
	%if %sysfunc(SESSFOUND(casauto)) = 0 %then %do; 
		cas casauto;
		caslib _all_ assign;
	%end;

	%local lmvLibrefRes lmvTabNmRes;
	%member_names (mpTable=&mpResultTable, 
					mpLibrefNameKey=lmvLibrefRes,
					mpMemberNameKey=lmvTabNmRes);
	/****** 1. Собираем 3 таблицы******/
	/* 1.1 Объединяем факт gc с прогнозом*/
	proc fedsql sessref=casauto;
		create table casuser.gc{options replace=true} as
			select
				t1.channel_cd,
				t1.PBO_LOCATION_ID, 
				t1.period_dt as sales_dt,
				t1.ff_day,
				t2.receipt_qty
			from
				&mpGcPredict. as t1
			inner join
				&mpGcFact. as t2
			on
				t1.period_dt = t2.sales_dt and
				t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID and
				t1.channel_cd = t2.channel_cd
			where 
				t1.channel_cd = 'ALL' and
				t1.period_dt >= &mpScoreStartDt. and
				t1.period_dt <= &mpScoreEndDt.
		;
	quit;

	/* 1.2 Соеденияем прогноз pmix ПБО с фактом */
	proc fedsql sessref=casauto;
		create table casuser.top{options replace=true} as
			select
				t1.*,
				t2.predict as p_sum_qty,
				(
					case 
						when t3.receipt_qty > 0 then t1.sum_qty / t3.receipt_qty * 1000
						else 0
					end
				) as upt,
				(
					case
						when t3.ff_day > 0 then t2.predict / t3.ff_day * 1000
						else 0 
					end
				) as p_upt
			from
				&mpPboFact. as t1
			left join
				"Analytics_Project_&mpProjectId".horizon as t2
			on
				t1.sales_dt = t2.sales_dt and
				t1.pbo_location_id = t2.pbo_location_id and
				t1.channel_cd = t2.channel_cd
			left join
				casuser.gc as t3
			on
				t1.sales_dt = t3.sales_dt and
				t1.pbo_location_id = t3.pbo_location_id
			where
				t1.sales_dt >= &mpScoreStartDt. and
				t1.sales_dt <= &mpScoreEndDt.
		;
	quit;

	/* 1.2 Реконсилируем прогноз с ПБО на мастеркод */
	proc fedsql sessref=casauto;
		/* 1.2.1 Считаем распределение прогноза на уровне мастеркода */
		create table casuser.middle_freq{options replace=true} as
			select
				t1.*,
				t1.p_sum_qty / t2.sum_prediction as pcnt_prediction
			from
				public.&mpMasterCodeTbl. as t1
			inner join
				(
				select
					t1.pbo_location_id,
					t1.sales_dt,
					t1.channel_cd,
					sum(t1.p_sum_qty) as sum_prediction
				from
					public.&mpMasterCodeTbl. as t1
				group by
					t1.pbo_location_id,
					t1.sales_dt,
					t1.channel_cd
				) as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = t2.sales_dt and
				t1.channel_cd = t2.channel_cd
			where
				t1.sales_dt >= &mpScoreStartDt. and
				t1.sales_dt <= &mpScoreEndDt.
		;
		/* 1.2.2 Реконсилируем прогноз с ПБО на мастеркод */
		create table casuser.middle{options replace=true} as
			select
				t1.*,
				coalesce(t1.pcnt_prediction * t2.p_sum_qty, t1.p_sum_qty) as p_rec_sum_qty
			from
				casuser.middle_freq as t1
			left join
				casuser.top as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = t2.sales_dt
		;
		/* 1.2.3 Добавляем UPT */
		create table casuser.middle{options replace=true} as
			select
				t1.*,
				(
					case 
						when t2.receipt_qty > 0 then t1.sum_qty / t2.receipt_qty * 1000
						else 0
					end
				) as upt,
				(
					case
						when t2.ff_day > 0 then t1.p_sum_qty / t2.ff_day * 1000
						else 0 
					end
				) as p_upt,
				(
					case
						when t2.ff_day > 0 then t1.p_rec_sum_qty / t2.ff_day * 1000
						else 0 
					end
				) as p_rec_upt
			from
				casuser.middle as t1
			left join
				casuser.gc as t2
			on
				t1.sales_dt = t2.sales_dt and
				t1.pbo_location_id = t2.pbo_location_id
		;
	quit;

	/* 1.3 Реконсилируем прогноз с мастеркода на товар */
	proc fedsql sessref=casauto;
		/* 1.3.1 Считаем распределение прогноза на уровне товара */
		create table casuser.low_freq{options replace=true} as
			select
				t1.*,
				t1.p_sum_qty / t2.sum_prediction as pcnt_prediction
			from
				&mpProductTable. as t1
			inner join
				(
				select
					t1.pbo_location_id,
					t1.sales_dt,
					t1.channel_cd,
					t1.prod_lvl4_id,
					sum(t1.p_sum_qty) as sum_prediction
				from
					&mpProductTable. as t1
				group by
					t1.pbo_location_id,
					t1.sales_dt,
					t1.channel_cd,
					t1.prod_lvl4_id
				) as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = t2.sales_dt and
				t1.channel_cd = t2.channel_cd and
				t1.prod_lvl4_id = t2.prod_lvl4_id
			where
				t1.sales_dt >= &mpScoreStartDt. and
				t1.sales_dt <= &mpScoreEndDt.
		;
		/* 1.3.2 Реконсилируем прогноз с мастеркода на товар */
		create table casuser.low{options replace=true} as
			select
				t1.*,
				t1.pcnt_prediction * t2.p_sum_qty as p_rec_sum_qty,
				t1.pcnt_prediction * t2.p_rec_sum_qty as p_rec_rec_sum_qty
			from
				casuser.low_freq as t1
			left join
				casuser.middle as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = t2.sales_dt and
				t1.prod_lvl4_id = t2.prod_lvl4_id
		;
		/* 1.3.3 Добавляем UPT */
		create table casuser.low{options replace=true} as
			select
				t1.*,
				(
					case 
						when t2.receipt_qty > 0 then t1.sum_qty / t2.receipt_qty * 1000
						else 0
					end
				) as upt,
				(
					case
						when t2.ff_day > 0 then t1.p_sum_qty / t2.ff_day * 1000
						else 0 
					end
				) as p_upt,
				(
					case
						when t2.ff_day > 0 then t1.p_rec_sum_qty / t2.ff_day * 1000
						else 0 
					end
				) as p_rec_upt,
				(
					case
						when t2.ff_day > 0 then t1.p_rec_rec_sum_qty / t2.ff_day * 1000
						else 0 
					end
				) as p_rec_rec_upt
			from
				casuser.low as t1
			left join
				casuser.gc as t2
			on
				t1.sales_dt = t2.sales_dt and
				t1.pbo_location_id = t2.pbo_location_id
		;
	quit;


	proc casutil;
		droptable casdata="&lmvTabNmRes." incaslib="&lmvLibrefRes." quiet;
		promote casdata="&lmvTabNmRes." incaslib="casuser" casout="&lmvLibrefRes.";
	run;

%mend rtp_5_reconcil_old;