/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для подготовки таблицы pmix_sal_abt в выбранную директорию (по умолчанию - mn_long), используемой в сквозном процессе
*		для прогнозирования временными рядами. На указанной таблице строится VF-проект,
*		ID которого используется в макросе 06_vf_month_aggregation
*	
*
*  ПАРАМЕТРЫ:
*	  mpVfPboProjName       - Наименование VF-проекта
*	  mpPmixSalAbt			- Наименование выходной таблицы (по умолчанию - mn_long.pmix_sal_abt)
*     mpPromoW1				- Наименование входной таблицы Promo_W1
*	  mpPromoD				- Наименование входной таблицы Promo_D
*	  mpPboSales			- Наименование входной таблицы TS_PBO_SALES
*	  mpWeatherW			- Наименование входной таблицы weather_w 
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
*     %vf_prepare_ts_abt_pmix_sep(mpVfPboProjName=pbo_sales_v2,
							mpPmixSalAbt=mn_long.pmix_sal_abt,
*							mpPromoW1=mn_long.promo_w1,
*							mpPromoD=mn_long.promo_d,
*							mpPboSales=mn_long.TS_pbo_sales,
*							mpWeatherW=mn_long.weather_w);
*
****************************************************************************
*  02-07-2020  Борзунов     Начальное кодирование
*  28-07-2020  Борзунов		Изменен промоут промежуточных таблиц на casuser. Целевой витрины на mn_long.
							Добавлен параметры mpPmixSalAbt mpPromoW1 mpPromoD mpPboSales mpWeatherW
*  11-08-2020  Борзунов		Добавлено получение ID VF-проекта по его имени + параметр mpVfPboProjName
*  06-10-2020  Д Звежинский Витрины собираются из продаж, восстановленных на периодах закрытия ПБО
****************************************************************************/
%macro vf_prepare_ts_abt_pmix_sep(mpVfPboProjName=pbo_sales_v1,
							mpPmixSalAbt=mn_long.pmix_sal_abt,
							mpPromoW1=mn_long.promo_w1,
							mpPromoD=mn_long.promo_d,
							mpPboSales=mn_long.TS_pbo_sales,
							mpWeatherW=mn_long.weather_w);

	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;
	
	%local lmvOutLibrefPmixSalAbt lmvOutTabNamePmixSalAbt lmvVfPboName lmvVfPboId;
	%let lmvInLib=ETL_IA;
	%let ETL_CURRENT_DT = %sysfunc(date());
	%let ETL_CURRENT_DTTM = %sysfunc(datetime());
	%let lmvReportDt=&ETL_CURRENT_DT.;
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	%member_names (mpTable=&mpPmixSalAbt, mpLibrefNameKey=lmvOutLibrefPmixSalAbt, mpMemberNameKey=lmvOutTabNamePmixSalAbt);
	/* Получение списка VF-проектов */
	%vf_get_project_list(mpOut=work.vf_project_list);
	/* Извлечение ID для VF-проекта по его имени */
	%let lmvVfPboName = &mpVfPboProjName.;
	%let lmvVfPboId = %vf_get_project_id_by_name(mpName=&lmvVfPboName., mpProjList=work.vf_project_list);
	
	/*0. Удаление целевых таблиц */
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixSalAbt." incaslib="&lmvOutLibrefPmixSalAbt." quiet;
	run;
	
	/*1. Протяжка рядов pmix_sales и их аккумуляция */
	*proc cas;
	*	timeData.timeSeries result =r /
		series={{name="sales_qty", Acc="sum", setmiss="missing"},
		{name="gross_sales_amt", Acc="sum", setmiss="missing"},
		{name="net_sales_amt", Acc="sum", setmiss="missing"},
		{name="sales_qty_promo", Acc="sum", setmiss="missing"}}
		tEnd= "&VF_FC_AGG_END_DT" /*VF_FC_START_DT+hor*/
		table={caslib="mn_long",name="pmix_sales", groupby={"PBO_LOCATION_ID","PRODUCT_ID","CHANNEL_CD"} ,
		where="sales_dt>=&VF_HIST_START_DT_SAS and channel_cd='ALL'"}
		trimId="LEFT"
		timeId="SALES_DT"
		interval="week.2"
		casOut={caslib="casuser",name="TS_pmix_sales",replace=True}
		;
	*	run;
	*quit;
	proc cas;
	timeData.timeSeries result =r /
		series={{name="sales_qty", Acc="sum", setmiss="missing"},
			{name="sales_qty_rest", Acc="sum", setmiss="missing"}}
		tEnd= "&vf_fc_agg_end_dt" 
		table={caslib="mn_long",name="pmix_sales_rest", groupby={"PBO_LOCATION_ID","PRODUCT_ID","CHANNEL_CD"} ,
	         where="sales_dt>=&vf_hist_start_dt_sas"}
		trimId="LEFT"
		timeId="SALES_DT"
		interval="week.2"
		casOut={caslib="casuser",name="TS_pmix_sales",replace=True}
		;
	run;
	quit;

	/*1.1 прогноз по чекам - как независимый фактор */
	proc fedsql sessref=casauto noprint;
		create table casuser.TS_WEEK_OUTFOR{options replace=true} as
			select * 
			from "Analytics_Project_&lmvVfPboId".horizon
		;
	quit;
	
	proc fedsql sessref=casauto noprint;
		create table casuser.gc_fc_fact{options replace=true} as
		select 	coalesce(t1.PBO_LOCATION_ID,t2.PBO_LOCATION_ID) as PBO_LOCATION_ID
				,coalesce(t1.CHANNEL_CD,t2.CHANNEL_CD) as CHANNEL_CD
				,coalesce(t1.SALES_DT,t2.SALES_DT) as SALES_DT
				,coalesce(t1.FF,t2.receipt_qty) as ff
		from casuser.TS_WEEK_OUTFOR t1
		full outer join &mpPboSales. t2
			on t1.pbo_location_id=t2.pbo_location_id 
			and t1.channel_cd=t2.channel_cd 
			and t1.sales_dt=t2.sales_dt
		;
	quit;

	%if %sysfunc(exist(mn_long.PRICE)) eq 0 %then %do;
		data mn_long.PRICE (replace=yes drop=valid_from_dttm valid_to_dttm);
			set &lmvInLib..PRICE(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
		run;

		proc fedsql sessref=casauto noprint;
			create table mn_long.price{options replace=true} as
			select 
			t1.PRODUCT_ID
			,t1.PBO_LOCATION_ID
			,t1.PRICE_TYPE
			,t1.START_DT
			,t1.END_DT
			,t1.NET_PRICE_AMT
			,t1.GROSS_PRICE_AMT
			from mn_long.PRICE t1
			;
		quit;
	%end;

	/*3.2 Цены - независимый фактор*/
	proc fedsql sessref=casauto noprint;
		select max(START_DT) as max_st_dt
				,min(START_DT) as min_st_dt
				,max(END_DT) as max_end_dt
				,min(end_dt) as min_end_dt
		from mn_long.PRICE
		;
	quit;

	/*приводим к ценам по дням*/
	data casuser.price_unfolded(replace=yes) / SESSREF=casauto;
		set mn_long.PRICE;
		where price_type='F';
		keep product_id pbo_location_id gross_price_amt sales_dt;
		format sales_dt date9.;
		do sales_dt=START_DT to min(END_DT,&VF_FC_AGG_END_DT_sas);
			output;
		end;
	run;

	/*избавляемся от возможных дубликатов по ключу товар-пбо-дата*/
	data casuser.price_nodup(replace=yes) / SESSREF=casauto;
		set casuser.price_unfolded;
		by product_id pbo_location_id sales_dt;
		if first.sales_dt then output;
	run;

	proc casutil;
		droptable casdata="price_unfolded" incaslib="casuser" quiet;
		run;
	quit;

	/*протягиваем неизвестные цены последним известным значением*/
	/*агрегируем до недель*/
	proc cas;
		timeData.timeSeries result =r /
		series={{name="gross_price_amt", setmiss="prev"}}
		tEnd= "&VF_FC_AGG_END_DT" /*VF_FC_START_DT+hor*/
		table={caslib="casuser",name="price_nodup", groupby={"PBO_LOCATION_ID","PRODUCT_ID"} }
		timeId="SALES_DT"
		trimId="LEFT"
		interval="day"
		casOut={caslib="casuser",name="TS_price_fact",replace=True}
		;
		timeData.timeSeries result =r /
		series={{name="gross_price_amt", acc="avg"}}
		tEnd= "&VF_FC_AGG_END_DT" /*VF_FC_START_DT+hor*/
		table={caslib="casuser",name="TS_price_fact", groupby={"PBO_LOCATION_ID","PRODUCT_ID"} }
		timeId="SALES_DT"
		trimId="LEFT"
		interval="week.2"
		casOut={caslib="casuser",name="TS_price_fact_agg",replace=True}
		;
	run;
	quit;
	
	proc casutil;
		droptable casdata="price_nodup" incaslib="casuser" quiet;
		droptable casdata="ts_price_fact" incaslib="casuser" quiet;
	run;
	quit;

	/*3.3 Погода*/
	/*  casuser.weather_w */

	/*3.4 Promo*/
	proc fedsql sessref=casauto noprint;
		create table casuser.promo_dp{options replace=true} as
		select t1.channel_cd
				,t1.pbo_location_id
				,t1.product_id
				,intnx('week.2',period_DT,0,'b') as period_dt
				,sum(promo) as sum_promo_mkup
		from &mpPromoW1. t1 
		group by 1,2,3,4
		;
	quit;

	/*3.5 media - есть по акциям*/
	proc fedsql sessref=casauto noprint;
		create table casuser.promo_pbo_prod_dist{options replace=true} as
			select distinct promo_group_id
						,promo_id
						,channel_cd
						,pbo_location_id
						,product_id
			from &mpPromoD.
		;
		/*оставляем разрез пбо-товар-неделя-promo_group_id, агрегируя по promo_id*/
		create table casuser.media_wps{options replace=true} as
			select intnx('week.2',t1.PERIOD_DT,0) as period_dt
						,t2.channel_cd
						,t2.pbo_location_id
						,t2.product_id
						,t2.promo_group_id
						,avg(trp) as trp 
			from mn_long.media t1
			inner join casuser.promo_pbo_prod_dist t2
				on t1.promo_group_id=t2.promo_group_id
			group by 1,2,3,4,5
		;
		/*агрегируем пбо-товар-неделя-promo_group_id до ПБО*/
		create table casuser.media_wp{options replace=true} as
			select period_dt
					,channel_cd
					,pbo_location_id
					,product_id
					,count(distinct t1.promo_group_id) as dist_promo
					,sum(t1.trp) as sum_trp
			from casuser.media_wps t1
			group by 1,2,3,4
		;
	quit;

	/*4. Джоин со справочниками */
	proc fedsql sessref=casauto noprint;
		create table casuser.&lmvOutTabNamePmixSalAbt.{options replace=true} as
			select t1.CHANNEL_CD
					,t1.SALES_DT
					,t1.PBO_LOCATION_ID
					,t1.product_id
					,case 
						when t1.sales_dt<&VF_FC_START_DT
						then  sum(t1.sales_QTY,t1.sales_qty_rest,0) 
					end as sum_sales_qty
					,t2.LVL2_ID
					,t2.LVL3_ID
					,t3.PROD_LVL2_ID
					,t3.PROD_LVL3_ID
					,t3.PROD_LVL4_ID
					,t4.ff as gc
					,t5.gross_price_amt
					,t6.sum_prec
					,t6.avg_prec
					,t6.count_prec
					,t6.avg_temp
					,t6.max_temp
					,t6.min_temp
					,coalesce(t7.SUM_TRP,0) as sum_trp
					,coalesce(t7.DIST_PROMO,0) as dist_promo
			from casuser.TS_pmix_sales t1
			left join mn_long.PBO_DICTIONARY t2
				on t1.pbo_location_id=t2.pbo_location_id
			left join mn_long.product_dictionary t3
				on t1.product_id=t3.product_id
			left join casuser.gc_fc_fact t4
				on t1.pbo_location_id=t4.pbo_location_id
				and t1.channel_cd=t4.channel_cd
				and t1.sales_dt=t4.sales_dt
			left join casuser.TS_price_fact_agg t5
				on t1.pbo_location_id=t5.pbo_location_id 
				and t1.product_id=t5.product_id 
				and t1.sales_dt=t5.sales_dt
			left join &mpWeatherW. t6 
				on t1.pbo_location_id=t6.pbo_location_id
				and t1.sales_dt=t6.period_dt
			left join casuser.media_wp t7 
				on t1.sales_dt=t7.period_dt 
				and t1.product_id=t7.product_id 
				and t1.pbo_location_id=t7.pbo_location_id
				and t1.channel_cd=t7.channel_cd
			left join casuser.promo_dp t8 on
				t1.sales_dt=t8.period_dt 
				and t1.channel_cd=t8.channel_cd 
				and t1.product_id=t8.product_id 
				and t1.pbo_location_id=t8.pbo_location_id
			where t1.sales_dt>=&VF_HIST_START_DT and t1.channel_cd='ALL'
		;
	quit;

	proc casutil;
	  droptable casdata="&lmvOutTabNamePmixSalAbt._dlv" incaslib="mn_long" quiet;
	  droptable casdata="&lmvOutTabNamePmixSalAbt._dlv" incaslib="public" quiet;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.&lmvOutTabNamePmixSalAbt._dlv{options replace=true} as
			select t1.CHANNEL_CD
					,t1.SALES_DT
					,t1.PBO_LOCATION_ID
					,t1.product_id
					,case 
						when t1.sales_dt<&VF_FC_START_DT
						then  sum(t1.sales_QTY,t1.sales_qty_rest,0) 
					end as sum_sales_qty
					,t2.LVL2_ID
					,t2.LVL3_ID
					,t3.PROD_LVL2_ID
					,t3.PROD_LVL3_ID
					,t3.PROD_LVL4_ID
					,t4.ff as gc
					,t5.gross_price_amt
					,t6.sum_prec
					,t6.avg_prec
					,t6.count_prec
					,t6.avg_temp
					,t6.max_temp
					,t6.min_temp
					,coalesce(t7.SUM_TRP,0) as sum_trp
					,coalesce(t7.DIST_PROMO,0) as dist_promo
			from casuser.TS_pmix_sales t1
			left join mn_long.PBO_DICTIONARY t2
				on t1.pbo_location_id=t2.pbo_location_id
			left join mn_long.product_dictionary t3
				on t1.product_id=t3.product_id
			left join casuser.gc_fc_fact t4
				on t1.pbo_location_id=t4.pbo_location_id
				and t1.channel_cd=t4.channel_cd
				and t1.sales_dt=t4.sales_dt
			left join casuser.TS_price_fact_agg t5
				on t1.pbo_location_id=t5.pbo_location_id 
				and t1.product_id=t5.product_id 
				and t1.sales_dt=t5.sales_dt
			left join &mpWeatherW. t6 
				on t1.pbo_location_id=t6.pbo_location_id
				and t1.sales_dt=t6.period_dt
			left join casuser.media_wp t7 
				on t1.sales_dt=t7.period_dt 
				and t1.product_id=t7.product_id 
				and t1.pbo_location_id=t7.pbo_location_id
				and t1.channel_cd=t7.channel_cd
			left join casuser.promo_dp t8 on
				t1.sales_dt=t8.period_dt 
				and t1.channel_cd=t8.channel_cd 
				and t1.product_id=t8.product_id 
				and t1.pbo_location_id=t8.pbo_location_id
			where t1.sales_dt>=&VF_HIST_START_DT and t1.channel_cd='DLV'
		;
	quit;

	proc casutil;
     promote casdata="&lmvOutTabNamePmixSalAbt._dlv" incaslib="casuser" outcaslib="mn_long";
	 save incaslib="mn_long" outcaslib="mn_long" casdata="&lmvOutTabNamePmixSalAbt._dlv" casout="&lmvOutTabNamePmixSalAbt._dlv.sashdat" replace;
	run;
/*
	data public.&lmvOutTabNamePmixSalAbt._dlv(promote=yes);
		set mn_long.&lmvOutTabNamePmixSalAbt._dlv;
	run;
*/	
	proc casutil;
		promote casdata="&lmvOutTabNamePmixSalAbt." incaslib="casuser" outcaslib="&lmvOutLibrefPmixSalAbt.";
		save incaslib="&lmvOutLibrefPmixSalAbt." outcaslib="&lmvOutLibrefPmixSalAbt." casdata="&lmvOutTabNamePmixSalAbt." casout="&lmvOutTabNamePmixSalAbt..sashdat" replace;
		droptable casdata="TS_pmix_sales" incaslib="casuser" quiet;
		droptable casdata="TS_WEEK_OUTFOR" incaslib="casuser" quiet;
		droptable casdata="gc_fc_fact" incaslib="casuser" quiet;
		droptable casdata="TS_price_fact_agg" incaslib="casuser" quiet;
		droptable casdata="promo_dp" incaslib="casuser" quiet;
		droptable casdata="promo_pbo_prod_dist" incaslib="casuser" quiet;
		droptable casdata="media_wps" incaslib="casuser" quiet;
		droptable casdata="media_wp" incaslib="casuser" quiet;
		droptable casdata="pmix_sales_rest" incaslib="casuser" quiet;
		
		droptable casdata="&lmvOutTabNamePmixSalAbt._dlv" incaslib="dm_abt" quiet;
		droptable casdata="&lmvOutTabNamePmixSalAbt" incaslib="dm_abt" quiet;
	run; 
	/*
	data dm_abt.&lmvOutTabNamePmixSalAbt._dlv(promote=yes);
		set mn_long.&lmvOutTabNamePmixSalAbt._dlv;
	run;
	*/
	data dm_abt.&lmvOutTabNamePmixSalAbt.(promote=yes);
		set &lmvOutLibrefPmixSalAbt..&lmvOutTabNamePmixSalAbt.;
	run;
	
	/* Сохраняем витрины для VF */
	proc casutil;
		save incaslib="dm_abt" outcaslib="dm_abt" casdata="&lmvOutTabNamePmixSalAbt." casout="&lmvOutTabNamePmixSalAbt..sashdat" replace;
		*save incaslib="dm_abt" outcaslib="dm_abt" casdata="&lmvOutTabNamePmixSalAbt._dlv" casout="&lmvOutTabNamePmixSalAbt._dlv.sashdat" replace;
	quit;
	
	cas casauto terminate;
	
%mend vf_prepare_ts_abt_pmix_sep;