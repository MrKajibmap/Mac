/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для подготовки таблицы pmix_sal_abt в выбранную директорию (по умолчанию - dm_abt), используемой в сквозном процессе
*		для прогнозирования временными рядами. На указанной таблице строится VF-проект,
*		ID которого используется в макросе 06_vf_month_aggregation
*	
*
*  ПАРАМЕТРЫ:
*	  mpVfPboProjName       - Наименование VF-проекта
*	  mpPmixSalAbt			- Наименование выходной таблицы (по умолчанию - dm_abt.pmix_sal_abt)
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
*     %vf_prepare_ts_abt_pmix_old(mpVfPboProjName=pbo_sales_v2,
							mpPmixSalAbt=dm_abt.pmix_sal_abt,
*							mpPromoW1=dm_abt.promo_w1,
*							mpPromoD=dm_abt.promo_d,
*							mpPboSales=dm_abt.TS_pbo_sales,
*							mpWeatherW=dm_abt.weather_w);
*
****************************************************************************
*  02-07-2020  Борзунов     Начальное кодирование
*  28-07-2020  Борзунов		Изменен промоут промежуточных таблиц на casuser. Целевой витрины на dm_abt.
							Добавлен параметры mpPmixSalAbt mpPromoW1 mpPromoD mpPboSales mpWeatherW
*  11-08-2020  Борзунов		Добавлено получение ID VF-проекта по его имени + параметр mpVfPboProjName
****************************************************************************/
%macro vf_prepare_ts_abt_pmix_old(mpVfPboProjName=pbo_sales_v2,
							mpPmixSalAbt=dm_abt.pmix_sal_abt,
							mpPromoW1=dm_abt.promo_w1,
							mpPromoD=dm_abt.promo_d,
							mpPboSales=dm_abt.TS_pbo_sales,
							mpWeatherW=dm_abt.weather_w);

	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;
	%local lmvOutLibrefPmixSalAbt lmvOutTabNamePmixSalAbt lmvVfPboName lmvVfPboId;
	
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
	proc cas;
		timeData.timeSeries result =r /
		series={{name="sales_qty", Acc="sum", setmiss="missing"},
		{name="gross_sales_amt", Acc="sum", setmiss="missing"},
		{name="net_sales_amt", Acc="sum", setmiss="missing"},
		{name="sales_qty_promo", Acc="sum", setmiss="missing"}}
		tEnd= "&VF_FC_AGG_END_DT" /*VF_FC_START_DT+hor*/
		table={caslib="casuser",name="pmix_sales", groupby={"PBO_LOCATION_ID","PRODUCT_ID","CHANNEL_CD"} ,
		where="sales_dt>=&VF_HIST_START_DT_SAS and channel_cd='ALL'"}
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

	/*3.2 Цены - независимый фактор*/
	proc fedsql sessref=casauto noprint;
		select max(START_DT) as max_st_dt
				,min(START_DT) as min_st_dt
				,max(END_DT) as max_end_dt
				,min(end_dt) as min_end_dt
		from casuser.PRICE
		;
	quit;

	/*приводим к ценам по дням*/
	data casuser.price_unfolded(replace=yes) / SESSREF=casauto;
		set casuser.PRICE;
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
						,pbo_location_id
						,product_id
			from &mpPromoD.
		;
		/*оставляем разрез пбо-товар-неделя-promo_group_id, агрегируя по promo_id*/
		create table casuser.media_wps{options replace=true} as
			select intnx('week.2',t1.PERIOD_DT,0) as period_dt
						,t2.pbo_location_id
						,t2.product_id
						,t2.promo_group_id
						,avg(trp) as trp 
			from casuser.media t1
			inner join casuser.promo_pbo_prod_dist t2
				on t1.promo_group_id=t2.promo_group_id
			group by 1,2,3,4
		;
		/*агрегируем пбо-товар-неделя-promo_group_id до ПБО*/
		create table casuser.media_wp{options replace=true} as
			select period_dt
					,pbo_location_id,product_id
					,count(distinct t1.promo_group_id) as dist_promo
					,sum(t1.trp) as sum_trp
			from casuser.media_wps t1
			group by 1,2,3
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
						then coalesce(t1.sales_QTY,0)+coalesce(t1.sales_qty_promo,0) 
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
			left join casuser.PBO_DICTIONARY t2
				on t1.pbo_location_id=t2.pbo_location_id
			left join casuser.product_dictionary t3
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
			left join casuser.promo_dp t8 on
				t1.sales_dt=t8.period_dt 
				and t1.channel_cd=t8.channel_cd 
				and t1.product_id=t8.product_id 
				and t1.pbo_location_id=t8.pbo_location_id
			where t1.sales_dt>=&VF_HIST_START_DT
		;
	quit;

	proc casutil;
		promote casdata="&lmvOutTabNamePmixSalAbt." incaslib="casuser" outcaslib="&lmvOutLibrefPmixSalAbt.";
		droptable casdata="TS_pmix_sales" incaslib="casuser" quiet;
		droptable casdata="TS_WEEK_OUTFOR" incaslib="casuser" quiet;
		droptable casdata="gc_fc_fact" incaslib="casuser" quiet;
		droptable casdata="TS_price_fact_agg" incaslib="casuser" quiet;
		droptable casdata="promo_dp" incaslib="casuser" quiet;
		droptable casdata="promo_pbo_prod_dist" incaslib="casuser" quiet;
		droptable casdata="media_wps" incaslib="casuser" quiet;
		droptable casdata="media_wp" incaslib="casuser" quiet;
	run; 
%mend vf_prepare_ts_abt_pmix_old;