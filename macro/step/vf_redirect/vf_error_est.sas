/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Сборка таблиц для оценки точности прогноза по месяцам и неделям
*
*  ПАРАМЕТРЫ:
*     Нет
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
*     %vf_error_est;
*
****************************************************************************
*  08-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf_error_est;

	%local lmvWeekData lmvMonthData lmvGcMonthData;
	%let lmvWeekData=dm_abt.ts_outfor;
	%let lmvMonthData=dm_abt.plan_pmix_month;
	%let lmvGcMonthData=dm_abt.plan_gc_month;
	
	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
	%end;
	
	caslib _all_ assign;
	/*Промо*/
	/*Expand PBO into leaf level*/
	proc fedsql sessref=casauto noprint;
		create table casuser.promo_pbo_exp1{options replace=true} as
			select t1.PROMO_ID
					,t2.PBO_LOCATION_ID 
			from casuser.PROMO_PBO t1
			inner join casuser.pbo_dictionary t2
			on t1.pbo_location_id=t2.LVL1_ID
		;
		create table casuser.promo_pbo_exp2{options replace=true} as
			select t1.PROMO_ID
					,t2.PBO_LOCATION_ID
			from casuser.PROMO_PBO t1
			inner join casuser.pbo_dictionary t2
			on t1.pbo_location_id=t2.LVL2_ID
		;
		create table casuser.promo_pbo_exp3{options replace=true} as
			select t1.PROMO_ID
					,t2.PBO_LOCATION_ID
			from casuser.PROMO_PBO t1
			inner join casuser.pbo_dictionary t2
			on t1.pbo_location_id=t2.LVL3_ID
		;
		create table casuser.promo_pbo_exp4{options replace=true} as
			select t1.PROMO_ID
					,t2.PBO_LOCATION_ID
			from casuser.PROMO_PBO t1
			inner join casuser.pbo_dictionary t2
			on t1.pbo_location_id=t2.pbo_location_id
		;
	quit;

	data casuser.promo_pbo_exp1(append=force);
		set casuser.promo_pbo_exp2
			casuser.promo_pbo_exp3
			casuser.promo_pbo_exp4
		;
	run;
	/*Expand products into leaf level*/
	proc fedsql sessref=casauto noprint;
		create table casuser.promo_prod_exp1{options replace=true} as
			select t1.PROMO_ID
					,t2.Product_ID
			from casuser.PROMO_PROD t1
			inner join casuser.product_dictionary t2
				on t1.product_id=t2.PROD_LVL1_ID
		;
		create table casuser.promo_prod_exp2{options replace=true} as
			select t1.PROMO_ID
					,t2.Product_ID
			from casuser.PROMO_PROD t1
			inner join casuser.product_dictionary t2
				on t1.product_id=t2.PROD_LVL2_ID
		;
		create table casuser.promo_prod_exp3{options replace=true} as
			select t1.PROMO_ID
					,t2.Product_ID
			from casuser.PROMO_PROD t1
			inner join casuser.product_dictionary t2
				on t1.product_id=t2.PROD_LVL3_ID
		;
		create table casuser.promo_prod_exp4{options replace=true} as
			select t1.PROMO_ID
					,t2.Product_ID
			from casuser.PROMO_PROD t1
			inner join casuser.product_dictionary t2
				on t1.product_id=t2.PROD_LVL4_ID
		;
		create table casuser.promo_prod_exp5{options replace=true} as
			select t1.PROMO_ID
					,t2.Product_ID
			from casuser.PROMO_PROD t1 
			inner join casuser.product_dictionary t2
				on t1.product_id=t2.product_id
		;
	quit;

	data casuser.promo_prod_exp1(append=force);
		set casuser.promo_prod_exp2
			casuser.promo_prod_exp3
			casuser.promo_prod_exp4
			casuser.promo_prod_exp5
		;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.promo_d{options replace=true} as
			select t1.START_DT
			,t1.END_DT
			,t1.channel_cd
			,t1.promo_id
			,t2.pbo_location_id
			,t3.product_id
			from casuser.promo t1
			inner join casuser.PROMO_PBO_exp1 t2
				on t1.promo_id=t2.promo_id
			inner join casuser.PROMO_PROD_exp1 t3
				on t1.promo_id=t3.promo_id
		;
	quit;

	data casuser.promo_day;
		set casuser.promo_d;
		format period_dt date9.;
		retain pr_ 1;
		do period_dt=start_dt to end_dt;
		if period_dt>=&VF_FC_START_MONTH_SAS and period_dt<=&VF_FC_AGG_END_DT_sas then output;
		end;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.promo_day_nodup{options replace=true} as
			select distinct channel_cd
							,pbo_location_id
							,product_id
							,period_dt
							,pr_
			from casuser.promo_day
		;
	quit;

	/*Вытащить факт*/
	proc cas;
		timeData.timeSeries result =r /
		series={{name="sales_qty", Acc="sum", setmiss=0},
		{name="sales_qty_promo", Acc="sum", setmiss=0}}
		tEnd= "&VF_FC_AGG_END_DT" 
		table={caslib="casuser",name="pmix_sales", groupby={"PBO_LOCATION_ID","PRODUCT_ID","CHANNEL_CD"} ,
		where="sales_dt>=&VF_FC_START_MONTH_SAS."}
		timeId="SALES_DT"
		interval="day"
		casOut={caslib="casuser",name="TS_pmix_fact_day",replace=True}
		;
		run;
	quit;

	proc fedsql sessref=casauto noprint;
		create table casuser.fact_by_day_promo{options replace=true} as 
		select t1.CHANNEL_CD
				, t1.PBO_LOCATION_ID
				, t1.PRODUCT_ID
				,t1.SALES_DT
				, coalesce(t1.sales_QTY,0)+coalesce(t1.sales_qty_promo,0) as sum_sales_qty
				,coalesce(t2.pr_,0) as promo
		from casuser.TS_pmix_fact_day t1
		left join casuser.promo_day_nodup t2
			on t1.channel_cd=t2.channel_cd 
			and t1.product_id=t2.product_id 
			and t1.pbo_location_id=t2.pbo_location_id
			and t1.sales_dt=t2.period_dt
		;
	quit;
	/*агрегация до разрезов товар-пбо-канал-промо-неделя или -месяц*/
	proc cas;
		timeData.timeSeries result =r /
		series={{name="sum_sales_qty", Acc="sum", setmiss=0}}
		tEnd= "&VF_FC_AGG_END_DT" 
		table={caslib="casuser",name="fact_by_day_promo", groupby={"PBO_LOCATION_ID","PRODUCT_ID","CHANNEL_CD","PROMO"} ,
		where="sales_dt>=&VF_FC_START_MONTH_SAS."}
		timeId="SALES_DT"
		interval="month"
		casOut={caslib="casuser",name="pmix_fact_month",replace=True}
		;
		run;
	quit;
	
	proc cas;
		timeData.timeSeries result =r /
		series={{name="sum_sales_qty", Acc="sum", setmiss=0}}
		tEnd= "&VF_FC_AGG_END_DT" 
		table={caslib="casuser",name="fact_by_day_promo", groupby={"PBO_LOCATION_ID","PRODUCT_ID","CHANNEL_CD"} ,
		where="sales_dt>=&VF_FC_START_DT_sas."}
		timeId="SALES_DT"
		interval="week.2"
		casOut={caslib="casuser",name="pmix_fact_week",replace=True}
		;
	run;
	quit;

	proc casutil;
		droptable casdata="TS_estimate_month" incaslib="casuser" quiet;
	run;

	proc fedsql sessref=casauto noprint;
	create table casuser.TS_estimate_month{options replace=true} as
		select t1.CHANNEL_CD
				,t1.PBO_LOCATION_ID
				,strip(put(t1.PRODUCT_ID,8.))||':'||coalesce(strip(t3.product_nm),'N/A') as PRODUCT_ID
				,t1.FF
				,t1.mon_DT
				,t1.promo
				,t2.sum_sales_qty
				,t3.PROD_LVL2_NM
				,t3.PROD_LVL3_NM
				,t3.PROD_LVL4_NM
				,t4.LVL2_ID
				,t4.LVL3_ID
	from &lmvMonthData t1 
	left join casuser.pmix_fact_month t2
		on t1.product_id=t2.product_id
		and t1.pbo_location_id=t2.pbo_location_id
		and t1.CHANNEL_CD=t2.CHANNEL_CD
		and t1.mon_dt=t2.sales_dt
		and t1.promo=t2.promo
	left join casuser.product_dictionary t3 
		on t1.product_id=t3.product_id
	left join casuser.pbo_dictionary t4
		on t1.pbo_location_id=t4.pbo_location_id
	;
	quit;

	proc casutil;
		promote casdata="TS_estimate_month" incaslib="casuser" outcaslib="casuser";
	run;
	/*-=-=-=-Alternative estimate_month: no promo in the key (promo=2)-=-=-=-=-*/
	proc cas;
		timeData.timeSeries result =r /
		series={{name="sales_qty", Acc="sum", setmiss=0},
		{name="sales_qty_promo", Acc="sum", setmiss=0}}
		tEnd= "&VF_FC_AGG_END_DT" 
		table={caslib="casuser",name="pmix_sales", groupby={"PBO_LOCATION_ID","PRODUCT_ID","CHANNEL_CD"} ,
		where="sales_dt>=&VF_FC_START_MONTH_SAS."}
		timeId="SALES_DT"
		interval="month"
		casOut={caslib="casuser",name="pmix_fact_month_np",replace=True}
		;
		run;
	quit;

	proc fedsql sessref=casauto noprint;
		create table casuser.lmvMonthData_agg_np {options replace=true} as	
			select  t1.CHANNEL_CD
					,t1.PBO_LOCATION_ID
					,t1.PRODUCT_ID
					,t1.mon_DT
					,sum(t1.FF) as ff
			from &lmvMonthData t1
			group by 1,2,3,4
		;
	quit;

	proc casutil;
		droptable casdata="TS_estimate_month_np" incaslib="casuser" quiet;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.TS_estimate_month_np{options replace=true} as
			select t1.CHANNEL_CD
					, t1.PBO_LOCATION_ID
					,strip(put(t1.PRODUCT_ID,8.))||':'||coalesce(strip(t3.product_nm),'N/A') as PRODUCT_ID
					,t1.FF
					, t1.mon_DT
					, cast(2 as double) as promo
					,coalesce(t2.sales_QTY,0)+coalesce(t2.sales_qty_promo,0) as sum_sales_qty
					,t3.PROD_LVL2_NM
					,t3.PROD_LVL3_NM
					,t3.PROD_LVL4_NM
					,t4.LVL2_ID
					,t4.LVL3_ID
			from casuser.lmvMonthData_agg_np t1
			left join casuser.pmix_fact_month_np t2
				on t1.product_id=t2.product_id
				and t1.pbo_location_id=t2.pbo_location_id
				and t1.CHANNEL_CD=t2.CHANNEL_CD
				and t1.mon_dt=t2.sales_dt
			left join casuser.product_dictionary t3 
				on t1.product_id=t3.product_id
			left join casuser.pbo_dictionary t4
				on t1.pbo_location_id=t4.pbo_location_id
		;
	quit;

	proc casutil;
		promote casdata="TS_estimate_month_np" incaslib="casuser" outcaslib="casuser";
	run;
	/*-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
	proc casutil;
		droptable casdata="TS_estimate_week" incaslib="casuser" quiet;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.TS_estimate_week{options replace=true} as
			select t1.CHANNEL_CD
					, t1.PBO_LOCATION_ID
					,strip(put(t1.PRODUCT_ID,8.))||':'||coalesce(strip(t3.product_nm),'N/A') as PRODUCT_ID
					,t1.FF
					, t1.sales_DT 
					,t2.sum_sales_qty
					,t3.PROD_LVL2_NM
					,t3.PROD_LVL3_NM
					,t3.PROD_LVL4_NM
					,t4.LVL2_ID
					,t4.LVL3_ID
			from &lmvWeekData t1 left join casuser.pmix_fact_week t2
				on t1.product_id=t2.product_id
				and t1.pbo_location_id=t2.pbo_location_id
				and t1.CHANNEL_CD=t2.CHANNEL_CD
				and t1.sales_dt=t2.sales_dt
			left join casuser.product_dictionary t3 
				on t1.product_id=t3.product_id
			left join casuser.pbo_dictionary t4
				on t1.pbo_location_id=t4.pbo_location_id
		;
	quit;

	proc casutil;
		promote casdata="TS_estimate_week" incaslib="casuser" outcaslib="casuser";
	run;
	/*-=-=-=-=-=-=-GC-=-=-=-=-=-=-=-=-=-=-*/
	proc cas;
		timeData.timeSeries result =r /
		series={{name="receipt_qty", Acc="sum", setmiss="missing"}}
		tEnd= "&VF_FC_AGG_END_DT" 
		table={caslib="casuser",name="pbo_sales", groupby={"PBO_LOCATION_ID","CHANNEL_CD"} ,
		where="sales_dt>=&VF_FC_START_MONTH_SAS and channel_cd='ALL'"}
		timeId="SALES_DT"
		interval="month"
		casOut={caslib="casuser",name="gc_fact_month",replace=True}
		;
		run;
	quit;
	proc casutil;
		droptable casdata="GC_estimate_month" incaslib="casuser" quiet;
	run;

	proc fedsql sessref=casauto noprint;
	create table casuser.GC_estimate_month{options replace=true} as
		select t1.CHANNEL_CD
				, t1.PBO_LOCATION_ID
				,t1.FF
				, t1.mon_DT
				,t2.receipt_qty
				,t4.LVL2_ID
				,t4.LVL3_ID
		from &lmvGcMonthData t1
		left join casuser.gc_fact_month t2
			on t1.pbo_location_id=t2.pbo_location_id
			and t1.CHANNEL_CD=t2.CHANNEL_CD
			and t1.mon_dt=t2.sales_dt
		left join casuser.pbo_dictionary t4
			on t1.pbo_location_id=t4.pbo_location_id
	;
	quit;

	proc casutil;
		promote casdata="GC_estimate_month" incaslib="casuser" outcaslib="casuser";
	run;

%mend vf_error_est;