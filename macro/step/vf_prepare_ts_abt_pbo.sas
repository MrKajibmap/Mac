/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для подготовки таблицы mn_long.pbo_sal_abt, используемой. в сквозном процессе
*		для прогнозирования временными рядами. На основе указанной таблицы создается VF-проект, 
*		ID которого используется при построении витрины mn_long.pmix_sal_abt макроса 04_vf_prepare_ts_abt
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
*     %vf_prepare_ts_abt_pbo(mpPboSalAbt=mn_long.pbo_sal_abt,
*							mpPromoW1=mn_long.promo_w1,
*							mpPromoD=mn_long.promo_d, 
*							mpPboSales=mn_long.TS_pbo_sales,
*							mpWeatherW=mn_long.weather_w);
*
****************************************************************************
*  02-07-2020  Борзунов     Начальное кодирование
*  10-07-2020  Д Звежинский Убраны промо с product_id=1
*  28-07-2020  Борзунов 	Добавлен параметры mpPboSalAbt,mpPromoW1,mpPromoD,mpPboSales,mpWeatherW
*  06-10-2020  Д Звежинский Витрины собираются из чеков, восстановленных на периодах закрытия ПБО
****************************************************************************/
%macro vf_prepare_ts_abt_pbo(mpPboSalAbt=mn_long.pbo_sal_abt,
							mpPromoW1=mn_long.promo_w1,
							mpPromoD=mn_long.promo_d, 
							mpPboSales=mn_long.TS_pbo_sales,
							mpWeatherW=mn_long.weather_w );
	/*Создать cas-сессию, если её нет*/
	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;
	%local lmvOutLibrefPboSalAbt
			lmvOutTabNamePboSalAbt 
			lmvOutTabNamePromoW1 
			lmvOutLibrefPromoW1 
			lmvOutTabNamePromoD 
			lmvOutLibrefPromoD 
			lmvOutTabNameTsPboSales 
			lmvOutLibrefTsPboSales 
			lmvOutTabNameWeatherW 
			lmvOutLibrefWeatherW
			;
	%member_names (mpTable=&mpPboSalAbt, mpLibrefNameKey=lmvOutLibrefPboSalAbt, mpMemberNameKey=lmvOutTabNamePboSalAbt);
	%member_names (mpTable=&mpPromoW1, mpLibrefNameKey=lmvOutLibrefPromoW1, mpMemberNameKey=lmvOutTabNamePromoW1);
	%member_names (mpTable=&mpPromoD, mpLibrefNameKey=lmvOutLibrefPromoD, mpMemberNameKey=lmvOutTabNamePromoD);
	%member_names (mpTable=&mpPboSales, mpLibrefNameKey=lmvOutLibrefTsPboSales, mpMemberNameKey=lmvOutTabNameTsPboSales);
	%member_names (mpTable=&mpWeatherW, mpLibrefNameKey=lmvOutLibrefWeatherW, mpMemberNameKey=lmvOutTabNameWeatherW);
	/* 0. Удаление целевых таблиц */
	proc casutil;  
		droptable casdata="&lmvOutTabNamePboSalAbt." incaslib="&lmvOutLibrefPboSalAbt." quiet;
		droptable casdata="&lmvOutTabNamePromoW1" incaslib="&lmvOutLibrefPromoW1" quiet;
		droptable casdata="&lmvOutTabNamePromoD." incaslib="&lmvOutLibrefPromoD" quiet;
		droptable casdata="&lmvOutTabNameWeatherW" incaslib="&lmvOutLibrefWeatherW" quiet;
		droptable casdata="&lmvOutTabNameTsPboSales" incaslib="&lmvOutLibrefTsPboSales" quiet;
	run;
	
	/*1. Протяжка рядов pbo_sales и их аккумуляция */
	proc cas;
		timeData.timeSeries result =r /
		series={{name="receipt_qty", Acc="sum", setmiss="missing"},
	            {name="receipt_qty_rest", Acc="sum", setmiss="missing"}}
		tEnd= "&vf_fc_agg_end_dt" 
		table={caslib="mn_long",name="pbo_sales_rest", groupby={"PBO_LOCATION_ID","CHANNEL_CD"} ,
	       where="sales_dt>=&vf_hist_start_dt_sas"}
		timeId="SALES_DT"
		interval="week.2"
		trimId="LEFT"
		casOut={caslib="casuser",name="&lmvOutTabNameTsPboSales",replace=True}
	;
	run;
	quit;

	/*1.1 Макроэкономич факторы*/
	data casuser.macro2;
		format period_dt date9.;
		drop pdt;
		set mn_long.macro(rename=(period_dt=pdt));
		by name pdt;
		name=substr(name,1,3);
		period_dt=intnx('week.2',pdt,0,'b');
		do until (period_dt>=intnx('week.2',intnx('month',pdt,3,'b'),0,'b'));
			output;
			period_dt=intnx('week.2',period_dt,1,'b');
		end;
	run;
	/*защита от дубликатов в таблице выше, которые могут появиться, 
	если в данных ошибутся периодом действия показателя (не 3 мес, а меньше)*/
	data casuser.macro1;
		set casuser.macro2;
		by name period_dt;
		if first.period_dt then output;
	run;

	proc cas;
		transpose.transpose /
		table={name="macro1", caslib="casuser", groupby={"period_dt"}} 
		attributes={{name="period_dt"}} 
		transpose={"factor_pct"} 
		prefix="A_" 
		id={"name"} 
		casout={name="macro_transposed", caslib="casuser", replace=true};
	quit;

	/*1.2 Погода*/
	proc fedsql sessref=casauto noprint;
		create table casuser.&lmvOutTabNameWeatherW{options replace=true} as
			select PBO_LOCATION_ID
					,intnx('week.2',datepart(PERIOD_DT),0) as period_dt
					,sum(PRECIPITATION) as sum_prec
					,avg(PRECIPITATION) as avg_prec
					,sum(case when PRECIPITATION>0.1 then 1 else 0 end) as count_prec
					,avg(TEMPERATURE) as avg_temp
					,max(temperature) as max_temp
					,min(temperature) as min_temp  
			from mn_long.WEATHER
			group by 1,2
		;
	quit;

	/*1.3 Competitors */
	proc fedsql sessref=casauto noprint;
		create table casuser.comp_media1{options replace=true} as
			select COMPETITOR_CD
					,REPORT_DT
					,sum(trp) as trp
			from mn_long.comp_media
			group by 1,2
		;
	quit;

	proc cas;
		transpose.transpose /
		table={name="comp_media1", caslib="casuser", groupby={"report_dt"}} 
		attributes={{name="report_dt"}} 
		transpose={"trp"} 
		prefix="T_" 
		id={"competitor_cd"} 
		casout={name="media_transposed", caslib="casuser", replace=true};
	quit;

	/*1.4 Promo*/
	/*Expand PBO into leaf level*/
	proc fedsql sessref=casauto noprint;
		create table casuser.promo_pbo_exp1{options replace=true} as
			select t1.PROMO_ID
					,t2.PBO_LOCATION_ID
			from mn_long.PROMO_PBO t1 inner join mn_long.PBO_DICTIONARY t2
				on t1.pbo_location_id=t2.LVL1_ID
		;
				
		create table casuser.promo_pbo_exp2{options replace=true} as
			select t1.PROMO_ID
					,t2.PBO_LOCATION_ID
			from mn_long.PROMO_PBO t1 inner join mn_long.PBO_DICTIONARY t2
				on t1.pbo_location_id=t2.LVL2_ID
		;
		create table casuser.promo_pbo_exp3{options replace=true} as
			select t1.PROMO_ID
					,t2.PBO_LOCATION_ID
			from mn_long.PROMO_PBO t1 inner join mn_long.PBO_DICTIONARY t2
				on t1.pbo_location_id=t2.LVL3_ID
		;
		create table casuser.promo_pbo_exp4{options replace=true} as
			select t1.PROMO_ID
					,t2.PBO_LOCATION_ID
			from mn_long.PROMO_PBO t1 inner join mn_long.PBO_DICTIONARY t2
				on t1.pbo_location_id=t2.pbo_location_id
		;
	quit;

	data casuser.promo_pbo_exp1(append=force);
		set casuser.promo_pbo_exp2
			casuser.promo_pbo_exp3
			casuser.promo_pbo_exp4;
	run;
	/*Expand products into leaf level*/
	proc fedsql sessref=casauto noprint;
		create table casuser.promo_prod_exp2{options replace=true} as
			select t1.PROMO_ID
					,t2.Product_ID
			from mn_long.PROMO_PROD t1 
			inner join mn_long.product_dictionary t2
				on t1.product_id=t2.PROD_LVL2_ID
		;
		create table casuser.promo_prod_exp3{options replace=true} as
			select t1.PROMO_ID
					,t2.Product_ID
			from mn_long.PROMO_PROD t1
			inner join mn_long.product_dictionary t2
				on t1.product_id=t2.PROD_LVL3_ID
		;
		create table casuser.promo_prod_exp4{options replace=true} as
			select t1.PROMO_ID
					,t2.Product_ID
			from mn_long.PROMO_PROD t1 
			inner join mn_long.product_dictionary t2
				on t1.product_id=t2.PROD_LVL4_ID
		;
		create table casuser.promo_prod_exp1{options replace=true} as
			select t1.PROMO_ID
					,t2.Product_ID
			from mn_long.PROMO_PROD t1 
			inner join mn_long.product_dictionary t2
				on t1.product_id=t2.product_id
		;
	quit;

	data casuser.promo_prod_exp1(append=force);
		set casuser.promo_prod_exp2 
			casuser.promo_prod_exp3 
			casuser.promo_prod_exp4
		;
	run;

	/*Join promo, expanded pbo and expanded products*/
	proc fedsql sessref=casauto noprint;
		create table casuser.&lmvOutTabNamePromoD.{options replace=true} as
			select t1.START_DT
					,t1.END_DT
					,t1.channel_cd
					,t1.promo_id
					,t1.promo_group_id
					,t2.pbo_location_id
					,t3.product_id
			from mn_long.promo t1 
			inner join casuser.PROMO_PBO_exp1 t2
				on t1.promo_id=t2.promo_id
			inner join casuser.PROMO_PROD_exp1 t3
				on t1.promo_id=t3.promo_id
		;
	quit;

	data casuser.promo_w2;
		set casuser.&lmvOutTabNamePromoD;
		format period_dt date9.;
		do period_dt=start_DT to min(end_DT,&VF_FC_AGG_END_DT_sas);
		output;
		end;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.&lmvOutTabNamePromoW1{options replace=true} as
			select distinct t1.channel_cd
							,t1.pbo_location_id
							,t1.product_id,t1.period_dt
							,cast(1 as double) as promo
			from casuser.promo_w2 t1
		;
	quit;

	/*агрегация дубликатов по ключу "магазин"*/
	proc fedsql sessref=casauto noprint;
		create table casuser.promo_da{options replace=true} as
			select t1.channel_cd
					,t1.pbo_location_id
					,intnx('week.2',period_DT,0,'b') as period_dt
					,sum(promo) as sum_promo_mkup
					,count(distinct product_id) as count_promo_product
			from casuser.&lmvOutTabNamePromoW1 t1 
			group by 1,2,3
		;
	quit;

	/*1.5 TRP в разрезе ПБО*/
	/*агрегируем дубли по ключу*/
	proc fedsql sessref=casauto noprint;
		create table casuser.promo_pbo_dist{options replace=true} as
			select distinct promo_group_id
							,promo_id
							,pbo_location_id
							,channel_cd 
			from casuser.&lmvOutTabNamePromoD
		;
	quit;
	
	proc fedsql sessref=casauto noprint;
		/*оставляем разрез пбо-неделя-promo_group_id, агрегируя по promo_id*/
		create table casuser.media_ws{options replace=true} as
			select intnx('week.2',t1.PERIOD_DT,0) as period_dt
					,t2.pbo_location_id
					,t2.channel_cd
					,t2.promo_group_id
					,avg(trp) as trp
			from mn_long.media t1 
			inner join casuser.promo_pbo_dist t2
				on t1.promo_group_id=t2.promo_group_id
			group by 1,2,3,4
		;
	quit;
	
	proc fedsql sessref=casauto noprint;
		/*агрегируем пбо-неделя-promo_group_id до ПБО*/
		create table casuser.media_w{options replace=true} as
			select period_dt
					,pbo_location_id
					,channel_cd
					,count(distinct t1.promo_group_id) as dist_promo/*сколько разных промо-кампаний действует на пбо одноверменно*/
					,sum(t1.trp) as sum_trp /*суммируем trp только по разным promo_group*/
			from casuser.media_ws t1
			group by 1,2,3
		;
	quit;

	/*2. Джоин со справочниками и indep */
	proc fedsql sessref=casauto noprint;
		create table casuser.&lmvOutTabNamePboSalAbt.{options replace=true} as
			select t1.CHANNEL_CD
					,t1.SALES_DT
					,t1.PBO_LOCATION_ID
					,case
						when t1.sales_dt<&VF_FC_START_DT 
						then coalesce(t1.RECEIPT_QTY,t1.Receipt_qty_rest,0) 
					end as receipt_qty
					,t2.LVL2_ID
					,t2.LVL3_ID
					,t3.AVG_PREC
					,t3.AVG_TEMP
					,t3.COUNT_PREC
					,t3.MAX_TEMP
					,t3.MIN_TEMP
					,t3.SUM_PREC
					,coalesce(t4.A_CPI,0) as a_cpi
					,coalesce(t4.A_GPD,0) as a_gpd
					,coalesce(t4.A_RDI,0) as a_rdi
					,coalesce(t5.T_BK,0) as trp_bk
					,coalesce(t5.T_KFC,0) as trp_kfc
					,coalesce(t6.COUNT_PROMO_PRODUCT,0) as COUNT_PROMO_PRODUCT
					,coalesce(t6.SUM_PROMO_MKUP,0) as SUM_PROMO_MKUP
					,coalesce(t7.sum_trp,0) as sum_trp
					,coalesce(t7.dist_promo,0) as dist_promo
			from casuser.&lmvOutTabNameTsPboSales t1 
			left join mn_long.PBO_DICTIONARY t2
				on t1.pbo_location_id=t2.pbo_location_id
			left join casuser.&lmvOutTabNameWeatherW t3
				on t1.pbo_location_id=t3.pbo_location_id
				and t1.sales_dt=t3.period_dt
			left join casuser.macro_transposed t4 
				on t1.sales_dt=t4.period_dt
			left join casuser.media_transposed t5
				on t1.sales_dt=t5.report_dt
			left join casuser.promo_da t6
				on t1.sales_dt=t6.period_dt
				and t1.pbo_location_id=t6.pbo_location_id
				and	t1.channel_cd=t6.channel_cd
			left join casuser.media_w t7
				on t1.sales_dt=t7.period_dt 
				and t1.pbo_location_id=t7.pbo_location_id
				and	t1.channel_cd=t7.channel_cd
			where t1.sales_dt>=&VF_HIST_START_DT and t1.channel_cd='ALL'
		;
	quit;

	/*For Building Blocks*/
	proc casutil;
	  droptable casdata="&lmvOutTabNamePboSalAbt._dlv" incaslib="mn_long" quiet;
	   *droptable casdata="&lmvOutTabNamePboSalAbt._dlv" incaslib="public" quiet;
	run;
	
	proc fedsql sessref=casauto;
	  create table casuser.&lmvOutTabNamePboSalAbt._dlv{options replace=true} as
	  select t1.CHANNEL_CD, t1.SALES_DT, t1.PBO_LOCATION_ID, 
	   case when t1.sales_dt<&vf_fc_start_dt then
	   sum(t1.RECEIPT_QTY,t1.Receipt_qty_rest,0) end as receipt_qty,
		t2.LVL2_ID,
		t2.LVL3_ID,
		t3.AVG_PREC,
		t3.AVG_TEMP, 
		t3.COUNT_PREC,
		t3.MAX_TEMP, 
		t3.MIN_TEMP, 
		t3.SUM_PREC,
		coalesce(t4.A_CPI,0) as a_cpi,
		coalesce(t4.A_GPD,0) as a_gpd,
		coalesce(t4.A_RDI,0) as a_rdi,
		coalesce(t5.T_BK,0) as trp_bk,
		coalesce(t5.T_KFC,0) as trp_kfc,
		coalesce(t6.COUNT_PROMO_PRODUCT,0) as COUNT_PROMO_PRODUCT, /*не очень хороший предиктор, особенно при наличии акций на все товары*/
		coalesce(t6.SUM_PROMO_MKUP,0) as SUM_PROMO_MKUP, /*число промо-дней на товары в магазине (за неделю) - аналогично*/
		coalesce(t7.sum_trp,0) as sum_trp,
		coalesce(t7.dist_promo,0) as dist_promo /*число различных промо-кампаний, идущих одновременно в пбо*/
	  from casuser.&lmvOutTabNameTsPboSales t1 
	  left join mn_long.PBO_DICTIONARY t2
	  on t1.pbo_location_id=t2.pbo_location_id
	  left join casuser.&lmvOutTabNameWeatherW t3
	  on t1.pbo_location_id=t3.pbo_location_id 
	  and t1.sales_dt=t3.period_dt
	  left join casuser.macro_transposed t4 
	  on t1.sales_dt=t4.period_dt
	  left join casuser.media_transposed t5
	  on t1.sales_dt=t5.report_dt
	  left join casuser.promo_da t6
	  on t1.sales_dt=t6.period_dt 
	  and t1.pbo_location_id=t6.pbo_location_id 
	  and t1.channel_cd=t6.channel_cd
	  left join casuser.media_w t7
	  on t1.sales_dt=t7.period_dt 
	  and t1.pbo_location_id=t7.pbo_location_id 
	  and t1.channel_cd=t7.channel_cd
	  where t1.sales_dt>=&vf_hist_start_dt and t1.channel_cd='DLV'
	;
	quit;
	proc casutil;
	  promote casdata="&lmvOutTabNamePboSalAbt._dlv" incaslib="casuser" outcaslib="mn_long";
	   save incaslib="mn_long" outcaslib="mn_long" casdata="&lmvOutTabNamePboSalAbt._dlv" casout="&lmvOutTabNamePboSalAbt._dlv.sashdat" replace;
	run;
	/*
	data public.&lmvOutTabNamePboSalAbt._dlv(promote=yes);
		set mn_long.&lmvOutTabNamePboSalAbt._dlv;
	run;
	*/
	proc casutil;  
		promote casdata="&lmvOutTabNamePboSalAbt." incaslib="casuser" outcaslib="&lmvOutLibrefPboSalAbt.";
		promote casdata="&lmvOutTabNamePromoW1" incaslib="casuser" outcaslib="&lmvOutLibrefPromoW1";
		promote casdata="&lmvOutTabNamePromoD." incaslib="casuser" outcaslib="&lmvOutLibrefPromoD"; 
		promote casdata="&lmvOutTabNameWeatherW" incaslib="casuser" outcaslib="&lmvOutLibrefWeatherW";
		promote casdata="&lmvOutTabNameTsPboSales" incaslib="casuser" outcaslib="&lmvOutLibrefTsPboSales";
		save incaslib="&lmvOutLibrefPboSalAbt." outcaslib="&lmvOutLibrefPboSalAbt." casdata="&lmvOutTabNamePboSalAbt." casout="&lmvOutTabNamePboSalAbt..sashdat" replace;
		droptable casdata="macro2" incaslib="casuser" quiet;
		droptable casdata="macro1" incaslib="casuser" quiet;	
		droptable casdata="macro_transposed" incaslib="casuser" quiet;
		droptable casdata="media_transposed" incaslib="casuser" quiet;
		droptable casdata="promo_pbo_exp1" incaslib="casuser" quiet;
		droptable casdata="promo_pbo_exp2" incaslib="casuser" quiet;
		droptable casdata="promo_pbo_exp3" incaslib="casuser" quiet;
		droptable casdata="promo_pbo_exp4" incaslib="casuser" quiet;
		droptable casdata="promo_prod_exp1" incaslib="casuser" quiet;
		droptable casdata="promo_prod_exp2" incaslib="casuser" quiet;
		droptable casdata="promo_prod_exp3" incaslib="casuser" quiet;
		droptable casdata="promo_prod_exp4" incaslib="casuser" quiet;
		droptable casdata="promo_prod_exp5" incaslib="casuser" quiet;
		droptable casdata="promo_w2" incaslib="casuser" quiet;
		droptable casdata="promo_da" incaslib="casuser" quiet;
		droptable casdata="promo_pbo_dist" incaslib="casuser" quiet;
		droptable casdata="media_ws" incaslib="casuser" quiet;
		droptable casdata="media_w" incaslib="casuser" quiet;
		droptable casdata="macro1" incaslib="casuser" quiet;
		droptable casdata="pbo_sales_rest" incaslib="mn_long" quiet;
		
		droptable casdata="&lmvOutTabNamePboSalAbt." incaslib="dm_abt" quiet;
		droptable casdata="&lmvOutTabNamePboSalAbt._dlv" incaslib="dm_abt" quiet;
	run;

	data dm_abt.&lmvOutTabNamePboSalAbt.(promote=yes);
		set &lmvOutLibrefPboSalAbt..&lmvOutTabNamePboSalAbt.;
	run;
	
/*
	data dm_abt.&lmvOutTabNamePboSalAbt._dlv(promote=yes);
		set mn_long.&lmvOutTabNamePboSalAbt._dlv;
	run;
*/	
	proc casutil;
		save incaslib="dm_abt" outcaslib="dm_abt" casdata="&lmvOutTabNamePboSalAbt." casout="&lmvOutTabNamePboSalAbt..sashdat" replace;
		*save incaslib="dm_abt" outcaslib="dm_abt" casdata="&lmvOutTabNamePboSalAbt._dlv" casout="&lmvOutTabNamePboSalAbt._dlv.sashdat" replace;
	quit;
	
	cas casauto terminate;

%mend vf_prepare_ts_abt_pbo;