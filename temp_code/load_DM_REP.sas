/* proc printto log='/opt/sas/mcd_config/temp_code/dm_rep_28052020.log'; */
/* run; */

%let lmvReportDt=%sysfunc(inputn(25MAY2020,date9.));
%let lmvReportDttm=%sysfunc(dhms(&lmvReportDt.,0,0,0));
/* Временный отбор данных */

	libname ETL_STG2 "/data/ETL_STG";

	data WORK.PRODUCT_HIERARCHY;
		set ETL_STG2.IA_PRODUCT_HIERARCHY;
	run;

	data WORK.PRODUCT_ATTRIBUTES;
		set ETL_STG2.IA_PRODUCT_ATTRIBUTES;
	run;

	data WORK.PRODUCT;
		set ETL_STG2.IA_PRODUCT;
	run;

	data WORK.PBO_LOC_HIERARCHY;
		set ETL_STG2.IA_PBO_LOC_HIERARCHY;
	run;

	data WORK.PBO_LOC_ATTRIBUTES;
		set ETL_STG2.IA_PBO_LOC_ATTRIBUTES;
	run;

	data WORK.PBO_LOCATION;
		set ETL_STG2.IA_PBO_LOCATION;
	run;

	data WORK.PBO_SALES 
	/* 	/view=WORK.PBO_SALES */
		;
		set ETL_STG2.IA_PBO_SALES_HISTORY(where=(sales_dt>=%sysfunc(intnx(dtyear,&lmvReportDttm.,0)) and sales_dt<&lmvReportDttm.));
		format sales_dt date9.;
		sales_dt=datepart(sales_dt);
	run;

	data WORK.PMIX_SALES 
	/* 	/view=WORK.PMIX_SALES */
		;
		set ETL_STG2.IA_PMIX_SALES_HISTORY(where=(sales_dt>=%sysfunc(intnx(dtyear,&lmvReportDttm.,0)) and sales_dt<=&lmvReportDttm.));
		format sales_dt date9.;
		sales_dt=datepart(sales_dt);
	run;

	data WORK.PRICE;
		set ETL_STG2.IA_PRICE_HISTORY(where=(end_dt>=%sysfunc(intnx(dtyear,&lmvReportDttm.,0)) and start_dt<=%sysfunc(intnx(dtyear,&lmvReportDttm.,0,e))));
		set ETL_STG2.IA_PRICE(where=(end_dt>=%sysfunc(intnx(dtyear,&lmvReportDttm.,0)) and start_dt<=%sysfunc(intnx(dtyear,&lmvReportDttm.,0,e))));
		format start_dt end_dt date9.;
		end_dt=datepart(end_dt);
		start_dt=datepart(start_dt);
	run;

	data WORK.COST_PRICE;
		set ETL_STG2.IA_COST_PRICE(where=(end_dt>=%sysfunc(intnx(dtyear,&lmvReportDttm.,0)) and start_dt<=%sysfunc(intnx(dtyear,&lmvReportDttm.,0,e))));
		format start_dt end_dt date9.;
		end_dt=datepart(end_dt);
		start_dt=datepart(start_dt);
	run;

	/* Отбираем действующие данные из ETL_IA */
	/* data WORK.PBO_SALES; */
	/* 	set ETL_IA.PBO_SALES (where=(valid_from_dttm<=&FCST_DT. and valid_to_dttm>=&FCST_DT.  */
	/* 		and sales_dt>=%sysfunc(intnx(year,&FCST_DT.,0)))); */
	/* run; */
	/*  */
	/* data WORK.PMIX_SALES; */
	/* 	set ETL_IA.PMIX_SALES (where=(valid_from_dttm<=&FCST_DT. and valid_to_dttm>=&FCST_DT.  */
	/* 		and sales_dt>=%sysfunc(intnx(year,&FCST_DT.,0)))); */
	/* run; */
	/*  */
	/* data WORK.PRODUCT; */
	/* 	set ETL_IA.PRODUCT (where=(valid_from_dttm<=&FCST_DT. and valid_to_dttm>=&FCST_DT.)); */
	/* run; */
	/*  */
	/* data WORK.PRODUCT_ATTRIBUTES; */
	/* 	set ETL_IA.PRODUCT_ATTRIBUTES (where=(valid_from_dttm<=&FCST_DT. and valid_to_dttm>=&FCST_DT.)); */
	/* run; */
	/*  */
	/* data WORK.PRODUCT_HIERARCHY; */
	/* 	set ETL_IA.PRODUCT_HIERARCHY (where=(valid_from_dttm<=&FCST_DT. and valid_to_dttm>=&FCST_DT.)); */
	/* run; */
	/*  */
	/* data WORK.PBO_LOCATION; */
	/* 	set ETL_IA.PBO_LOCATION (where=(valid_from_dttm<=&FCST_DT. and valid_to_dttm>=&FCST_DT.)); */
	/* run; */
	/*  */
	/* data WORK.PBO_LOC_ATTRIBUTES; */
	/* 	set ETL_IA.PRODUCT_ATTRIBUTES (where=(valid_from_dttm<=&FCST_DT. and valid_to_dttm>=&FCST_DT.)); */
	/* run; */
	/*  */
/* 	data WORK.PBO_LOC_HIERARCHY; */
/* 		set ETL_IA.PRODUCT_HIERARCHY (where=(valid_from_dttm<=&FCST_DT. and valid_to_dttm>=&FCST_DT.)); */
/* 	run; */
/*  */
/* 	data WORK.PRICE; */
/* 		set ETL_IA.PRICE (where=(valid_from_dttm<=&FCST_DT. and valid_to_dttm>=&FCST_DT. */
/* 			and end_dt>=%sysfunc(intnx(year,&FCST_DT.,0)))); */
/* 	run; */
/*  */
/* 	data WORK.COST_PRICE; */
/* 		set ETL_IA.PRICE (where=(valid_from_dttm<=&FCST_DT. and valid_to_dttm>=&FCST_DT. */
/* 			and end_dt>=%sysfunc(intnx(year,&FCST_DT.,0)))); */
/* 	run; */

	/* Разворачиваем иерархии в справочниках */
	proc sql;
		create table WORK.PRODUCT_ALL_LVL as
		select 
			ph1.product_id as PARENT_PRODUCT_ID_1
			,p1.product_nm as PARENT_PRODUCT_NM_1
			,ph2.product_id as PARENT_PRODUCT_ID_2
			,p2.product_nm as PARENT_PRODUCT_NM_2
			,ph3.product_id as PARENT_PRODUCT_ID_3
			,p3.product_nm as PARENT_PRODUCT_NM_3
			,ph4.product_id as PARENT_PRODUCT_ID_4
			,p4.product_nm as PARENT_PRODUCT_NM_4
			,ph5.product_id as PRODUCT_ID
			,p5.product_nm as PRODUCT_NM
			,pa1.product_attr_value as OFFER_TYPE
			,pa2.product_attr_value as ITEM_SIZE
			,pa3.product_attr_value as PRODUCT_SUBGROUP_1
			,pa4.product_attr_value as PRODUCT_SUBGROUP_2
		from WORK.PRODUCT_HIERARCHY ph1
		inner join WORK.PRODUCT_HIERARCHY ph2
			on ph2.product_lvl=2
			and ph2.parent_product_id=ph1.product_id
		inner join WORK.PRODUCT_HIERARCHY ph3
			on ph3.product_lvl=3
			and ph3.parent_product_id=ph2.product_id
		inner join WORK.PRODUCT_HIERARCHY ph4
			on ph4.product_lvl=4
			and ph4.parent_product_id=ph3.product_id
		inner join WORK.PRODUCT_HIERARCHY ph5
			on ph5.product_lvl=5
			and ph5.parent_product_id=ph4.product_id
		left join WORK.PRODUCT p1
			on p1.product_id=ph1.product_id
		left join WORK.PRODUCT p2
			on p2.product_id=ph2.product_id
		left join WORK.PRODUCT p3
			on p3.product_id=ph3.product_id
		left join WORK.PRODUCT p4
			on p4.product_id=ph4.product_id
		left join WORK.PRODUCT p5
			on p5.product_id=ph5.product_id
		left join WORK.PRODUCT_ATTRIBUTES pa1
			on pa1.product_id=ph5.product_id
			and pa1.product_attr_nm='OFFER_TYPE'
		left join WORK.PRODUCT_ATTRIBUTES pa2
			on pa2.product_id=ph5.product_id
			and pa2.product_attr_nm='ITEM_SIZE'
		left join WORK.PRODUCT_ATTRIBUTES pa3
			on pa3.product_id=ph5.product_id
			and pa3.product_attr_nm='PRODUCT_SUBGROUP_1'
		left join WORK.PRODUCT_ATTRIBUTES pa4
			on pa4.product_id=ph5.product_id
			and pa4.product_attr_nm='PRODUCT_SUBGROUP_2'
		where ph1.product_lvl=1
		;
	quit;

	proc sql;
		create table WORK.PBO_LOC_ALL_LVL as
		select 
			ph1.pbo_location_id as PARENT_PBO_LOCATION_ID_1
			,p1.pbo_location_nm as PARENT_PBO_LOCATION_NM_1
			,ph2.pbo_location_id as PARENT_PBO_LOCATION_ID_2
			,p2.pbo_location_nm as PARENT_PBO_LOCATION_NM_2
			,ph3.pbo_location_id as PARENT_PBO_LOCATION_ID_3
			,p3.pbo_location_nm as PARENT_PBO_LOCATION_NM_3
			,ph4.pbo_location_id as PBO_LOCATION_ID
			,p4.pbo_location_nm as PBO_LOCATION_NM
			,pa1.pbo_loc_attr_value as BUILDING_TYPE
			,pa2.pbo_loc_attr_value as COMPANY
			,pa3.pbo_loc_attr_value as PRICE_LEVEL
			,input(pa4.pbo_loc_attr_value,ddmmyy10.) as OPEN_DATE format=date9.
			,pa5.pbo_loc_attr_value as BREAKFAST
			,pa6.pbo_loc_attr_value as WINDOW_TYPE
			,pa7.pbo_loc_attr_value as DELIVERY
			,input(pa8.pbo_loc_attr_value,ddmmyy10.) as DELIVERY_OPEN_DATE format=date9.
			,pa9.pbo_loc_attr_value as MCCAFE_TYPE
		from WORK.PBO_LOC_HIERARCHY ph1
		inner join WORK.PBO_LOC_HIERARCHY ph2
			on ph2.pbo_location_lvl=2
			and ph2.parent_pbo_location_id=ph1.pbo_location_id
		inner join WORK.PBO_LOC_HIERARCHY ph3
			on ph3.pbo_location_lvl=3
			and ph3.parent_pbo_location_id=ph2.pbo_location_id
		inner join WORK.PBO_LOC_HIERARCHY ph4
			on ph4.pbo_location_lvl=4
			and ph4.parent_pbo_location_id=ph3.pbo_location_id
		left join WORK.PBO_LOCATION p1
			on p1.pbo_location_id=ph1.pbo_location_id
		left join WORK.PBO_LOCATION p2
			on p2.pbo_location_id=ph2.pbo_location_id
		left join WORK.PBO_LOCATION p3
			on p3.pbo_location_id=ph3.pbo_location_id
		left join WORK.PBO_LOCATION p4
			on p4.pbo_location_id=ph4.pbo_location_id
		left join WORK.PBO_LOC_ATTRIBUTES pa1
			on pa1.pbo_location_id=ph4.pbo_location_id
			and pa1.pbo_loc_attr_nm='BUILDING_TYPE'
		left join WORK.PBO_LOC_ATTRIBUTES pa2
			on pa2.pbo_location_id=ph4.pbo_location_id
			and pa2.pbo_loc_attr_nm='COMPANY'
		left join WORK.PBO_LOC_ATTRIBUTES pa3
			on pa3.pbo_location_id=ph4.pbo_location_id
			and pa3.pbo_loc_attr_nm='PRICE_LEVEL'
		left join WORK.PBO_LOC_ATTRIBUTES pa4
			on pa4.pbo_location_id=ph4.pbo_location_id
			and pa4.pbo_loc_attr_nm='OPEN_DATE'
		left join WORK.PBO_LOC_ATTRIBUTES pa5
			on pa5.pbo_location_id=ph4.pbo_location_id
			and pa5.pbo_loc_attr_nm='BREAKFAST'
		left join WORK.PBO_LOC_ATTRIBUTES pa6
			on pa6.pbo_location_id=ph4.pbo_location_id
			and pa6.pbo_loc_attr_nm='WINDOW_TYPE'
		left join WORK.PBO_LOC_ATTRIBUTES pa7
			on pa7.pbo_location_id=ph4.pbo_location_id
			and pa7.pbo_loc_attr_nm='DELIVERY'
		left join WORK.PBO_LOC_ATTRIBUTES pa8
			on pa8.pbo_location_id=ph4.pbo_location_id
			and pa8.pbo_loc_attr_nm='DELIVERY_OPEN_DATE'
		left join WORK.PBO_LOC_ATTRIBUTES pa9
			on pa9.pbo_location_id=ph4.pbo_location_id
			and pa9.pbo_loc_attr_nm='MCCAFE_TYPE'
		where ph1.pbo_location_lvl=1
		;
	quit;
	
	/* Подготовка данных о ценах*/
	data cost_price_full (drop=start_dt end_dt);
		set WORK.COST_PRICE;
		do business_date=max(intnx('year',&lmvReportDt.,0),start_dt) to min(intnx('year',&lmvReportDt.,0,'e'),end_dt);
			output;
		end;
	run;

	data price_f (drop=start_dt end_dt);
		set WORK.PRICE(where=(price_type='F'));
		do business_date=max(intnx('year',&lmvReportDt.,0),start_dt) to min(intnx('year',&lmvReportDt.,0,'e'),end_dt);
			output;
		end;
	run;
	
	data price_R (drop=start_dt end_dt);
		set WORK.PRICE(where=(price_type='R'));
		do business_date=max(intnx('year',&lmvReportDt.,0),start_dt) to min(intnx('year',&lmvReportDt.,0,'e'),end_dt);
			output;
		end;
	run;
	
	data price_P (drop=start_dt end_dt);
		set WORK.PRICE(where=(price_type='P'));
		do business_date=max(intnx('year',&lmvReportDt.,0),start_dt) to min(intnx('year',&lmvReportDt.,0,'e'),end_dt);
			output;
		end;
	run;

	/*Выкидывание дублей необходимо только при отборе из etl_stg2*/
	proc sort data=price_f nodupkey;
		by product_id pbo_location_id business_date;
	run;
	proc sort data=price_r nodupkey;
		by product_id pbo_location_id business_date;
	run;
	proc sort data=price_p nodupkey;
		by product_id pbo_location_id business_date;
	run;

	proc datasets library=work;
	    modify cost_price_full;
	              index create idx1=(product_id pbo_location_id business_date);
	    modify price_r;
	              index create idx1=(product_id pbo_location_id business_date);
	    modify price_f;
	              index create idx1=(product_id pbo_location_id business_date);
	    modify price_p;
	              index create idx1=(product_id pbo_location_id business_date);
	quit;

	/* Объединяем исторические данные по продажам */
	proc sql;
		create table DM_REP.SALES_ALL_HIST as
		select 
			pmix.product_id
			,coalesce(pmix.pbo_location_id,pbo.pbo_location_id) as pbo_location_id
			,coalesce(pmix.channel_cd,pbo.channel_cd) as channel_cd
			,coalesce(pmix.sales_dt,pbo.sales_dt) as business_date format=date9.
			,pbo.receipt_qty as GC
			,pmix.sales_qty as UNITS
		from WORK.PBO_SALES pbo
		inner join WORK.PMIX_SALES pmix
			on pmix.channel_cd=pbo.channel_cd
			and pmix.pbo_location_id=pbo.pbo_location_id
			and pmix.sales_dt=pbo.sales_dt
		;
	quit;

	proc datasets library=dm_rep;
	    modify SALES_ALL_HIST;
	              index create idx1=(product_id pbo_location_id business_date);	
	quit;

	/* Подтягиваем цены и справочники */
	proc sql;
		create table DM_REP.SALES_PRICE_HIST as
		select 
			sls.PRODUCT_ID
			,sls.PBO_LOCATION_ID
			,sls.CHANNEL_CD
			,sls.BUSINESS_DATE
			,cpr.FOOD_COST_AMT + cpr.PAPER_COST_AMT + cpr.NON_PRODUCT_COST_AMT as COST
			,coalesce(pprm.NET_PRICE_AMT,pfct.NET_PRICE_AMT,preg.NET_PRICE_AMT) as NET_PRICE
			,coalesce(pprm.GROSS_PRICE_AMT,pfct.GROSS_PRICE_AMT,preg.GROSS_PRICE_AMT) as GROSS_PRICE
			,sls.GC
			,sls.GC-ceil(sls.GC*0.1) as PLAN_GC
			,. as FORECAST_GC
			,sls.UNITS
			,sls.UNITS-ceil(sls.UNITS*0.1) as PLAN_UNITS
			,. as FORECAST_UNITS
		from DM_REP.SALES_ALL_HIST sls
		left  join WORK.cost_price_full(idxname=idx1) cpr
			on cpr.product_id=sls.product_id
			and cpr.pbo_location_id=sls.pbo_location_id
			and cpr.business_date=sls.business_date
		left join WORK.price_r(idxname=idx1) preg
			on preg.product_id=sls.product_id
			and preg.pbo_location_id=sls.pbo_location_id
			and preg.business_date=sls.business_date
		left join WORK.price_f(idxname=idx1) pfct
			on pfct.product_id=sls.product_id
			and pfct.pbo_location_id=sls.pbo_location_id
			and pfct.business_date=sls.business_date
		left join WORK.PRICE_p(idxname=idx1) pprm
			on pprm.product_id=sls.product_id
			and pprm.pbo_location_id=sls.pbo_location_id
			and pprm.business_date=sls.business_date
		;
	quit;

	data DM_REP.VA_DATAMART_HIST (drop=rc);
		set DM_REP.SALES_PRICE_HIST;
		if 0 then do;
			set WORK.PRODUCT_ALL_LVL;
			set WORK.PBO_LOC_ALL_LVL;
		end;
		if _n_=1 then do;
			declare hash hprd(dataset: "WORK.PRODUCT_ALL_LVL");
			rc = hprd.defineKey('product_id');
			rc = hprd.defineData('PARENT_PRODUCT_ID_1','PARENT_PRODUCT_NM_1','PARENT_PRODUCT_ID_2','PARENT_PRODUCT_NM_2','PARENT_PRODUCT_ID_3','PARENT_PRODUCT_NM_3','PARENT_PRODUCT_ID_4','PARENT_PRODUCT_NM_4','PRODUCT_NM','OFFER_TYPE','ITEM_SIZE','PRODUCT_SUBGROUP_1','PRODUCT_SUBGROUP_2');
			rc = hprd.defineDone();
	
			declare hash hloc(dataset: "WORK.PBO_LOC_ALL_LVL");
			rc = hloc.defineKey('pbo_location_id');
			rc = hloc.defineData('PARENT_PBO_LOCATION_ID_1','PARENT_PBO_LOCATION_NM_1','PARENT_PBO_LOCATION_ID_2','PARENT_PBO_LOCATION_NM_2','PARENT_PBO_LOCATION_ID_3','PARENT_PBO_LOCATION_NM_3','PBO_LOCATION_NM','BUILDING_TYPE','COMPANY','PRICE_LEVEL','OPEN_DATE','BREAKFAST','WINDOW_TYPE','DELIVERY','DELIVERY_OPEN_DATE','MCCAFE_TYPE');
			rc = hloc.defineDone();
		end;
	
		rc = hprd.find();
		rc = hloc.find();
	run;

	/* Данные из прогноза */

	proc sql;
		create table work.va_datamart_hist_srt as
		select PBO_LOCATION_ID, PRODUCT_ID, CHANNEL_CD, BUSINESS_DATE, GC, UNITS
		from dm_rep.va_datamart_hist
		order by PBO_LOCATION_ID, PRODUCT_ID, CHANNEL_CD, BUSINESS_DATE desc;
	quit;

	data dm_rep.fcst_simulate(drop=i j GC_sum UNITS_sum);
		set work.va_datamart_hist_srt;
		by PBO_LOCATION_ID PRODUCT_ID CHANNEL_CD;
		retain GC_sum UNITS_sum i 0;
		if i<=14 then do;
			GC_sum=GC_sum+GC;
			UNITS_sum=UNITS_sum+UNITS;
			i+1;
		end;
		if last.CHANNEL_CD then do;
			GC=.;
			FORECAST_GC=GC_sum/i;
			PLAN_GC=FORECAST_GC-ceil(FORECAST_GC*0.1);
			UNITS=.;
			FORECAST_UNITS=UNITS_sum/i;
			PLAN_UNITS=FORECAST_UNITS-ceil(FORECAST_UNITS*0.1);
			do business_date=&lmvReportDt. to &lmvReportDt.+89;
				output;
			end;
			FORECAST_GC=.;
			FORECAST_UNITS=.;
			do j=month(&lmvReportDt.+89) to month(intnx('year',&lmvReportDt.,0,'e'))-1;	
				business_date=intnx('month',intnx('year',&lmvReportDt.,0),j);
				output;
			end;
			i=0;
			GC_sum=0;
			UNITS_sum=0;
		end;
	run;	

	proc datasets library=dm_rep;
	    modify fcst_simulate;
	              index create idx1=(product_id pbo_location_id business_date);	
	quit;

	proc sql;
		create table work.FCST_PRICE as
		select 
			fcst.PRODUCT_ID
			,fcst.PBO_LOCATION_ID
			,fcst.BUSINESS_DATE
			,cpr.FOOD_COST_AMT + cpr.PAPER_COST_AMT + cpr.NON_PRODUCT_COST_AMT as COST
			,coalesce(pprm.NET_PRICE_AMT,pfct.NET_PRICE_AMT,preg.NET_PRICE_AMT) as NET_PRICE
			,coalesce(pprm.GROSS_PRICE_AMT,pfct.GROSS_PRICE_AMT,preg.GROSS_PRICE_AMT) as GROSS_PRICE
			,fcst.PLAN_GC
			,fcst.FORECAST_GC
			,fcst.PLAN_UNITS
			,fcst.FORECAST_UNITS
		from dm_rep.fcst_simulate fcst
		left  join WORK.cost_price_full(idxname=idx1) cpr
			on cpr.product_id=fcst.product_id
			and cpr.pbo_location_id=fcst.pbo_location_id
			and cpr.business_date=fcst.business_date
		left join WORK.price_r(idxname=idx1) preg
			on preg.product_id=fcst.product_id
			and preg.pbo_location_id=fcst.pbo_location_id
			and preg.business_date=fcst.business_date
		left join WORK.price_f(idxname=idx1) pfct
			on pfct.product_id=fcst.product_id
			and pfct.pbo_location_id=fcst.pbo_location_id
			and pfct.business_date=fcst.business_date
		left join WORK.PRICE_p(idxname=idx1) pprm
			on pprm.product_id=fcst.product_id
			and pprm.pbo_location_id=fcst.pbo_location_id
			and pprm.business_date=fcst.business_date
		;
	quit;
	
	data dm_rep.VA_DATAMART_FCST (drop=rc);
		set work.FCST_PRICE;
		if 0 then do;
			set WORK.PRODUCT_ALL_LVL;
			set WORK.PBO_LOC_ALL_LVL;
		end;
		if _n_=1 then do;
			declare hash hprd(dataset: "WORK.PRODUCT_ALL_LVL");
			rc = hprd.defineKey('product_id');
			rc = hprd.defineData('PARENT_PRODUCT_ID_1','PARENT_PRODUCT_NM_1','PARENT_PRODUCT_ID_2','PARENT_PRODUCT_NM_2','PARENT_PRODUCT_ID_3','PARENT_PRODUCT_NM_3','PARENT_PRODUCT_ID_4','PARENT_PRODUCT_NM_4','PRODUCT_NM','OFFER_TYPE','ITEM_SIZE','PRODUCT_SUBGROUP_1','PRODUCT_SUBGROUP_2');
			rc = hprd.defineDone();
	
			declare hash hloc(dataset: "WORK.PBO_LOC_ALL_LVL");
			rc = hloc.defineKey('pbo_location_id');
			rc = hloc.defineData('PARENT_PBO_LOCATION_ID_1','PARENT_PBO_LOCATION_NM_1','PARENT_PBO_LOCATION_ID_2','PARENT_PBO_LOCATION_NM_2','PARENT_PBO_LOCATION_ID_3','PARENT_PBO_LOCATION_NM_3','PBO_LOCATION_NM','BUILDING_TYPE','COMPANY','PRICE_LEVEL','OPEN_DATE','BREAKFAST','WINDOW_TYPE','DELIVERY','DELIVERY_OPEN_DATE','MCCAFE_TYPE');
			rc = hloc.defineDone();
		end;
	
		rc = hprd.find();
		rc = hloc.find();
	run;

	data DM_REP.VA_DATAMART;
		set DM_REP.VA_DATAMART_HIST dm_rep.VA_DATAMART_FCST;
	run;


proc datasets lib=dm_rep;
	modify va_datamart;
		attrib _all_ label='';
run;