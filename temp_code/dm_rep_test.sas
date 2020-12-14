proc printto log="/opt/sas/mcd_config/temp_code/dm_rep_test_28052020.log";
run;

%let lmvReportDt=%sysfunc(inputn(25MAY2020,date9.));

libname ETL_STG2 "/data/ETL_STG";

data WORK.COST_PRICE;
		set ETL_STG2.IA_COST_PRICE(where=(end_dt>=%sysfunc(intnx(dtyear,&FCST_DTTM.,0,'e'))));
		format start_dt end_dt date9.;
		end_dt=datepart(end_dt);
		start_dt=datepart(start_dt);
	run;

data WORK.PRICE;
	set ETL_STG2.IA_PRICE;
	format start_dt end_dt date9.;
	end_dt=datepart(end_dt);
	start_dt=datepart(start_dt);
run;

proc sort data=WORK.PRICE;
	by product_id pbo_location_id;
quit;

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

data cost_price_full (drop=start_dt end_dt);
	set WORK.COST_PRICE;
	do business_date=max(intnx('year',&lmvReportDt.,0),start_dt) to min(intnx('year',&lmvReportDt.,0,'e'),end_dt);
		output;
	end;
run;

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
		,pa4.pbo_loc_attr_value as OPEN_DATE
		,pa5.pbo_loc_attr_value as BREAKFAST
		,pa6.pbo_loc_attr_value as WINDOW_TYPE
		,pa7.pbo_loc_attr_value as DELIVERY
		,pa8.pbo_loc_attr_value as DELIVERY_OPEN_DATE
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

/* proc datasets library=dm_rep; */
/*     modify fcst_simulate; */
/*               index create idx1=(product_id pbo_location_id business_date);	 */
/* quit; */

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

data dm_rep.fcst_master_tst1;
	set dm_rep.fcst_master_tst;
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
	