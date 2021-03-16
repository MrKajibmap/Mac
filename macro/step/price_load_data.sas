%macro price_load_data;
	%local lmvInLib
		   lmvReportDt
		   lmvReportDttm
		;
		
	%let lmvInLib=ETL_IA;
	%let lmvReportDt=&ETL_CURRENT_DT.;
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	
	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;

	/* Подготовка входных данных */
	%add_promotool_marks(mpOutCaslib=casuser,
							mpPtCaslib=pt);

	proc casutil;
	  droptable casdata="promo" incaslib="casuser" quiet;
	  droptable casdata="promo_pbo" incaslib="casuser" quiet;
	  droptable casdata="promo_prod" incaslib="casuser" quiet;
	  droptable casdata="pbo_dictionary" incaslib="casuser" quiet;
	run;

	data CASUSER.promo (replace=yes);
		set CASUSER.promo_enh;
	run;
	
	data CASUSER.promo_x_pbo (replace=yes);
		set CASUSER.promo_pbo_enh;
	run;
	
	data CASUSER.promo_x_product (replace=yes);
		set casuser.promo_prod_enh;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.promo {options replace=true} as 
		select CHANNEL_CD
		,PROMO_ID
		,PROMO_GROUP_ID
		,PROMO_MECHANICS
		,PROMO_NM
		,SEGMENT_ID
		,PROMO_PRICE_AMT
		,NP_GIFT_PRICE_AMT
		,start_dt
		,end_dt
		from casuser.promo
		where start_dt is not null and end_dt is not null
		;
	quit;

	proc fedsql sessref=casauto noprint;
		create table casuser.promo_pbo {options replace=true} as 
		select PBO_LOCATION_ID,PROMO_ID
		from casuser.promo_X_PBO
		;
	quit;

	proc fedsql sessref=casauto noprint;
		create table casuser.promo_prod {options replace=true} as 
		select GIFT_FLAG,OPTION_NUMBER,PRODUCT_ID,PRODUCT_QTY,PROMO_ID
		from casuser.promo_X_PRODUCT
		;
	quit;
	
	proc casutil;
		droptable casdata="price" incaslib="casuser" quiet;
	run;
	
	data CASUSER.PRICE (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..PRICE(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

/* 	VAT */
	proc casutil;
		droptable casdata="VAT" incaslib="casuser" quiet;
	run;
	
	data CASUSER.VAT (replace=yes drop=valid_from_dttm valid_to_dttm);
        set &lmvInLib..vat(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
    run;

/* 	PRODUCT_ATTR */
	proc casutil;
		droptable casdata="PRODUCT_ATTRIBUTES" incaslib="casuser" quiet;
	run;

	data CASUSER.PRODUCT_ATTRIBUTES (replace=yes drop=valid_from_dttm valid_to_dttm);
        set &lmvInLib..PRODUCT_ATTRIBUTES (where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
    run;

	proc fedsql sessref=casauto noprint;
		create table casuser.PRICE{options replace=true} as
		select 
		PRODUCT_ID
		,PBO_LOCATION_ID
		,PRICE_TYPE
		,NET_PRICE_AMT
		,GROSS_PRICE_AMT
		,START_DT
		,END_DT
		from casuser.PRICE
		;
	quit;
	
	/* PBO_DICTIONARY */
	data CASUSER.PBO_LOCATION (replace=yes drop=valid_from_dttm valid_to_dttm);
        set &lmvInLib..pbo_location(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
    run;
    
    data CASUSER.PBO_LOC_HIERARCHY (replace=yes drop=valid_from_dttm valid_to_dttm);
        set &lmvInLib..PBO_LOC_HIERARCHY(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
    run;

    data CASUSER.PBO_LOC_ATTRIBUTES (replace=yes drop=valid_from_dttm valid_to_dttm);
        set &lmvInLib..PBO_LOC_ATTRIBUTES(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
    run;
    
    proc fedsql sessref=casauto noprint;
       create table casuser.pbo_loc_attr{options replace=true} as
            select distinct *
            from casuser.PBO_LOC_ATTRIBUTES
            ;
    quit;

    proc cas;
    transpose.transpose /
       table={name="pbo_loc_attr", caslib="casuser", groupby={"pbo_location_id"}} 
       attributes={{name="pbo_location_id"}} 
       transpose={"PBO_LOC_ATTR_VALUE"} 
       prefix="A_" 
       id={"PBO_LOC_ATTR_NM"} 
       casout={name="attr_transposed", caslib="casuser", replace=true};
    quit;

    proc fedsql sessref=casauto noprint;
       create table casuser.pbo_hier_flat{options replace=true} as
            select t1.pbo_location_id, 
                   t2.PBO_LOCATION_ID as LVL3_ID,
                   t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
                   1 as LVL1_ID
            from 
            (select * from casuser.PBO_LOC_HIERARCHY where pbo_location_lvl=4) as t1
            left join 
            (select * from casuser.PBO_LOC_HIERARCHY where pbo_location_lvl=3) as t2
            on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
            ;
    quit;

    proc fedsql sessref=casauto noprint;
       create table casuser.pbo_dictionary{options replace=true} as
       select t2.pbo_location_id, 
           coalesce(t2.lvl3_id,-999) as lvl3_id,
           coalesce(t2.lvl2_id,-99) as lvl2_id,
           cast(1 as double) as lvl1_id,
           coalesce(t14.pbo_location_nm,'NA') as pbo_location_nm,
           coalesce(t13.pbo_location_nm,'NA') as lvl3_nm,
           coalesce(t12.pbo_location_nm,'NA') as lvl2_nm,
           cast(inputn(t3.A_OPEN_DATE,'ddmmyy10.') as date) as A_OPEN_DATE,
           cast(inputn(t3.A_CLOSE_DATE,'ddmmyy10.') as date) as A_CLOSE_DATE,
           t3.A_PRICE_LEVEL,
           t3.A_DELIVERY,
           t3.A_AGREEMENT_TYPE,
           t3.A_BREAKFAST,
           t3.A_BUILDING_TYPE,
           t3.A_COMPANY,
           t3.A_DRIVE_THRU,
           t3.A_MCCAFE_TYPE,
           t3.A_WINDOW_TYPE
       from casuser.pbo_hier_flat t2
       left join casuser.attr_transposed t3
       on t2.pbo_location_id=t3.pbo_location_id
       left join casuser.pbo_location t14
       on t2.pbo_location_id=t14.pbo_location_id
       left join casuser.pbo_location t13
       on t2.lvl3_id=t13.pbo_location_id
       left join casuser.pbo_location t12
       on t2.lvl2_id=t12.pbo_location_id
       ;
    quit;

    proc casutil;
		promote casdata="pbo_dictionary" incaslib="casuser" outcaslib="casuser";
		promote casdata="PRICE" incaslib="casuser" outcaslib="casuser";
		promote casdata="promo" incaslib="casuser" outcaslib="casuser";
		promote casdata="promo_prod" incaslib="casuser" outcaslib="casuser";
		promote casdata="promo_pbo" incaslib="casuser" outcaslib="casuser";
      droptable casdata='pbo_loc_attr' incaslib='casuser' quiet;
      droptable casdata='pbo_location' incaslib='casuser' quiet;
      droptable casdata='PBO_LOC_HIERARCHY' incaslib='casuser' quiet;
      droptable casdata='PBO_LOC_ATTRIBUTES' incaslib='casuser' quiet;
      droptable casdata='pbo_hier_flat' incaslib='casuser' quiet;
      droptable casdata='attr_transposed' incaslib='casuser' quiet;
    quit;

%mend price_load_data;

%price_load_data;
