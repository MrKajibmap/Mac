LIBNAME ORA ORACLE USER=SAS_USER SCHEMA=sas_interf PASSWORD=shdD2393 PATH=WARE;

libname ETL_STG "/data/ETL_STG";

/* data etl_stg.ia_assort_matrix; */
/* 	set ora.ia_assort_matrix; */
/* run; */
/* data etl_stg.IA_EVENTS; */
/* 	set ora.IA_EVENTS; */
/* run; */
/* data etl_stg.IA_PBO_LOC_ATTRIBUTES; */
/* 	set ora.IA_PBO_LOC_ATTRIBUTES; */
/* run; */
/* data etl_stg.IA_PBO_LOC_HIERARCHY; */
/* 	set ora.IA_PBO_LOC_HIERARCHY; */
/* run; */
/* data etl_stg.IA_PBO_LOCATION; */
/* 	set ora.IA_PBO_LOCATION; */
/* run; */
/* data etl_stg.IA_PRICE; */
/* 	set ora.IA_PRICE; */
/* run; */
/* data etl_stg.IA_PRICE_HISTORY; */
/* 	set ora.IA_PRICE_HISTORY; */
/* run; */
/* data etl_stg.IA_PRODUCT; */
/* 	set ora.IA_PRODUCT; */
/* run; */
/* data etl_stg.IA_PRODUCT_ATTRIBUTES; */
/* 	set ora.IA_PRODUCT_ATTRIBUTES; */
/* run; */
/* data etl_stg.IA_PRODUCT_HIERARCHY; */
/* 	set ora.IA_PRODUCT_HIERARCHY; */
/* run; */
/* data etl_stg.IA_WEATHER; */
/* 	set ora.IA_WEATHER; */
/* run; */
data etl_stg.IA_PROMO;
	set ora.IA_PROMO;
run;

data etl_stg.ia_media;
	set ora.ia_media;
run;
/* data etl_stg.ia_comp_media; */
/* 	set ora.ia_comp_media; */
/* run; */
/*  */
/* data etl_stg.ia_competitor; */
/* 	set ora.ia_competitor; */
/* run; */
/*  */
/* data etl_stg.ia_product_chain; */
/* 	set ora.ia_product_chain; */
/* run; */
/*  */
/* data etl_stg.ia_pbo_close_period; */
/* 	set ora.ia_pbo_close_period; */
/* run; */
/*  */
/* data etl_stg.ia_cost_price; */
/* 	set ora.ia_cost_price; */
/* run; */
/*  */
/* data etl_stg.ia_macro_factor; */
/* 	set ora.ia_macro_factor; */
/* run; */
/*  */
/* data etl_stg.ia_segment; */
/* 	set ora.ia_segment; */
/* run; */
/*  */
/* data etl_stg.ia_channel; */
/* 	set ora.ia_channel; */
/* run; */
/*  */
/* data etl_stg.ia_weather; */
/* 	set ora.ia_weather; */
/* run; */
/*  */
data etl_stg.IA_PROMO_X_PRODUCT;
	set ora.IA_PROMO_X_PRODUCT;
run;

data etl_stg.IA_PROMO_X_PBO;
	set ora.IA_PROMO_X_PBO;
run;

data etl_stg.IA_PBO_SALES;
	set ora.IA_PBO_SALES;
run;
data etl_stg.IA_PMIX_SALES;
	set ora.IA_PMIX_SALES;
run;
data etl_stg.IA_PBO_SALES_HISTORY;
	set ora.IA_PBO_SALES_HISTORY;
run;
data etl_stg.IA_PMIX_SALES_HISTORY;
	set ora.IA_PMIX_SALES_HISTORY;