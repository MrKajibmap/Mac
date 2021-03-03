/*****************************************************************
*
*	This code creates feature space and calculates similar promo.
*
*	
*	KEY STAGES:
*		- Adding promo_mechanics 
*		- Adding mastercodes
*		- Adding pbo
*		- Adding product category (prod_lvl3_id)
*		- Adding product category (prod_lvl2_id)
*		- Finding 10 similar promo for 4 specific promo
*
*
*	FINAL TABLES:
*		- feature_space
*		- promo_names_all (with promo_nm) and top_all (with promo_id)
*
/*****************************************************************


/*****************************************************************
*
* Adding promo_mechanics to feature_space  
*
******************************************************************/
options mprint nomprintnest nomlogic nomlogicnest nosymbolgen mcompilenote=all mreplace;
/*Создать cas-сессию, если её нет*/
%macro assign;
%let casauto_ok = %sysfunc(SESSFOUND ( casauto)) ;
%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
 cas casauto;
 caslib _all_ assign;
%end;
%mend;
%assign

libname ETL_STG "/data2/etl_stg_28_08_2020";
libname tmp "/data2/TMP";
%let inlib=ETL_STG;

/* Создаем справочник товаоров */
proc casutil;
	droptable casdata="product_dictionary_ml" incaslib="public" quiet;	
	load data=&inlib..IA_product casout='ia_product' outcaslib='public' replace;
	load data=&inlib..IA_product_HIERARCHY casout='IA_product_HIERARCHY' outcaslib='public' replace;
	load data=&inlib..IA_product_ATTRIBUTES casout='IA_product_ATTRIBUTES' outcaslib='public' replace;
run;
  
proc cas;
transpose.transpose /
   table={name="ia_product_attributes", caslib="public", groupby={"product_id"}} 
   attributes={{name="product_id"}} 
   transpose={"PRODUCT_ATTR_VALUE"} 
   prefix="A_" 
   id={"PRODUCT_ATTR_NM"} 
   casout={name="attr_transposed", caslib="public", replace=true};
quit;

proc fedsql sessref=casauto;
	create table public.product_hier_flat{options replace=true} as
		select 
			t1.product_id, 
			t2.product_id  as LVL4_ID,
			t3.product_id  as LVL3_ID,
			t3.PARENT_product_id as LVL2_ID, 
			1 as LVL1_ID
		from 
			(select * from public.ia_product_hierarchy where product_lvl=5) as t1
		left join 
			(select * from public.ia_product_hierarchy where product_lvl=4) as t2
		on 
			t1.PARENT_PRODUCT_ID=t2.PRODUCT_ID
		left join 
			(select * from public.ia_product_hierarchy where product_lvl=3) as t3
		on
			t2.PARENT_PRODUCT_ID=t3.PRODUCT_ID
 	;
quit;

proc fedsql sessref=casauto;
	create table public.product_dictionary_ml{options replace=true} as
		select 
			t1.product_id, 
			coalesce(t1.lvl4_id,-9999) as prod_lvl4_id,
			coalesce(t1.lvl3_id,-999) as prod_lvl3_id,
			coalesce(t1.lvl2_id,-99) as prod_lvl2_id,
			coalesce(t15.product_nm,'NA') as product_nm,
			coalesce(t14.product_nm,'NA') as prod_lvl4_nm,
			coalesce(t13.product_nm,'NA') as prod_lvl3_nm,
			coalesce(t12.product_nm,'NA') as prod_lvl2_nm,
			t3.A_HERO,
			t3.A_ITEM_SIZE,
			t3.A_OFFER_TYPE,
			t3.A_PRICE_TIER
		from
			public.product_hier_flat as t1
		left join
			public.attr_transposed as t3
		on
			t1.product_id = t3.product_id
		left join
			PUBLIC.IA_product as t15
		on
			t1.product_id = t15.product_id
		left join
			PUBLIC.IA_product as t14
		on
			t1.lvl4_id = t14.product_id
		left join
			PUBLIC.IA_product as t13
		on
			t1.lvl3_id = t13.product_id
		left join
			PUBLIC.IA_product as t12
		on
			t1.lvl2_id = t12.product_id
	;
quit;


proc sql;
create table work.promo as
select promo_id, promo_mechanics 
from etl_stg.ia_promo;

/* Renaming some promo_mechanics, so it won't have '/', ' ' etc */
proc sql;
create table work.promo as
select *, (case promo_mechanics
when 'BOGO / 1+1' 
then 'BOGO'
when 'EVM/Set'
then 'EVM' 
when 'Non-Product Gift' 
then 'Non_Product_Gift'
when 'Product Gift' 
then 'Product_Gift'
when 'NP Promo Support' 
then 'NP_Promo_Support'
when '1+1%' 
then 'one_plus_one'
when 'Other: Discount for volume' 
then 'Other_Discount_volume'
when 'Other: Digital (app)' 
then 'Other_Digital_app'
else promo_mechanics end) as promo_mechanics_renamed
from work.promo;

alter table work.promo
drop column promo_mechanics;
quit;

proc sort data=work.promo;
by promo_id promo_mechanics_renamed;
run;

/* Transposing the table */
proc transpose data=work.promo
			   prefix = feature_
			   out=work.space_promo;
			   by promo_id;
			   var promo_mechanics_renamed;
			   id promo_mechanics_renamed;
			     
run;

proc sql;
alter table work.space_promo
drop column _NAME_;
quit;

/* Filling columns with 0 and 1 */
data work.feature_space_promo;
set work.space_promo;
array n{*} _character_;
do i=1 to dim(n);
	if n{i} ='' then n{i}='0';
	else n{i}='1';
end;
run;

/* Changing formats of columns */
proc sql;
create table feature_space_promo as
select promo_id, input(feature_Discount, best.) as feature_Discount,
input(feature_Pairs, best.) as feature_Pairs,
input(feature_BOGO, best.) as feature_BOGO,
input(feature_EVM, best.) as feature_EVM,
input(feature_Non_Product_Gift, best.) as feature_Non_Product_Gift,
input(feature_Product_Gift, best.) as feature_Product_Gift,
input(feature_NP_Promo_Support, best.) as feature_NP_Promo_Support,
input(feature_one_plus_one, best.) as feature_one_plus_one,
input(feature_Other_Discount_volume, best.) as feature_Other_Discount_volume,
input(feature_Other_Digital_app, best.) as feature_Other_Digital_app
from work.feature_space_promo;
quit;


/*****************************************************************
*
* Adding mastercodes to feature_space  
*
******************************************************************/
proc sql;
create table work.lvl4 as
select unique(product_id), prod_lvl4_id
from public.product_dictionary_ml;

create table work.promo_product as
select promo_id, product_id
from etl_stg.ia_promo_x_product;

create table work.promo_prod_lvl4 as
select t.promo_id, t.product_id, l.prod_lvl4_id
from work.promo_product as t, work.lvl4 as l
where t.product_id = l.product_id
group by t.promo_id, t.product_id;

/* Adding promo 734 which applies to all products */
create table work.promo_734 as
select unique(prod_lvl4_id)
from work.promo_prod_lvl4
where prod_lvl4_id <> .;

alter table work.promo_734
add promo_id numeric;
quit;

data work.promo_734;
set work.promo_734;

array n{*} _numeric_;
do i=1 to dim(n);
	if n{i} =. then n{i}=734;
end;
run;

proc sql;
alter table work.promo_734
add product_id numeric;
quit;

data work.promo_734;
set work.promo_734;

array n{*} _numeric_;
do i=1 to dim(n);
	if n{i} =. then n{i}=1;
end;
run;

proc sql;
alter table work.promo_734
drop column i;
quit;

/* Adding promo 990 which applies to all products */
proc sql;
create table work.promo_990 as
select unique(prod_lvl4_id)
from work.promo_prod_lvl4
where prod_lvl4_id <> .;

alter table work.promo_990
add promo_id numeric;
quit;

data work.promo_990;
set work.promo_990;

array n{*} _numeric_;
do i=1 to dim(n);
	if n{i} =. then n{i}=990;
end;
run;

proc sql;
alter table work.promo_990
add product_id numeric;
quit;

data work.promo_990;
set work.promo_990;

array n{*} _numeric_;
do i=1 to dim(n);
	if n{i} =. then n{i}=1;
end;
run;

proc sql;
alter table work.promo_990
drop column i;
quit;

/* The union of these two tables  */
proc sql;
create table all_prod as
select * 
from work.promo_734
union
select *
from work.promo_990;

INSERT INTO all_prod (prod_lvl4_id, promo_id, product_id)
VALUES (1, 734, 1)
VALUES (1, 990, 1);


create table all_product as
select promo_id, product_id, prod_lvl4_id
from work.all_prod
order by prod_lvl4_id;


create table work.mast as
select *
from work.promo_prod_lvl4
union 
select *
from work.all_product;

create table work.mastercodes as
select distinct promo_id, prod_lvl4_id
from work.mast;

quit;

/* Transposing the table */
proc transpose data=work.mastercodes
			   prefix = feature_mastercode_
			   out=work.space_mastercodes;
			   by promo_id;
			   var prod_lvl4_id;
			   id prod_lvl4_id;
run;

proc sql;
alter table work.space_mastercodes
drop column _NAME_;
quit;

/* Filling the columns with 0 and 1 */
data work.feature_space_mastercodes;
set work.space_mastercodes;

array n{*} _numeric_;
do i=2 to dim(n);
	if n{i} =. then n{i}=0;
	else n{i}=1;
end;
run;

/* Promo plus mastercode */
proc sql;
create table work.feature_space as
select *
from feature_space_promo, feature_space_mastercodes
where feature_space_promo.promo_id=feature_space_mastercodes.promo_id;

alter table work.feature_space
drop column i;
quit;

data work.feature_space;
set work.feature_space(rename=(feature_mastercode_1=feature_all_product));
run;


/*****************************************************************
*
* Adding pbo to feature_space  
*
******************************************************************/
proc sql;
   create table work.pbo_lvl as
        select t1.Pbo_location_ID, 
               t2.Pbo_location_ID  as LVL3_ID,
               t3.Pbo_location_ID  as LVL2_ID,
               1 as LVL1_ID
        from 
        (select * from etl_stg.ia_pbo_loc_hierarchy where pbo_location_lvl=4) as t1
        left join 
        (select * from etl_stg.ia_pbo_loc_hierarchy where pbo_location_lvl=3) as t2
        on t1.PARENT_pbo_location_id=t2.Pbo_location_ID
        left join 
        (select * from etl_stg.ia_pbo_loc_hierarchy where pbo_location_lvl=2) as t3
        on t2.PARENT_pbo_location_id=t3.Pbo_location_ID;
quit;

proc sql;
create table loc_prod as
select t1.promo_id, t1.pbo_location_id, t2.lvl2_id
from etl_stg.ia_promo_x_pbo as t1, work.pbo_lvl as t2
where t1.pbo_location_id = t2.pbo_location_id;
quit;

proc sql;
create table work.pbo as
select distinct l.promo_id, p.pbo_location_nm
from work.loc_prod as l, etl_stg.ia_pbo_location as p
where l.lvl2_id = p.pbo_location_id;
quit;

/* Renaming some pbo, so it won't have '-', ' ' etc */
proc sql;
create table work.pbo as
select *, (case pbo_location_nm
when 'Far East' 
then 'Far_East'
when 'Moscow City'
then 'Moscow_City' 
when 'North-West' 
then 'North_West'
when 'Northern Caucasia' 
then 'Northern_Caucasia'
else pbo_location_nm end) as pbo_location_name
from work.pbo;

alter table work.pbo
drop column pbo_location_nm;

/* Transposing the table */
proc transpose data=work.pbo
			   prefix = feature_
			   out=work.space_pbo;
			   by promo_id;
			   var pbo_location_name;
			   id pbo_location_name;
run;

proc sql;
alter table work.space_pbo
drop column _LABEL_, _NAME_;
quit;

/* Filling columns with 0 and 1 */
data work.space_pbo;
set work.space_pbo;
array n{*} _character_;
do i=1 to dim(n);
	if n{i} ='' then n{i}='0';
	else n{i}='1';
end;
run;

/* Changing formats of columns */
proc sql;
create table space_pbo as
select promo_id, input(feature_Central, best.) as feature_Central,
input(feature_Far_East, best.) as feature_Far_East,
input(feature_Moscow_City, best.) as feature_Moscow_City,
input(feature_North_West, best.) as feature_North_West,
input(feature_Northern_Caucasia, best.) as feature_Northern_Caucasia,
input(feature_Siberia, best.) as feature_Siberia,
input(feature_Ural, best.) as feature_Ural,
input(feature_Volga, best.) as feature_Volga
from work.space_pbo;
quit;

/* Pbo plus promo and mastercode */
proc sql;
create table work.feature_space as
select *
from feature_space, space_pbo
where feature_space.promo_id=space_pbo.promo_id;

alter table work.feature_space
drop column i;

quit;


/*****************************************************************
*
* Adding product category (prod_lvl3_id) to feature_space  
*
******************************************************************/
proc sql;
create table lvl3 as
select unique(product_id), prod_lvl3_id
from public.product_dictionary_ml;

create table work.promo_prod_lvl3 as
select t.promo_id, t.product_id, l.prod_lvl3_id
from work.promo_product as t, work.lvl3 as l
where t.product_id = l.product_id
group by t.promo_id, t.product_id;

/* Adding promo 734 which applies to all products */
proc sql;
create table work.promo_734_lvl3 as
select unique(prod_lvl3_id)
from work.promo_prod_lvl3
where prod_lvl3_id <> .;

alter table work.promo_734_lvl3
add promo_id numeric;
quit;

data work.promo_734_lvl3;
set work.promo_734_lvl3;
array n{*} _numeric_;
do i=1 to dim(n);
	if n{i} =. then n{i}=734;
end;
run;

proc sql;
alter table work.promo_734_lvl3
add product_id numeric;
quit;

data work.promo_734_lvl3;
set work.promo_734_lvl3;
array n{*} _numeric_;
do i=1 to dim(n);
	if n{i} =. then n{i}=1;
end;
run;

proc sql;
alter table work.promo_734_lvl3
drop column i;
quit;

/* Adding promo 990 which applies to all products */
proc sql;
create table work.promo_990_lvl3 as
select unique(prod_lvl3_id)
from work.promo_prod_lvl3
where prod_lvl3_id <> .;

alter table work.promo_990_lvl3
add promo_id numeric;
quit;

data work.promo_990_lvl3;
set work.promo_990_lvl3;
array n{*} _numeric_;
do i=1 to dim(n);
	if n{i} =. then n{i}=990;
end;
run;

proc sql;
alter table work.promo_990_lvl3
add product_id numeric;
quit;

data work.promo_990_lvl3;
set work.promo_990_lvl3;
array n{*} _numeric_;
do i=1 to dim(n);
	if n{i} =. then n{i}=1;
end;
run;

proc sql;
alter table work.promo_990_lvl3
drop column i;
quit;

/* The union of these two tables  */
proc sql;
create table all_prod_lvl3 as
select * 
from work.promo_734_lvl3
union
select *
from work.promo_990_lvl3;

INSERT INTO all_prod_lvl3 (prod_lvl3_id, promo_id, product_id)
VALUES (1, 734, 1)
VALUES (1, 990, 1);


create table all_product_lvl3 as
select promo_id, product_id, prod_lvl3_id
from work.all_prod_lvl3
order by prod_lvl3_id;


proc sql;
create table work.categ3 as
select *
from work.promo_prod_lvl3
union 
select *
from work.all_product_lvl3;

create table category_3 as
select distinct promo_id, prod_lvl3_id
from work.categ3;

quit;

/* Transposing the table */
proc transpose data=work.category_3
			   prefix = feature_prod_lvl3_id_
			   out=work.space_category_3;
			   by promo_id;
			   var prod_lvl3_id;
			   id prod_lvl3_id;
run;

proc sql;
alter table work.space_category_3
drop column _NAME_;
quit;

/* Filling the columns with 0 and 1 */
data work.prod_category_lvl3;
set work.space_category_3;
array n{*} _numeric_;
do i=2 to dim(n);
	if n{i} =. then n{i}=0;
	else n{i}=1;
end;
run;


/*****************************************************************
*
* Adding product category (prod_lvl2_id) to feature_space  
*
******************************************************************/
proc sql;
create table lvl2 as
select unique(product_id), prod_lvl2_id
from public.product_dictionary_ml;

create table work.promo_prod_lvl2 as
select t.promo_id, t.product_id, l.prod_lvl2_id
from work.promo_product as t, work.lvl2 as l
where t.product_id = l.product_id
group by t.promo_id, t.product_id;

/* Adding promo 734 which applies to all products */
proc sql;
create table work.promo_734_lvl2 as
select unique(prod_lvl2_id)
from work.promo_prod_lvl2
where prod_lvl2_id <> .;

alter table work.promo_734_lvl2
add promo_id numeric;
quit;

data work.promo_734_lvl2;
set work.promo_734_lvl2;
array n{*} _numeric_;
do i=1 to dim(n);
	if n{i} =. then n{i}=734;
end;
run;

proc sql;
alter table work.promo_734_lvl2
add product_id numeric;
quit;

data work.promo_734_lvl2;
set work.promo_734_lvl2;
array n{*} _numeric_;
do i=1 to dim(n);
	if n{i} =. then n{i}=1;
end;
run;

proc sql;
alter table work.promo_734_lvl2
drop column i;
quit;

/* Adding promo 990 which applies to all products */
proc sql;
create table work.promo_990_lvl2 as
select unique(prod_lvl2_id)
from work.promo_prod_lvl2
where prod_lvl2_id <> .;

alter table work.promo_990_lvl2
add promo_id numeric;
quit;

data work.promo_990_lvl2;
set work.promo_990_lvl2;
array n{*} _numeric_;
do i=1 to dim(n);
	if n{i} =. then n{i}=990;
end;
run;

proc sql;
alter table work.promo_990_lvl2
add product_id numeric;
quit;

data work.promo_990_lvl2;
set work.promo_990_lvl2;
array n{*} _numeric_;
do i=1 to dim(n);
	if n{i} =. then n{i}=1;
end;
run;

proc sql;
alter table work.promo_990_lvl2
drop column i;
quit;

/* The union of these two tables  */
proc sql;
create table all_prod_lvl2 as
select * 
from work.promo_734_lvl2
union
select *
from work.promo_990_lvl2;

INSERT INTO all_prod_lvl2 (prod_lvl2_id, promo_id, product_id)
VALUES (1, 734, 1)
VALUES (1, 990, 1);


create table all_product_lvl2 as
select promo_id, product_id, prod_lvl2_id
from work.all_prod_lvl2
order by prod_lvl2_id;


proc sql;
create table work.categ2 as
select *
from work.promo_prod_lvl2
union 
select *
from work.all_product_lvl2;

create table category_2 as
select distinct promo_id, prod_lvl2_id
from work.categ2;

quit;

/* Transposing the table */
proc transpose data=work.category_2
			   prefix = feature_prod_lvl2_id_
			   out=work.space_category_2;
			   by promo_id;
			   var prod_lvl2_id;
			   id prod_lvl2_id;
run;

proc sql;
alter table work.space_category_2
drop column _NAME_;
quit;

/* Filling the columns with 0 and 1 */
data work.prod_category_lvl2;
set work.space_category_2;
array n{*} _numeric_;
do i=2 to dim(n);
	if n{i} =. then n{i}=0;
	else n{i}=1;
end;
run;

/* Getting final table of feature space */
proc sql;
create table work.feature_space as
select *
from work.feature_space as s, work.prod_category_lvl3 as f1, work.prod_category_lvl2 as f2
where s.promo_id=f1.promo_id AND s.promo_id=f2.promo_id;

alter table work.feature_space
drop column i, feature_prod_lvl3_id_1, feature_prod_lvl2_id_1;
quit;

/*****************************************************************
*
* Finding 10 similar promo for 4 specific promo 
*
******************************************************************/

proc sql;
	create table work.promo_renamed as
    select cats('promo_', promo_id) as promo_id, *
    from work.feature_space;
quit;

proc distance data=work.promo_renamed out=work.distance method=Euclid shape=square nostd;
	var interval(feature_Discount--feature_Volga);
	id promo_id;
run;

proc sql;
create table work.specific_promo as
select *
from work.distance
where promo_id IN ('promo_981', 'promo_983', 'promo_1001', 'promo_1002');
quit;

proc transpose data=work.specific_promo
			   out=work.transposed;
run;

proc sql;
create table work.similar981 as
select _NAME_, col1
from work.transposed
where col1 <> .
order by col1;
quit;

proc sql;
create table work.similar983 as
select _NAME_, col2
from work.transposed
where col2 <> .
order by col2;
quit;

proc sql;
create table work.similar1001 as
select _NAME_, col3
from work.transposed
where col3 <> .
order by col3;
quit;

proc sql;
create table work.similar1002 as
select _NAME_, col4
from work.transposed
where col4 <> .
order by col4;
quit;

proc sql;
	create table top_promo981 as
	select monotonic() as TOP, _NAME_ as promo_981
	from work.similar981 (obs=10)
	where promo_981 <> 'promo_981';
quit;

proc sql;
	create table top_promo983 as
	select monotonic() as TOP, _NAME_ as promo_983
	from work.similar983 (obs=10)
	where promo_983 <> 'promo_983';
quit;

proc sql;
	create table top_promo1001 as
	select monotonic() as TOP, _NAME_ as promo_1001
	from work.similar1001 (obs=10)
	where promo_1001 <> 'promo_1001';
quit;

proc sql;
	create table top_promo1002 as
	select monotonic() as TOP, _NAME_ as promo_1002
	from work.similar1002 (obs=10)
	where promo_1002 <> 'promo_1002';

    create table top_all as
	select *
	from top_promo981, top_promo983, top_promo1001, top_promo1002
	where top_promo981.top = top_promo983.top
	AND top_promo983.top = top_promo1001.top
	AND top_promo1001.top = top_promo1002.top;
quit;

proc sql;
	create table without_prefix as
	select TOP, input(substr(promo_981, 7),best.) as n1, input(substr(promo_983, 7),best.) as n2, input(substr(promo_1001, 7),best.) as n3, input(substr(promo_1002, 7),best.) as n4
	from work.top_all;
quit;

proc sql;
	create table promo_names1 as
	select TOP, promo_nm as McFest_Summer_cheeseburger_35
	from work.without_prefix, etl_stg.ia_promo
	where promo_id=n1;

	create table promo_names2 as
	select TOP, promo_nm as McFest_summer_Sundae_49
	from work.without_prefix, etl_stg.ia_promo
	where promo_id=n2;

	create table promo_names3 as
	select TOP, promo_nm as McFest_summer_Chickenburger_39
	from work.without_prefix, etl_stg.ia_promo
	where promo_id=n3;

	create table promo_names4 as
	select TOP, promo_nm as McFest_summer_Big_Mac_99
	from work.without_prefix, etl_stg.ia_promo
	where promo_id=n4;

	create table promo_names_all as
	select *
	from promo_names1, promo_names2, promo_names3, promo_names4
	where promo_names1.top = promo_names2.top
	AND promo_names2.top = promo_names3.top
	AND promo_names3.top = promo_names4.top
	order by TOP;
quit;