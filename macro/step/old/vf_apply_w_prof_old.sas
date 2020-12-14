/*Применение недельного профиля*/
/*
	%vf_apply_w_prof_old(&lmvOutLibrefOutfor..&lmvOutTabNameOutfor.,
					&lmvOutLibrefOutfor..&lmvOutTabNameOutforgc.,
					public.nnet_wp_scored1,
					public.daily_gc,
					&mpInEventsMkup.,
					&mpInWpGc.,
					&mpOutNnetWp.);
*/

%macro vf_apply_w_prof_old(pmix_in
						,gc_in
						,pmix_out
						,gc_out
						,eventsmkup
						,wpgctab
						,nnetwp);
	/* Example:
	%apply_w_prof(public.TS_OUTFOR,public.TS_OUTFORGC,
				public.nnet_wp_scored1,public.daily_gc);
	*/
	/*Входные таблицы*/
	/*Выходные таблицы*/
	/*Параметры*/
	%local	lmvOutLibrefNnetWp
			lmvOutTabNameNnetWp;
	%let wpgc=&wpgctab;
	%let loc_gc_out=&gc_out;
	%let lmvInLib=ETL_IA;
	%let etl_current_dt = %sysfunc(today());
	%let ETL_CURRENT_DTTM = %sysfunc(datetime());
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	/*Праздники по дням_j Ключ: ресторан-дата*/
	proc fedsql sessref=casauto;
		create table public.holiday_mkup{options replace=true} as
			select distinct WEEK_DT
							,PBO_LOCATION_ID
							,START_DT
							,case
								when weekday(START_DT)>1 then weekday(START_DT)-1 
								else 7 
							end as weekday
							,1 as mkup
			from &eventsmkup. t2 
		;
	quit;

	proc cas;
		transpose.transpose /
		table={name="holiday_mkup", caslib="public", groupby={"week_dt","PBO_LOCATION_ID"}} 
		attributes={{name="week_dt"},{name="PBO_LOCATION_ID"} }
		transpose={"mkup"} 
		prefix="H_" 
		id={"weekday"} 
		casout={name="holiday_transp", caslib="public", replace=true};
	quit;

	/*Промо-факторы_i_j
	1. Флаги промо-механик разных типов (i=1..N) по дням (j=1..7) одной и той же недели в разрезе товар-ресторан. 
	2. Характеристики промо (цены, скидки).
	Ключ: товар-ресторан-дата*/
	/*Expand PBO into leaf level*/
	proc fedsql sessref=casauto;
		create table public.promo_pbo_exp1{options replace=true} as
			select t1.PROMO_ID
					,t2.PBO_LOCATION_ID
			from casuser.PROMO_PBO t1 
			inner join casuser.pbo_dictionary t2
				on t1.pbo_location_id=t2.LVL1_ID
		;
		create table public.promo_pbo_exp2{options replace=true} as
			select t1.PROMO_ID
					,t2.PBO_LOCATION_ID
			from casuser.PROMO_PBO t1 
			inner join casuser.pbo_dictionary t2
				on t1.pbo_location_id=t2.LVL2_ID
		;
		create table public.promo_pbo_exp3{options replace=true} as
			select t1.PROMO_ID
					,t2.PBO_LOCATION_ID
			from casuser.PROMO_PBO t1
			inner join casuser.pbo_dictionary t2
				on t1.pbo_location_id=t2.LVL3_ID
		;
		create table public.promo_pbo_exp4{options replace=true} as
			select t1.PROMO_ID,t2.PBO_LOCATION_ID
			from casuser.PROMO_PBO t1 
			inner join casuser.pbo_dictionary t2
				on t1.pbo_location_id=t2.pbo_location_id
		;
	quit;

	data public.promo_pbo_exp1(append=force);
		set public.promo_pbo_exp2
			public.promo_pbo_exp3
			public.promo_pbo_exp4
		;
	run;
	/*Expand products into leaf level*/
	proc fedsql sessref=casauto;
	/*create table public.promo_prod_exp1{options replace=true} as
	select t1.PROMO_ID,t2.Product_ID
	from casuser.PROMO_PROD t1 inner join casuser.product_dictionary t2
	on t1.product_id=t2.PROD_LVL1_ID;*/
		create table public.promo_prod_exp1{options replace=true} as
			select t1.PROMO_ID
					,t2.Product_ID
			from casuser.PROMO_PROD t1
			inner join casuser.product_dictionary t2
				on t1.product_id=t2.PROD_LVL2_ID
		;
		create table public.promo_prod_exp3{options replace=true} as
			select t1.PROMO_ID
					,t2.Product_ID
			from casuser.PROMO_PROD t1
			inner join casuser.product_dictionary t2
				on t1.product_id=t2.PROD_LVL3_ID
		;
		create table public.promo_prod_exp4{options replace=true} as
			select t1.PROMO_ID
					,t2.Product_ID
			from casuser.PROMO_PROD t1 
			inner join casuser.product_dictionary t2
				on t1.product_id=t2.PROD_LVL4_ID
		;
		create table public.promo_prod_exp5{options replace=true} as
			select t1.PROMO_ID,t2.Product_ID
			from casuser.PROMO_PROD t1 
			inner join casuser.product_dictionary t2
				on t1.product_id=t2.product_id
		;
	quit;

	data public.promo_prod_exp1(append=force);
	set public.promo_prod_exp3
		public.promo_prod_exp4
		public.promo_prod_exp5
		;
	run;

	proc fedsql sessref=casauto;
		create table public.promo_d{options replace=true} as
			select
				t1.START_DT
				,t1.END_DT
				,t1.channel_cd
				,t1.promo_id
				,t2.pbo_location_id
				,t3.product_id
			from casuser.promo t1
				inner join PUBLIC.PROMO_PBO_exp1 t2
					on t1.promo_id=t2.promo_id
				inner join PUBLIC.PROMO_PROD_exp1 t3
					on t1.promo_id=t3.promo_id
		;
	quit;

	/*транспонирование флагов промо*/
	data public.promo_w;
		set public.promo_d;
		format period_dt week_dt date9.;
		keep pr_: week_dt pbo_location_id channel_cd product_id;
		array PR_ {7};
		do i=1 to 7;
			PR_{i}=0;
		end;
		do period_dt=start_DT to end_DT ;
			if weekday(period_dt)>1 then wkday=weekday(period_dt)-1;
			   else wkday=7;
			PR_{wkday}=1;     
			if wkday=7 or period_dt=end_dt then do;
			  week_dt=intnx('week.2',period_dt,0);
			  output;
			  do i=1 to 7;
				PR_{i}=0;
			  end;
			end;
		end;
	run;
	/* тут бы ещё избавиться от дубликатов */
	proc fedsql sessref=casauto;
		create table public.promo_w_nodup{options replace=true} as 
			select week_dt
					,pbo_location_id
					,channel_cd
					,product_id
					,max(pr_1) as pr_1
					,max(pr_2) as pr_2
					,max(pr_3) as pr_3
					,max(pr_4) as pr_4
					,max(pr_5) as pr_5
					,max(pr_6) as pr_6
					,max(pr_7) as pr_7
			from public.promo_w
			group by 1,2,3,4
		;
	quit;

	/*ТОП 1,2,3 день по числу заказов
	В разрезе (Prod_Hier_2 - PBO_location_id - месяц начала недели)
	*/
	data CASUSER.pmix_sales(replace=yes  drop=valid_from_dttm valid_to_dttm);
			set &lmvInLib..pmix_sales(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	
	run;
	proc fedsql sessref=casauto;
		create table public.days_cat{options replace=true} as
			select case 
						when weekday(t1.SALES_DT)>1 then weekday(t1.SALES_DT)-1
						else 7 
					end as weekday,
					t2.prod_lvl2_id,
					month(cast(t1.sales_dt as date)) as mon_start,
					t3.lvl3_id,
					coalesce(sum(t1.sales_QTY),0)+coalesce(sum(t1.sales_qty_promo),0) as sales_wd_cat
			from casuser.pmix_sales t1 
				left join casuser.PRODUCT_DICTIONARY t2
					on t1.product_id=t2.product_id
				left join casuser.PBO_DICTIONARY t3
					on t1.pbo_location_id=t3.pbo_location_id
			where t1.sales_dt>=	date'2018-01-04'
					and t1.sales_dt<=date %tslit(&vf_hist_end_dt)
			group by 1,2,3,4
		;
	quit;
	/*Calculate ranks*/
	data public.days_cat_ranked;
		set public.days_cat;
		by lvl3_id mon_start prod_lvl2_id;
		retain sales1-sales7 ;
		array sales{7} ;
		array ranks_s{4}; /*сколько рангов?*/
		if first.prod_lvl2_id then do i=1 to 7;
			sales{i}=0;
		end;
		sales{weekday}=sales_wd_cat;
		if last.prod_lvl2_id then do;
			do i=1 to dim(ranks_s);
				x=largest(i,of sales(*));
				ranks_s{i}=whichn(x,of sales(*));
			end;
			output;
		end;
	run;

	proc fedsql sessref=casauto;
		create table public.wp_abt{options replace=true} as
		select t1.CHANNEL_CD
				,t1.PRODUCT_ID,
				t1.PBO_LOCATION_ID,t1.sales_dt as WEEK_DT,
				t1.MON_START,
				t1.MON_END,
				t1.FF,
				coalesce(t2.H_1,0) as H_1,
				coalesce(t2.H_2,0) as H_2,
				coalesce(t2.H_3,0) as H_3,
				coalesce(t2.H_4,0) as H_4,
				coalesce(t2.H_5,0) as H_5,
				coalesce(t2.H_6,0) as H_6,
				coalesce(t2.H_7,0) as H_7,
				coalesce(t3.PR_1,0) as PR_1,
				coalesce(t3.PR_2,0) as PR_2,
				coalesce(t3.PR_3,0) as PR_3,
				coalesce(t3.PR_4,0) as PR_4,
				coalesce(t3.PR_5,0) as PR_5,
				coalesce(t3.PR_6,0) as PR_6,
				coalesce(t3.PR_7,0) as PR_7,
				t7.ranks_s1, t7.ranks_s2, t7.ranks_s3, t7.ranks_s4 
		from &pmix_in t1 
			left join public.holiday_transp t2
				on t1.sales_dt = t2.week_dt and t1.pbo_location_id=t2.pbo_location_id
			left join public.promo_w_nodup t3
				on t1.sales_dt = t3.week_dt and t1.pbo_location_id=t3.pbo_location_id
				and t1.product_id=t3.product_id and t1.channel_cd=t3.channel_cd
			left join casuser.PRODUCT_DICTIONARY t4
				on t1.product_id=t4.product_id
			left join casuser.PBO_DICTIONARY t5
				on t1.pbo_location_id=t5.pbo_location_id
			left join public.days_cat_ranked t7
				on t7.lvl3_id=t5.lvl3_id and t7.mon_start=t1.mon_start and  
				t7.prod_lvl2_id=t4.prod_lvl2_id;
	quit;

	/*если таблицы nnet_wp нет в CAS, поднять её сохранённую версию (в Public)*/
	%member_names (mpTable=&nnetwp, mpLibrefNameKey=lmvOutLibrefNnetWp, mpMemberNameKey=lmvOutTabNameNnetWp);
	proc cas;
		table.tableExists result = rc / caslib="&lmvOutLibrefNnetWp." name="&lmvOutTabNameNnetWp.";
		if rc=0  then do;
			loadtable / caslib="&lmvOutLibrefNnetWp.",
						path="&lmvOutTabNameNnetWp._attr.sashdat",
						casout={caslib="&lmvOutLibrefNnetWp." name='attr2', replace=true};
			loadtable / caslib="&lmvOutLibrefNnetWp.",
						path="&lmvOutTabNameNnetWp..sashdat",
						casout={caslib="&lmvOutLibrefNnetWp." name="&lmvOutTabNameNnetWp.", replace=true};
			attribute / task='ADD',
						   caslib="&lmvOutLibrefNnetWp.",
						name="&lmvOutTabNameNnetWp.",
						attrtable='attr2';
			table.promote / name="&lmvOutTabNameNnetWp." caslib="&lmvOutLibrefNnetWp." target="&lmvOutTabNameNnetWp." targetlib="&lmvOutLibrefNnetWp.";
		end;
		else print("Table &lmvOutLibrefNnetWp..&lmvOutTabNameNnetWp. already loaded");
	quit;

	proc nnet data=public.wp_abt inmodel=&lmvOutLibrefNnetWp..&lmvOutTabNameNnetWp.;
		SCORE OUT=public.nnet_wp_scored  copyvars=(FF CHANNEL_CD PRODUCT_ID PBO_LOCATION_ID WEEK_DT PR_1 PR_2 PR_3 PR_4 PR_5 PR_6 PR_7);
	run;

	proc fedsql sessref=casauto;
		create table &pmix_out{options replace=true} as
			select /*столбцы д.быть в одном порядке для последующего append*/
				t1.FF,t1.CHANNEL_CD,t1.product_id,
				t1.pbo_location_id,t1.WEEK_DT,
				PR_1,PR_2,PR_3,PR_4,PR_5,PR_6,PR_7,
				I_WEEKDAY,
				P_WEEKDAY1,P_WEEKDAY2,P_WEEKDAY3,P_WEEKDAY4,P_WEEKDAY5,P_WEEKDAY6,P_WEEKDAY7,
				cast (1 as double) as fl_scd
			from public.nnet_wp_scored t1 
		;
	*drop table public.nnet_wp_scored;
	quit;

	/*-=-=-=-=-=-=-=- GC -=-=-=-=-=-=-*/
	/*Для всех товаров-пбо-недель нужно посчитать недельные профили*/
	proc fedsql sessref=casauto;
		create table public.weeks_to_scoregc{options replace=true} as
			select CHANNEL_CD
					,PBO_LOCATION_ID
					,SALES_DT
					,FF
			from &gc_in
		;
	quit;
	option dsoptions=nonote2err;
	proc fedsql sessref=casauto;
		create table public.weeks_with_wp{options replace=true} as
		select t1.CHANNEL_CD,t1.PBO_LOCATION_ID,
				t1.SALES_DT,t1.FF,
				coalesce(prday_1,1/7) as prday_1,
				coalesce(prday_2,1/7) as prday_2,
				coalesce(prday_3,1/7) as prday_3,
				coalesce(prday_4,1/7) as prday_4,
				coalesce(prday_5,1/7) as prday_5,
				coalesce(prday_6,1/7) as prday_6,
				coalesce(prday_7,1/7) as prday_7
		from public.weeks_to_scoregc t1
			left join &wpgc t2 
				on t1.pbo_location_id=t2.pbo_location_id 
				and t1.channel_cd=t2.channel_cd;
	quit;
	option dsoptions=note2err;
	%let str_lib=%scan(&loc_gc_out,1,%nrstr(.));
	%let str_tab=%scan(&loc_gc_out,2,%nrstr(.));

	proc casutil;
		droptable casdata="&str_tab" incaslib="&str_lib" quiet;
	run; 
option dsoptions=nonote2err;
	/*агрегация по недельному профилю с учетом PLM*/
	data &gc_out;
		set public.weeks_with_wp;
		array prday_{7};
		keep CHANNEL_CD PBO_LOCATION_ID period_dt mon_dt FF;
		format period_dt mon_dt date9.;
		length period_dt 8;
		period_dt=sales_dt;
		fc=ff;
		do while (period_dt<=sales_dt+6);
			mon_dt=intnx('month',period_dt,0,'b');
			ff=fc*prday_{period_dt-sales_dt+1};
			output;
			period_dt+1;
		end;
	run;
option dsoptions=note2err;
	proc casutil;
		promote casdata="&str_tab" incaslib="&str_lib" outcaslib="&str_lib";
	run;
%mend vf_apply_w_prof_old;


/*Применение недельного профиля*/
%macro vf_apply_w_prof_old1(pmix_in,gc_in,pmix_out,gc_out,
		eventsmkup,wpgctab,
		nnetwp);
/* Example:
%apply_w_prof(public.TS_OUTFOR,public.TS_OUTFORGC,
				public.nnet_wp_scored1,public.daily_gc);
*/
/*Входные таблицы*/
/*Выходные таблицы*/
/*Параметры*/
	%local	lmvOutLibrefNnetWp
			lmvOutTabNameNnetWp;
%let wpgc=&wpgctab;
%let loc_gc_out=&gc_out;
option dsoptions=nonote2err;
/*Праздники по дням_j Ключ: ресторан-дата*/
proc fedsql sessref=casauto;
create table public.holiday_mkup{options replace=true} as
select distinct WEEK_DT,PBO_LOCATION_ID,START_DT,
case when weekday(START_DT)>1 then weekday(START_DT)-1 else 7 end as weekday,
1 as mkup
from &eventsmkup. t2 ;
quit;

proc cas;
transpose.transpose /
   table={name="holiday_mkup", caslib="public", groupby={"week_dt","PBO_LOCATION_ID"}} 
   attributes={{name="week_dt"},{name="PBO_LOCATION_ID"} }
   transpose={"mkup"} 
   prefix="H_" 
   id={"weekday"} 
   casout={name="holiday_transp", caslib="public", replace=true};
quit;

/*Промо-факторы_i_j
1. Флаги промо-механик разных типов (i=1..N) по дням (j=1..7) одной и той же недели в разрезе товар-ресторан. 
2. Характеристики промо (цены, скидки).
Ключ: товар-ресторан-дата*/
/*Expand PBO into leaf level*/
proc fedsql sessref=casauto;
  create table public.promo_pbo_exp1{options replace=true} as
  select t1.PROMO_ID,t2.PBO_LOCATION_ID
  from casuser.PROMO_PBO t1 inner join casuser.pbo_dictionary t2
  on t1.pbo_location_id=t2.LVL1_ID;
  create table public.promo_pbo_exp2{options replace=true} as
  select t1.PROMO_ID,t2.PBO_LOCATION_ID
  from casuser.PROMO_PBO t1 inner join casuser.pbo_dictionary t2
  on t1.pbo_location_id=t2.LVL2_ID;
  create table public.promo_pbo_exp3{options replace=true} as
  select t1.PROMO_ID,t2.PBO_LOCATION_ID
  from casuser.PROMO_PBO t1 inner join casuser.pbo_dictionary t2
  on t1.pbo_location_id=t2.LVL3_ID;
  create table public.promo_pbo_exp4{options replace=true} as
  select t1.PROMO_ID,t2.PBO_LOCATION_ID
  from casuser.PROMO_PBO t1 inner join casuser.pbo_dictionary t2
  on t1.pbo_location_id=t2.pbo_location_id;
quit;

data public.promo_pbo_exp1(append=force);
	set public.promo_pbo_exp2 public.promo_pbo_exp3 public.promo_pbo_exp4;
run;
/*Expand products into leaf level*/
proc fedsql sessref=casauto;
  /*create table public.promo_prod_exp1{options replace=true} as
  select t1.PROMO_ID,t2.Product_ID
  from casuser.PROMO_PROD t1 inner join casuser.product_dictionary t2
  on t1.product_id=t2.PROD_LVL1_ID;*/
  create table public.promo_prod_exp1{options replace=true} as
  select t1.PROMO_ID,t2.Product_ID
  from casuser.PROMO_PROD t1 inner join casuser.product_dictionary t2
  on t1.product_id=t2.PROD_LVL2_ID;
  create table public.promo_prod_exp3{options replace=true} as
  select t1.PROMO_ID,t2.Product_ID
  from casuser.PROMO_PROD t1 inner join casuser.product_dictionary t2
  on t1.product_id=t2.PROD_LVL3_ID;
  create table public.promo_prod_exp4{options replace=true} as
  select t1.PROMO_ID,t2.Product_ID
  from casuser.PROMO_PROD t1 inner join casuser.product_dictionary t2
  on t1.product_id=t2.PROD_LVL4_ID;
  create table public.promo_prod_exp5{options replace=true} as
  select t1.PROMO_ID,t2.Product_ID
  from casuser.PROMO_PROD t1 inner join casuser.product_dictionary t2
  on t1.product_id=t2.product_id;
quit;

data public.promo_prod_exp1(append=force);
	set public.promo_prod_exp3 public.promo_prod_exp4
		public.promo_prod_exp5;
run;

proc fedsql sessref=casauto;
  create table public.promo_d{options replace=true} as
  select
  t1.START_DT,t1.END_DT,t1.channel_cd,t1.promo_id,
  t2.pbo_location_id,
  t3.product_id
  from casuser.promo t1 inner join PUBLIC.PROMO_PBO_exp1 t2
  on t1.promo_id=t2.promo_id
  inner join PUBLIC.PROMO_PROD_exp1 t3
  on t1.promo_id=t3.promo_id
 ;
quit;

/*транспонирование флагов промо*/
data public.promo_w;
  set public.promo_d;
  format period_dt week_dt date9.;
  keep pr_: week_dt pbo_location_id channel_cd product_id;
  array PR_ {7};
  do i=1 to 7;
   PR_{i}=0;
  end;
  do period_dt=start_DT to end_DT ;
    if weekday(period_dt)>1 then wkday=weekday(period_dt)-1;
       else wkday=7;
    PR_{wkday}=1;     
    if wkday=7 or period_dt=end_dt then do;
      week_dt=intnx('week.2',period_dt,0);
      output;
      do i=1 to 7;
        PR_{i}=0;
      end;
    end;
  end;
run;
/* тут бы ещё избавиться от дубликатов */
proc fedsql sessref=casauto;
create table public.promo_w_nodup{options replace=true} as 
select week_dt,pbo_location_id,channel_cd,product_id,
max(pr_1) as pr_1,
max(pr_2) as pr_2,
max(pr_3) as pr_3,
max(pr_4) as pr_4,
max(pr_5) as pr_5,
max(pr_6) as pr_6,
max(pr_7) as pr_7
from public.promo_w
group by 1,2,3,4;
quit;

/*ТОП 1,2,3 день по числу заказов
В разрезе (Prod_Hier_2 - PBO_location_id - месяц начала недели)
*/

proc fedsql sessref=casauto;
create table public.days_cat{options replace=true} as
select case when weekday(t1.SALES_DT)>1 then weekday(t1.SALES_DT)-1 else 7 end as weekday,
t2.prod_lvl2_id,
month(cast(t1.sales_dt as date)) as mon_start,
t3.lvl3_id,
coalesce(sum(t1.sales_QTY),0)+coalesce(sum(t1.sales_qty_promo),0) as sales_wd_cat
from casuser.pmix_sales t1 left join casuser.PRODUCT_DICTIONARY t2
on t1.product_id=t2.product_id
left join casuser.PBO_DICTIONARY t3
on t1.pbo_location_id=t3.pbo_location_id
where t1.sales_dt>=	date'2018-01-04' and t1.sales_dt<=date %tslit(&vf_hist_end_dt)
group by 1,2,3,4;
quit;
/*Calculate ranks*/
data public.days_cat_ranked;
  set public.days_cat;
  by lvl3_id mon_start prod_lvl2_id;
  retain sales1-sales7 ;
  array sales{7} ;
  array ranks_s{4}; /*сколько рангов?*/
  if first.prod_lvl2_id then do i=1 to 7;
     sales{i}=0;
  end;
  sales{weekday}=sales_wd_cat;
  if last.prod_lvl2_id then do;
	do i=1 to dim(ranks_s);
     x=largest(i,of sales(*));
	 ranks_s{i}=whichn(x,of sales(*));
    end;
    output;
  end;
run;

proc fedsql sessref=casauto;
  create table public.wp_abt{options replace=true} as
  select t1.CHANNEL_CD,t1.PRODUCT_ID,
   t1.PBO_LOCATION_ID,t1.sales_dt as WEEK_DT,
   t1.MON_START,
   t1.MON_END,
   t1.FF,
   coalesce(t2.H_1,0) as H_1,
   coalesce(t2.H_2,0) as H_2,
   coalesce(t2.H_3,0) as H_3,
   coalesce(t2.H_4,0) as H_4,
   coalesce(t2.H_5,0) as H_5,
   coalesce(t2.H_6,0) as H_6,
   coalesce(t2.H_7,0) as H_7,
   coalesce(t3.PR_1,0) as PR_1,
   coalesce(t3.PR_2,0) as PR_2,
   coalesce(t3.PR_3,0) as PR_3,
   coalesce(t3.PR_4,0) as PR_4,
   coalesce(t3.PR_5,0) as PR_5,
   coalesce(t3.PR_6,0) as PR_6,
   coalesce(t3.PR_7,0) as PR_7,
   t7.ranks_s1, t7.ranks_s2, t7.ranks_s3, t7.ranks_s4 
  from &pmix_in t1 left join public.holiday_transp t2
  on t1.sales_dt = t2.week_dt and t1.pbo_location_id=t2.pbo_location_id
  left join public.promo_w_nodup t3
  on t1.sales_dt = t3.week_dt and t1.pbo_location_id=t3.pbo_location_id
     and t1.product_id=t3.product_id and t1.channel_cd=t3.channel_cd
  left join casuser.PRODUCT_DICTIONARY t4
  on t1.product_id=t4.product_id
  left join casuser.PBO_DICTIONARY t5
  on t1.pbo_location_id=t5.pbo_location_id
  left join public.days_cat_ranked t7
  on t7.lvl3_id=t5.lvl3_id and t7.mon_start=t1.mon_start and  
     t7.prod_lvl2_id=t4.prod_lvl2_id;
quit;

	/*если таблицы nnet_wp нет в CAS, поднять её сохранённую версию (в Public)*/
	%member_names (mpTable=&nnetwp, mpLibrefNameKey=lmvOutLibrefNnetWp, mpMemberNameKey=lmvOutTabNameNnetWp);
	proc cas;
		table.tableExists result = rc / caslib="&lmvOutLibrefNnetWp." name="&lmvOutTabNameNnetWp.";
		if rc=0  then do;
			loadtable / caslib="&lmvOutLibrefNnetWp.",
						path="&lmvOutTabNameNnetWp._attr.sashdat",
						casout={caslib="&lmvOutLibrefNnetWp." name='attr2', replace=true};
			loadtable / caslib="&lmvOutLibrefNnetWp.",
						path="&lmvOutTabNameNnetWp..sashdat",
						casout={caslib="&lmvOutLibrefNnetWp." name="&lmvOutTabNameNnetWp.", replace=true};
			attribute / task='ADD',
						   caslib="&lmvOutLibrefNnetWp.",
						name="&lmvOutTabNameNnetWp.",
						attrtable='attr2';
			table.promote / name="&lmvOutTabNameNnetWp." caslib="&lmvOutLibrefNnetWp." target="&lmvOutTabNameNnetWp." targetlib="&lmvOutLibrefNnetWp.";
		end;
		else print("Table &lmvOutLibrefNnetWp..&lmvOutTabNameNnetWp. already loaded");
	quit;
	
	proc nnet data=public.wp_abt inmodel=&lmvOutLibrefNnetWp..&lmvOutTabNameNnetWp.;
		SCORE OUT=public.nnet_wp_scored  copyvars=(FF CHANNEL_CD PRODUCT_ID PBO_LOCATION_ID WEEK_DT PR_1 PR_2 PR_3 PR_4 PR_5 PR_6 PR_7);
	run;

proc fedsql sessref=casauto;
create table &pmix_out{options replace=true} as
select /*столбцы д.быть в одном порядке для последующего append*/
t1.FF,t1.CHANNEL_CD,t1.product_id,
t1.pbo_location_id,t1.WEEK_DT,
PR_1,PR_2,PR_3,PR_4,PR_5,PR_6,PR_7,
I_WEEKDAY,
P_WEEKDAY1,P_WEEKDAY2,P_WEEKDAY3,P_WEEKDAY4,P_WEEKDAY5,P_WEEKDAY6,P_WEEKDAY7,
cast (1 as double) as fl_scd
from public.nnet_wp_scored t1 
;
*drop table public.nnet_wp_scored;
quit;

/*-=-=-=-=-=-=-=- GC -=-=-=-=-=-=-*/
/*Для всех товаров-пбо-недель нужно посчитать недельные профили*/
proc fedsql sessref=casauto;
create table public.weeks_to_scoregc{options replace=true} as
select CHANNEL_CD,PBO_LOCATION_ID,
SALES_DT,FF
from &gc_in
;
quit;

proc fedsql sessref=casauto;
create table public.weeks_with_wp{options replace=true} as
select t1.CHANNEL_CD,t1.PBO_LOCATION_ID,
t1.SALES_DT,t1.FF,
coalesce(prday_1,1/7) as prday_1,
coalesce(prday_2,1/7) as prday_2,
coalesce(prday_3,1/7) as prday_3,
coalesce(prday_4,1/7) as prday_4,
coalesce(prday_5,1/7) as prday_5,
coalesce(prday_6,1/7) as prday_6,
coalesce(prday_7,1/7) as prday_7
from public.weeks_to_scoregc t1 left join &wpgc t2 
on t1.pbo_location_id=t2.pbo_location_id and t1.channel_cd=t2.channel_cd;
quit;

%let str_lib=%scan(&loc_gc_out,1,%nrstr(.));
%let str_tab=%scan(&loc_gc_out,2,%nrstr(.));

proc casutil;
droptable casdata="&str_tab" incaslib="&str_lib" quiet;
run; 

/*агрегация по недельному профилю с учетом PLM*/
data &gc_out;
  set public.weeks_with_wp;
  array prday_{7};
  keep CHANNEL_CD PBO_LOCATION_ID period_dt mon_dt FF;
  format period_dt mon_dt date9.;
  length period_dt 8;
  period_dt=sales_dt;
  fc=ff;
  do while (period_dt<=sales_dt+6);
    mon_dt=intnx('month',period_dt,0,'b');
	ff=fc*prday_{period_dt-sales_dt+1};
    output;
    period_dt+1;
  end;
run;

proc casutil;
	droptable casdata="nnet_wp_scored1" incaslib="dm_abt" quiet;
promote casdata="&str_tab" incaslib="&str_lib" outcaslib="&str_lib";
promote casdata="nnet_wp_scored1" incaslib="public" outcaslib="dm_abt";
run;
%mend vf_apply_w_prof_old1;
