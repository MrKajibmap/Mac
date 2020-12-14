/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для cоздания модели недельного профиля
*	Для разбивки по дням и переагрегации недель до месяцев
*
*  ПАРАМЕТРЫ:
*     Нет
*	  &mpPromoW.
*
******************************************************************
*  Использует: 
*	  mpInEventsMkup   		- таблица events_mkup
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %macro vf_train_week_profile_gc_sep(mpInEventsMkup=dm_abt.events_mkup,
*		mpNnetWp=dm_abt.nnet_wp1,
*		mpPromo_W=dm_abt.promo_w);
*
****************************************************************************
*  02-07-2020  Борзунов     Начальное кодирование
*  28-07-2020  Борзунов		Добавлены параметры 
****************************************************************************/
%macro vf_train_week_profile_gc_sep(mpInEventsMkup=dm_abt.events_mkup,
								 mpNnetWp=dm_abt.nnet_wp1,
								 mpPromo_W=dm_abt.promo_w 
								 );

	%local lmvOutLibrefNnetWp lmvOutTabNameNnetWp
			lmvOutLibrefPromoW lmvOutTabNamePromoW
			;
	
	%member_names (mpTable=&mpNnetWp, mpLibrefNameKey=lmvOutLibrefNnetWp, mpMemberNameKey=lmvOutTabNameNnetWp);
	%member_names (mpTable=&mpPromo_W, mpLibrefNameKey=lmvOutLibrefPromoW, mpMemberNameKey=lmvOutTabNamePromoW);
	
	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;
	
	/*0. Удаление целевых таблиц */
	proc casutil;
		droptable casdata="&lmvOutTabNamePromoW." incaslib="&lmvOutLibrefPromoW." quiet;
		droptable casdata="&lmvOutTabNameNnetWp." incaslib="&lmvOutLibrefNnetWp." quiet;
	run;
	
	proc cas;
		timeData.timeSeries result =r /
			series={{name="sales_qty", Acc="sum", setmiss=0},
				{name="gross_sales_amt", Acc="sum", setmiss=0},
				{name="net_sales_amt", Acc="sum", setmiss=0},
				{name="sales_qty_promo", Acc="sum", setmiss=0}}
			tStart="2018-07-09" /*понедельник*/
			tEnd= "&VF_HIST_END_DT" 
			table={caslib="mn_long",name="pmix_sales", groupby={"PBO_LOCATION_ID","PRODUCT_ID","CHANNEL_CD"} }
			timeId="SALES_DT"
			trimId="LEFT"
			interval="week.2"
			casOut={caslib="casuser",name="TS_pmix_sales",replace=True}
			;
		run;
	quit;

	/*таблица с пропорциями*/
	proc fedsql sessref=casauto noprint;
		create table casuser.daily_orders{options replace=true} as
			select t1.PRODUCT_ID
					,t1.PBO_LOCATION_ID
					,t1.CHANNEL_CD
					,case 
						when weekday(t1.SALES_DT)>1 
						then weekday(t1.SALES_DT)-1
						else 7 
					end as weekday
					,t2.SALES_DT as week_dt,t1.sales_dt
					,month(cast(t2.sales_dt as date)) as mon_start /*Месяц, на который приходится начало недели*/
					,month(cast(intnx('week.2',t2.sales_dt,0,'e') as date)) as mon_end /*Месяц, на который приходится окончание недели*/
					,case 
						when (coalesce(t2.sales_QTY,0)+coalesce(t2.sales_qty_promo,0)) != 0
						then (coalesce(t1.sales_QTY,0)+coalesce(t1.sales_qty_promo,0)) / (coalesce(t2.sales_QTY,0)+coalesce(t2.sales_qty_promo,0))
						else 0 
					end as prop
			from casuser.ts_pmix_sales t2
			inner join mn_long.pmix_sales t1
				on t2.product_id=t1.product_id
				and t2.pbo_location_id=t1.pbo_location_id
				and t1.channel_cd=t2.channel_cd
				and intnx('week.2',t1.sales_dt,0)=t2.sales_dt
		;
	quit;

	/*Праздники по дням_j Ключ: ресторан-дата*/

	proc fedsql sessref=casauto noprint;
		create table casuser.holiday_mkup{options replace=true} as
			select distinct WEEK_DT
							,PBO_LOCATION_ID
							,START_DT
							,case 
								when weekday(START_DT)>1 
								then weekday(START_DT)-1 
								else 7  
							end as weekday
							,1 as mkup
		from &mpInEventsMkup. t2 
		;
	quit;

	proc cas;
		transpose.transpose /
		   table={name="holiday_mkup", caslib="casuser", groupby={"week_dt","PBO_LOCATION_ID"}} 
		   attributes={{name="week_dt"},{name="PBO_LOCATION_ID"} }
		   transpose={"mkup"} 
		   prefix="H_" 
		   id={"weekday"} 
		   casout={name="holiday_transp", caslib="casuser", replace=true};
	quit;

	/*Промо-факторы_i_j
	1. Флаги промо-механик разных типов (i=1..N) по дням (j=1..7) одной и той же недели в разрезе товар-ресторан. 
	2. Характеристики промо (цены, скидки).
	Ключ: товар-ресторан-дата*/
	/*Expand PBO into leaf level*/
	proc fedsql sessref=casauto noprint;
	  create table casuser.promo_pbo_exp1{options replace=true} as
		select t1.PROMO_ID
				,t2.PBO_LOCATION_ID
		from mn_long.PROMO_PBO t1
		inner join mn_long.PBO_DICTIONARY t2
			on t1.pbo_location_id=t2.LVL1_ID
		;
	  create table casuser.promo_pbo_exp2{options replace=true} as
		select t1.PROMO_ID
				,t2.PBO_LOCATION_ID
		from mn_long.PROMO_PBO t1 
		inner join mn_long.PBO_DICTIONARY t2
			on t1.pbo_location_id=t2.LVL2_ID
		;
	  create table casuser.promo_pbo_exp3{options replace=true} as
		select t1.PROMO_ID
				,t2.PBO_LOCATION_ID
		from mn_long.PROMO_PBO t1 
		inner join mn_long.PBO_DICTIONARY t2
			on t1.pbo_location_id=t2.LVL3_ID
		;
	  create table casuser.promo_pbo_exp4{options replace=true} as
		  select t1.PROMO_ID
				,t2.PBO_LOCATION_ID
		  from mn_long.PROMO_PBO t1 
		  inner join mn_long.PBO_DICTIONARY t2
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
	proc fedsql sessref=casauto;
	  create table casuser.promo_prod_exp1{options replace=true} as
		  select t1.PROMO_ID
				,t2.Product_ID
		  from mn_long.PROMO_PROD t1 
		  inner join mn_long.product_dictionary t2
			on t1.product_id=t2.PROD_LVL1_ID;
	  create table casuser.promo_prod_exp2{options replace=true} as
		  select t1.PROMO_ID
				,t2.Product_ID
		  from mn_long.PROMO_PROD t1 
		  inner join mn_long.product_dictionary t2
			on t1.product_id=t2.PROD_LVL2_ID;
	  create table casuser.promo_prod_exp3{options replace=true} as
		  select t1.PROMO_ID
				,t2.Product_ID
		  from mn_long.PROMO_PROD t1 
		  inner join mn_long.product_dictionary t2
			on t1.product_id=t2.PROD_LVL3_ID;
	  create table casuser.promo_prod_exp4{options replace=true} as
		  select t1.PROMO_ID
				,t2.Product_ID
		  from mn_long.PROMO_PROD t1 
		  inner join mn_long.product_dictionary t2
			on t1.product_id=t2.PROD_LVL4_ID;
	  create table casuser.promo_prod_exp5{options replace=true} as
		  select t1.PROMO_ID
				,t2.Product_ID
		  from mn_long.PROMO_PROD t1
		  inner join mn_long.product_dictionary t2
			on t1.product_id=t2.product_id;
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
		  from mn_long.promo t1 
		  inner join casuser.PROMO_PBO_exp1 t2
			on t1.promo_id=t2.promo_id
		  inner join casuser.PROMO_PROD_exp1 t3
			on t1.promo_id=t3.promo_id
	 ;
	quit;

	/*транспонирование флагов промо*/
	data casuser.&lmvOutTabNamePromoW.;
	  set casuser.promo_d;
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
	
	proc fedsql sessref=casauto noprint;
		create table casuser.promo_w_nodup{options replace=true} as 
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
			from casuser.&lmvOutTabNamePromoW.
			group by 1,2,3,4;
	quit;

	/*ТОП 1,2,3 день по числу заказов
	В разрезе (Prod_Hier_2 - PBO_location_id - месяц начала недели)
	*/

	proc fedsql sessref=casauto noprint;
		create table casuser.days_cat{options replace=true} as
			select case 
						when weekday(t1.SALES_DT)>1 
						then weekday(t1.SALES_DT)-1 
						else 7 
					end as weekday
					,t2.prod_lvl2_id
					,month(cast(t1.sales_dt as date)) as mon_start
					,t3.lvl3_id
					,coalesce(sum(t1.sales_QTY),0)+coalesce(sum(t1.sales_qty_promo),0) as sales_wd_cat
			from mn_long.pmix_sales t1
			left join mn_long.product_dictionary t2
				on t1.product_id=t2.product_id
			left join mn_long.PBO_DICTIONARY t3
				on t1.pbo_location_id=t3.pbo_location_id
			where t1.sales_dt>=	date'2018-01-04' 
				and t1.sales_dt<=date %tslit(&VF_HIST_END_DT)
			group by 1,2,3,4
		;
	quit;
	/*Calculate ranks*/
	data casuser.days_cat_ranked;
	  set casuser.days_cat;
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
	/*final join*/

	proc fedsql sessref=casauto noprint;
	  create table casuser.wp_abt{options replace=true} as
		  select t1.CHANNEL_CD
				,t1.PRODUCT_ID
				,t1.PBO_LOCATION_ID
				,t1.WEEK_DT
				,t1.WEEKDAY
				,t1.PROP
				,t1.MON_START
				,t1.MON_END
				,coalesce(t2.H_1,0) as H_1
				,coalesce(t2.H_2,0) as H_2
				,coalesce(t2.H_3,0) as H_3
				,coalesce(t2.H_4,0) as H_4
				,coalesce(t2.H_5,0) as H_5
				,coalesce(t2.H_6,0) as H_6
				,coalesce(t2.H_7,0) as H_7
				,coalesce(t3.PR_1,0) as PR_1
				,coalesce(t3.PR_2,0) as PR_2
				,coalesce(t3.PR_3,0) as PR_3
				,coalesce(t3.PR_4,0) as PR_4
				,coalesce(t3.PR_5,0) as PR_5
				,coalesce(t3.PR_6,0) as PR_6
				,coalesce(t3.PR_7,0) as PR_7
				,t7.ranks_s1
				,t7.ranks_s2
				,t7.ranks_s3
				,t7.ranks_s4
		  from casuser.daily_orders t1 
			left join casuser.holiday_transp t2
				on t1.week_dt = t2.week_dt
				and t1.pbo_location_id=t2.pbo_location_id
			left join casuser.promo_w_nodup t3
				on t1.week_dt = t3.week_dt 
				and t1.pbo_location_id=t3.pbo_location_id
				and t1.product_id=t3.product_id 
				and t1.channel_cd=t3.channel_cd
			left join mn_long.product_dictionary t4
				on t1.product_id=t4.product_id
			left join mn_long.PBO_DICTIONARY t5
				on t1.pbo_location_id=t5.pbo_location_id
			left join casuser.days_cat_ranked t7
				on t7.lvl3_id=t5.lvl3_id 
				and t7.mon_start=t1.mon_start
				and t7.prod_lvl2_id=t4.prod_lvl2_id
			where t1.prop>0
		;
	quit;

	proc nnet data=casuser.wp_abt;
	  architecture mlp;
	  input MON_START MON_END H_1 H_2 H_3 H_4 H_5 H_6 H_7
		  PR_1 PR_2 PR_3 PR_4 PR_5 PR_6 PR_7 
		  ranks_s1 ranks_s2 ranks_s3 ranks_s4 / level=nom;
	  target weekday / level=nom;
	  weight prop;
	  train outmodel=casuser.&lmvOutTabNameNnetWp.;
	  partition fraction(validate=0.3);
	  hidden 8 ;
	  hidden 8 ;
	  /*autotune useparameters =custom objective=mcll searchmethod=ga 
		tuningparameters=(nhidden(lb=1 ub=2 init=1)
						  nunits1(lb=1 ub=25 init=8)
						  nunits2(lb=1 ub=25 init=8)
						  regl1(lb=0 ub=1e-2 init=1e-3)
						  regl2(lb=0 ub=1e-2 init=1e-3))
	  MAXTIME=100000;*/
	run;

	proc casutil;
	  promote casdata="&lmvOutTabNameNnetWp." incaslib="casuser" outcaslib="&lmvOutLibrefNnetWp.";
	  promote casdata="&lmvOutTabNamePromoW." incaslib="casuser" outcaslib="&lmvOutLibrefPromoW.";
	run;
	/*Store model as a permanent table*/
	proc cas;
			save / table={name="&lmvOutTabNameNnetWp." caslib="&lmvOutLibrefNnetWp."},
				  name="&lmvOutTabNameNnetWp..sashdat",
					caslib="&lmvOutLibrefNnetWp.",
				   replace=true;
			attribute / task = 'CONVERT',
						caslib="&lmvOutLibrefNnetWp.",
						name="&lmvOutTabNameNnetWp.",
						attrtable="&lmvOutTabNameNnetWp..attr";
			save /  table={name="&lmvOutTabNameNnetWp..attr" caslib="&lmvOutLibrefNnetWp."},
					caslib="&lmvOutLibrefNnetWp.",
					name="&lmvOutTabNameNnetWp._attr.sashdat",
					replace=true;
		  run;
	quit;

%mend vf_train_week_profile_gc_sep;

