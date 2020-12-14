/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Применение недельного профиля - переразбивка прогноза pmix до разреза месяц-флаг промо,
*		прогноза gc - до разреза месяц
*
*  ПАРАМЕТРЫ:
*	  mpVfPmixProjName  - наименование VF-проекта PMIX
*	  mpVfPboProjName  - наименование VF-проекта PBO
*     mpOutPmix			- наименование целевой помесячно агрегированной таблицы PMIX
*	  mpOutGc			- наименование целевой помесячно агрегированной таблицы GC
*     mpOutOutforgc		- наименование целевой помесячно агрегированной таблицы OUTFORGC
*     mpOutOutfor		- наименование целевой помесячно агрегированной таблицы OUTFOR
*	  mpOutNnetWp		- наименование целевой таблицы inmodel для proc nnet
*	  mpInWpGc			- наименование входной таблицы wp_gc
*	  mpPrmt			- флаг промоута таблицы в целевую библиотеку-Принимает 2 значения - Y/N
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
*     %vf_month_aggregation;
*	  %vf_month_aggregation(mpVfPmixProjName=pmix_sales_v1,
*							mpVfPboProjName=pbo_sales_v2,
*							mpOutPmix=sas_lib.plan_pmix_month,
*							mpOutGc=sas_lib.plan_gc_month, 
*							mpOutOutforgc=sas_lib.TS_OUTFORGC,
*							mpOutOutfor=sas_lib.TS_OUTFOR,
*							mpOutNnetWp=dm_abt.nnet_wp1,
*							mpInWpGc=dm_abt.wp_gc,
*							mpPrmt=Y)
*
****************************************************************************
*  02-07-2020  Борзунов     Начальное кодирование
*  21-07-2020  Борзунов		Добавлено сохранение таблиц plan_pmix_month,plan_gc_month
*							 на диск в формате .sashdat
*  11-08-2020  Борзунов		Добавлено получение ID VF-проекта по его имени + 2 параметра mpVfPmixProjName, mpVfPboProjName
****************************************************************************/

%macro vf_month_aggregation(mpVfPmixProjName=pmix_sales_v1,
							mpVfPboProjName=pbo_sales_v2,
							mpInEventsMkup=dm_abt.events_mkup,
							mpOutPmix=dm_abt.plan_pmix_month,
							mpOutGc=dm_abt.plan_gc_month, 
							mpOutOutforgc=dm_abt.TS_OUTFORGC,
							mpOutOutfor=dm_abt.TS_OUTFOR, 
							mpOutNnetWp=dm_abt.nnet_wp1,
							mpInWpGc=dm_abt.wp_gc,
							mpPrmt=Y) ;

	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;
	
	%local	lmvOutLibrefPmix 
			lmvOutTabNamePmix 
			lmvOutLibrefGc 
			lmvOutTabNameGc 
			lmvOutLibrefOutforgc 
			lmvOutTabNameOutforgc 
			lmvOutLibrefOutfor 
			lmvOutTabNameOutfor 
			lmvOutLibrefNnetWp
			lmvOutTabNameNnetWp
			lmvVfPmixName
			lmvVfPmixId
			lmvVfPboName
			lmvVfPboId
			;
	
	%member_names (mpTable=&mpOutPmix, mpLibrefNameKey=lmvOutLibrefPmix, mpMemberNameKey=lmvOutTabNamePmix); 
	%member_names (mpTable=&mpOutOutforgc, mpLibrefNameKey=lmvOutLibrefOutforgc, mpMemberNameKey=lmvOutTabNameOutforgc); 
	%member_names (mpTable=&mpOutGc, mpLibrefNameKey=lmvOutLibrefGc, mpMemberNameKey=lmvOutTabNameGc); 
	%member_names (mpTable=&mpOutOutfor, mpLibrefNameKey=lmvOutLibrefOutfor, mpMemberNameKey=lmvOutTabNameOutfor);
	%member_names (mpTable=&mpOutNnetWp, mpLibrefNameKey=lmvOutLibrefNnetWp, mpMemberNameKey=lmvOutTabNameNnetWp);


	/* Получение списка VF-проектов */
	%vf_get_project_list(mpOut=work.vf_project_list);
	/* Извлечение ID для VF-проекта PMIX по его имени */
	%let lmvVfPmixName = &mpVfPmixProjName.;
	%let lmvVfPmixId = %vf_get_project_id_by_name(mpName=&lmvVfPmixName., mpProjList=work.vf_project_list);
	
	/* Извлечение ID для VF-проекта PBO по его имени */
	%let lmvVfPboName = &mpVfPboProjName.;
	%let lmvVfPboId = %vf_get_project_id_by_name(mpName=&lmvVfPboName., mpProjList=work.vf_project_list);
	
	/* 0. Удаление целевых таблиц */
	%if &mpPrmt. = Y %then %do;
		proc casutil;
			droptable casdata="&lmvOutTabNameGc." incaslib="&lmvOutLibrefGc." quiet;
			droptable casdata="&lmvOutTabNameOutfor." incaslib="&lmvOutLibrefOutfor." quiet;
			droptable casdata="&lmvOutTabNamePmix." incaslib="&lmvOutLibrefPmix." quiet;
			droptable casdata="&lmvOutTabNameOutforgc." incaslib="&lmvOutLibrefPmix." quiet;
		run;
	%end;
	/*Вытащить данные из проекта*/
	proc fedsql sessref=casauto noprint;
		create table casuser.&lmvOutTabNameOutfor.{options replace=true} as
			select t1.*
					,month(cast(t1.SALES_DT as date)) as MON_START
					,month(cast(intnx('day', cast(t1.SALES_DT as date),6) as date)) as MON_END
			from "Analytics_Project_&lmvVfPmixId".horizon t1
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
	proc fedsql sessref=casauto;
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
			inner join casuser.PRODUCT_DICTIONARY t2
				on t1.product_id=t2.PROD_LVL1_ID
		;
		create table casuser.promo_prod_exp2{options replace=true} as
			select t1.PROMO_ID
					,t2.Product_ID
			from casuser.PROMO_PROD t1 inner join casuser.PRODUCT_DICTIONARY t2
			on t1.product_id=t2.PROD_LVL2_ID
		;
		create table casuser.promo_prod_exp3{options replace=true} as
			select t1.PROMO_ID
					,t2.Product_ID
			from casuser.PROMO_PROD t1
			inner join casuser.PRODUCT_DICTIONARY t2
				on t1.product_id=t2.PROD_LVL3_ID
		;
		create table casuser.promo_prod_exp4{options replace=true} as
			select t1.PROMO_ID
					,t2.Product_ID
			from casuser.PROMO_PROD t1
			inner join casuser.PRODUCT_DICTIONARY t2
			on t1.product_id=t2.PROD_LVL4_ID
		;
		create table casuser.promo_prod_exp5{options replace=true} as
			select t1.PROMO_ID
					,t2.Product_ID
			from casuser.PROMO_PROD t1
			inner join casuser.PRODUCT_DICTIONARY t2
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

	/*транспонирование флагов промо*/
	data casuser.promo_w;
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
		
	/* тут бы ещё избавиться от дубликатов */
	proc fedsql sessref=casauto;
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
			from casuser.promo_w
			group by 1,2,3,4;
	quit;

	/*ТОП 1,2,3 день по числу заказов
	В разрезе (Prod_Hier_2 - PBO_location_id - месяц начала недели)
	*/

	proc fedsql sessref=casauto;
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
			from casuser.pmix_sales t1
			left join casuser.PRODUCT_DICTIONARY t2
				on t1.product_id=t2.product_id
			left join casuser.pbo_dictionary t3
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

	proc fedsql sessref=casauto noprint;
		create table casuser.wp_abt{options replace=true} as
			select t1.CHANNEL_CD
					,t1.PRODUCT_ID
					,t1.PBO_LOCATION_ID
					,t1.sales_dt as WEEK_DT
					,t1.MON_START
					,t1.MON_END
					,t1.FF
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
			from casuser.&lmvOutTabNameOutfor. t1 
			left join casuser.holiday_transp t2
				on t1.sales_dt = t2.week_dt 
				and t1.pbo_location_id=t2.pbo_location_id
			left join casuser.promo_w_nodup t3
				on t1.sales_dt = t3.week_dt 
				and t1.pbo_location_id=t3.pbo_location_id
				and t1.product_id=t3.product_id 
				and t1.channel_cd=t3.channel_cd
			left join casuser.PRODUCT_DICTIONARY t4
				on t1.product_id=t4.product_id
			left join casuser.pbo_dictionary t5
				on t1.pbo_location_id=t5.pbo_location_id
			left join casuser.days_cat_ranked t7
				on t7.lvl3_id=t5.lvl3_id 
				and t7.mon_start=t1.mon_start 
				and t7.prod_lvl2_id=t4.prod_lvl2_id;
	quit;


			
	/*TODO: если таблицы nnet_wp нет в CAS, поднять её сохранённую версию (в Public)*/
	proc cas;
		table.tableExists result = rc / caslib="&lmvOutLibrefNnetWp." name="&lmvOutTabNameNnetWp.";
		if rc=0  then do;
			loadtable / caslib="&lmvOutLibrefNnetWp.",
						path="&lmvOutTabNameNnetWp._attr.sashdat",
						casout={caslib="&lmvOutLibrefNnetWp." name='attr2', replace=true};
			loadtable / caslib=/*"&lmvOutLibrefNnetWp."*/"public",
						path="&lmvOutTabNameNnetWp..sashdat",
						casout={caslib="&lmvOutLibrefNnetWp." name="&lmvOutTabNameNnetWp.", replace=true};
			attribute / task='ADD',
						   caslib="&lmvOutLibrefNnetWp.",
						name="&lmvOutTabNameNnetWp.",
						attrtable='attr2';
						/* table.promote / name="&lmvOutTabNameNnetWp." caslib=="&lmvOutLibrefNnetWp." target="&lmvOutTabNameNnetWp." targetlib=="&lmvOutLibrefNnetWp."; */
			table.promote / name="&lmvOutTabNameNnetWp." caslib="&lmvOutLibrefNnetWp." target="&lmvOutTabNameNnetWp." targetlib="&lmvOutLibrefNnetWp.";
		end;
		else print("Table &lmvOutLibrefNnetWp..&lmvOutTabNameNnetWp. already loaded");
	quit;
	
	proc nnet data=casuser.wp_abt inmodel=&lmvOutLibrefNnetWp..&lmvOutTabNameNnetWp.;
		SCORE OUT=casuser.nnet_wp_scored  copyvars=(FF CHANNEL_CD PRODUCT_ID PBO_LOCATION_ID WEEK_DT PR_1 PR_2 PR_3 PR_4 PR_5 PR_6 PR_7);
	run;

	/* агрегация по недельному профилю и переразбивка по флагу промо */
	data casuser.month_2;
		set casuser.nnet_wp_scored;
		array p_weekday{7};
		array PR_{7};
		keep CHANNEL_CD PBO_LOCATION_ID PRODUCT_ID mon_dt FF promo
		/*p_prev_: p_next_:*/ ;
		format mon_dt date9.;
		cur_dt=week_dt;
/* test coalesce */
		fc=coalesce(ff,0);
		p_prev_0=0; /*пропорция на более ранний месяц, promo=0*/
		p_prev_1=0; /*пропорция на более ранний месяц, promo=1*/
		p_next_0=0; /*пропорция на более поздний месяц, promo=0*/
		p_next_1=0; /*пропорция на более поздний месяц, promo=0*/
		miss_prof=nmiss(of p_weekday:);
		if miss_prof>0 then
			do i=1 to 7;
				p_weekday{i}=1./7.;
			end;
		do while (cur_dt<=week_dt+6);
			if month(cur_dt)=month(week_dt) then do;
				if pr_{cur_dt-week_dt+1}=0 then
					p_prev_0+p_weekday{cur_dt-week_dt+1};
				else
					p_prev_1+p_weekday{cur_dt-week_dt+1};
			end;
			else do;
				if pr_{cur_dt-week_dt+1}=0 then
					p_next_0+p_weekday{cur_dt-week_dt+1};
				else
					p_next_1+p_weekday{cur_dt-week_dt+1};
			end;
			cur_dt+1;
		end;
		ff=fc*p_prev_0;mon_dt=intnx('month',week_dt,0,'b');promo=0;output;
		ff=fc*p_prev_1;mon_dt=intnx('month',week_dt,0,'b');promo=1;output;
		if month(week_dt) ne month(week_dt+6) then do; 
			ff=fc*p_next_0;mon_dt=intnx('month',week_dt,1,'b');promo=0;output;
			ff=fc*p_next_1;mon_dt=intnx('month',week_dt,1,'b');promo=1;output;
		end;
	run;

	/*в прогноз добавляем имеющийся факт за 1 неполный месяц прогноза*/
	/*разметка промо только для нужных дат*/
	data casuser.promo_w_fact;
		set casuser.promo_d;
		format period_dt date9.;
		retain pr_ 1;
		keep pr_ period_dt pbo_location_id channel_cd product_id;
		do period_dt=start_DT to end_DT ;
			if period_dt<&VF_FC_START_DT_sas and period_dt>=intnx('month',&VF_FC_START_DT_sas,0,'b')
			then output;
		end;
	run;

	/*удаляем дубликаты, если есть*/
	proc fedsql sessref=casauto noprint;
		create table casuser.promo_w_fct_nodup{options replace=true}
			as select distinct pr_
								,period_dt
								,pbo_location_id
								,channel_cd
								,product_id
			from casuser.promo_w_fact
			where channel_cd='ALL'
		;
	quit;

	proc fedsql sessref=casauto noprint;
		create table casuser.pmix_from_fact{options replace=true} as
			select t1.CHANNEL_CD
					,t1.PBO_LOCATION_ID
					,t1.PRODUCT_ID
					,cast(intnx('month',&VF_FC_START_DT,0,'b') as date) as mon_dt
					,coalesce(sum(case when t2.pr_!=1 then sales_QTY end),0)+coalesce(sum(case when t2.pr_!=1 then sales_qty_promo end),0) as Promo_0
					,coalesce(sum(case when t2.pr_=1 then sales_QTY end),0)+coalesce(sum(case when t2.pr_=1 then sales_qty_promo end),0) as Promo_1
			from casuser.pmix_sales t1 
				left join casuser.promo_w_fct_nodup t2
					on t1.sales_dt=t2.period_dt 
					and t1.pbo_location_id=t2.pbo_location_id 
					and t1.channel_cd=t2.channel_cd 
					and t1.product_id=t2.product_id
			where sales_dt<&VF_FC_START_DT 
					and sales_dt>=intnx('month',&VF_FC_START_DT,0,'b')
			group by 1,2,3,4
		;
	quit;

	/*перевод промо в ключ*/
	proc fedsql sessref=casauto noprint;
		create table casuser.month_1_0{options replace=true} as
			select t1.CHANNEL_CD
					,t1.PBO_LOCATION_ID
					,t1.PRODUCT_ID
					,t1.mon_dt
					,Promo_0 as ff
					,cast(0 as double) as promo
			from  casuser.pmix_from_fact t1
		;
		create table casuser.month_1_1{options replace=true} as
			select t1.CHANNEL_CD
					,t1.PBO_LOCATION_ID
					,t1.PRODUCT_ID
					,t1.mon_dt
					,Promo_1 as ff
					,cast(1 as double) as promo
			from casuser.pmix_from_fact t1
		;
	quit;

	/*финальная агрегация прогноза*/
	data casuser.month_1_0(append=yes);
		set casuser.month_1_1;
	run;
	data casuser.month_1_0(append=yes);
		set casuser.month_2;
	run;

	proc fedsql sessref=casauto;
		create table casuser.&lmvOutTabNamePmix.{options replace=true} as
			select CHANNEL_CD
					,PBO_LOCATION_ID
					,PRODUCT_ID
					,promo
					,mon_dt
					,sum(FF) as FF
			from casuser.month_1_0
			group by 1,2,3,4,5
		;
	quit;


	/*Вытащить данные из проекта*/
	proc fedsql sessref=casauto noprint;
		create table casuser.&lmvOutTabNameOutforgc.{options replace=true} as
			select *
			from "Analytics_Project_&lmvVfPboId".horizon;
	quit;

	/*Для каких товаров-пбо-недель нужно посчитать недельные профили?*/
	proc fedsql sessref=casauto noprint;
		create table casuser.weeks_to_scoregc{options replace=true} as
			select CHANNEL_CD
					,PBO_LOCATION_ID
					,SALES_DT
					,FF
			from casuser.&lmvOutTabNameOutforgc.
			where month(cast(SALES_DT as date)) != 
			month(cast(intnx('day', cast(SALES_DT as date),6) as date)) 
		;
	quit;

	proc fedsql sessref=casauto noprint;
		create table casuser.weeks_with_wp{options replace=true} as
			select t1.CHANNEL_CD
					,t1.PBO_LOCATION_ID
					,t1.SALES_DT
					,t1.FF
					,coalesce(prday_1,1/7) as prday_1
					,coalesce(prday_2,1/7) as prday_2
					,coalesce(prday_3,1/7) as prday_3
					,coalesce(prday_4,1/7) as prday_4
					,coalesce(prday_5,1/7) as prday_5
					,coalesce(prday_6,1/7) as prday_6
					,coalesce(prday_7,1/7) as prday_7
			from casuser.weeks_to_scoregc t1
			left join &mpInWpGc. t2 
			on t1.pbo_location_id=t2.pbo_location_id 
			and t1.channel_cd=t2.channel_cd
		;
	quit;

	/*агрегация прогноза до месяцев*/
	proc fedsql sessref=casauto noprint;
		create table casuser.month_1gc{options replace=true} as
			select CHANNEL_CD
					,PBO_LOCATION_ID
					,cast(intnx('month',SALES_DT,0,'b') as date) as mon_dt
					,sum(FF) as FF
			from casuser.&lmvOutTabNameOutforgc.
			where month(cast(SALES_DT as date)) = 
					month(cast(intnx('day', cast(SALES_DT as date),6) as date)) 
			group by 1,2,3
		;
	quit;
	/*агрегация по недельному профилю*/
	data casuser.month_2gc;
		set casuser.weeks_with_wp;
		array prday_{7};
		keep CHANNEL_CD PBO_LOCATION_ID mon_dt FF;
		format mon_dt date9.;
		cur_dt=sales_dt;
		fc=coalesce(ff,0);
		p_prev=0; /*пропорция 1 месяца*/
		do while (month(cur_dt) = month(sales_dt));
			p_prev+prday_{cur_dt-sales_dt+1};
			cur_dt+1;
		end;
		ff=fc*p_prev;mon_dt=intnx('month',sales_dt,0,'b');output;
		ff=fc*(1-p_prev);mon_dt=intnx('month',sales_dt,1,'b');output;
	run;
	/*в прогноз добавляем имеющийся факт за 1 неполный месяц прогноза*/
	proc fedsql sessref=casauto noprint;
		create table casuser.gc_from_fact{options replace=true} as
			select CHANNEL_CD
					,PBO_LOCATION_ID
					,cast(intnx('month',&VF_FC_START_DT,0,'b') as date) as mon_dt
					,sum(receipt_qty) as FF
			from casuser.pbo_sales
			where sales_dt<&VF_FC_START_DT 
					and sales_dt>=intnx('month',&VF_FC_START_DT,0,'b')
					and channel_cd='ALL'
			group by 1,2,3
		;
	quit;
	/*финальная агрегация*/
	data casuser.month_1gc(append=yes);
		set casuser.month_2gc;
	run;
	data casuser.month_1gc(append=yes);
		set casuser.gc_from_fact;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.&lmvOutTabNameGc.{options replace=true} as
			select CHANNEL_CD
					,PBO_LOCATION_ID
					,mon_dt
					,sum(FF) as FF
			from casuser.month_1gc
			group by 1,2,3
		;
	quit;
	
	data casuser.&lmvOutTabNameGc.(replace=yes);
		set casuser.&lmvOutTabNameGc;
		format mon_dt yymon7.;
	run;
	
	data casuser.&lmvOutTabNamePmix.(replace=yes);
		set casuser.&lmvOutTabNamePmix.;
		format mon_dt yymon7.;
	run;

	%if &mpPrmt. = Y %then %do;
		proc casutil;
			promote casdata="&lmvOutTabNameGc." incaslib="casuser" outcaslib="&lmvOutLibrefGc.";
			/* %if %sysfunc(upcase(&lmvOutLibrefGc)) <> CASUSER %then %do; */
				save incaslib="&lmvOutLibrefGc." outcaslib="&lmvOutLibrefGc." casdata="&lmvOutTabNameGc." casout="&lmvOutTabNameGc..sashdat" replace;
			/* %end;		*/
			promote casdata="&lmvOutTabNameOutfor." incaslib="casuser" outcaslib="&lmvOutLibrefOutfor.";
			
			promote casdata="plan_pmix_month" incaslib="casuser" outcaslib="&lmvOutLibrefPmix." casout="&lmvOutTabNamePmix.";
			/* %if %sysfunc(upcase(&lmvOutLibrefPmix)) <> CASUSER %then %do; */
				save incaslib="&lmvOutLibrefPmix." outcaslib="&lmvOutLibrefPmix." casdata="&lmvOutTabNamePmix." casout="&lmvOutTabNamePmix..sashdat" replace;
			/* %end;		*/	
			promote casdata="TS_OUTFORGC" incaslib="casuser" outcaslib="&lmvOutLibrefOutforgc.";
		run;

	/*	%dp_export_csv(mpInput=&lmvOutTabNameGc..&lmvOutLibrefGc.
						, mpTHREAD_CNT=30
						, mpPath=/data/dm_rep/);
		%dp_export_csv(mpInput=&lmvOutLibrefPmix..&lmvOutTabNamePmix.
						, mpTHREAD_CNT=30
						, mpPath=/data/dm_rep/);
						*/
	%end;
	%else %do;
		data &lmvOutLibrefGc..&lmvOutTabNameGc.;
			set casuser.&lmvOutTabNameGc.;
		run;
		
		data &lmvOutLibrefOutfor..&lmvOutTabNameOutfor.;
			set casuser.&lmvOutTabNameOutfor.;
		run;
		
		data &lmvOutLibrefPmix..&lmvOutTabNamePmix.;
			set casuser.&lmvOutTabNamePmix.;
		run;
		
		data &lmvOutLibrefOutforgc..&lmvOutTabNameOutforgc.;
			set casuser.&lmvOutTabNameOutforgc.;
		run;
	%end;
%mend vf_month_aggregation;