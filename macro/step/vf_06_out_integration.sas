/* 
%vf_6_out_integration(mpVfPmixProjName=pmix_sales_v1,
							mpVfPboProjName=pbo_sales_v1,
							mpMLPmixTabName=DM_ABT.PLAN_PMIX_MONTH,
							mpInEventsMkup=dm_abt.events_mkup,
							mpInWpGc=dm_abt.wp_gc,
							mpOutPmixLt=casuser.plan_pmix_month,
							mpOutGcLt=casuser.plan_gc_month, 
							mpOutUptLt=casuser.plan_upt_month, 
							mpOutOutforgc=casuser.TS_OUTFORGC,
							mpOutOutfor=casuser.TS_OUTFOR, 
							mpOutNnetWp=public.nnet_wp1,
							mpPrmt=N) ;
							*/

%macro vf_6_out_integration(mpVfPmixProjName=pmix_sales_v1,
							mpVfPboProjName=pbo_sales_v1,
							mpMLPmixTabName=DM_ABT.PLAN_PMIX_MONTH,
							mpInEventsMkup=dm_abt.events_mkup,
							mpInWpGc=dm_abt.wp_gc,
							mpOutPmixLt=casuser.plan_pmix_month,
							mpOutGcLt=casuser.plan_gc_month, 
							mpOutUptLt=casuser.plan_upt_month, 
							mpOutOutforgc=casuser.TS_OUTFORGC,
							mpOutOutfor=casuser.TS_OUTFOR, 
							mpOutNnetWp=public.nnet_wp1,
							mpPrmt=N) ;

	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;
	
	%local	
			lmvOutLibrefPmixLt 
			lmvOutTabNamePmixLt 
			lmvOutLibrefGcLt 
			lmvOutTabNameGcLt
			lmvOutLibrefUptLt 
			lmvOutTabNameUptLt  
			lmvOutLibrefOutforgc 
			lmvOutTabNameOutforgc 
			lmvOutLibrefOutfor 
			lmvOutTabNameOutfor 
			lmvVfPmixName
			lmvVfPmixId
			lmvVfPboName
			lmvVfPboId
			lmvInEventsMkup
			;
			
	%let lmvInLib=ETL_IA;
	%let etl_current_dt = %sysfunc(today());
	%let ETL_CURRENT_DTTM = %sysfunc(datetime());
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	%member_names (mpTable=&mpOutOutfor, mpLibrefNameKey=lmvOutLibrefOutfor, mpMemberNameKey=lmvOutTabNameOutfor);
	%member_names (mpTable=&mpOutOutforgc, mpLibrefNameKey=lmvOutLibrefOutforgc, mpMemberNameKey=lmvOutTabNameOutforgc); 
	%member_names (mpTable=&mpOutGcLt, mpLibrefNameKey=lmvOutLibrefGcLt, mpMemberNameKey=lmvOutTabNameGcLt); 
	%member_names (mpTable=&mpOutPmixLt, mpLibrefNameKey=lmvOutLibrefPmixLt, mpMemberNameKey=lmvOutTabNamePmixLt); 
	%member_names (mpTable=&mpOutUptLt, mpLibrefNameKey=lmvOutLibrefUptLt, mpMemberNameKey=lmvOutTabNameUptLt); 
	
	/* Получение списка VF-проектов */
	%vf_get_project_list(mpOut=work.vf_project_list);
	/* Извлечение ID для VF-проекта PMIX по его имени */
	%let lmvVfPmixName = &mpVfPmixProjName.;
	%let lmvVfPmixId = %vf_get_project_id_by_name(mpName=&lmvVfPmixName., mpProjList=work.vf_project_list);
	
	/* Извлечение ID для VF-проекта PBO по его имени */
	%let lmvVfPboName = &mpVfPboProjName.;
	%let lmvVfPboId = %vf_get_project_id_by_name(mpName=&lmvVfPboName., mpProjList=work.vf_project_list);
	%let lmvInEventsMkup=&mpInEventsMkup;
	/* 0. Удаление целевых таблиц */
	%if &mpPrmt. = Y %then %do;
		proc casutil;
			droptable casdata="&lmvOutTabNameGcLt." incaslib="&lmvOutLibrefGcLt." quiet;
			droptable casdata="&lmvOutTabNamePmixLt." incaslib="&lmvOutLibrefPmixLt." quiet;
			droptable casdata="&lmvOutTabNameUptLt." incaslib="&lmvOutLibrefUptLt." quiet;
		run;
	%end;
	/*Вытащить данные из проекта*/
	proc fedsql sessref=casauto noprint;
		create table &lmvOutLibrefOutfor..&lmvOutTabNameOutfor.{options replace=true} as
			select t1.*
					,month(cast(t1.SALES_DT as date)) as MON_START
					,month(cast(intnx('day', cast(t1.SALES_DT as date),6) as date)) as MON_END
			from "Analytics_Project_&lmvVfPmixId".horizon t1
		;
	quit;
	proc fedsql sessref=casauto noprint;
		create table &lmvOutLibrefOutforGc..&lmvOutTabNameOutforGc.{options replace=true} as
			select t1.*
					,month(cast(t1.SALES_DT as date)) as MON_START
					,month(cast(intnx('day', cast(t1.SALES_DT as date),6) as date)) as MON_END
			from "Analytics_Project_&lmvVfPboId".horizon t1
		;
	quit;
	%if &mpPrmt. = Y %then %do;
		proc casutil;
			promote casdata="&lmvOutTabNameOutfor." incaslib="&lmvOutLibrefOutfor." outcaslib="&lmvOutLibrefOutfor.";
			promote casdata="&lmvOutTabNameOutforgc." incaslib="&lmvOutLibrefOutforgc." outcaslib="&lmvOutLibrefOutforgc.";
		run;
	%end;
    

	/*1. применяем к недельным прогнозам недельные профили*/
	%vf_apply_w_prof(&lmvOutLibrefOutfor..&lmvOutTabNameOutfor.,
					&lmvOutLibrefOutfor..&lmvOutTabNameOutforgc.,
					public.nnet_wp_scored1,
					public.daily_gc,
					&mpInEventsMkup.,
					&mpInWpGc.,
					&mpOutNnetWp.);

	data public.pmix_daily_ ;
	  set public.nnet_wp_scored1;
	  array p_weekday{7};
	  array PR_{7};
	  keep CHANNEL_CD PBO_LOCATION_ID PRODUCT_ID period_dt mon_dt FF promo;
	  format period_dt mon_dt date9.;
	  period_dt=week_dt;
	  fc=ff;
	  if fc = . then fc = 0;
	  miss_prof=nmiss(of p_weekday:);
	  if miss_prof>0 then
		do i=1 to 7;
		p_weekday{i}=1./7.;
		end;
	  do while (period_dt<=week_dt+6);
		mon_dt=intnx('month',period_dt,0,'b');
		promo=pr_{period_dt-week_dt+1};
		ff=fc*p_weekday{period_dt-week_dt+1};
		output;
		period_dt+1;
	  end;
	run;

   proc fedsql sessref=casauto;
	   create table public.pmix_daily{options replace=true} as
			select t1.channel_cd, t1.PBO_LOCATION_ID, t1.PRODUCT_ID,
				t1.promo,
				t1.period_dt,
				t1.mon_dt,
				coalesce(t4.FF,t1.ff) as ff
			from public.pmix_daily_ t1 left join 
			(select t2.PBO_LOCATION_ID, t2.PRODUCT_ID, t2.MON_DT, t3.channel_cd,
					t2.FF
					from &mpMLPmixTabName t2 
					left join DM_ABT.ENCODING_CHANNEL_CD t3
					on t2.channel_cd=t3.channel_cd_id
			where t2.MON_DT between &VF_FC_START_DT and &VF_FC_END_SHORT_DT ) t4
				on t1.PBO_LOCATION_ID=t4.PBO_LOCATION_ID 
				and t1.PRODUCT_ID=t4.PRODUCT_ID and
				t1.period_dt = t4.MON_DT
				and t1.channel_cd=t4.channel_cd;
   quit;

	/*2. Объединяем таблицы долгосрочного прогноза и краткосрочного - с приоритетом краткосрочного*/

	/*3. Таблицы по дням - GC, Pmix*/
	/*Вычисление цен на будущее*/
	/*приводим к ценам по дням*/
	 data CASUSER.price (replace=yes  drop=valid_from_dttm valid_to_dttm);
			set &lmvInLib..price(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
		/*	and end_dt<=&lmvScoreEndDate. and start_dt>=&lmvStartDate.*/));
	run;
	
	data public.price_unfolded;
	 set casuser.PRICE;
	 where price_type='F';
	 keep product_id pbo_location_id net_price_amt gross_price_amt sales_dt;
	 format sales_dt date9.;
	 do sales_dt=START_DT to min(END_DT,&vf_fc_agg_end_dt_sas);
	   output;
	 end;
	run;

	/*избавляемся от возможных дубликатов цен по ключу товар-пбо-дата*/
	data public.price_nodup;
	  set public.price_unfolded;
	  by product_id pbo_location_id sales_dt;
	  if first.sales_dt then output;
	run;

	proc casutil;
	  droptable casdata="price_unfolded" incaslib="public" quiet;
	run;
	quit;
	 
	/*протягиваем неизвестные цены последним известным значением до горизонта прогнозирования*/
	proc cas;
	timeData.timeSeries result =r /
		series={{name="gross_price_amt", setmiss="prev"},
				{name="net_price_amt", setmiss="prev"}}
		tEnd= "&vf_fc_agg_end_dt" /*fc_start_dt+hor*/
		table={caslib="public",name="price_nodup", groupby={"PBO_LOCATION_ID","PRODUCT_ID"} }
		timeId="SALES_DT"
		trimId="LEFT"
		interval="day"
		casOut={caslib="public",name="TS_price_fact",replace=True}
		;
	run;
	quit;
	proc casutil;
	  droptable casdata="price_nodup" incaslib="public" quiet;
	run;
	quit;
	
	/*5. Агрегация до месяцев GC, UPT, Pmix, до макс горизонта долгосрочного прогнза*/
	%if &mpPrmt. = Y %then %do;
		proc casutil;
			droptable casdata="&lmvOutTabNameGcLt." incaslib="&lmvOutLibrefGcLt." quiet;
			droptable casdata="&lmvOutTabNameUptLt." incaslib="&lmvOutLibrefUptLt." quiet;
			droptable casdata="&lmvOutTabNamePmixLt." incaslib="&lmvOutLibrefPmixLt." quiet;
		quit;
	%end;
	/*Units*/
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt.{options replace=true} as
			select
			cast(t1.product_id as integer) as PROD /*– ИД продукта*/,
			cast(t1.pbo_location_id as integer) as LOCATION /*– ИД ресторана*/,
			cast(intnx('month',t1.MON_DT,0,'b') as date) as DATA /*– Месяц прогноза или факта в формате (дата 1-го числа месяца прогноза или факта).*/,
			'RUR' as CURRENCY /*– Валюта, значение по умолчанию RUR*/,
			'CORP' as ORG /*– Организация, значение по умолчанию CORP*/,
			sum(case when promo=0 then t1.FF else 0 end) 
			   as BASE_FCST_QNT_MON /*– базовый прогноз*/,
			sum(case when promo=1 then t1.FF else 0 end)
			   as PROMO_FCST_QNT_MON /*– прогноз промо*/,
			sum(FF) as TOTAL_FCST_QNT_MON /*– сумма прогноза базового и промо*/,
			sum(FF) as OVERRIDED_FCST_QNT_MON /*– сумма прогноза базового и промо*/,
			1 as OVERRIDE_TRIGGER_QNT_MON /*– тригер оверрайда, по умолчанию значение 1*/,
			sum(case when promo=0 then t1.ff*t2.gross_price_amt else 0 end)
			   as BASE_FCST_RUR_MON /*– базовый прогноз в РУБ*/,
			sum(case when promo=1 then t1.ff*t2.gross_price_amt else 0 end)
			   as PROMO_FCST_RUR_MON /*– промо прогноз в РУБ*/,
			sum(t1.ff*t2.gross_price_amt)
			   as TOTAL_FCST_RUR_MON /*– суммарный прогноз в РУБ*/,
			sum(t1.ff*t2.gross_price_amt)
			   as OVERRIDED_FCST_RUR_MON /*– Прогноз с учетом оверрйда РУБ (считается в ETL путем умножения средней цены на прогноз с учетом оверрайдов).*/,
			case when abs(sum(t1.ff))>1e-5 then sum(t1.ff*t2.gross_price_amt)/sum(t1.ff) else 0 end
			   as AVG_PRICE /*– средняя цена. Считается в ETL как отношение прогноз в руб/прогноз в шт в разрезе СКЮ/ПБО*/
		from public.pmix_daily t1 left join public.ts_price_fact t2 on
			t1.product_id=t2.product_id and t1.pbo_location_id=t2.pbo_location_id and
			   t1.MON_DT=t2.sales_dt
		where t1.channel_cd='ALL' 
		group by 1,2,3,4,5;
	quit;
	/*GC*/
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefGcLt..&lmvOutTabNameGcLt.{options replace=true} as
			select
			1 as PROD /*– ИД продукта на верхнем уровне (ALL Product, значение = 1)*/,
			cast(t1.pbo_location_id as integer) as LOCATION /*– ИД ресторана*/,
			cast(intnx('month',t1.period_dt,0,'b') as date) as DATA /*– Дата прогноза или факта (месяц)*/,
			'RUR' as CURRENCY /*– Валюта, значение по умолчанию RUR*/,
			'CORP' as ORG /*– Организация, значение по умолчанию CORP*/,
			sum(t1.ff) as BASE_FORECAST_GC_M /*– базовый прогноз по чекам*/,
			sum(t1.ff) as OVERRIDED_FCST_GC /*– базовый прогноз по чекам (плюс логика сохранения оверрайдов)*/,
			1 as OVERRIDE_TRIGGER /*– тригер оверрайда, по умолчанию значение 1*/
		from public.daily_gc t1
		where channel_cd='ALL'
		group by 1,2,3,4,5;
	quit;
	/*UPT*/
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptLt..&lmvOutTabNameUptLt.{options replace=true} as
			select
			cast(t1.prod as integer) as PROD /*– ИД продукта*/, 
			cast(t1.location as integer) as LOCATION /*– ИД ресторана*/,
			t1.data as DATA /*– Дата прогноза или факта (месяц)*/,
			'RUR' as CURRENCY /*– Валюта, значение по умолчанию RUR*/,
			'CORP' as ORG /*– Организация, значение по умолчанию CORP*/,
			case when t2.BASE_FORECAST_GC_M is not null and abs(t2.BASE_FORECAST_GC_M)>1e-5 
			   then t1.BASE_FCST_QNT_MON/t2.BASE_FORECAST_GC_M*1000 
			   else 0
			   end
			   as BASE_FCST_UPT /*– базовый прогноз*/,
			case when t2.BASE_FORECAST_GC_M is not null and abs(t2.BASE_FORECAST_GC_M)>1e-5 
			   then t1.PROMO_FCST_RUR_MON/t2.BASE_FORECAST_GC_M*1000 
			   else 0
			   end
			   as PROMO_FCST_UPT /*– промо прогноз*/,
			case when t2.BASE_FORECAST_GC_M is not null and abs(t2.BASE_FORECAST_GC_M)>1e-5 
			   then t1.TOTAL_FCST_QNT_MON/t2.BASE_FORECAST_GC_M*1000 
			   else 0
			   end
			   as TOTAL_FCST_UPT /*– суммарный прогноз*/,
			case when t2.BASE_FORECAST_GC_M is not null and abs(t2.BASE_FORECAST_GC_M)>1e-5 
			   then t1.TOTAL_FCST_QNT_MON/t2.BASE_FORECAST_GC_M*1000 
			   else 0
			   end
			   as OVERRIDED_FCST_UP /*– суммарный прогноз (с учетом логики сохранения оверрайдов)*/,
			1 as OVERRIDE_TRIGGER_UPT /*– тригер для сохранения оверрайда, по умолчанию равен 1*/
		from &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt. t1 
		left join &lmvOutLibrefGcLt..&lmvOutTabNameGcLt. t2
			on t1.location=t2.location and t1.data=t2.data
		  ;
	quit;
	/* Приведение к формату даты */
	
	data &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt. (replace=yes);
		set &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt.;
		format DATA yymon7.;
	run;
	
	data &lmvOutLibrefUptLt..&lmvOutTabNameUptLt.(replace=yes);
		set &lmvOutLibrefUptLt..&lmvOutTabNameUptLt.;
		format DATA yymon7.;
	run;
	
	data &lmvOutLibrefGcLt..&lmvOutTabNameGcLt. (replace=yes);
		set &lmvOutLibrefGcLt..&lmvOutTabNameGcLt.;
		format DATA yymon7.;
	run;

	

%mend vf_6_out_integration;