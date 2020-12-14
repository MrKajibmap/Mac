%macro rtp_7_out_integration_old(mpVfPmixProjName=pmix_sales_v1,
							mpVfPboProjName=pbo_sales_v1,
							mpMLPmixTabName=dm_abt.pmix_reconciled_full,
							mpInEventsMkup=dm_abt.events_mkup,
							mpInWpGc=dm_abt.wp_gc,
							mpOutPmixLt=dm_abt.plan_pmix_month,
							mpOutGcLt=dm_abt.plan_gc_month, 
							mpOutUptLt=dm_abt.plan_upt_month, 
							mpOutPmixSt=dm_abt.plan_pmix_day,
							mpOutGcSt=dm_abt.plan_gc_day, 
							mpOutUptSt=dm_abt.plan_upt_day, 
							mpOutOutforgc=dm_abt.TS_OUTFORGC,
							mpOutOutfor=dm_abt.TS_OUTFOR, 
							mpOutNnetWp=public.nnet_wp1,
							mpPrmt=N) ;

	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;
	
	%local	lmvOutLibrefPmixSt 
			lmvOutTabNamePmixSt 
			lmvOutLibrefGcSt 
			lmvOutTabNameGcSt 
			lmvOutLibrefUptSt 
			lmvOutTabNameUptSt 
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
	%member_names (mpTable=&mpOutGcSt, mpLibrefNameKey=lmvOutLibrefGcSt, mpMemberNameKey=lmvOutTabNameGcSt); 
	%member_names (mpTable=&mpOutPmixSt, mpLibrefNameKey=lmvOutLibrefPmixSt, mpMemberNameKey=lmvOutTabNamePmixSt); 
	%member_names (mpTable=&mpOutUptSt, mpLibrefNameKey=lmvOutLibrefUptSt, mpMemberNameKey=lmvOutTabNameUptSt); 
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
			droptable casdata="&lmvOutTabNameGcSt." incaslib="&lmvOutLibrefGcSt." quiet;
			droptable casdata="&lmvOutTabNamePmixSt." incaslib="&lmvOutLibrefPmixSt." quiet;
			droptable casdata="&lmvOutTabNameUptSt." incaslib="&lmvOutLibrefUptSt." quiet;
			droptable casdata="&lmvOutTabNameGcLt." incaslib="&lmvOutLibrefGcLt." quiet;
			droptable casdata="&lmvOutTabNamePmixLt." incaslib="&lmvOutLibrefPmixLt." quiet;
			droptable casdata="&lmvOutTabNameUptLt." incaslib="&lmvOutLibrefUptLt." quiet;
			droptable casdata="&lmvOutTabNameOutfor." incaslib="&lmvOutLibrefOutfor." quiet;
			droptable casdata="&lmvOutTabNameOutforgc." incaslib="&lmvOutLibrefOutforgc." quiet;
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
				coalesce(t4.P_REC_REC_SUM_QTY,t1.ff) as ff
			from public.pmix_daily_ t1 left join 
			(select t2.PBO_LOCATION_ID, t2.PRODUCT_ID, t2.sales_dt, t3.channel_cd,
					t2.P_REC_REC_SUM_QTY 
					from &mpMLPmixTabName t2 
					left join DM_ABT.ENCODING_CHANNEL_CD t3
					on t2.channel_cd=t3.channel_cd_id
			where t2.sales_dt between &VF_FC_START_DT and &VF_FC_END_SHORT_DT ) t4
				on t1.PBO_LOCATION_ID=t4.PBO_LOCATION_ID 
				and t1.PRODUCT_ID=t4.PRODUCT_ID and
				t1.period_dt = t4.sales_dt
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
	%if &mpPrmt. = Y %then %do;
		proc casutil;
			droptable casdata="&lmvOutTabNameGcSt." incaslib="&lmvOutLibrefGcSt." quiet;
			droptable casdata="&lmvOutTabNameUptSt." incaslib="&lmvOutLibrefUptSt." quiet;
			droptable casdata="&lmvOutTabNamePmixSt." incaslib="&lmvOutLibrefPmixSt." quiet;
		quit;
	%end;
	/*Units*/
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt.{options replace=true} as
			select 
				cast(t1.product_id as integer) as PROD /*– ИД продукта*/,
				cast(t1.pbo_location_id as integer) as LOCATION /*– ИД ресторана*/,
				t1.period_dt as DATA /*– Дата прогноза или факта (день)*/,
				'RUR' as CURRENCY /*– Валюта, значение по умолчанию RUR*/,
				'CORP' as ORG /*– Организация, значение по умолчанию CORP*/,
				case when promo=0 then t1.FF else 0 end
				as BASE_FCST_QNT_DAY /*– базовый прогноз (заполняется, если в этом разрезе 
								товар-ПБО-день не было ни одной промо-акции, =0 иначе)*/,
				case when t1.promo=1 then t1.FF else 0 end
				as PROMO_FCST_QNT_DAY /*– прогноз промо (заполняется, если в этом разрезе 
								товар-ПБО-день была одна и более промо-акций, =0 иначе)*/,
				t1.FF as TOTAL_FCST_QNT_DAY /*– сумма прогноза базового и промо*/,
				t1.FF as OVERRIDED_FCST_QNT_DAY /*– сумма прогноза базового и промо (чем отличается от предыдущей строки?)*/,
				1 as OVERRIDE_TRIGGER_QNT_DAY /*– тригер оверрайда, по умолчанию значение 1*/,
				case when promo=0 then t1.ff*t2.gross_price_amt else 0 end
				as BASE_FCST_RUR_DAY /*– базовый прогноз в РУБ (для пересчета штук в рубли используется net-цена? 
							Или gross? заполняется, если в этом разрезе товар-ПБО-день нет ни одной промо-акции)*/,
				case when promo=1 then t1.ff*t2.gross_price_amt else 0 end
				as PROMO_FCST_RUR_DAY /*– промо прогноз в РУБ (заполняется, если в этом разрезе товар-ПБО-день есть одна и более промо-акций)*/,
				t1.ff*t2.gross_price_amt as TOTAL_FCST_RUR_DAY /*– суммарный прогноз в РУБ*/,
				t1.ff*t2.gross_price_amt as OVERRIDED_FCST_RUR_DAY /*– Прогноз с учетом оверрйда РУБ (считается в ETL путем умножения средней цены на прогноз с учетом оверрайдов).*/,
				t2.gross_price_amt as AVG_PRICE /*– средняя цена. Считается в ETL как отношение прогноз в руб/прогноз в шт в разрезе СКЮ/ПБО*/
		from public.pmix_daily t1 
		left join public.ts_price_fact t2 on
			t1.product_id=t2.product_id
			and t1.pbo_location_id=t2.pbo_location_id and
		   t1.period_dt=t2.sales_dt
		where t1.channel_cd='ALL' 
				and t1.period_dt between &VF_FC_START_DT and &VF_FC_END_SHORT_DT;
	quit;

	/* GC:*/
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefGcSt..&lmvOutTabNameGcSt.{options replace=true} as
			select 
				1 as PROD /*– ИД продукта на верхнем уровне (ALL Product, значение = 1)*/,
				cast(pbo_location_id as integer) as LOCATION /*– ИД ресторана*/,
				period_dt as DATA /*– Дата прогноза или факта (день)*/,
				'RUR' as CURRENCY /*– Валюта, значение по умолчанию RUR*/,
				'CORP' as ORG /*– Организация, значение по умолчанию CORP*/,
				FF as BASE_FCST_GC_DAY /*– базовый прогноз */,
				0 as PROMO_FCST_GC_DAY /*– прогноз промо*/,
				FF as TOTAL_FCST_GC_DAY /*– сумма прогноза базового и промо*/,
				FF as OVERRIDED_FCST_GC_DAY /*– сумма прогноза базового и промо с учетом оверрайдов*/,
				1 as OVERRIDE_TRIGGER_GC_D /*– тригер оверрайда, по умолчанию значение 1*/
		from public.daily_gc
		where channel_cd='ALL' 
			and period_dt between &VF_FC_START_DT and &VF_FC_END_SHORT_DT;
	quit;

	/*4. UPT по дням*/
	/*Прогноз UPT рассчитывается из прогноза в ШТ и GC по формуле
	Прогноз UPT(Товар, ПБО, день) = Прогноз в ШТ(Товар, ПБО, день)/Прогноз GC(ПБО, день)*1000
	*/
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptSt..&lmvOutTabNameUptSt.{options replace=true} as
			select 
				cast(t1.prod as integer) as PROD /*– ИД продукта на верхнем уровне (ALL Product, значение = 1) */,
				cast(t1.location as integer) as LOCATION /*– ИД ресторана*/,
				t1.data as DATA /*– Дата прогноза или факта (день)*/,
				'RUR' as CURRENCY /*– Валюта, значение по умолчанию RUR*/,
				'CORP' as ORG /*– Организация, значение по умолчанию CORP*/,
			case when t2.BASE_FCST_GC_DAY is not null and abs(t2.BASE_FCST_GC_DAY)> 1e-5 
			   then t1.BASE_FCST_QNT_DAY/t2.BASE_FCST_GC_DAY*1000 
			   else 0
			   end
			   as BASE_FCST_UPT_DAY /*– базовый прогноз, = Прогноз в ШТ(Товар, ПБО, день)/Прогноз GC(ПБО, день)*1000,
							если в разрезе Товар-ПБО-день нет ни одной промо-акции, =0 иначе.*/,
			case when t2.BASE_FCST_GC_DAY is not null and abs(t2.BASE_FCST_GC_DAY)> 1e-5
			   then t1.PROMO_FCST_QNT_DAY/t2.BASE_FCST_GC_DAY*1000 
			   else 0
			   end
			   as PROMO_FCST_UPT_DAY /*– прогноз промо, = Прогноз в ШТ(Товар, ПБО, день)/Прогноз GC(ПБО, день)*1000, 
							если в разрезе Товар-ПБО-день есть одна или более промо-акций, =0 иначе.*/,
			   1 as OVERRIDE_TRIGGER_UPT_D /*– тригер оверрайда, по умолчанию значение 1*/
		from &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt. t1 
		left join &lmvOutLibrefGcSt..&lmvOutTabNameGcSt. t2
				on t1.location=t2.location and t1.data=t2.data;
	quit;
	%if &mpPrmt. = Y %then %do;
		proc casutil;
			promote casdata="&lmvOutTabNamePmixSt." incaslib="&lmvOutLibrefPmixSt." outcaslib="&lmvOutLibrefPmixSt.";
			promote casdata="&lmvOutTabNameGcSt." incaslib="&lmvOutLibrefGcSt." outcaslib="&lmvOutLibrefGcSt.";
			promote casdata="&lmvOutTabNameUptSt." incaslib="&lmvOutLibrefUptSt." outcaslib="&lmvOutLibrefUptSt.";
		quit;
	%end;
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
			cast(intnx('month',t1.period_dt,0,'b') as date) as DATA /*– Месяц прогноза или факта в формате (дата 1-го числа месяца прогноза или факта).*/,
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
			   t1.period_dt=t2.sales_dt
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
	
	/* ПРОМОУТ ТОТАЛ */
	
	%if &mpPrmt. = Y %then %do;
		proc casutil;
			promote casdata="&lmvOutTabNamePmixLt." incaslib="&lmvOutLibrefPmixLt." outcaslib="&lmvOutLibrefPmixLt.";
			promote casdata="&lmvOutTabNameGcLt." incaslib="&lmvOutLibrefGcLt." outcaslib="&lmvOutLibrefGcLt.";
			promote casdata="&lmvOutTabNameUptLt." incaslib="&lmvOutLibrefUptLt." outcaslib="&lmvOutLibrefUptLt.";
		quit;
	%end;
	
	
	/* Создание разметки по комп-некомп ресторанам */
	%load_komp_matrix;
	/* non-komp*/
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptLt..&lmvOutTabNameUptLt._nonkomp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefUptLt..&lmvOutTabNameUptLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.non_komp = 1
		;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._nonkomp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.non_komp = 1
		;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefGcLt..&lmvOutTabNameGcLt._nonkomp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefGcLt..&lmvOutTabNameGcLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.non_komp = 1
		;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptSt..&lmvOutTabNameUptSt._nonkomp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefUptSt..&lmvOutTabNameUptSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.non_komp = 1
		;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._nonkomp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.non_komp = 1
		;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefGcSt..&lmvOutTabNameGcSt._nonkomp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefGcSt..&lmvOutTabNameGcSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.non_komp = 1
		;
	quit;	
		
		
	/* komp*/
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptLt..&lmvOutTabNameUptLt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefUptLt..&lmvOutTabNameUptLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.komp = 1
		;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.komp = 1
		;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefGcLt..&lmvOutTabNameGcLt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefGcLt..&lmvOutTabNameGcLt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.komp = 1
		;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptSt..&lmvOutTabNameUptSt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefUptSt..&lmvOutTabNameUptSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.komp = 1
		;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.komp = 1
		;
	quit;
	
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefGcSt..&lmvOutTabNameGcSt._komp{options replace=true} as
		select t1.* 
		from &lmvOutLibrefGcSt..&lmvOutTabNameGcSt. t1
		inner join casuser.komp_matrix t2
			on t1.LOCATION = t2.pbo_location_id
			and t2.komp = 1
		;
	quit;	
	/*6. Сброс в файлы csv*/
	/* Приведение к формату даты KOMP|NONKOMP (месяцы)*/
	
	data &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._nonkomp (replace=yes);
		set &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._nonkomp;
		format DATA yymon7.;
	run;
	
	data &lmvOutLibrefUptLt..&lmvOutTabNameUptLt._nonkomp(replace=yes);
		set &lmvOutLibrefUptLt..&lmvOutTabNameUptLt._nonkomp;
		format DATA yymon7.;
	run;
	
	data &lmvOutLibrefGcLt..&lmvOutTabNameGcLt._nonkomp (replace=yes);
		set &lmvOutLibrefGcLt..&lmvOutTabNameGcLt._nonkomp;
		format DATA yymon7.;
	run;
	
	data &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._komp (replace=yes);
		set &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._komp;
		format DATA yymon7.;
	run;
	
	data &lmvOutLibrefUptLt..&lmvOutTabNameUptLt._komp(replace=yes);
		set &lmvOutLibrefUptLt..&lmvOutTabNameUptLt._komp;
		format DATA yymon7.;
	run;
	
	data &lmvOutLibrefGcLt..&lmvOutTabNameGcLt._komp (replace=yes);
		set &lmvOutLibrefGcLt..&lmvOutTabNameGcLt._komp;
		format DATA yymon7.;
	run;
	
	%let mvOutpath=/data/dm_rep/;
	/* Сохраняем в CSV */
	proc casutil;
			promote casdata="&lmvOutTabNamePmixSt._komp" incaslib="&lmvOutLibrefPmixSt." outcaslib="&lmvOutLibrefPmixSt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._komp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixSt._komp" incaslib="&lmvOutLibrefPmixSt." quiet;
	quit;
	
	proc casutil;
			promote casdata="&lmvOutTabNameGcSt._komp" incaslib="&lmvOutLibrefGcSt." outcaslib="&lmvOutLibrefGcSt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefGcSt..&lmvOutTabNameGcSt._komp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNameGcSt._komp" incaslib="&lmvOutLibrefGcSt." quiet;
	quit;
	
	
	proc casutil;
		promote casdata="&lmvOutTabNameUptSt._komp" incaslib="&lmvOutLibrefUptSt." outcaslib="&lmvOutLibrefUptSt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefUptSt..&lmvOutTabNameUptSt._komp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNameUptSt._komp" incaslib="&lmvOutLibrefUptSt." quiet;
	quit;
	
	proc casutil;
		promote casdata="&lmvOutTabNamePmixLt._komp" incaslib="&lmvOutLibrefPmixLt." outcaslib="&lmvOutLibrefPmixLt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._komp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixLt._komp" incaslib="&lmvOutLibrefPmixLt." quiet;
	quit;
	
	proc casutil;
			promote casdata="&lmvOutTabNameGcLt._komp" incaslib="&lmvOutLibrefGcLt." outcaslib="&lmvOutLibrefGcLt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefGcLt..&lmvOutTabNameGcLt._komp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNameGcLt._komp" incaslib="&lmvOutLibrefGcLt." quiet;
	quit;
	
	proc casutil;
			promote casdata="&lmvOutTabNameUptLt._komp" incaslib="&lmvOutLibrefUptLt." outcaslib="&lmvOutLibrefUptLt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefUptLt..&lmvOutTabNameUptLt._komp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNameUptLt._komp" incaslib="&lmvOutLibrefUptLt." quiet;
	quit;
	
	proc casutil;
			promote casdata="&lmvOutTabNamePmixSt._nonkomp" incaslib="&lmvOutLibrefPmixSt." outcaslib="&lmvOutLibrefPmixSt.";
	quit;
		%dp_export_csv(mpInput=&lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt._nonkomp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixSt._nonkomp" incaslib="&lmvOutLibrefPmixSt." quiet;
	quit;
	
	proc casutil;
			promote casdata="&lmvOutTabNameGcSt._nonkomp" incaslib="&lmvOutLibrefGcSt." outcaslib="&lmvOutLibrefGcSt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefGcSt..&lmvOutTabNameGcSt._nonkomp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNameGcSt._nonkomp" incaslib="&lmvOutLibrefGcSt." quiet;
	quit;
	
	proc casutil;
			promote casdata="&lmvOutTabNameUptSt._nonkomp" incaslib="&lmvOutLibrefUptSt." outcaslib="&lmvOutLibrefUptSt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefUptSt..&lmvOutTabNameUptSt._nonkomp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNameUptSt._nonkomp" incaslib="&lmvOutLibrefUptSt." quiet;
	quit;
	
	proc casutil;
			promote casdata="&lmvOutTabNamePmixLt._nonkomp" incaslib="&lmvOutLibrefPmixLt." outcaslib="&lmvOutLibrefPmixLt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt._nonkomp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNamePmixLt._nonkomp" incaslib="&lmvOutLibrefPmixLt." quiet;
	quit;
	
	proc casutil;
			promote casdata="&lmvOutTabNameGcLt._nonkomp" incaslib="&lmvOutLibrefGcLt." outcaslib="&lmvOutLibrefGcLt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefGcLt..&lmvOutTabNameGcLt._nonkomp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNameGcLt._nonkomp" incaslib="&lmvOutLibrefGcLt." quiet;
	quit;
	
	proc casutil;
		promote casdata="&lmvOutTabNameUptLt._nonkomp" incaslib="&lmvOutLibrefUptLt." outcaslib="&lmvOutLibrefUptLt.";
	quit;
	%dp_export_csv(mpInput=&lmvOutLibrefUptLt..&lmvOutTabNameUptLt._nonkomp
					, mpTHREAD_CNT=30
					, mpPath=&mvOutpath.);
	proc casutil;
		droptable casdata="&lmvOutTabNameUptLt._nonkomp" incaslib="&lmvOutLibrefUptLt." quiet;
	quit;

%mend rtp_7_out_integration_old;