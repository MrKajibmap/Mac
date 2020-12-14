%macro rtp_7_out_integration(mpVfPmixProjName=pmix_sales_v2,
							mpVfPboProjName=pbo_sales_v1,
							mpMLPmixTabName=dm_abt.pmix_reconciled_full,
							mpInEventsMkup=dm_abt.events_mkup,
							mpInWpGc=dm_abt.wp_gc,
							mpOutPmixLt=casuser.plan_pmix_month1,
							mpOutGcLt=casuser.plan_gc_month1, 
							mpOutUptLt=casuser.plan_upt_month1, 
							mpOutPmixSt=casuser.plan_pmix_day1,
							mpOutGcSt=casuser.plan_gc_day1, 
							mpOutUptSt=casuser.plan_upt_day1, 
							mpOutOutforgc=casuser.TS_OUTFORGC,
							mpOutOutfor=casuser.TS_OUTFOR, 
							mpOutNnetWp=casuser.nnet_wp1,
							mpPrmt=Y) ;

	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;
	%member_exists_list(mpMemberList=&mpMLPmixTabName.
								&mpInEventsMkup.
								&mpInWpGc.
								&mpOutNnetWp.
								);
								
								

	%let ETL_CURRENT_DT = %sysfunc(date());
	%let ETL_CURRENT_DTTM = %sysfunc(datetime());
	%let lmvInLib=ETL_IA;
	%let lmvReportDt=&ETL_CURRENT_DT.;
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	
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
			lmvInLib
			lmvReportDt
			lmvReportDttm
			;
			
	%let lmvInLib=ETL_IA;
	%let lmvReportDt=&ETL_CURRENT_DT.;
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
/*входные параметры для plm*/	
	/*1 ассорт матрица*/
	%let lmvAMTab=CASUSER.ASSORT_MATRIX;
	%if %sysfunc(exist(casuser.assort_matrix)) eq 0 %then %do;
		data CASUSER.assort_matrix (replace=yes  drop=valid_from_dttm valid_to_dttm);
			set &lmvInLib..assort_matrix(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
			));
		run;
	%end;
	/*2 информация о временных закрытиях*/
	%let lmvPBOCloseTab=casuser.PBO_CLOSE_PERIOD;
	%if %sysfunc(exist(casuser.pbo_close_period)) eq 0 %then %do;
		data CASUSER.pbo_close_period (replace=yes  drop=valid_from_dttm valid_to_dttm);
				set &lmvInLib..pbo_close_period(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
		run;
	%end;
	/*3 таблица lifecycle*/
	%let lmvLCTab=CASUSER.PRODUCT_CHAIN;
	%if %sysfunc(exist(casuser.PRODUCT_CHAIN)) eq 0 %then %do;

		data CASUSER.product_chain (replace=yes drop=valid_from_dttm valid_to_dttm);
			set &lmvInLib..product_chain(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
		run;

		proc fedsql sessref=casauto noprint;
			create table casuser.product_chain{options replace=true} as
			  select 
				LIFECYCLE_CD
				,PREDECESSOR_DIM2_ID
				,PREDECESSOR_PRODUCT_ID
				,SCALE_FACTOR_PCT
				,SUCCESSOR_DIM2_ID
				,SUCCESSOR_PRODUCT_ID
				,PREDECESSOR_END_DT
				,SUCCESSOR_START_DT
			  from casuser.product_chain
			;
		quit;
	%end;
	
	%member_names (mpTable=&mpOutOutfor, mpLibrefNameKey=lmvOutLibrefOutfor, mpMemberNameKey=lmvOutTabNameOutfor);
	%member_names (mpTable=&mpOutOutforgc, mpLibrefNameKey=lmvOutLibrefOutforgc, mpMemberNameKey=lmvOutTabNameOutforgc); 
	%member_names (mpTable=&mpOutGcSt, mpLibrefNameKey=lmvOutLibrefGcSt, mpMemberNameKey=lmvOutTabNameGcSt); 
	%member_names (mpTable=&mpOutPmixSt, mpLibrefNameKey=lmvOutLibrefPmixSt, mpMemberNameKey=lmvOutTabNamePmixSt); 
	%member_names (mpTable=&mpOutUptSt, mpLibrefNameKey=lmvOutLibrefUptSt, mpMemberNameKey=lmvOutTabNameUptSt); 
	%member_names (mpTable=&mpOutGcLt, mpLibrefNameKey=lmvOutLibrefGcLt, mpMemberNameKey=lmvOutTabNameGcLt); 
	%member_names (mpTable=&mpOutPmixLt, mpLibrefNameKey=lmvOutLibrefPmixLt, mpMemberNameKey=lmvOutTabNamePmixLt); 
	%member_names (mpTable=&mpOutUptLt, mpLibrefNameKey=lmvOutLibrefUptLt, mpMemberNameKey=lmvOutTabNameUptLt); 
/*Вытаскиваем прогнозы из VF*/	
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
/*0.9 Вытащить данные из проекта*/
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
					casuser.nnet_wp_scored1,
					casuser.daily_gc,
					&mpInEventsMkup.,
					&mpInWpGc.,
					&mpOutNnetWp.,
					casshort);

	data casuser.pmix_daily_ ;
	  set casuser.nnet_wp_scored1;
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

/*1.5 Прогноз новых товаров*/
  %vf_new_product(mpInCaslib=casshort);
/*Выходная таблица:casuser.npf_prediction*/
*proc fedsql sessref=casauto;
*select min(sales_dt) as min_dt, max(sales_dt) as max_dt
*from casuser.NPF_PREDICTION;
*quit;

/*2. Объединяем таблицы долгосрочного прогноза и краткосрочного - с приоритетом краткосрочного*/
	data casuser.promo_w2;
	  set casuser.promo_d; /*table from vf_apply_w_prof*/
	  format period_dt date9.;
	  do period_dt=start_DT to min(end_DT,&vf_fc_agg_end_dt_sas);
		output;
	  end;
	run;

	proc fedsql sessref=casauto;
	  create table casuser.promo_w1{options replace=true} as
	  select distinct t1.channel_cd,t1.pbo_location_id,
	  t1.product_id,t1.period_dt, 
	  cast(1 as double) as promo
	  from casuser.promo_w2 t1;
	quit;

	/*Сводим краткосрочный и долгосрочный (по дням) прогноз с приоритетом краткосрочн.*/
  proc fedsql sessref=casauto;
   create table casuser.pmix_daily{options replace=true} as
		select 
			coalesce(t4.channel_cd,t1.channel_cd) as channel_cd, 
			coalesce(t4.pbo_location_id,t1.PBO_LOCATION_ID) as PBO_LOCATION_ID, 
			coalesce(t4.product_id,t1.PRODUCT_ID) as product_id,
			coalesce(t4.sales_dt,t1.period_dt) as period_dt,
			coalesce(cast(intnx('month',t4.sales_dt,0) as date),t1.mon_dt) as mon_dt,
			coalesce(t4.P_REC_REC_SUM_QTY,t1.ff) as ff
		from casuser.pmix_daily_ t1 full outer join 
		(select t2.PBO_LOCATION_ID, t2.PRODUCT_ID, t2.sales_dt, t3.channel_cd,
				t2.P_REC_REC_SUM_QTY from
                &mpMLPmixTabName t2 left join DM_ABT.ENCODING_CHANNEL_CD t3
				on t2.channel_cd=t3.channel_cd_id
				where t2.sales_dt between &VF_FC_START_DT and &VF_FC_END_SHORT_DT ) t4
            on t1.PBO_LOCATION_ID=t4.PBO_LOCATION_ID and t1.PRODUCT_ID=t4.PRODUCT_ID and
            t1.period_dt = t4.sales_dt and t1.channel_cd=t4.channel_cd
   ;
   quit;

	/*2.1 TODO: вычисление матриц временных закрытий и допустимых дней продаж*/
	data casuser.days_pbo_date_close; /*дни когда пбо будет уже закрыт (навсегда)*/
	  set casuser.pbo_dictionary;
	  format period_dt date9.;
	  keep PBO_LOCATION_ID CHANNEL_CD period_dt;
	  CHANNEL_CD="ALL"; 
	  if A_CLOSE_DATE ne . and A_CLOSE_DATE<=&vf_fc_agg_end_dt_sas then 
	  do period_dt= max(A_CLOSE_DATE,&vf_fc_start_dt_sas) to &vf_fc_agg_end_dt_sas;
	    output;
	  end;
	run;
	
	data casuser.days_pbo_close; /*дни когда пбо будет временно закрыт*/
	  set &lmvPBOCloseTab.;
	  format period_dt date9.;
	  keep PBO_LOCATION_ID CHANNEL_CD period_dt;
	  if channel_cd="ALL" ;
	  if (end_dt>=&vf_fc_start_dt_sas and end_dt<=&vf_fc_agg_end_dt_sas) 
	  or (start_dt>=&vf_fc_start_dt_sas and start_dt<=&vf_fc_agg_end_dt_sas) 
	  or (start_dt<=&vf_fc_start_dt_sas and &vf_fc_start_dt_sas<=end_dt)
	  then
	  do period_dt=max(start_dt,&vf_fc_start_dt_sas) to min(&vf_fc_agg_end_dt_sas,end_dt);
	    output;
	  end;
	run;
	
	data casuser.days_pbo_close(append=force); /*дни когда закрыто ПБО - никаких продаж быть не должно*/
	  set casuser.days_pbo_date_close;
	run;
	
	proc fedsql sessref=casauto; /*убираем дубликаты*/
	create table casuser.days_pbo_close{options replace=true} as
	select distinct * from casuser.days_pbo_close;
	quit;

/*2.2 TODO: обработка замен T*/
	proc fedsql sessref=casauto;
		create table casuser.plm_t{options replace=true} as
		select LIFECYCLE_CD, PREDECESSOR_DIM2_ID, PREDECESSOR_PRODUCT_ID,
		SUCCESSOR_DIM2_ID, SUCCESSOR_PRODUCT_ID, SCALE_FACTOR_PCT,
		coalesce(PREDECESSOR_END_DT,cast(intnx('day',SUCCESSOR_START_DT,-1) as date)) as PREDECESSOR_END_DT, 
		SUCCESSOR_START_DT
		from &lmvLCTab
		where LIFECYCLE_CD='T' 
		and coalesce(PREDECESSOR_END_DT,cast(intnx('day',SUCCESSOR_START_DT,-1) as date))<=date %tslit(&vf_fc_agg_end_dt)
		and successor_start_dt>=intnx('month',&vf_fc_start_dt,-3);
		/*фильтр, отсекающий "старые" замены 
		Замены случившиеся больше 3 мес назад отсекаются 
		Замены позднее fc_agg_end_dt отсекаем*/
	quit;

    /*predcessor будет продаваться до predecessor_end_dt (включ), все остальные даты ПОСЛЕ удаляем*/
    proc fedsql sessref=casauto; 
		create table casuser.predessor_periods_t{options replace=true} as
		select PREDECESSOR_DIM2_ID as pbo_location_id,
		PREDECESSOR_PRODUCT_ID as product_id,
		min(PREDECESSOR_END_DT) as end_dt
		from casuser.plm_t group by 1,2
		;
	quit;

/*2.3 TODO: обработка выводов D*/
	proc fedsql sessref=casauto;
		create table casuser.plm_d{options replace=true} as
		select LIFECYCLE_CD, PREDECESSOR_DIM2_ID, PREDECESSOR_PRODUCT_ID,
		SUCCESSOR_DIM2_ID, SUCCESSOR_PRODUCT_ID, SCALE_FACTOR_PCT,
		PREDECESSOR_END_DT, SUCCESSOR_START_DT
		from &lmvLCTab
		where LIFECYCLE_CD='D'
		and predecessor_end_dt<=date %tslit(&vf_fc_agg_end_dt);
		/*старые выводы не отсекаем
		  выводы позднее fc_agg_end_dt отсекаем*/
	quit;

/*2.4 TODO: insert-update новых товаров по дням по ключу в pmix_daily до PLM
		с приоритетом новых товаров*/
	proc fedsql sessref=casauto;
		create table casuser.pmix_daily_new{options replace=true} as
		select 
		coalesce(t1.SALES_DT,t2.period_dt) as period_dt,
		coalesce(t1.product_id,t2.PRODUCT_ID) as product_id,
		coalesce(t1.channel_cd,t2.channel_cd) as channel_cd, 
		coalesce(t1.pbo_location_id,t2.PBO_LOCATION_ID) as PBO_LOCATION_ID, 
		coalesce(cast(intnx('month',t1.sales_dt,0) as date),t2.mon_dt) as mon_dt,
		coalesce(t1.P_SUM_QTY,t2.ff) as ff
		from casuser.npf_prediction t1 full outer join casuser.pmix_daily t2
			on t1.SALES_DT =t2.period_dt and t1.product_id=t2.product_id and 
			t1.channel_cd=t2.channel_cd and t1.pbo_location_id=t2.pbo_location_id
		;
	quit;

	
/*2.51 Добавление в АМ информации из новинок */
	proc fedsql sessref=casauto;
		create table casuser.AM_new{options replace=true} as
		select product_id,pbo_location_id, start_dt,end_dt
		from &lmvAMTab. t1;
	quit;
    
    data casuser.AM_new(append=yes);
		set casuser.future_product_chain(rename=(period_start_dt=start_dt 
												period_end_dt=end_dt));
	run;

/*2.52 TODO: применение T,D PLM к прогнозам casuser.pmix_daily + новые товары, 
		учет таблиц постоянных+временных закрытий*/
	/*формирование таблицы товар-ПБО-день, которые должны быть в прогнозе - на основании АМ*/
	proc fedsql sessref=casauto;
		create table casuser.plm_dist{options replace=true} as
		select pbo_location_id,product_id, start_dt,end_dt
		from casuser.AM_new
		where start_dt between &vf_fc_start_dt and date %tslit(&vf_fc_agg_end_dt)
			  or &vf_fc_start_dt between start_dt and end_dt; /*нужны записи AM, пересекающиеся с периодом прогнозирования*/
	quit;
	
	data casuser.days_prod_sale; /*Дни когда товар должен продаваться по информации из АМ*/
	  set casuser.plm_dist;
	  format period_dt date9.;
	  keep PBO_LOCATION_ID PRODUCT_ID period_dt;
	  do period_dt=max(start_dt,&vf_fc_start_dt_sas) to min(&vf_fc_agg_end_dt_sas,end_dt);
	    output;
	  end;
	run;

	/*удалить дубликаты*/
	data casuser.days_prod_sale1;
		set casuser.days_prod_sale;
		by PBO_LOCATION_ID PRODUCT_ID period_dt;
		if first.period_dt then output;
	run;
	
	proc fedsql sessref=casauto;
	  /*удалить отсюда периоды D */
	  create table casuser.plm_sales_mask{options replace=true} as
	  select t1.PBO_LOCATION_ID, t1.PRODUCT_ID, t1.period_dt
	  from  casuser.days_prod_sale1 t1 left join casuser.plm_d t2
	  on t1.product_id=t2.PREDECESSOR_PRODUCT_ID and t1.pbo_location_id=t2.PREDECESSOR_DIM2_ID
	  where t1.period_dt<coalesce(t2.PREDECESSOR_END_DT,cast(intnx('day',date %tslit(&vf_fc_agg_end_dt),1) as date));
	quit;

	proc casutil;
			droptable casdata="plm_sales_mask1" incaslib="dm_abt" quiet;
	run;

	proc fedsql sessref=casauto;
	  /*удалить отсюда периоды временного и постоянного закрытия ПБО */
	  create table casuser.plm_sales_mask1{options replace=true} as
		  select t1.PBO_LOCATION_ID, t1.PRODUCT_ID, t1.period_dt
		  from  casuser.plm_sales_mask t1 left join casuser.DAYS_PBO_CLOSE t3
		  on t1.pbo_location_id=t3.pbo_location_id and t1.period_dt=t3.period_dt
		  /*пересечь с casuser.days_pbo_close - 
		  когда ПБО закрыт по любым причинам,
		  эти дни не должны попадать в неё по ключу ПБО - канал*/
		  left join casuser.predessor_periods_t t4
		  on t1.pbo_location_id=t4.pbo_location_id and t1.product_id=t4.product_id
		/*из plm_sales_mask1 удаляем для predcessor периоды с датой >end_dt*/
		  where t3.pbo_location_id is null and t3.period_dt is null
		  and ((t1.period_dt<=t4.end_dt and t4.end_dt is not null) or t4.end_dt=.)
		   /*если ряд есть в predcessor - оставляем всё <=даты вывода, если нет - не смотрим на дату*/
	;
	quit;
/*=-==========================-*/
/* вообще это надо перенести в предобработку и копировать историю под новыми id 
   но тогда мы портим историю и нельзя применять иерархическое прогнозирование?*/
    proc fedsql sessref=casauto; /*создаём дубликаты прогнозов, копируя predesessor
								под id successor*/
		create table casuser.successor_fc{options replace=true} as
			select
			t1.period_DT,
			t2.SUCCESSOR_PRODUCT_ID as product_id,
			t1.CHANNEL_CD,
			t2.SUCCESSOR_DIM2_ID as pbo_location_id,
			t1.mon_dt,
			t1.FF*coalesce(t2.SCALE_FACTOR_PCT,100.)/100. as FF
			from casuser.pmix_daily_new t1 inner join casuser.plm_t t2 on
			t1.PRODUCT_ID=t2.PREDECESSOR_PRODUCT_ID and t1.PBO_LOCATION_ID=PREDECESSOR_DIM2_ID
			where t1.period_dt>=successor_start_dt;
	quit;
/*добавить замены в pmix_daily_new, 
 Не append! Приоритет у successor_fc! 
 флаг промо для замены? - оставляем из predcessor*/
	*data casuser.pmix_daily_new(append=force); 
	*  set casuser.successor_fc;
	*run;
    proc fedsql sessref=casauto;
		create table casuser.pmix_daily_new_{options replace=true} as
			select coalesce(t1.period_dt,t2.period_dt) as period_dt,
				coalesce(t1.product_id,t2.PRODUCT_ID) as product_id,
				coalesce(t1.channel_cd,t2.channel_cd) as channel_cd, 
				coalesce(t1.pbo_location_id,t2.PBO_LOCATION_ID) as PBO_LOCATION_ID, 
				coalesce(t1.mon_dt,t2.mon_dt) as mon_dt,
				coalesce(t1.ff,t2.ff) as ff
			from casuser.successor_fc t1 full outer join casuser.pmix_daily_new t2
				on t1.period_dt =t2.period_dt and t1.product_id=t2.product_id and 
				t1.channel_cd=t2.channel_cd and t1.pbo_location_id=t2.pbo_location_id
	;
	quit;

/*TODO: рассчитать таблицу с флагом промо, добавить её к финальной таблице pmix*/
	proc casutil;
			droptable casdata="fc_w_plm" incaslib="casuser" quiet;
	run;
	
	proc fedsql sessref=casauto; /*наложение plm на прогноз*/
		create table casuser.fc_w_plm{options replace=true} as 
			select t1.CHANNEL_CD,t1.PBO_LOCATION_ID,t1.PRODUCT_ID,t1.period_dt,
			t1.FF,
			coalesce(tpr.promo,0) as promo
			from casuser.pmix_daily_new_ t1 inner join casuser.plm_sales_mask1 t2 /*дни когда товар ДОЛЖЕН продаваться*/
			on t1.PBO_LOCATION_ID=t2.PBO_LOCATION_ID and t1.PRODUCT_ID=t2.PRODUCT_ID and t1.period_dt=t2.period_dt
			left join casuser.promo_w1 tpr 
			on tpr.channel_cd=t1.channel_cd and tpr.pbo_location_id=t1.PBO_LOCATION_ID and
				tpr.product_id=t1.PRODUCT_ID and tpr.period_dt=t1.period_dt
			;
	quit;

	proc casutil;
			promote casdata="plm_sales_mask1" incaslib="casuser" outcaslib="dm_abt";
			promote casdata="fc_w_plm" incaslib="casuser" outcaslib="casuser";
	quit;
/*======================================*/
/*2.6 TODO: прогнозы GC от отдела развития - добавить к прогнозу GC insert-update*/

/*2.7 TODO: Применение таблицы постоянных+временных закрытий к прогнозам GC*/
	proc fedsql sessref=casauto;
		create table casuser.fc_w_plm_gc{options replace=true} as 
			select t1.CHANNEL_CD,t1.PBO_LOCATION_ID,t1.period_dt,FF
			from casuser.daily_gc t1 left join casuser.days_pbo_close t2
			on t1.PBO_LOCATION_ID=t2.PBO_LOCATION_ID and t1.period_dt=t2.period_dt 
			   and t1.CHANNEL_CD=t2.CHANNEL_CD
			where t2.PBO_LOCATION_ID is null and t2.period_dt is null
			   and t2.CHANNEL_CD  is null /*не должно быть инфо о закрытии*/
			;
	quit;

data CASUSER.price (replace=yes  drop=valid_from_dttm valid_to_dttm);
			set etl_ia.price(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
run;
/*3. Вычисление цен на будущее*/
	/*приводим к ценам по дням*/
	data casuser.price_unfolded;
	 set casuser.PRICE;
	 where price_type='F';
	 keep product_id pbo_location_id net_price_amt gross_price_amt sales_dt;
	 format sales_dt date9.;
	 do sales_dt=START_DT to min(END_DT,&vf_fc_agg_end_dt_sas);
	   output;
	 end;
	run;

	/*избавляемся от возможных дубликатов цен по ключу товар-пбо-дата*/
	data casuser.price_nodup;
	  set casuser.price_unfolded;
	  by product_id pbo_location_id sales_dt;
	  if first.sales_dt then output;
	run;

	proc casutil;
	  droptable casdata="price_unfolded" incaslib="casuser" quiet;
	run;
	quit;
	 
	/*протягиваем неизвестные цены последним известным значением до горизонта прогнозирования*/
	proc cas;
	timeData.timeSeries result =r /
		series={{name="gross_price_amt", setmiss="prev"},
				{name="net_price_amt", setmiss="prev"}}
		tEnd= "&vf_fc_agg_end_dt" /*fc_start_dt+hor*/
		table={caslib="casuser",name="price_nodup", groupby={"PBO_LOCATION_ID","PRODUCT_ID"} }
		timeId="SALES_DT"
		trimId="LEFT"
		interval="day"
		casOut={caslib="casuser",name="TS_price_fact",replace=True}
		;
	run;
	quit;
	proc casutil;
	  droptable casdata="price_nodup" incaslib="casuser" quiet;
	run;
	quit;
	%if &mpPrmt. = Y %then %do;
		proc casutil;
		droptable casdata="&lmvOutTabNameGcSt." incaslib="&lmvOutLibrefGcSt." quiet;
		droptable casdata="&lmvOutTabNameUptSt." incaslib="&lmvOutLibrefUptSt." quiet;
		droptable casdata="&lmvOutTabNamePmixSt." incaslib="&lmvOutLibrefPmixSt." quiet;
		quit;
	%end;

/*4. Формирование таблиц по дням*/
/*4.1 Units*/
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
			from casuser.fc_w_plm t1 left join casuser.ts_price_fact t2 on
			t1.product_id=t2.product_id and t1.pbo_location_id=t2.pbo_location_id and
			   t1.period_dt=t2.sales_dt
			where t1.channel_cd='ALL' and t1.period_dt between &VF_FC_START_DT and &VF_FC_END_SHORT_DT;
	quit;

/*4.2 GC:*/
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
			from casuser.fc_w_plm_gc
			where channel_cd='ALL' and period_dt between &VF_FC_START_DT and &VF_FC_END_SHORT_DT;
	quit;

/*4.3 UPT по дням*/
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
		from &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt. t1 left join &lmvOutLibrefGcSt..&lmvOutTabNameGcSt. t2
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
/*5.1 Units*/
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
			from casuser.fc_w_plm t1 left join casuser.ts_price_fact t2 on
				t1.product_id=t2.product_id and t1.pbo_location_id=t2.pbo_location_id and
				   t1.period_dt=t2.sales_dt
				where t1.channel_cd='ALL' 
				group by 1,2,3,4,5;
	quit;
/*5.2 GC*/
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
			from casuser.fc_w_plm_gc t1
				where channel_cd='ALL'
				group by 1,2,3,4,5;
	quit;
/*5.3 UPT*/
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
			from &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt. t1 left join &lmvOutLibrefGcLt..&lmvOutTabNameGcLt. t2
			  on t1.location=t2.location and t1.data=t2.data;
	quit;
	%if &mpPrmt. = Y %then %do;
		proc casutil;
		
		promote casdata="pmix_daily" incaslib="casuser" outcaslib="mn_long";
		save incaslib="mn_long" outcaslib="mn_long" casdata="pmix_daily" casout="pmix_daily.sashdat" replace;
		
		promote casdata="&lmvOutTabNamePmixLt." incaslib="&lmvOutLibrefPmixLt." outcaslib="&lmvOutLibrefPmixLt.";
		save incaslib="&lmvOutLibrefPmixLt." outcaslib="&lmvOutLibrefPmixLt." casdata="&lmvOutTabNamePmixLt." casout="&lmvOutTabNamePmixLt..sashdat" replace;
		promote casdata="&lmvOutTabNameGcLt." incaslib="&lmvOutLibrefGcLt." outcaslib="&lmvOutLibrefGcLt.";
		save incaslib="&lmvOutLibrefGcLt." outcaslib="&lmvOutLibrefGcLt." casdata="&lmvOutTabNameGcLt." casout="&lmvOutTabNameGcLt..sashdat" replace;
		promote casdata="&lmvOutTabNameUptLt." incaslib="&lmvOutLibrefUptLt." outcaslib="&lmvOutLibrefUptLt.";
		save incaslib="&lmvOutLibrefUptLt." outcaslib="&lmvOutLibrefUptLt." casdata="&lmvOutTabNameUptLt." casout="&lmvOutTabNameUptLt..sashdat" replace;
		quit;
	%end;
%mend rtp_7_out_integration;
