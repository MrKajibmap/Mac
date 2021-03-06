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
							mpOutNnetWp=public.nnet_wp1,
							mpPrmt=Y) ;

	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;
	%let lmvInLib=ETL_IA;
	%let ETL_CURRENT_DT = %sysfunc(date());
	%let ETL_CURRENT_DTTM = %sysfunc(datetime());
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
			;
/*������� ��������� ��� plm*/	
	/*1 ������ �������*/
	%let lmvAMTab=CASUSER.ASSORT_MATRIX;
	%if %sysfunc(exist(casuser.assort_matrix)) eq 0 %then %do;
		data CASUSER.assort_matrix (replace=yes  drop=valid_from_dttm valid_to_dttm);
			set &lmvInLib..assort_matrix(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
			/*and end_dt<=&lmvEndDate. and start_dt>=&lmvStartDate.*/));
		run;
	%end;
	/*2 ���������� � ��������� ���������*/
	%let lmvPBOCloseTab=casuser.PBO_CLOSE_PERIOD;
	%if %sysfunc(exist(casuser.pbo_close_period)) eq 0 %then %do;
		data CASUSER.pbo_close_period (replace=yes  drop=valid_from_dttm valid_to_dttm);
				set &lmvInLib..pbo_close_period(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
					and end_dt<=&lmvScoreEndDate. and start_dt>=&lmvStartDate.));
		run;
	%end;
	/*3 ������� lifecycle*/
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
	
	%let lmvReportDttm = %sysfunc(datetime());
	
	%member_names (mpTable=&mpOutOutfor, mpLibrefNameKey=lmvOutLibrefOutfor, mpMemberNameKey=lmvOutTabNameOutfor);
	%member_names (mpTable=&mpOutOutforgc, mpLibrefNameKey=lmvOutLibrefOutforgc, mpMemberNameKey=lmvOutTabNameOutforgc); 
	%member_names (mpTable=&mpOutGcSt, mpLibrefNameKey=lmvOutLibrefGcSt, mpMemberNameKey=lmvOutTabNameGcSt); 
	%member_names (mpTable=&mpOutPmixSt, mpLibrefNameKey=lmvOutLibrefPmixSt, mpMemberNameKey=lmvOutTabNamePmixSt); 
	%member_names (mpTable=&mpOutUptSt, mpLibrefNameKey=lmvOutLibrefUptSt, mpMemberNameKey=lmvOutTabNameUptSt); 
	%member_names (mpTable=&mpOutGcLt, mpLibrefNameKey=lmvOutLibrefGcLt, mpMemberNameKey=lmvOutTabNameGcLt); 
	%member_names (mpTable=&mpOutPmixLt, mpLibrefNameKey=lmvOutLibrefPmixLt, mpMemberNameKey=lmvOutTabNamePmixLt); 
	%member_names (mpTable=&mpOutUptLt, mpLibrefNameKey=lmvOutLibrefUptLt, mpMemberNameKey=lmvOutTabNameUptLt); 
/*����������� �������� �� VF*/	
	/* ��������� ������ VF-�������� */
	%vf_get_project_list(mpOut=work.vf_project_list);
	/* ���������� ID ��� VF-������� PMIX �� ��� ����� */
	%let lmvVfPmixName = &mpVfPmixProjName.;
	%let lmvVfPmixId = %vf_get_project_id_by_name(mpName=&lmvVfPmixName., mpProjList=work.vf_project_list);
	
	/* ���������� ID ��� VF-������� PBO �� ��� ����� */
	%let lmvVfPboName = &mpVfPboProjName.;
	%let lmvVfPboId = %vf_get_project_id_by_name(mpName=&lmvVfPboName., mpProjList=work.vf_project_list);
	%let lmvInEventsMkup=&mpInEventsMkup;
/* 0. �������� ������� ������ */
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
/*0.9 �������� ������ �� �������*/
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
    

/*1. ��������� � ��������� ��������� ��������� �������*/
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

/*1.5 ������� ����� �������*/
  %vf_new_product;
/*�������� �������:public.npf_prediction*/
*proc fedsql sessref=casauto;
*select min(sales_dt) as min_dt, max(sales_dt) as max_dt
*from PUBLIC.NPF_PREDICTION;
*quit;

/*2. ���������� ������� ������������� �������� � �������������� - � ����������� ��������������*/
data public.promo_w2;
  set public.promo_d; /*table from vf_apply_w_prof*/
  format period_dt date9.;
  do period_dt=start_DT to min(end_DT,&vf_fc_agg_end_dt_sas);
    output;
  end;
run;

proc fedsql sessref=casauto;
  create table public.promo_w1{options replace=true} as
  select distinct t1.channel_cd,t1.pbo_location_id,
  t1.product_id,t1.period_dt, 
  cast(1 as double) as promo
  from public.promo_w2 t1;
quit;

/*������ ������������� � ������������ (�� ����) ������� � ����������� �����������.*/
  proc fedsql sessref=casauto;
   create table public.pmix_daily{options replace=true} as
		select 
			coalesce(t4.channel_cd,t1.channel_cd) as channel_cd, 
			coalesce(t4.pbo_location_id,t1.PBO_LOCATION_ID) as PBO_LOCATION_ID, 
			coalesce(t4.product_id,t1.PRODUCT_ID) as product_id,
			coalesce(t4.sales_dt,t1.period_dt) as period_dt,
			coalesce(cast(intnx('month',t4.sales_dt,0) as date),t1.mon_dt) as mon_dt,
			coalesce(t4.P_REC_REC_SUM_QTY,t1.ff) as ff
		from public.pmix_daily_ t1 full outer join 
		(select t2.PBO_LOCATION_ID, t2.PRODUCT_ID, t2.sales_dt, t3.channel_cd,
				t2.P_REC_REC_SUM_QTY from
                &mpMLPmixTabName t2 left join DM_ABT.ENCODING_CHANNEL_CD t3
				on t2.channel_cd=t3.channel_cd_id
				where t2.sales_dt between &VF_FC_START_DT and &VF_FC_END_SHORT_DT ) t4
            on t1.PBO_LOCATION_ID=t4.PBO_LOCATION_ID and t1.PRODUCT_ID=t4.PRODUCT_ID and
            t1.period_dt = t4.sales_dt and t1.channel_cd=t4.channel_cd
   ;
   quit;

/*2.1 TODO: ���������� ������ ��������� �������� � ���������� ���� ������*/
	data public.days_pbo_date_close; /*��� ����� ��� ����� ��� ������ (��������)*/
	  set casuser.pbo_dictionary;
	  format period_dt date9.;
	  keep PBO_LOCATION_ID CHANNEL_CD period_dt;
	  CHANNEL_CD="ALL"; 
	  if A_CLOSE_DATE ne . and A_CLOSE_DATE<=&vf_fc_agg_end_dt_sas then 
	  do period_dt= max(A_CLOSE_DATE,&vf_fc_start_dt_sas) to &vf_fc_agg_end_dt_sas;
	    output;
	  end;
	run;
	
	data public.days_pbo_close; /*��� ����� ��� ����� �������� ������*/
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
	
	data public.days_pbo_close(append=force); /*��� ����� ������� ��� - ������� ������ ���� �� ������*/
	  set public.days_pbo_date_close;
	run;
	
	proc fedsql sessref=casauto; /*������� ���������*/
	create table public.days_pbo_close{options replace=true} as
	select distinct * from public.days_pbo_close;
	quit;

/*2.2 TODO: ��������� ����� T*/
	proc fedsql sessref=casauto;
	create table public.plm_t{options replace=true} as
	select LIFECYCLE_CD, PREDECESSOR_DIM2_ID, PREDECESSOR_PRODUCT_ID,
	SUCCESSOR_DIM2_ID, SUCCESSOR_PRODUCT_ID, SCALE_FACTOR_PCT,
	coalesce(PREDECESSOR_END_DT,cast(intnx('day',SUCCESSOR_START_DT,-1) as date)) as PREDECESSOR_END_DT, 
    SUCCESSOR_START_DT
	from &lmvLCTab
	where LIFECYCLE_CD='T' 
	and coalesce(PREDECESSOR_END_DT,cast(intnx('day',SUCCESSOR_START_DT,-1) as date))<=date %tslit(&vf_fc_agg_end_dt)
	and successor_start_dt>=intnx('month',&vf_fc_start_dt,-3);
	/*������, ���������� "������" ������ 
	������ ����������� ������ 3 ��� ����� ���������� 
	������ ������� fc_agg_end_dt ��������*/
	quit;

    /*predcessor ����� ����������� �� predecessor_end_dt (�����), ��� ��������� ���� ����� �������*/
    proc fedsql sessref=casauto; 
	create table public.predessor_periods_t{options replace=true} as
	select PREDECESSOR_DIM2_ID as pbo_location_id,
	PREDECESSOR_PRODUCT_ID as product_id,
	min(PREDECESSOR_END_DT) as end_dt
	from public.plm_t group by 1,2
	;
	quit;

/*2.3 TODO: ��������� ������� D*/
	proc fedsql sessref=casauto;
	create table public.plm_d{options replace=true} as
	select LIFECYCLE_CD, PREDECESSOR_DIM2_ID, PREDECESSOR_PRODUCT_ID,
	SUCCESSOR_DIM2_ID, SUCCESSOR_PRODUCT_ID, SCALE_FACTOR_PCT,
	PREDECESSOR_END_DT, SUCCESSOR_START_DT
	from &lmvLCTab
	where LIFECYCLE_CD='D'
	and predecessor_end_dt<=date %tslit(&vf_fc_agg_end_dt);
	/*������ ������ �� ��������
	  ������ ������� fc_agg_end_dt ��������*/
	quit;

/*2.4 TODO: insert-update ����� ������� �� ���� �� ����� � pmix_daily �� PLM
		� ����������� ����� �������*/
	proc fedsql sessref=casauto;
	create table public.pmix_daily_new{options replace=true} as
	select 
	coalesce(t1.SALES_DT,t2.period_dt) as period_dt,
	coalesce(t1.product_id,t2.PRODUCT_ID) as product_id,
	coalesce(t1.channel_cd,t2.channel_cd) as channel_cd, 
	coalesce(t1.pbo_location_id,t2.PBO_LOCATION_ID) as PBO_LOCATION_ID, 
	coalesce(cast(intnx('month',t1.sales_dt,0) as date),t2.mon_dt) as mon_dt,
	coalesce(t1.P_SUM_QTY,t2.ff) as ff
	from public.npf_prediction t1 full outer join public.pmix_daily t2
		on t1.SALES_DT =t2.period_dt and t1.product_id=t2.product_id and 
		t1.channel_cd=t2.channel_cd and t1.pbo_location_id=t2.pbo_location_id
	;
	quit;

/*2.51 ���������� � �� ���������� �� ������� */
	proc fedsql sessref=casauto;
	create table public.AM_new{options replace=true} as
	select product_id,pbo_location_id, start_dt,end_dt
	from &lmvAMTab. t1;
	quit;
    
    data public.AM_new(append=yes);
	set public.future_product_chain(rename=(period_start_dt=start_dt 
											period_end_dt=end_dt));
	run;

/*2.52 TODO: ���������� T,D PLM � ��������� public.pmix_daily + ����� ������, 
		���� ������ ����������+��������� ��������*/
	/*������������ ������� �����-���-����, ������� ������ ���� � �������� - �� ��������� ��*/
	proc fedsql sessref=casauto;
	create table public.plm_dist{options replace=true} as
	select pbo_location_id,product_id, start_dt,end_dt
	from public.AM_new
	where start_dt between &vf_fc_start_dt and date %tslit(&vf_fc_agg_end_dt)
	      or &vf_fc_start_dt between start_dt and end_dt; /*����� ������ AM, �������������� � �������� ���������������*/
	quit;
	
	data public.days_prod_sale; /*��� ����� ����� ������ ����������� �� ���������� �� ��*/
	  set public.plm_dist;
	  format period_dt date9.;
	  keep PBO_LOCATION_ID PRODUCT_ID period_dt;
	  do period_dt=max(start_dt,&vf_fc_start_dt_sas) to min(&vf_fc_agg_end_dt_sas,end_dt);
	    output;
	  end;
	run;

	/*������� ���������*/
	data public.days_prod_sale1;
	set public.days_prod_sale;
	by PBO_LOCATION_ID PRODUCT_ID period_dt;
	if first.period_dt then output;
	run;
	
	proc fedsql sessref=casauto;
	  /*������� ������ ������� D */
	  create table public.plm_sales_mask{options replace=true} as
	  select t1.PBO_LOCATION_ID, t1.PRODUCT_ID, t1.period_dt
	  from  public.days_prod_sale1 t1 left join public.plm_d t2
	  on t1.product_id=t2.PREDECESSOR_PRODUCT_ID and t1.pbo_location_id=t2.PREDECESSOR_DIM2_ID
	  where t1.period_dt<coalesce(t2.PREDECESSOR_END_DT,cast(intnx('day',date %tslit(&vf_fc_agg_end_dt),1) as date));
	quit;

	proc casutil;
			droptable casdata="plm_sales_mask1" incaslib="dm_abt" quiet;
	run;

	proc fedsql sessref=casauto;
	  /*������� ������ ������� ���������� � ����������� �������� ��� */
	  create table public.plm_sales_mask1{options replace=true} as
	  select t1.PBO_LOCATION_ID, t1.PRODUCT_ID, t1.period_dt
	  from  public.plm_sales_mask t1 left join PUBLIC.DAYS_PBO_CLOSE t3
	  on t1.pbo_location_id=t3.pbo_location_id and t1.period_dt=t3.period_dt
	  /*�������� � public.days_pbo_close - 
	  ����� ��� ������ �� ����� ��������,
	  ��� ��� �� ������ �������� � �� �� ����� ��� - �����*/
	  left join public.predessor_periods_t t4
	  on t1.pbo_location_id=t4.pbo_location_id and t1.product_id=t4.product_id
	/*�� plm_sales_mask1 ������� ��� predcessor ������� � ����� >end_dt*/
	  where t3.pbo_location_id is null and t3.period_dt is null
	  and ((t1.period_dt<=t4.end_dt and t4.end_dt is not null) or t4.end_dt=.)
	   /*���� ��� ���� � predcessor - ��������� �� <=���� ������, ���� ��� - �� ������� �� ����*/
	;
	quit;
/*=-==========================-*/
/* ������ ��� ���� ��������� � ������������� � ���������� ������� ��� ������ id 
   �� ����� �� ������ ������� � ������ ��������� ������������� ���������������?*/
    proc fedsql sessref=casauto; /*������ ��������� ���������, ������� predesessor
								��� id successor*/
		create table public.successor_fc{options replace=true} as
		select
		t1.period_DT,
		t2.SUCCESSOR_PRODUCT_ID as product_id,
		t1.CHANNEL_CD,
		t2.SUCCESSOR_DIM2_ID as pbo_location_id,
		t1.mon_dt,
		t1.FF*coalesce(t2.SCALE_FACTOR_PCT,100.)/100. as FF
		from public.pmix_daily_new t1 inner join public.plm_t t2 on
		t1.PRODUCT_ID=t2.PREDECESSOR_PRODUCT_ID and t1.PBO_LOCATION_ID=PREDECESSOR_DIM2_ID
		where t1.period_dt>=successor_start_dt;
	quit;
/*�������� ������ � pmix_daily_new, 
 �� append! ��������� � successor_fc! 
 ���� ����� ��� ������? - ��������� �� predcessor*/
	*data public.pmix_daily_new(append=force); 
	*  set public.successor_fc;
	*run;
    proc fedsql sessref=casauto;
		create table public.pmix_daily_new_{options replace=true} as
		select coalesce(t1.period_dt,t2.period_dt) as period_dt,
			coalesce(t1.product_id,t2.PRODUCT_ID) as product_id,
			coalesce(t1.channel_cd,t2.channel_cd) as channel_cd, 
			coalesce(t1.pbo_location_id,t2.PBO_LOCATION_ID) as PBO_LOCATION_ID, 
			coalesce(t1.mon_dt,t2.mon_dt) as mon_dt,
			coalesce(t1.ff,t2.ff) as ff
		from public.successor_fc t1 full outer join public.pmix_daily_new t2
			on t1.period_dt =t2.period_dt and t1.product_id=t2.product_id and 
			t1.channel_cd=t2.channel_cd and t1.pbo_location_id=t2.pbo_location_id
	;
	quit;

/*TODO: ���������� ������� � ������ �����, �������� � � ��������� ������� pmix*/
	proc casutil;
			droptable casdata="fc_w_plm" incaslib="public" quiet;
	run;
	
	proc fedsql sessref=casauto; /*��������� plm �� �������*/
		create table public.fc_w_plm{options replace=true} as 
		select t1.CHANNEL_CD,t1.PBO_LOCATION_ID,t1.PRODUCT_ID,t1.period_dt,
		t1.FF,
		coalesce(tpr.promo,0) as promo
		from public.pmix_daily_new_ t1 inner join public.plm_sales_mask1 t2 /*��� ����� ����� ������ �����������*/
		on t1.PBO_LOCATION_ID=t2.PBO_LOCATION_ID and t1.PRODUCT_ID=t2.PRODUCT_ID and t1.period_dt=t2.period_dt
		left join public.promo_w1 tpr 
		on tpr.channel_cd=t1.channel_cd and tpr.pbo_location_id=t1.PBO_LOCATION_ID and
            tpr.product_id=t1.PRODUCT_ID and tpr.period_dt=t1.period_dt
		;
	quit;

	proc casutil;
			promote casdata="plm_sales_mask1" incaslib="public" outcaslib="dm_abt";
			promote casdata="fc_w_plm" incaslib="public" outcaslib="public";
	quit;
/*======================================*/
/*2.6 TODO: �������� GC �� ������ �������� - �������� � �������� GC insert-update*/

/*2.7 TODO: ���������� ������� ����������+��������� �������� � ��������� GC*/
	proc fedsql sessref=casauto;
		create table public.fc_w_plm_gc{options replace=true} as 
		select t1.CHANNEL_CD,t1.PBO_LOCATION_ID,t1.period_dt,FF
		from public.daily_gc t1 left join public.days_pbo_close t2
		on t1.PBO_LOCATION_ID=t2.PBO_LOCATION_ID and t1.period_dt=t2.period_dt 
		   and t1.CHANNEL_CD=t2.CHANNEL_CD
		where t2.PBO_LOCATION_ID is null and t2.period_dt is null
		   and t2.CHANNEL_CD  is null /*�� ������ ���� ���� � ��������*/
        ;
	quit;

data CASUSER.price (replace=yes  drop=valid_from_dttm valid_to_dttm);
			set etl_ia.price(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
run;
/*3. ���������� ��� �� �������*/
	/*�������� � ����� �� ����*/
	data public.price_unfolded;
	 set casuser.PRICE;
	 where price_type='F';
	 keep product_id pbo_location_id net_price_amt gross_price_amt sales_dt;
	 format sales_dt date9.;
	 do sales_dt=START_DT to min(END_DT,&vf_fc_agg_end_dt_sas);
	   output;
	 end;
	run;

	/*����������� �� ��������� ���������� ��� �� ����� �����-���-����*/
	data public.price_nodup;
	  set public.price_unfolded;
	  by product_id pbo_location_id sales_dt;
	  if first.sales_dt then output;
	run;

	proc casutil;
	  droptable casdata="price_unfolded" incaslib="public" quiet;
	run;
	quit;
	 
	/*����������� ����������� ���� ��������� ��������� ��������� �� ��������� ���������������*/
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

/*4. ������������ ������ �� ����*/
/*4.1 Units*/
	proc fedsql sessref=casauto;
	create table &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt.{options replace=true} as
	select 
		cast(t1.product_id as integer) as PROD /*� �� ��������*/,
		cast(t1.pbo_location_id as integer) as LOCATION /*� �� ���������*/,
		t1.period_dt as DATA /*� ���� �������� ��� ����� (����)*/,
		'RUR' as CURRENCY /*� ������, �������� �� ��������� RUR*/,
		'CORP' as ORG /*� �����������, �������� �� ��������� CORP*/,
		case when promo=0 then t1.FF else 0 end
		as BASE_FCST_QNT_DAY /*� ������� ������� (�����������, ���� � ���� ������� 
						�����-���-���� �� ���� �� ����� �����-�����, =0 �����)*/,
		case when t1.promo=1 then t1.FF else 0 end
		as PROMO_FCST_QNT_DAY /*� ������� ����� (�����������, ���� � ���� ������� 
						�����-���-���� ���� ���� � ����� �����-�����, =0 �����)*/,
		t1.FF as TOTAL_FCST_QNT_DAY /*� ����� �������� �������� � �����*/,
		t1.FF as OVERRIDED_FCST_QNT_DAY /*� ����� �������� �������� � ����� (��� ���������� �� ���������� ������?)*/,
		1 as OVERRIDE_TRIGGER_QNT_DAY /*� ������ ���������, �� ��������� �������� 1*/,
		case when promo=0 then t1.ff*t2.gross_price_amt else 0 end
		as BASE_FCST_RUR_DAY /*� ������� ������� � ��� (��� ��������� ���� � ����� ������������ net-����? 
					��� gross? �����������, ���� � ���� ������� �����-���-���� ��� �� ����� �����-�����)*/,
		case when promo=1 then t1.ff*t2.gross_price_amt else 0 end
		as PROMO_FCST_RUR_DAY /*� ����� ������� � ��� (�����������, ���� � ���� ������� �����-���-���� ���� ���� � ����� �����-�����)*/,
		t1.ff*t2.gross_price_amt as TOTAL_FCST_RUR_DAY /*� ��������� ������� � ���*/,
		t1.ff*t2.gross_price_amt as OVERRIDED_FCST_RUR_DAY /*� ������� � ������ �������� ��� (��������� � ETL ����� ��������� ������� ���� �� ������� � ������ ����������).*/,
		t2.gross_price_amt as AVG_PRICE /*� ������� ����. ��������� � ETL ��� ��������� ������� � ���/������� � �� � ������� ���/���*/
		from public.fc_w_plm t1 left join public.ts_price_fact t2 on
		t1.product_id=t2.product_id and t1.pbo_location_id=t2.pbo_location_id and
		   t1.period_dt=t2.sales_dt
		where t1.channel_cd='ALL' and t1.period_dt between &VF_FC_START_DT and &VF_FC_END_SHORT_DT;
	quit;

/*4.2 GC:*/
	proc fedsql sessref=casauto;
	create table &lmvOutLibrefGcSt..&lmvOutTabNameGcSt.{options replace=true} as
	select 
		1 as PROD /*� �� �������� �� ������� ������ (ALL Product, �������� = 1)*/,
		cast(pbo_location_id as integer) as LOCATION /*� �� ���������*/,
		period_dt as DATA /*� ���� �������� ��� ����� (����)*/,
		'RUR' as CURRENCY /*� ������, �������� �� ��������� RUR*/,
		'CORP' as ORG /*� �����������, �������� �� ��������� CORP*/,
		FF as BASE_FCST_GC_DAY /*� ������� ������� */,
		0 as PROMO_FCST_GC_DAY /*� ������� �����*/,
		FF as TOTAL_FCST_GC_DAY /*� ����� �������� �������� � �����*/,
		FF as OVERRIDED_FCST_GC_DAY /*� ����� �������� �������� � ����� � ������ ����������*/,
		1 as OVERRIDE_TRIGGER_GC_D /*� ������ ���������, �� ��������� �������� 1*/
		from public.fc_w_plm_gc
		where channel_cd='ALL' and period_dt between &VF_FC_START_DT and &VF_FC_END_SHORT_DT;
	quit;

/*4.3 UPT �� ����*/
	/*������� UPT �������������� �� �������� � �� � GC �� �������
	������� UPT(�����, ���, ����) = ������� � ��(�����, ���, ����)/������� GC(���, ����)*1000
	*/
	proc fedsql sessref=casauto;
	create table &lmvOutLibrefUptSt..&lmvOutTabNameUptSt.{options replace=true} as
	select 
		cast(t1.prod as integer) as PROD /*� �� �������� �� ������� ������ (ALL Product, �������� = 1) */,
		cast(t1.location as integer) as LOCATION /*� �� ���������*/,
		t1.data as DATA /*� ���� �������� ��� ����� (����)*/,
		'RUR' as CURRENCY /*� ������, �������� �� ��������� RUR*/,
		'CORP' as ORG /*� �����������, �������� �� ��������� CORP*/,
	case when t2.BASE_FCST_GC_DAY is not null and abs(t2.BASE_FCST_GC_DAY)> 1e-5 
	   then t1.BASE_FCST_QNT_DAY/t2.BASE_FCST_GC_DAY*1000 
	   else 0
	   end
	   as BASE_FCST_UPT_DAY /*� ������� �������, = ������� � ��(�����, ���, ����)/������� GC(���, ����)*1000,
					���� � ������� �����-���-���� ��� �� ����� �����-�����, =0 �����.*/,
	case when t2.BASE_FCST_GC_DAY is not null and abs(t2.BASE_FCST_GC_DAY)> 1e-5
	   then t1.PROMO_FCST_QNT_DAY/t2.BASE_FCST_GC_DAY*1000 
	   else 0
	   end
	   as PROMO_FCST_UPT_DAY /*� ������� �����, = ������� � ��(�����, ���, ����)/������� GC(���, ����)*1000, 
					���� � ������� �����-���-���� ���� ���� ��� ����� �����-�����, =0 �����.*/,
	   1 as OVERRIDE_TRIGGER_UPT_D /*� ������ ���������, �� ��������� �������� 1*/
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

/*5. ��������� �� ������� GC, UPT, Pmix, �� ���� ��������� ������������� �������*/
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
	cast(t1.product_id as integer) as PROD /*� �� ��������*/,
	cast(t1.pbo_location_id as integer) as LOCATION /*� �� ���������*/,
	cast(intnx('month',t1.period_dt,0,'b') as date) as DATA /*� ����� �������� ��� ����� � ������� (���� 1-�� ����� ������ �������� ��� �����).*/,
	'RUR' as CURRENCY /*� ������, �������� �� ��������� RUR*/,
	'CORP' as ORG /*� �����������, �������� �� ��������� CORP*/,
	sum(case when promo=0 then t1.FF else 0 end) 
	   as BASE_FCST_QNT_MON /*� ������� �������*/,
	sum(case when promo=1 then t1.FF else 0 end)
	   as PROMO_FCST_QNT_MON /*� ������� �����*/,
	sum(FF) as TOTAL_FCST_QNT_MON /*� ����� �������� �������� � �����*/,
	sum(FF) as OVERRIDED_FCST_QNT_MON /*� ����� �������� �������� � �����*/,
	1 as OVERRIDE_TRIGGER_QNT_MON /*� ������ ���������, �� ��������� �������� 1*/,
	sum(case when promo=0 then t1.ff*t2.gross_price_amt else 0 end)
	   as BASE_FCST_RUR_MON /*� ������� ������� � ���*/,
	sum(case when promo=1 then t1.ff*t2.gross_price_amt else 0 end)
	   as PROMO_FCST_RUR_MON /*� ����� ������� � ���*/,
	sum(t1.ff*t2.gross_price_amt)
	   as TOTAL_FCST_RUR_MON /*� ��������� ������� � ���*/,
	sum(t1.ff*t2.gross_price_amt)
	   as OVERRIDED_FCST_RUR_MON /*� ������� � ������ �������� ��� (��������� � ETL ����� ��������� ������� ���� �� ������� � ������ ����������).*/,
	case when abs(sum(t1.ff))>1e-5 then sum(t1.ff*t2.gross_price_amt)/sum(t1.ff) else 0 end
	   as AVG_PRICE /*� ������� ����. ��������� � ETL ��� ��������� ������� � ���/������� � �� � ������� ���/���*/
	from public.fc_w_plm t1 left join public.ts_price_fact t2 on
		t1.product_id=t2.product_id and t1.pbo_location_id=t2.pbo_location_id and
		   t1.period_dt=t2.sales_dt
		where t1.channel_cd='ALL' 
		group by 1,2,3,4,5;
	quit;
/*5.2 GC*/
	proc fedsql sessref=casauto;
	create table &lmvOutLibrefGcLt..&lmvOutTabNameGcLt.{options replace=true} as
	select
	1 as PROD /*� �� �������� �� ������� ������ (ALL Product, �������� = 1)*/,
	cast(t1.pbo_location_id as integer) as LOCATION /*� �� ���������*/,
	cast(intnx('month',t1.period_dt,0,'b') as date) as DATA /*� ���� �������� ��� ����� (�����)*/,
	'RUR' as CURRENCY /*� ������, �������� �� ��������� RUR*/,
	'CORP' as ORG /*� �����������, �������� �� ��������� CORP*/,
	sum(t1.ff) as BASE_FORECAST_GC_M /*� ������� ������� �� �����*/,
	sum(t1.ff) as OVERRIDED_FCST_GC /*� ������� ������� �� ����� (���� ������ ���������� ����������)*/,
	1 as OVERRIDE_TRIGGER /*� ������ ���������, �� ��������� �������� 1*/
	from public.fc_w_plm_gc t1
		where channel_cd='ALL'
		group by 1,2,3,4,5;
	quit;
/*5.3 UPT*/
	proc fedsql sessref=casauto;
	create table &lmvOutLibrefUptLt..&lmvOutTabNameUptLt.{options replace=true} as
	select
	cast(t1.prod as integer) as PROD /*� �� ��������*/, 
	cast(t1.location as integer) as LOCATION /*� �� ���������*/,
	t1.data as DATA /*� ���� �������� ��� ����� (�����)*/,
	'RUR' as CURRENCY /*� ������, �������� �� ��������� RUR*/,
	'CORP' as ORG /*� �����������, �������� �� ��������� CORP*/,
	case when t2.BASE_FORECAST_GC_M is not null and abs(t2.BASE_FORECAST_GC_M)>1e-5 
	   then t1.BASE_FCST_QNT_MON/t2.BASE_FORECAST_GC_M*1000 
	   else 0
	   end
	   as BASE_FCST_UPT /*� ������� �������*/,
	case when t2.BASE_FORECAST_GC_M is not null and abs(t2.BASE_FORECAST_GC_M)>1e-5 
	   then t1.PROMO_FCST_RUR_MON/t2.BASE_FORECAST_GC_M*1000 
	   else 0
	   end
	   as PROMO_FCST_UPT /*� ����� �������*/,
	case when t2.BASE_FORECAST_GC_M is not null and abs(t2.BASE_FORECAST_GC_M)>1e-5 
	   then t1.TOTAL_FCST_QNT_MON/t2.BASE_FORECAST_GC_M*1000 
	   else 0
	   end
	   as TOTAL_FCST_UPT /*� ��������� �������*/,
	case when t2.BASE_FORECAST_GC_M is not null and abs(t2.BASE_FORECAST_GC_M)>1e-5 
	   then t1.TOTAL_FCST_QNT_MON/t2.BASE_FORECAST_GC_M*1000 
	   else 0
	   end
	   as OVERRIDED_FCST_UP /*� ��������� ������� (� ������ ������ ���������� ����������)*/,
	1 as OVERRIDE_TRIGGER_UPT /*� ������ ��� ���������� ���������, �� ��������� ����� 1*/
	from &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt. t1 left join &lmvOutLibrefGcLt..&lmvOutTabNameGcLt. t2
	  on t1.location=t2.location and t1.data=t2.data;
	quit;
	%if &mpPrmt. = Y %then %do;
		proc casutil;
		promote casdata="&lmvOutTabNamePmixLt." incaslib="&lmvOutLibrefPmixLt." outcaslib="&lmvOutLibrefPmixLt.";
		save incaslib="&lmvOutLibrefPmixLt." outcaslib="&lmvOutLibrefPmixLt." casdata="&lmvOutTabNamePmixLt." casout="&lmvOutTabNamePmixLt..sashdat" replace;
		promote casdata="&lmvOutTabNameGcLt." incaslib="&lmvOutLibrefGcLt." outcaslib="&lmvOutLibrefGcLt.";
		save incaslib="&lmvOutLibrefGcLt." outcaslib="&lmvOutLibrefGcLt." casdata="&lmvOutTabNameGcLt." casout="&lmvOutTabNameGcLt..sashdat" replace;
		promote casdata="&lmvOutTabNameUptLt." incaslib="&lmvOutLibrefUptLt." outcaslib="&lmvOutLibrefUptLt.";
		save incaslib="&lmvOutLibrefUptLt." outcaslib="&lmvOutLibrefUptLt." casdata="&lmvOutTabNameUptLt." casout="&lmvOutTabNameUptLt..sashdat" replace;
		quit;
	%end;
%mend rtp_7_out_integration;
