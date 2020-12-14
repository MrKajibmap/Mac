%macro rtp_5_reconcil(mpFSAbt = public.pbo_train,
					mpMasterCodeTbl = dm_abt.MASTER_CODE_DAYS_RESULT,
					mpProductTable = dm_abt.PMIX_SCORE_RES,
					mpResultTable = DM_ABT.PMIX_RECONCILED_FULL
					);
					
	options mprint nomprintnest nomlogic nomlogicnest nosymbolgen mcompilenote=all mreplace;
	/*Создать cas-сессию, если её нет*/
	%if %sysfunc(SESSFOUND(casauto)) = 0 %then %do; 
		cas casauto sessopts=(metrics=true);
		caslib _all_ assign;
	%end;
	/*  Проверка на существование входных таблиц */
	%member_exists_list(mpMemberList=&mpFSAbt.
								&mpMasterCodeTbl.
								&mpProductTable.
								);
								
	%local lmvLibrefRes
			lmvTabNmRes
			lmvLibrefAbt 
			lmvTabNmAbt 
			lmvScoreStartDt 
			lmvScoreEndDt
			lmvLeadnhor
	;
	%let lmvScoreStartDt = &VF_FC_START_DT;
	%let lmvScoreEndDt = &VF_FC_END_SHORT_DT;
	%let lmvLeadnhor=%eval(&VF_FC_END_SHORT_DT_SAS-&VF_FC_START_DT_SAS+1);
	
	%member_names (mpTable=&mpResultTable, 
					mpLibrefNameKey=lmvLibrefRes,
					mpMemberNameKey=lmvTabNmRes);
	%member_names (mpTable=&mpFSAbt, 
					mpLibrefNameKey=lmvLibrefAbt,
					mpMemberNameKey=lmvTabNmAbt);
    /******0. Прогноз units на уровне магазина *********/
	
	proc cas;
		timeData.forecast status=rc /
		dependents={{name="sum_qty"}}
		mode="DYNAMIC" 
		predictors={{name="A_CPI"} ,
					{name="A_GPD"}, 
					{name="A_RDI"},
					{name="COMP_TRP_BK"},
					{name="COMP_TRP_KFC"},
					{name="COUNT_PROMO_PRODUCT"},
					{name="COUNT_TRP"},
					{name="MEAN_PRICE"},
					{name="NUNIQUE_PROMO"},
					{name="SUM_TRP"},
					{name="precipitation"},
					{name="temperature"}}
		result="HORONLY"
		tEnd= "&vf_hist_end_dt_sas" 
		table={caslib="&lmvLibrefAbt",name="&lmvTabNmAbt", groupby={"PBO_LOCATION_ID","CHANNEL_CD"} ,
			   where="sales_dt<=&vf_hist_end_dt_sas and channel_cd='ALL'"}
		timeId="SALES_DT"
		interval="day"
		lead=&lmvLeadnhor
		forOut={caslib="public",name="fcst_pbo_pmix",replace=True}
		selectOut={caslib="public",name="dlv_sel_pmix",replace=True}
		specOut={caslib="public",name="dlv_spec_pmix",replace=True}
		indepOut={caslib="public",name="dlv_ind_pmix",replace=True}
		infoOut={caslib="public",name="dlv_info_pmix",replace=True}
		;
		quit;
		data public.fcst_pbo_pmix_;
		set public.fcst_pbo_pmix;
		if predict<0 then predict=0;
	run;
	/****** 1. Собираем 3 таблицы******/
	
	/* 1.2 Реконсилируем прогноз с ПБО на мастеркод */
	proc fedsql sessref=casauto;
		/* 1.2.1 Считаем распределение прогноза на уровне мастеркода */
		create table casuser.middle_freq{options replace=true} as
			select
				t1.*,
				t1.p_sum_qty / t2.sum_prediction as pcnt_prediction
			from
				&mpMasterCodeTbl. as t1
			inner join
				(
				select
					t1.pbo_location_id,
					t1.sales_dt,
					t1.channel_cd,
					sum(t1.p_sum_qty) as sum_prediction
				from
					&mpMasterCodeTbl. as t1
				group by
					t1.pbo_location_id,
					t1.sales_dt,
					t1.channel_cd
				) as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = t2.sales_dt and
				t1.channel_cd = t2.channel_cd
			where
				t1.sales_dt >= &lmvScoreStartDt. and
				t1.sales_dt <= &lmvScoreEndDt.
		;
		/* 1.2.2 Реконсилируем прогноз с ПБО на мастеркод */
		create table casuser.middle{options replace=true} as
			select
				t1.*,
				coalesce(t1.pcnt_prediction * t2.predict, t1.p_sum_qty) as p_rec_sum_qty
			from
				casuser.middle_freq as t1
			left join
				public.fcst_pbo_pmix_ as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = t2.sales_dt
		;
		
	quit;

	/* 1.3 Реконсилируем прогноз с мастеркода на товар */
	proc fedsql sessref=casauto;
		/* 1.3.1 Считаем распределение прогноза на уровне товара */
		create table casuser.low_freq{options replace=true} as
			select
				t1.*,
				t1.p_sum_qty / t2.sum_prediction as pcnt_prediction
			from
				&mpProductTable. as t1
			inner join
				(
				select
					t1.pbo_location_id,
					t1.sales_dt,
					t1.channel_cd,
					t1.prod_lvl4_id,
					sum(t1.p_sum_qty) as sum_prediction
				from
					&mpProductTable. as t1
				group by
					t1.pbo_location_id,
					t1.sales_dt,
					t1.channel_cd,
					t1.prod_lvl4_id
				) as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = t2.sales_dt and
				t1.channel_cd = t2.channel_cd and
				t1.prod_lvl4_id = t2.prod_lvl4_id
			where
				t1.sales_dt >= &lmvScoreStartDt. and
				t1.sales_dt <= &lmvScoreEndDt.
		;
		/* 1.3.2 Реконсилируем прогноз с мастеркода на товар */
		create table casuser.low{options replace=true} as
			select
				t1.*,
				t1.pcnt_prediction * t2.p_sum_qty as p_rec_sum_qty,
				t1.pcnt_prediction * t2.p_rec_sum_qty as p_rec_rec_sum_qty
			from
				casuser.low_freq as t1
			left join
				casuser.middle as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = t2.sales_dt and
				t1.prod_lvl4_id = t2.prod_lvl4_id
		;
	
	quit;

	proc casutil;
		droptable casdata="&lmvTabNmRes." incaslib="&lmvLibrefRes." quiet;
		promote casdata="low" incaslib="casuser" casout="&lmvTabNmRes." outcaslib="&lmvLibrefRes";
		save incaslib="&lmvLibrefRes." outcaslib="&lmvLibrefRes." casdata="&lmvTabNmRes." casout="&lmvTabNmRes..sashdat" replace;
	run;

%mend rtp_5_reconcil;