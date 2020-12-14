/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для создания копии витрины промо-прогноза, обогащенной данными по промо заданного promo_calculation_rk
*
*  ПАРАМЕТРЫ:
*     mpPromoCalculationRk  - promo_calculation_rk из модели данных Promo Tool
*     mpIn                  - исходная витрина для скоринга прогноза
*	  mpOut 		        - выходная таблица с данными
*     mpPromote             - флаг, выполнять ли promote таблицы mpOut
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
*     %promo_calc_update_dm_abt(mpPromoCalculationRk=1,mpOut=DM_ABT.FCST_DATAMART_1);
*
****************************************************************************
*  28-04-2020  Михайлова     Начальное кодирование
*  25-06-2020  Борзунов		 Добавлена загрузка таблицы в PG (schema=dm_abt)
*  30-06-2020  Михайлова     Таблица с регулярной витриной прогнозирования перенесена из CAS в PG; изменены опции загрузки в PG через bulkload; отбор справочников из ETL_IA
*  03-07-2020  Михайлова     Перенос витрин в CAS
*  27-07-2020  Михайлова     Добавлена выгрузка таблицы с промо для дальнейшего использования в витринах VA
*  19-08-2020  Михайлова     Изменен механизм подтягивания промо к витрине по орг. структуре. Иерархия элемента int_org_rk из dim_point теперь определяется по иерархии ПБО в ETL_IA
****************************************************************************/

%macro promo_calc_update_dm_abt(mpPromoCalculationRk=,
								mpIn=PUBLIC.ML_SCORE,
								mpOut=CASUSER.ML_SCORE_&mpPromoCalculationRk,
								mpOutPromo=CASUSER.PROMO_&mpPromoCalculationRk,
								mpPromote=N);

	%local lmvPromoRkList
			lmvOutLibref 
			lmvOutTabName 
			lmvCASSESS
			lmvCASSessExist
			lmvOutPromoLibref
			lmvOutPromoTabName
			;

	%let lmvCASSESS = casauto;
	
	/*Создать cas-сессию, если её нет*/
	%let lmvCASSessExist = %sysfunc(SESSFOUND (&lmvCASSESS)) ;
	%if &lmvCASSessExist = 0 %then %do;
	 cas &lmvCASSESS;
	 caslib _all_ assign;
	%end;
	
	proc sql;
		create table work.product_hierarchy as
		select *
		from etl_ia.product_hierarchy
		where valid_from_dttm<=&ETL_CURRENT_DTTM. and valid_to_dttm>=&ETL_CURRENT_DTTM.
		;
	quit;
	
	proc sql;
		create table work.pbo_loc_hierarchy as
		select *
		from etl_ia.pbo_loc_hierarchy
		where valid_from_dttm<=&ETL_CURRENT_DTTM. and valid_to_dttm>=&ETL_CURRENT_DTTM.
		;
	quit;
	
	proc sql noprint;
		select promo_rk into :lmvPromoRkList separated by ' '
		from pt.promo_x_promo_calculation
		where promo_calculation_rk=&mpPromoCalculationRk
		;
	quit;
	
	proc sort data=pt.promo_detail(where=(promo_rk in (&lmvPromoRkList) and promo_dtl_cd='mechanicsType')) out=work.promo_detail_srt;
		by promo_rk;
	run;
	
	proc transpose data=work.promo_detail_srt out=work.promo_detail;
		by promo_rk;
		id promo_dtl_cd;
		var promo_dtl_vle;
	run;
	
	proc sql noprint;
		create table work.promo_x_product as
		select 
			promo_rk, input(promo_dtl_vle,best.) as product_id
			,input(scan(promo_dtl_cd,2,'_'),best.) as integer1
			,input(scan(promo_dtl_cd,3,'_'),best.) as integer2
		from pt.promo_detail
		where promo_rk in (&lmvPromoRkList) and promo_dtl_cd contains 'mechPromoSkuId'
		;
	quit;
	
	proc sql noprint;
		create table work.promo_x_price as
		select 
			promo_rk, input(promo_dtl_vle,best.) as promo_price
			,input(scan(promo_dtl_cd,2,'_'),best.) as integer1
			,input(scan(promo_dtl_cd,3,'_'),best.) as integer2
		from pt.promo_detail
		where promo_rk in (&lmvPromoRkList) and promo_dtl_cd contains 'mechPrice'
		;
	quit;
	
	proc sql noprint;
		create table work.promo_pt as
		select distinct
			promo.promo_rk as promo_id
			,promo.promo_nm
			,dp.int_org_rk
			,pxp.product_id
			,channel.channel_cd
			,pxpr.promo_price
			,datepart(promo.promo_start_dttm) as start_dt
			,datepart(promo.promo_end_dttm) as end_dt
			,pd.mechanicsType as promo_mechanics
		from pt.promo promo
		left join pt.promo_x_dim_point pxdp
			on pxdp.promo_rk=promo.promo_rk
		left join pt.dim_point dp
			on dp.dim_point_rk=pxdp.dim_point_rk
		left join work.promo_x_product pxp
			on pxp.promo_rk=promo.promo_rk
		left join work.promo_x_price pxpr
			on pxpr.promo_rk=promo.promo_rk
			and pxpr.integer1=pxp.integer1
			and pxpr.integer2=pxp.integer2
		left join work.promo_detail pd
			on pd.promo_rk=promo.promo_rk
		left join etl_stg2.channel_lookup channel
			on dp.channel_rk=channel.pt_member_rk
		where promo.promo_rk in (&lmvPromoRkList)
		;
	quit;
	
	/*Удаление дубликатов на случай, если для промо-акции было выбрано несколько сегментов - для нас не важен сегмент*/
	proc sort data=work.promo nodupkey;
		by promo_id int_org_rk product_id;
	run;
	
	/*Подтягиваем уровень ПБО*/
	proc sql;
		create table work.promo as
		select t1.*, t2.pbo_location_lvl
		from work.promo_pt t1
		left join work.pbo_loc_hierarchy t2
			on t2.pbo_location_id=t1.int_org_rk
		;
	quit;
	
	/*Выгрузка данных о промо для использование в витринах VA*/
	%member_names (mpTable=&mpOutPromo, mpLibrefNameKey=lmvOutPromoLibref, mpMemberNameKey=lmvOutPromoTabName);
	proc casutil;
		load data=work.promo casout="&lmvOutPromoTabName" outcaslib="&lmvOutPromoLibref" replace;
	run;
		
	/* Создаем таблицу иерархии PBO */
	proc sql;
		create table work.pbo_hier_flat as
			select
				t1.pbo_location_id, 
				t2.PBO_LOCATION_ID as LVL3_ID,
				t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
				1 as LVL1_ID
			from 
				(select * from work.pbo_loc_hierarchy where pbo_location_lvl=4) as t1
			left join 
				(select * from work.pbo_loc_hierarchy where pbo_location_lvl=3) as t2
			on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
		;
	quit;
	
	/* Добавляем к таблице промо ПБО и товары */
	proc sql noprint;
		/* Создаем иерархию товаров для вычисления ID регулярного товара */
		create table work.product_hier_flat as
			select
				t1.product_id, 
				t2.product_id as LVL4_ID,
				t3.product_id as LVL3_ID,
				t3.PARENT_product_id as LVL2_ID, 
				1 as LVL1_ID
			from 
				(select * from work.product_hierarchy where product_lvl=5) as t1
			left join 
				(select * from work.product_hierarchy where product_lvl=4) as t2
			on 
				t1.PARENT_PRODUCT_ID=t2.PRODUCT_ID
			left join 
				(select * from work.product_hierarchy where product_lvl=3) as t3
			on 
				t2.PARENT_PRODUCT_ID=t3.PRODUCT_ID
		;
	quit;
		
	proc sql;
		/* Расписываем промо механики */
		create table work.promo_ml as 
			select
				t1.promo_id,
				t1.product_id,
				t1.int_org_rk,
				t1.pbo_location_lvl,
				t1.START_DT,
				t1.END_DT,
				t2.CHANNEL_CD_ID as CHANNEL_CD,
				case when t1.PROMO_MECHANICS = 'BOGO / 1+1' then 1 else 0 end as bogo,
				case when t1.PROMO_MECHANICS = 'Discount' then 1 else 0 end as discount,
				case when t1.PROMO_MECHANICS = 'EVM/Set' then 1 else 0 end as evm_set,
				case when t1.PROMO_MECHANICS = 'Non-Product Gift' then 1 else 0 end as non_product_gift,
				case when t1.PROMO_MECHANICS = 'Pairs' then 1 else 0 end as pairs,
				case when t1.PROMO_MECHANICS = 'Product Gift' then 1 else 0 end as product_gift,
				case when t1.PROMO_MECHANICS = 'Other: Discount for volume' then 1 else 0 end as other_promo,
				case when t1.PROMO_MECHANICS = 'NP Promo Support' then 1 else 0 end as support
			from
				work.promo as t1
				left join dm_abt.encoding_channel_cd t2
					on t2.CHANNEL_CD = t1.CHANNEL_CD
		;
	quit;

	data work.promo_ml;
		set work.promo_ml;
		format SALES_DT date9.;
		do SALES_DT=START_DT to END_DT;
			output;
		end;
		drop START_DT END_DT;
	run;

	proc sql;
		create table work.promo_ml as
		select product_id, 
				int_org_rk,
				pbo_location_lvl,
				CHANNEL_CD,
				SALES_DT,
				max(bogo) as bogo,
				max(discount) as discount,
				max(evm_set) as evm_set,
				max(non_product_gift) as non_product_gift,
				max(pairs) as pairs,
				max(product_gift) as product_gift,
				max(other_promo) as other_promo,
				max(support) as support
		from work.promo_ml
		group by product_id, 
				int_org_rk,
				CHANNEL_CD,
				SALES_DT
		;
	quit;
	
	proc sql;
		/* 	Добавляем side_promo_flag */
			create table work.promo_ml_main_code as 
			select
				(MOD(t2.LVL4_ID, 10000)) AS product_MAIN_CODE,
				t1.int_org_rk,
				t1.pbo_location_lvl,
				t1.SALES_DT,
				t1.CHANNEL_CD,
				case
					when t1.product_id = MOD(t2.LVL4_ID, 10000) then 0
					else 1
				end as side_promo_flag
					
			from
				work.promo_ml as t1 
			left join
				work.product_hier_flat as t2
			on 
				t1.product_id = t2.product_id
		;
			create table work.promo_ml_main_code as 
			select
				product_MAIN_CODE,
				int_org_rk,
				pbo_location_lvl,
				SALES_DT,
				CHANNEL_CD,
				max(side_promo_flag) as side_promo_flag					
			from
				work.promo_ml_main_code 
			group by product_MAIN_CODE,
				int_org_rk,
				pbo_location_lvl,
				SALES_DT,
				CHANNEL_CD
		;
	quit;
	
	proc casutil;
		load data=work.pbo_hier_flat casout='pbo_hier_flat' outcaslib='CASUSER' replace;
		load data=work.promo_ml casout='promo_ml' outcaslib='CASUSER' replace;
		load data=work.promo_ml_main_code casout='promo_ml_main_code' outcaslib='CASUSER' replace;
	run;
	
	%member_names (mpTable=&mpOut, mpLibrefNameKey=lmvOutLibref, mpMemberNameKey=lmvOutTabName); 
	
	proc casutil;
	  droptable casdata="&lmvOutTabName" incaslib="&lmvOutLibref" quiet;
	run;

	/* Соединяем с витриной */
	proc fedsql SESSREF=&lmvCASSESS.;
		create table &mpOut {options replace = true} as 
			select
				t1.PBO_LOCATION_ID,
				t1.PRODUCT_ID,
				t1.CHANNEL_CD,
				t1.SALES_DT,
				t1.SUM_QTY,
				t1.GROSS_PRICE_AMT,
				t1.lag_halfyear_avg,
				t1.lag_halfyear_med,
				t1.lag_month_avg,
				t1.lag_month_med,
				t1.lag_qtr_avg,
				t1.lag_qtr_med,
				t1.lag_week_avg,
				t1.lag_week_med,
				t1.lag_year_avg,
				t1.lag_year_med,
				t1.lag_halfyear_std,
				t1.lag_month_std,
				t1.lag_qtr_std,
				t1.lag_week_std,
				t1.lag_year_std,
				t1.lag_halfyear_pct10,
				t1.lag_halfyear_pct90,
				t1.lag_month_pct10,
				t1.lag_month_pct90,
				t1.lag_qtr_pct10,
				t1.lag_qtr_pct90,
				t1.lag_week_pct10,
				t1.lag_week_pct90,
				t1.lag_year_pct10,
				t1.lag_year_pct90,
				coalesce(t2.OTHER_PROMO,0) as OTHER_PROMO,
				coalesce(t2.SUPPORT,0) as SUPPORT,
				coalesce(t2.bogo,0) as bogo,
				coalesce(t2.discount,0) as discount,
				coalesce(t2.evm_set,0) as evm_set,
				coalesce(t2.non_product_gift,0) as non_product_gift,
				coalesce(t2.pairs,0) as pairs,
				coalesce(t2.product_gift, 0) as product_gift,
				coalesce(t3.side_promo_flag, 0) as side_promo_flag,
				t1.A_CPI,
				t1.A_GPD,
				t1.A_RDI,
				t1.TEMPERATURE,
				t1.PRECIPITATION,
				t1.COMP_TRP_BK,
				t1.COMP_TRP_KFC,
				t1.SUM_TRP,
				t1.PROD_LVL4_ID,
				t1.PROD_LVL3_ID,
				t1.PROD_LVL2_ID,
				t1.HERO,
				t1.ITEM_SIZE,
				t1.OFFER_TYPE,
				t1.PRICE_TIER,
				t1.LVL3_ID,
				t1.LVL2_ID,
				t1.AGREEMENT_TYPE,
				t1.BREAKFAST,
				t1.BUILDING_TYPE,
				t1.COMPANY,
				t1.DELIVERY,
				t1.DRIVE_THRU,
				t1.MCCAFE_TYPE,
				t1.PRICE_LEVEL,
				t1.WINDOW_TYPE,
				t1.week,
				t1.weekday,
				t1.month,
				t1.weekend_flag,
				t1.DEFENDER_DAY,
				t1.FEMALE_DAY,
				t1.MAY_HOLIDAY,
				t1.NEW_YEAR,
				t1.RUSSIA_DAY,
				t1.SCHOOL_START,
				t1.STUDENT_DAY,
				t1.SUMMER_START,
				t1.VALENTINE_DAY,
				t1.PRICE_RANK,
				t1.PRICE_INDEX
			from
				&mpIn. as t1
			left join CASUSER.pbo_hier_flat pbo
				on pbo.pbo_location_id=t1.pbo_location_id
			left join
				CASUSER.promo_ml as t2
			on
				t1.product_id = t2.product_id and
				(case when t2.pbo_location_lvl=1 then pbo.LVL1_ID
					when t2.pbo_location_lvl=2 then pbo.LVL2_ID
					when t2.pbo_location_lvl=3 then pbo.LVL3_ID
					when t2.pbo_location_lvl=4 then pbo.pbo_location_id end)=t2.int_org_rk and
				t1.CHANNEL_CD = t2.CHANNEL_CD and
				t1.SALES_DT = t2.SALES_DT
			left join
				CASUSER.promo_ml_main_code as t3
			on
				t1.product_id = t3.product_MAIN_CODE and
				(case when t3.pbo_location_lvl=1 then pbo.LVL1_ID
					when t3.pbo_location_lvl=2 then pbo.LVL2_ID
					when t3.pbo_location_lvl=3 then pbo.LVL3_ID
					when t3.pbo_location_lvl=4 then pbo.pbo_location_id end)=t3.int_org_rk and
				t1.CHANNEL_CD = t3.CHANNEL_CD and
				t1.SALES_DT = t3.SALES_DT
		;
	quit;
	
	proc casutil;
	  droptable casdata="pbo_hier_flat" incaslib="CASUSER" quiet;
	  droptable casdata="promo_ml" incaslib="CASUSER" quiet;
	  droptable casdata="promo_ml_main_code" incaslib="CASUSER" quiet;
	run;
	
	%if &mpPromote=Y %then %do;
		proc casutil;
			promote casdata="&lmvOutTabName" incaslib="&lmvOutLibref" outcaslib="&lmvOutLibref";
			promote casdata="&lmvOutPromoTabName" incaslib="&lmvOutPromoLibref" outcaslib="&lmvOutPromoLibref";
		run;
		quit;
		
		%if &lmvCASSessExist = 0 %then %do;
			cas &lmvCASSESS. terminate;
		%end;
	%end;
%mend promo_calc_update_dm_abt;