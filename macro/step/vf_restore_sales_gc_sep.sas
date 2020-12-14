%macro vf_restore_sales_gc_sep;
	option dsoptions=nonote2err;
	/*входные таблицы mn_long.pmix_sales, pbo_sales, выходные: pbo_sales_rest, pmix_sales_rest*/
	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;
	
	%let delta=14;
	
	data CASUSER.pbo_cl_per_int;
		set mn_long.PBO_CLOSE_PERIOD;
		by CHANNEL_CD PBO_LOCATION_ID START_DT;
		keep CHANNEL_CD PBO_LOCATION_ID start_int_dt end_int_dt ;
		format start_int_dt end_int_dt date9.;
		retain start_int_dt end_int_dt;
		l_st_dt=lag(start_dt);
		l_en_dt=lag(end_dt);
		if first.pbo_location_id then do;
			l_st_dt=.;
			l_en_dt=.;
			start_int_dt=start_dt;
		end;
		if START_DT>l_en_dt+1 and l_en_dt ne . then do;
			end_int_dt=l_en_dt;
			output;
			start_int_dt=start_dt;
		end;
		if last.pbo_location_id then do;
			end_int_dt=end_dt;
			output;
		end;
	run;
	
	proc fedsql sessref=casauto;
		create table CASUSER.means_to_restore{options replace=true} as
			select 
					t1.CHANNEL_CD,t1.PBO_LOCATION_ID,t1.start_int_dt,t1.end_int_dt,
					t2.product_id,
					/*среднее за 14 дней до начала*/
					coalesce(
					avg(case 
					when t2.sales_dt between intnx('day',t1.start_int_dt,-&delta.) and intnx('day',t1.start_int_dt,-1)
					then sum(SALES_QTY,SALES_QTY_PROMO)
					end),0) as avg_b,
					/*среднее за 14 дней после конца*/
					case when t1.end_int_dt<date %tslit(&vf_hist_end_dt) then
					/*если конец периода до конца истории - считаем второе среднее*/
					coalesce(
					avg(case 
					when t2.sales_dt between intnx('day',t1.end_int_dt,1) and intnx('day',t1.end_int_dt,&delta.)
					then sum(SALES_QTY,SALES_QTY_PROMO)
					end),0) 
					else
					/*если конец периода после конца истории - используем уровень до начала периода*/
					coalesce(
					avg(case 
					when t2.sales_dt between intnx('day',t1.start_int_dt,-&delta.) and intnx('day',t1.start_int_dt,-1)
					then sum(SALES_QTY,SALES_QTY_PROMO)
					end),0) end as avg_a
			from CASUSER.pbo_cl_per_int t1 
			left join mn_long.PMIX_SALES t2
				on t1.channel_cd=t2.channel_cd 
				and t1.pbo_location_id=t2.pbo_location_id
				and (t2.sales_dt between intnx('day',t1.start_int_dt,-&delta.) and intnx('day',t1.start_int_dt,-1)
				or t2.sales_dt between intnx('day',t1.end_int_dt,1) and intnx('day',t1.end_int_dt,&delta.))
			where t1.start_int_dt<=date %tslit(&vf_hist_end_dt) /*только интервалы, начавшиеся в прошлом*/
			group by t1.CHANNEL_CD
					,t1.PBO_LOCATION_ID
					,t1.start_int_dt
					,t1.end_int_dt
					,t2.product_id
		;
	quit;

	/*необходимо протягивать данные по дням (для последующего запроса)*/
	proc cas;
		timeData.timeSeries result =r /
		series={{name="sales_qty", Acc="sum", setmiss="missing"},
		{name="sales_qty_promo", Acc="sum", setmiss="missing"}}
		tEnd= "&vf_hist_end_dt" 
		table={caslib="mn_long",name="pmix_sales", groupby={"PBO_LOCATION_ID","PRODUCT_ID","CHANNEL_CD"} ,
		orderBy={"PBO_LOCATION_ID","PRODUCT_ID","CHANNEL_CD","SALES_DT"},
		where="sales_dt>=&vf_hist_start_dt_sas and CHANNEL_CD in ('ALL', 'DLV')"}
		trimId="LEFT"
		timeId="SALES_DT"
		interval="day" /*!!!*/
		casOut={caslib="CASUSER",name="TS_pmix_sales_day",replace=True}
		;
	run;
	quit;

	/*восстановить спрос*/
	proc casutil;
		droptable casdata='pmix_sales_rest' incaslib='mn_long' quiet;
	run;

	proc fedsql sessref=casauto;
		create table casuser.pmix_sales_rest{options replace=true} as
			select t1.channel_cd, t1.pbo_location_id, t1.producT_id,t1.sales_dt,
				sum(SALES_QTY,SALES_QTY_PROMO) as sales_qty,
				case when t2.start_int_dt<=t1.sales_dt and t1.sales_dt<=t2.end_int_dt then
				/*нужно рассчитать восст продажи*/
				coalesce(t2.avg_b,0)+ 
				(t1.sales_dt-t2.start_int_dt+1)
				*(coalesce(t2.avg_a,t2.avg_b,0)-coalesce(t2.avg_b,0) ) / 
				(t2.end_int_dt-t2.start_int_dt+1)
				end as sales_qty_rest
			from CASUSER.TS_PMIX_SALES_day t1 left join CASUSER.means_to_restore t2
			on t1.CHANNEL_CD=t2.channel_cd and t1.PBO_LOCATION_ID=t2.pbo_location_id 
			and t1.product_id=t2.product_id and
			t1.sales_dt between t2.start_int_dt and t2.end_int_dt
		;
	quit;
	/*данные уже будут протянуты по дням*/
	proc casutil;
		promote casdata='pmix_sales_rest' incaslib='casuser' outcaslib='mn_long';
		save casdata="pmix_sales_rest" casout="pmix_sales_rest.sashdat" incaslib="mn_long" outcaslib="mn_long" replace compress;
		droptable casdata='pmix_sales_rest' incaslib='CASUSER' quiet;
		droptable casdata='TS_PMIX_SALES_day' incaslib='CASUSER' quiet;
	run;

	proc fedsql sessref=casauto;
		create table CASUSER.means_to_restore_gc{options replace=true} as
			select 
					t1.CHANNEL_CD
					,t1.PBO_LOCATION_ID
					,t1.start_int_dt
					,t1.end_int_dt
					/*среднее за 14 дней до начала*/
					,coalesce(
						avg(case 
						when t2.sales_dt between intnx('day',t1.start_int_dt,-&delta.) and intnx('day',t1.start_int_dt,-1)
						then RECEIPT_QTY
						end),0) as avg_b
					/*среднее за 14 дней после конца*/
					,case 
						when t1.end_int_dt<date %tslit(&vf_hist_end_dt) then
						/*если конец периода до конца истории - считаем второе среднее*/
						coalesce(
						avg(case 
								when t2.sales_dt between intnx('day',t1.end_int_dt,1) and intnx('day',t1.end_int_dt,&delta.)
								then RECEIPT_QTY
							end),0) 
						else
							coalesce(
							avg(case 
								when t2.sales_dt between intnx('day',t1.start_int_dt,-&delta.) and intnx('day',t1.start_int_dt,-1)
								then RECEIPT_QTY
								end),0)
							end as avg_a
				from CASUSER.pbo_cl_per_int t1 
				left join mn_long.PBO_SALES t2
					on t1.channel_cd=t2.channel_cd 
					and t1.pbo_location_id=t2.pbo_location_id
					and (t2.sales_dt between intnx('day',t1.start_int_dt,-&delta.) and intnx('day',t1.start_int_dt,-1)
					or t2.sales_dt between intnx('day',t1.end_int_dt,1) and intnx('day',t1.end_int_dt,&delta.))
				where t1.start_int_dt<=date %tslit(&vf_hist_end_dt) /*только интервалы, начавшиеся в прошлом*/
				group by t1.CHANNEL_CD
							,t1.PBO_LOCATION_ID
							,t1.start_int_dt
							,t1.end_int_dt
	;
	quit;

	proc cas;
		timeData.timeSeries result =r /
		series={{name="receipt_qty", Acc="sum", setmiss="missing"}}
		tEnd= "&vf_hist_end_dt" 
		table={caslib="mn_long",name="pbo_sales", groupby={"PBO_LOCATION_ID","CHANNEL_CD"} ,
		where="sales_dt>=&vf_hist_start_dt_sas and CHANNEL_CD in ('ALL', 'DLV')"}
		timeId="SALES_DT"
		interval="day"
		trimId="LEFT"
		casOut={caslib="CASUSER",name="TS_pbo_sales_day",replace=True}
		;
	run;
	quit;

	/*восстановить чеки*/
	proc casutil;
		droptable casdata='pbo_sales_rest' incaslib='mn_long' quiet;
	run;

	proc fedsql sessref=casauto;
		create table casuser.pbo_sales_rest{options replace=true} as
			select t1.channel_cd
					, t1.pbo_location_id
					, t1.sales_dt
					, receipt_qty
					,case
						when t2.start_int_dt<=t1.sales_dt and t1.sales_dt<=t2.end_int_dt 
						then
							/*нужно рассчитать восст продажи*/
							coalesce(t2.avg_b,0)+ 
							(t1.sales_dt-t2.start_int_dt+1)
							*(coalesce(t2.avg_a,t2.avg_b,0)-coalesce(t2.avg_b,0) ) / 
							(t2.end_int_dt-t2.start_int_dt+1)
					end as receipt_qty_rest
			from CASUSER.TS_Pbo_SALES_day t1
			left join CASUSER.means_to_restore_gc t2
				on t1.CHANNEL_CD=t2.channel_cd 
				and t1.PBO_LOCATION_ID=t2.pbo_location_id 
				and t1.sales_dt between t2.start_int_dt and t2.end_int_dt
		;
	quit;
	/*данные уже будут протянуты по дням*/
	proc casutil;
		promote casdata='pbo_sales_rest' incaslib='casuser' outcaslib='mn_long';
		save casdata="pbo_sales_rest" casout="pbo_sales_rest.sashdat" incaslib="mn_long" outcaslib="mn_long" replace compress;
		droptable casdata='means_to_restore_gc' incaslib='CASUSER' quiet;
		droptable casdata='TS_Pbo_SALES_day' incaslib='CASUSER' quiet;
	quit;
	
	cas casauto terminate;
	
%mend vf_restore_sales_gc_sep;