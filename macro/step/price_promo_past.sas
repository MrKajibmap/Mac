%macro price_promo_past(mpOutTable=, mpBatchValue=);
	%local lmvIterCounter
			lmvPromoList1210
			lmvPromoList345
			lmvPromoList68
			lmvPromoList7
			lmvPboUsedNum
			lmvPboTotalNum
			lmvOutTableName
			lmvOutTableCLib
			lmvBatchValue
			;
	%let lmvBatchValue = &mpBatchValue.;
	%member_names (mpTable=&mpOutTable, mpLibrefNameKey=lmvOutTableCLib, mpMemberNameKey=lmvOutTableName);
	
	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;
	
	proc casutil;  
		droptable casdata="&lmvOutTableName" incaslib="&lmvOutTableCLib" quiet;
	run;
	
	/* Джойн с двумя справочниками. Создание промо-разметки CHANNEL_CD - SKU - ПБО - период- Флаг_промо */
	proc fedsql sessref=casauto noprint;
		create table casuser.PROMO_FILT_SKU_PBO{options replace=true} as
			select distinct t1.CHANNEL_CD,
	 			t1.PROMO_ID,
				t1.PROMO_MECHANICS,
				t3.PRODUCT_ID,
				t3.OPTION_NUMBER,
				t2.PBO_LOCATION_ID,
				t1.start_dt,
				t1.end_dt,
				1 as promo_flag
		from casuser.PROMO t1
	    inner join casuser.PROMO_PBO t2
	        on t1.PROMO_ID = t2.PROMO_ID
		inner join casuser.PROMO_PROD t3
	        on t1.PROMO_ID = t3.PROMO_ID
		where t1.CHANNEL_CD = 'ALL'
		;
	quit;
	
	/* Создание пустой таблицы айдишников ПБО, в которой будут храниться уже посчитанные */
	data CASUSER.PBO_USED (keep=PBO_LOCATION_ID used_flag);
		set CASUSER.PROMO_FILT_SKU_PBO;
		where PBO_LOCATION_ID < -1000;
		used_flag = 1;
	run;
	
	proc fedsql sessref=casauto noprint;
		create table CASUSER.pbo_list_tmp{options replace=true} as
			select distinct t1.PBO_LOCATION_ID
			from CASUSER.PROMO_FILT_SKU_PBO t1
		;
	quit;
	
	data _NULL_;
		if 0 then set CASUSER.PBO_USED nobs=n;
		call symputx('lmvPboUsedNum',n);
		stop;
	run;
	data _NULL_;
		if 0 then set CASUSER.pbo_list_tmp nobs=n;
		call symputx('lmvPboTotalNum',n);
		stop;
	run;
	%let lmvIterCounter = 1;
	
	%do %while (&lmvPboUsedNum. < &lmvPboTotalNum.);
	
		/* Создание батча PBO start */
		proc fedsql sessref=casauto noprint;
			create table CASUSER.pbo_list{options replace=true} as
				select distinct t1.PBO_LOCATION_ID
				from CASUSER.PROMO_FILT_SKU_PBO t1
				left join CASUSER.PBO_USED t2
					on (t1.PBO_LOCATION_ID=t2.PBO_LOCATION_ID)
				where t2.used_flag = . /*исключение уже посчитанных*/
			;
		quit;
		data CASUSER.PBO_LIST_BATCH;
			set CASUSER.pbo_list(obs=&lmvBatchValue.);
			used_flag = 1;
		run;
		
		/* добавление в список посчитанных айдишников ПБО */
		data CASUSER.PBO_USED;
			set CASUSER.PBO_LIST_BATCH CASUSER.PBO_USED;
		run;
	
		proc fedsql sessref=casauto noprint;
			create table casuser.PROMO_FILT_SKU_PBO_BATCH{options replace=true} as
				select t1.*
			from casuser.PROMO_FILT_SKU_PBO t1
			inner join CASUSER.PBO_LIST_BATCH t2
				on (t1.PBO_LOCATION_ID=t2.PBO_LOCATION_ID)
			;
			
			create table CASUSER.PRICE_BATCH{options replace=true} as
				select t1.*
			from CASUSER.PRICE t1
			inner join CASUSER.PBO_LIST_BATCH t2
				on (t1.PBO_LOCATION_ID=t2.PBO_LOCATION_ID)
			;

		quit;
		
		/* Создание батча PBO end */
			
		/* Переход от start_dt end_dt интеревалов к подневному списку в ПРОМО разметке*/
		data casuser.PROMO_FILT_SKU_PBO_BATCH_DAYS (RENAME=(start_dt=day_dt) keep=CHANNEL_CD PBO_LOCATION_ID PRODUCT_ID OPTION_NUMBER PROMO_ID PROMO_MECHANICS PROMO_FLAG start_dt freq_num);
			set CASUSER.PROMO_FILT_SKU_PBO_BATCH;
			retain FREQ_NUM;
			by CHANNEL_CD PRODUCT_ID PBO_LOCATION_ID PROMO_ID;
			if first.PROMO_ID then FREQ_NUM = 0;
			FREQ_NUM = FREQ_NUM + 1;
			output;
		    do while ((start_dt < end_dt) and (start_dt < &VF_HIST_END_DT_SAS.));
		        start_dt = intnx('days', start_dt, 1);
		        output;
		    end;
		run;
		
		/* Переход от start_dt end_dt интеревалов к подневному списку в ФАКТИЧЕСКИХ ценах */
		data casuser.PRICE_BATCH_DAYS(rename=(start_dt=day_dt) keep=product_id pbo_location_id start_dt net_price_amt gross_price_amt);
			set CASUSER.PRICE_BATCH;
			output;
		    do while ((start_dt < end_dt) and (start_dt < &VF_HIST_END_DT_SAS.));
		        start_dt = intnx('days', start_dt, 1);
		        output;
		    end;
		run;		

/* 		=-=-=-=-=-=-=-=-=-=MECHANICS №1,2,10=-=-=-=-=-=-=-=-=-= */		

		%let lmvPromoList1210 = ('NP Promo Support', 'Discount', 'Product Gift');
	
		/* Вычисление средней фактической цены в период промо, когда факт не миссинг*/
		proc fedsql sessref=casauto noprint;
			create table CASUSER.PRICE_BATCH_DAYS_MECH1210_1{options replace=true} as
				select t1.product_id,
					   t1.pbo_location_id,
					   t1.PROMO_ID,
					   t1.PROMO_MECHANICS,
					   t1.FREQ_NUM,
					   mean(t2.net_price_amt) as mean_net,
					   mean(t2.gross_price_amt) as mean_gross
			from CASUSER.PROMO_FILT_SKU_PBO_BATCH_DAYS t1
			left join CASUSER.PRICE_BATCH_DAYS t2
				on (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
					and t1.product_id = t2.product_id
					and t1.day_dt = t2.day_dt)
			where t2.net_price_amt is not missing and t1.PROMO_MECHANICS in &lmvPromoList1210
			group by t1.product_id,
					 t1.pbo_location_id,
					 t1.PROMO_ID,
					 t1.PROMO_MECHANICS,
					 t1.FREQ_NUM
			;
		quit;
		
		
		/* Джойн промо-цены с фактической разметкой. Промо=факт, миссинги факта в дни промо проставляются на среднюю фактическую цену за период */
		proc fedsql sessref=casauto noprint;
			create table CASUSER.PRICE_BATCH_DAYS_MECH1210_2{options replace=true} as
				select t1.product_id,
					   t1.pbo_location_id,
					   t1.PROMO_ID,
					   t1.day_dt,
					   t2.net_price_amt,
					   t2.gross_price_amt,
					   t3.mean_net,
					   t3.mean_gross,
					   coalesce(t2.net_price_amt, t3.mean_net) as promo_net_price_amt,
					   coalesce(t2.gross_price_amt, t3.mean_gross) as promo_gross_price_amt
			from CASUSER.PROMO_FILT_SKU_PBO_BATCH_DAYS t1
			left join CASUSER.PRICE_BATCH_DAYS t2
				on (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
					and t1.product_id = t2.product_id
					and t1.day_dt = t2.day_dt)
			left join CASUSER.PRICE_BATCH_DAYS_MECH1210_1 t3
				on (t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID
					and t1.product_id = t3.product_id
					and t1.PROMO_ID = t3.PROMO_ID
					and t1.FREQ_NUM = t3.FREQ_NUM)
			where t1.PROMO_MECHANICS in &lmvPromoList1210
			;
		quit;
		
	
/* 		=-=-=-=-=-=-=-=-=-=MECHANICS №3, №4, №5-=-=-=-=-=-=-=-=-= */
	
		%let lmvPromoList345 = ('BOGO / 1+1', 'N+1', '1+1%');

		/*Таблица с факт ценами для каждой даты промо*/
		proc fedsql sessref=casauto noprint;
			create table CASUSER.PROMO_BATCH_DAYS_MECH345_1{options replace=true} as
				select t1.product_id,
					   t1.pbo_location_id,
					   t1.PROMO_ID,
					   t1.PROMO_MECHANICS,
					   t1.FREQ_NUM,
					   t1.day_dt,
					   t2.net_price_amt,
					   t2.gross_price_amt,
					   (case
							   when (t2.net_price_amt is missing) or (t2.gross_price_amt is missing) then 0
							   else 1
						   end) as nonmiss_flg
			from CASUSER.PROMO_FILT_SKU_PBO_BATCH_DAYS t1
			left join CASUSER.PRICE_BATCH_DAYS t2
				on (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
					and t1.product_id = t2.product_id
					and t1.day_dt = t2.day_dt)
			where t1.PROMO_MECHANICS in &lmvPromoList345
		;
		quit;
		

			
		/*Создание словаря с количеством немиссинговых фактических цен и количеством товаров в наборе в промо */

		proc fedsql sessref=casauto noprint;
			create table CASUSER.PROMO_BATCH_DAYS_MECH345_DICT{options replace=true} as
				select t1.pbo_location_id,
					   t1.PROMO_ID,
					   t1.FREQ_NUM,
					   t1.day_dt,
					   sum(t1.nonmiss_flg) as sum_nonmiss_flg,
					   count(t1.pbo_location_id) as count_skus
			from CASUSER.PROMO_BATCH_DAYS_MECH345_1 t1
			group by t1.pbo_location_id,
					   t1.PROMO_ID,
					   t1.FREQ_NUM,
					   t1.day_dt
		;
		quit;		
		
		/*Джойн справочника к основной таблице*/
		
		proc fedsql sessref=casauto noprint;
			create table CASUSER.PROMO_BATCH_DAYS_MECH345_2{options replace=true} as
				select t1.product_id,
					   t1.pbo_location_id,
					   t1.PROMO_ID,
					   t1.PROMO_MECHANICS,
					   t1.FREQ_NUM,
					   t1.day_dt,
					   t1.net_price_amt,
					   t1.gross_price_amt,
					   t1.nonmiss_flg,
					   t2.sum_nonmiss_flg,
					   t2.count_skus
			from CASUSER.PROMO_BATCH_DAYS_MECH345_1 t1
			left join CASUSER.PROMO_BATCH_DAYS_MECH345_DICT t2
				on (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
					and t1.FREQ_NUM = t2.FREQ_NUM
					and t1.PROMO_ID = t2.PROMO_ID
					and t1.day_dt = t2.day_dt)
		;
		quit;
	

		/* Подсчет средней цены внутри одного дня*/
		proc fedsql sessref=casauto noprint;
			create table CASUSER.PROMO_BATCH_DAYS_MECH345_3{options replace=true} as
				select t1.pbo_location_id,
					   t1.PROMO_ID,
					   t1.FREQ_NUM,
					   t1.day_dt,
					   mean(t1.net_price_amt) as day_mean_net,
					   mean(t1.gross_price_amt) as day_mean_gross
				from CASUSER.PROMO_BATCH_DAYS_MECH345_2 t1
				group by t1.pbo_location_id,
					 t1.PROMO_ID,
					 t1.FREQ_NUM,
					 t1.day_dt
			;
		quit;


		/*Подсчет средней цены внутри промо периода */
		proc fedsql sessref=casauto noprint;
			create table CASUSER.PROMO_BATCH_DAYS_MECH345_4{options replace=true} as
				select t1.pbo_location_id,
					   t1.PROMO_ID,
					   t1.FREQ_NUM,
					   mean(t1.net_price_amt) as period_mean_net,
					   mean(t1.gross_price_amt) as period_mean_gross
				from CASUSER.PROMO_BATCH_DAYS_MECH345_2 t1
				where t1.sum_nonmiss_flg = t1.count_skus
				group by t1.pbo_location_id,
					 t1.PROMO_ID,
					 t1.FREQ_NUM
			;
		quit;

		proc fedsql sessref=casauto noprint;
			create table CASUSER.PROMO_BATCH_DAYS_MECH345_5{options replace=true} as
				select t1.PROMO_ID,
					   t1.product_id,
					   t1.pbo_location_id,
					   t1.FREQ_NUM,
					   t1.day_dt,
					   t1.net_price_amt as net_price_amt_old,
					   t1.gross_price_amt as gross_price_amt_old,
					   t2.day_mean_net,
					   t2.day_mean_gross,
					   t3.period_mean_net,
					   t3.period_mean_gross,
					   t1.nonmiss_flg,
					   t1.sum_nonmiss_flg,
					   t1.count_skus,
					    (case 
					           when sum_nonmiss_flg = count_skus then t2.day_mean_net
							   when sum_nonmiss_flg = 0 then t3.period_mean_net
						end) as promo_net_price_amt,
						(case
					           when sum_nonmiss_flg = count_skus then t2.day_mean_gross
							   when sum_nonmiss_flg = 0 then t3.period_mean_gross
						end) as promo_gross_price_amt
				from CASUSER.PROMO_BATCH_DAYS_MECH345_2 t1
				left join CASUSER.PROMO_BATCH_DAYS_MECH345_3 t2
					on (t1.pbo_location_id=t2.pbo_location_id and
						t1.PROMO_ID=t2.PROMO_ID and
						t1.FREQ_NUM=t2.FREQ_NUM and
						t1.day_dt=t2.day_dt)
				left join CASUSER.PROMO_BATCH_DAYS_MECH345_4 t3
					on (t1.pbo_location_id=t3.pbo_location_id and
						t1.PROMO_ID=t3.PROMO_ID and
						t1.FREQ_NUM=t3.FREQ_NUM)
			;
		quit;



/* 		=-=-=-=-=-=-=-=-=-=MECHANICS №6 №8-=-=-=-=-=-=-=-=-= */

		%let lmvPromoList68 = ('EVM/Set', 'Pairs');

		/*Таблица с фактичекими ценами для каждой даты промо*/
		proc fedsql sessref=casauto noprint;
			create table CASUSER.PROMO_BATCH_DAYS_MECH68_1{options replace=true} as
				select t1.product_id,
					   t1.pbo_location_id,
					   t1.OPTION_NUMBER,
					   t1.PROMO_ID,
					   t1.PROMO_MECHANICS,
					   t1.FREQ_NUM,
					   t1.day_dt,
					   t2.net_price_amt as fact_net_price,
					   t2.gross_price_amt as fact_gross_price,
					   (case 
					           when (t2.net_price_amt is missing) or (t2.gross_price_amt is missing) then 0
							   else 1
					   end) as nonmiss_flg
			from CASUSER.PROMO_FILT_SKU_PBO_BATCH_DAYS t1
			left join CASUSER.PRICE_BATCH_DAYS t2
				on (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
					and t1.product_id = t2.product_id
					and t1.day_dt = t2.day_dt)		
			where t1.PROMO_MECHANICS in &lmvPromoList68
			;
		quit;
		
		/*Создание словаря с количеством немиссинговых фактических цен и количеством товаров в наборе в промо */

		proc fedsql sessref=casauto noprint;
			create table CASUSER.PROMO_BATCH_DAYS_MECH68_DICT{options replace=true} as
				select t1.pbo_location_id,
					   t1.PROMO_ID,
					   t1.FREQ_NUM,
					   t1.OPTION_NUMBER,
					   t1.day_dt,
					   sum(t1.nonmiss_flg) as sum_nonmiss_flg,
					   count(t1.pbo_location_id) as count_skus
			from CASUSER.PROMO_BATCH_DAYS_MECH68_1 t1
			group by t1.pbo_location_id,
					   t1.PROMO_ID,
					   t1.FREQ_NUM,
					   t1.OPTION_NUMBER,
					   t1.day_dt
		;
		quit;
		
		/*Джойн справочника к основной таблице*/

		proc fedsql sessref=casauto noprint;
			create table CASUSER.PROMO_BATCH_DAYS_MECH68_2{options replace=true} as
				select t1.product_id,
					   t1.pbo_location_id,
					   t1.PROMO_ID,
					   t1.PROMO_MECHANICS,
					   t1.FREQ_NUM,
					   t1.OPTION_NUMBER,
					   t1.day_dt,
					   t1.fact_net_price,
					   t1.fact_gross_price,
					   t1.nonmiss_flg,
					   t2.sum_nonmiss_flg,
					   t2.count_skus
			from CASUSER.PROMO_BATCH_DAYS_MECH68_1 t1
			left join CASUSER.PROMO_BATCH_DAYS_MECH68_DICT t2
				on (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
					and t1.FREQ_NUM = t2.FREQ_NUM
					and t1.OPTION_NUMBER = t2.OPTION_NUMBER
					and t1.PROMO_ID = t2.PROMO_ID
					and t1.day_dt = t2.day_dt)
		;
		quit;

		/* Подсчет МИНИМАЛЬНОЙ ФАКТИЧЕСКОЙ цены для конкретной позиции внутри одного промо-ДНЯ для тех наблюдений, у которых нет ни одного миссинга в фактических ценах в позиции*/
		proc fedsql sessref=casauto noprint;
			create table CASUSER.PROMO_BATCH_DAYS_MECH68_3{options replace=true} as
				select t1.pbo_location_id,
					   t1.PROMO_ID,
					   t1.OPTION_NUMBER,
					   t1.FREQ_NUM,
					   t1.day_dt,
					   min(t1.fact_net_price) as day_min_net,
					   min(t1.fact_gross_price) as day_min_gross
				from CASUSER.PROMO_BATCH_DAYS_MECH68_2 t1
				where t1.sum_nonmiss_flg = t1.count_skus
				group by t1.pbo_location_id,
					 t1.PROMO_ID,
					 t1.OPTION_NUMBER,
					 t1.FREQ_NUM,
					 t1.day_dt
			;
		quit;
		
		/* Подсчет СРЕДНЕЙ МИНИМАЛЬНОЙ ФАКТИЧЕСКОЙ цены для конкретной позиции внутри одного промо-ПЕРИОДА*/
		proc fedsql sessref=casauto noprint;
			create table CASUSER.PROMO_BATCH_DAYS_MECH68_4{options replace=true} as
				select t1.pbo_location_id,
					   t1.PROMO_ID,
					   t1.OPTION_NUMBER,
					   t1.FREQ_NUM,
					   mean(t1.day_min_net) as period_mean_net,
					   mean(t1.day_min_gross) as period_mean_gross
				from CASUSER.PROMO_BATCH_DAYS_MECH68_3 t1
				group by t1.pbo_location_id,
					 t1.PROMO_ID,
					 t1.OPTION_NUMBER,
					 t1.FREQ_NUM
			;
		quit;

		proc fedsql sessref=casauto noprint;
			create table CASUSER.PROMO_BATCH_DAYS_MECH68_5{options replace=true} as
				select t1.product_id,
					   t1.pbo_location_id,
					   t1.PROMO_ID,
					   t1.PROMO_MECHANICS,
					   t1.FREQ_NUM,
					   t1.OPTION_NUMBER,
					   t1.day_dt,
					   t1.fact_net_price,
					   t1.fact_gross_price,
					   t2.day_min_net as fact_day_min_net,
					   t2.day_min_gross as fact_day_min_gross,
					   t3.period_mean_net as fact_period_mean_net,
					   t3.period_mean_gross as fact_period_mean_gross,
					   t1.nonmiss_flg,
					   t1.sum_nonmiss_flg,
					   t1.count_skus,
					   (case /*правило импутации если на всем периоде промо нет ФАКТ цен?*/
					           when t1.sum_nonmiss_flg = t1.count_skus then t2.day_min_net
							   else t3.period_mean_net
						end) as promo_net_price_amt,
					   (case
					           when t1.sum_nonmiss_flg = t1.count_skus then t2.day_min_gross
							   else t3.period_mean_gross
						end) as promo_gross_price_amt					
				from CASUSER.PROMO_BATCH_DAYS_MECH68_2 t1
				left join CASUSER.PROMO_BATCH_DAYS_MECH68_3 t2
					on (t1.pbo_location_id=t2.pbo_location_id and
						t1.PROMO_ID=t2.PROMO_ID and
						t1.OPTION_NUMBER=t2.OPTION_NUMBER and 
						t1.FREQ_NUM=t2.FREQ_NUM and
						t1.day_dt=t2.day_dt)
				left join CASUSER.PROMO_BATCH_DAYS_MECH68_4 t3
					on (t1.pbo_location_id=t3.pbo_location_id and
						t1.PROMO_ID=t3.PROMO_ID and
						t1.OPTION_NUMBER=t3.OPTION_NUMBER and
						t1.FREQ_NUM=t3.FREQ_NUM)
			;
		quit;
	

/* 		=-=-=-=-=-=-=-=-=-=MECHANICS №7-=-=-=-=-=-=-=-=-= */

		%let lmvPromoList7 = ('Non-Product Gift');


		/*Таблица с факт ценами для каждой даты промо*/
		proc fedsql sessref=casauto noprint;
			create table CASUSER.PROMO_BATCH_DAYS_MECH7_1{options replace=true} as
				select t1.product_id,
					   t1.pbo_location_id,
					   t1.PROMO_ID,
					   t1.PROMO_MECHANICS,
					   t1.FREQ_NUM,
					   t1.day_dt,
					   t2.net_price_amt,
					   t2.gross_price_amt,
					   (case
							   when (t2.net_price_amt is missing) or (t2.gross_price_amt is missing) then 0
							   else 1
						   end) as nonmiss_flg
			from CASUSER.PROMO_FILT_SKU_PBO_BATCH_DAYS t1
			left join CASUSER.PRICE_BATCH_DAYS t2
				on (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
					and t1.product_id = t2.product_id
					and t1.day_dt = t2.day_dt)
			where t1.PROMO_MECHANICS in &lmvPromoList7
		;
		quit;

		/*Подсчет средней цены внутри промо периода для РЕГ и ФАКТ цен */
		proc fedsql sessref=casauto noprint;
			create table CASUSER.PROMO_BATCH_DAYS_MECH7_2{options replace=true} as
				select t1.product_id,
					   t1.pbo_location_id,
					   t1.PROMO_ID,
					   t1.FREQ_NUM,
					   mean(t1.net_price_amt) as period_mean_net,
					   mean(t1.gross_price_amt) as period_mean_gross
				from CASUSER.PROMO_BATCH_DAYS_MECH7_1 t1
				where t1.nonmiss_flg = 1
				group by t1.pbo_location_id,
					 t1.product_id,
					 t1.PROMO_ID,
					 t1.FREQ_NUM
			;
		quit;

		proc fedsql sessref=casauto noprint;
			create table CASUSER.PROMO_BATCH_DAYS_MECH7_3{options replace=true} as
				select t1.PROMO_ID,
					   t1.product_id,
					   t1.pbo_location_id,
					   t1.FREQ_NUM,
					   t1.day_dt,
					   t1.net_price_amt,
					   t1.gross_price_amt,
					   t2.period_mean_net,
					   t2.period_mean_gross,
					   t1.nonmiss_flg,
					    (case 
					           when t1.nonmiss_flg = 1 then t1.net_price_amt
							   else t2.period_mean_net
						end) as promo_net_price_amt,
						(case
					           when t1.nonmiss_flg = 1 then t1.gross_price_amt
							   else t2.period_mean_gross
						end) as promo_gross_price_amt
				from CASUSER.PROMO_BATCH_DAYS_MECH7_1 t1
				left join CASUSER.PROMO_BATCH_DAYS_MECH7_2 t2
					on (t1.pbo_location_id=t2.pbo_location_id and
						t1.product_id=t2.product_id and
						t1.PROMO_ID=t2.PROMO_ID and
						t1.FREQ_NUM=t2.FREQ_NUM)
			;
		quit;

/* 		=-=-=-=-=-=-=-=-=-=MECHANICS №7 END-=-=-=-=-=-=-=-=-= */


	/* Объединение всех посчиатанных промо механик в одну таблцицу*/
		data CASUSER.PROMO_PRICE_ALL_MECHANICS(drop=promo_net_price_amt promo_gross_price_amt);
			set CASUSER.PRICE_BATCH_DAYS_MECH1210_2 (keep=promo_id product_id pbo_location_id day_dt promo_net_price_amt promo_gross_price_amt)
				CASUSER.PROMO_BATCH_DAYS_MECH345_5 (keep=promo_id product_id pbo_location_id day_dt promo_net_price_amt promo_gross_price_amt)
				CASUSER.PROMO_BATCH_DAYS_MECH68_5 (keep=promo_id product_id pbo_location_id day_dt promo_net_price_amt promo_gross_price_amt)
				CASUSER.PROMO_BATCH_DAYS_MECH7_3 (keep=promo_id product_id pbo_location_id day_dt promo_net_price_amt promo_gross_price_amt)
			;
			where day_dt between &VF_HIST_START_DT_SAS. and &VF_HIST_END_DT_SAS;			
			if promo_net_price_amt = . then do;
				net_price_amt = promo_net_price_amt;
				gross_price_amt = promo_gross_price_amt;
			end;
			else do;
				net_price_amt = round(promo_net_price_amt, 0.01);
				gross_price_amt = round(promo_gross_price_amt, 0.01);
			end;
		run;
		
		/* Переход от подневной гранулярности к периодной */
		
		data CASUSER.PROMO_INTERVALS(rename=(price_net=net_price_amt price_gro=gross_price_amt));
			set CASUSER.PROMO_PRICE_ALL_MECHANICS;
			by promo_id pbo_location_id product_id day_dt;
			keep promo_id pbo_location_id product_id start_dt end_dt price_net price_gro;
			format start_dt end_dt date9.;
			retain start_dt end_dt price_net price_gro l_gross_price;
			
			l_gross_price = lag(gross_price_amt);
			l_day_dt = lag(day_dt);
			
			/*первое наблюдение в ряду - сбрасываем хар-ки интервала*/
			if first.product_id then do;
				start_dt = day_dt;
				end_dt =.;
				price_net = net_price_amt;
				price_gro = gross_price_amt;
				l_gross_price = .z;
				l_day_dt = .;
			end;
			
			/*сбрасываем текущий интервал, готовим следующий*/
			if (gross_price_amt ne l_gross_price or l_day_dt ne day_dt-1) and not first.product_id then do;
				end_dt = l_day_dt;
				output;
				start_dt = day_dt;
				end_dt = .;
				price_net = net_price_amt;
				price_gro = gross_price_amt;
			end;
			if last.product_id then do;
				end_dt = day_dt;
				output;
			end;
		run;

	/* 	Накопление результативной таблицы */
		%if &lmvIterCounter. = 1 %then %do;
			data CASUSER.&lmvOutTableName;
				set CASUSER.PROMO_INTERVALS;
			run;
		%end;
		%else %do;
			data CASUSER.&lmvOutTableName;
				set CASUSER.&lmvOutTableName
					CASUSER.PROMO_INTERVALS;
			run;
		%end;
		
		%let lmvIterCounter = %eval(&lmvIterCounter. + 1);
		data _NULL_;
			if 0 then set CASUSER.PBO_USED nobs=n;
			call symputx('lmvPboUsedNum',n);
			stop;
		run;

	%end;

	proc casutil;
		promote casdata="&lmvOutTableName" incaslib="casuser" outcaslib="&lmvOutTableCLib";
	run;


	proc casutil;  
		droptable casdata="PROMO_FILT_SKU_PBO" incaslib="CASUSER" quiet;
		droptable casdata="PBO_USED" incaslib="CASUSER" quiet;
		droptable casdata="pbo_list_tmp" incaslib="CASUSER" quiet;
		droptable casdata="pbo_list" incaslib="CASUSER" quiet;
		droptable casdata="PBO_LIST_BATCH" incaslib="CASUSER" quiet;
		droptable casdata="PBO_USED" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_FILT_SKU_PBO_BATCH" incaslib="CASUSER" quiet;
		droptable casdata="PRICE_BATCH" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_FILT_SKU_PBO_BATCH_DAYS" incaslib="CASUSER" quiet;
		droptable casdata="PRICE_BATCH_DAYS" incaslib="CASUSER" quiet;
		droptable casdata="PRICE_BATCH_DAYS_MECH1210_1" incaslib="CASUSER" quiet;
		droptable casdata="PRICE_BATCH_DAYS_MECH1210_2" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_BATCH_DAYS_MECH345_1" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_BATCH_DAYS_MECH345_DICT" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_BATCH_DAYS_MECH345_2" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_BATCH_DAYS_MECH345_3" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_BATCH_DAYS_MECH345_4" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_BATCH_DAYS_MECH345_5" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_BATCH_DAYS_MECH68_1" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_BATCH_DAYS_MECH68_DICT" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_BATCH_DAYS_MECH68_2" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_BATCH_DAYS_MECH68_3" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_BATCH_DAYS_MECH68_4" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_BATCH_DAYS_MECH68_5" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_BATCH_DAYS_MECH7_1" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_BATCH_DAYS_MECH7_2" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_BATCH_DAYS_MECH7_3" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_PRICE_ALL_MECHANICS" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_INTERVALS" incaslib="CASUSER" quiet;
	run;

%mend price_promo_past;
