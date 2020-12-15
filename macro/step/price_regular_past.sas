%macro price_regular_past(mpOutTable=, mpBatchValue=);
	%local lmvPromoList
		   lmvPromoProductIds
		   lmvIterCounter
		   lmvPboUsedNum
		   lmvPboTotalNum
		   lmvOutTableName
		   lmvOutTableCLib
		   lmvBatchValue
		;
		
	%member_names (mpTable=&mpOutTable, mpLibrefNameKey=lmvOutTableCLib, mpMemberNameKey=lmvOutTableName);
	
	%let lmvBatchValue = &mpBatchValue.;
	
	%let lmvInLib=ETL_IA;
	%let ETL_CURRENT_DT = %sysfunc(date());
	%let ETL_CURRENT_DTTM = %sysfunc(datetime());
	%let lmvReportDt=&ETL_CURRENT_DT.;
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	
	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;

	/* Подготовка входных данных */
	*%add_promotool_marks(mpIntLibref=casuser,mpExtLibref=pt);
	%add_promotool_marks(mpOutCaslib=casuser,
							mpPtCaslib=pt);
							
	proc casutil;
	  droptable casdata="promo" incaslib="casuser" quiet;
	  droptable casdata="promo_pbo" incaslib="casuser" quiet;
	  droptable casdata="promo_prod" incaslib="casuser" quiet;
	run;
	
	data CASUSER.promo (replace=yes);
		/* set &lmvInLib..promo(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.)); */
		set CASUSER.promo_enh;
	run;
	
	data CASUSER.promo_x_pbo (replace=yes);
		/* set &lmvInLib..promo_x_pbo(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.)); */
		set CASUSER.promo_pbo_enh;
	run;
	
	data CASUSER.promo_x_product (replace=yes);
		/* set &lmvInLib..promo_x_product(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.)); */
		set casuser.promo_prod_enh;
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.promo {options replace=true} as 
		select CHANNEL_CD
		,PROMO_ID
		,PROMO_GROUP_ID
		,PROMO_MECHANICS
		,PROMO_NM
		,SEGMENT_ID
		,PROMO_PRICE_AMT
		,NP_GIFT_PRICE_AMT
		,start_dt
		,end_dt
		from casuser.promo
		where start_dt is not null and end_dt is not null
		;
	quit;

	proc fedsql sessref=casauto noprint;
		create table casuser.promo_pbo {options replace=true} as 
		select PBO_LOCATION_ID,PROMO_ID
		from casuser.promo_X_PBO
		;
	quit;

	proc fedsql sessref=casauto noprint;
		create table casuser.promo_prod {options replace=true} as 
		select GIFT_FLAG,OPTION_NUMBER,PRODUCT_ID,PRODUCT_QTY,PROMO_ID
		from casuser.promo_X_PRODUCT
		;
	quit;
	
		proc casutil;
	  droptable casdata="price" incaslib="casuser" quiet;
	run;
	
	data CASUSER.PRICE (replace=yes drop=valid_from_dttm valid_to_dttm);
		set ETL_IA.PRICE(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc fedsql sessref=casauto noprint;
		create table casuser.PRICE{options replace=true} as
		select 
		PRODUCT_ID
		,PBO_LOCATION_ID
		,PRICE_TYPE
		,NET_PRICE_AMT
		,GROSS_PRICE_AMT
		,START_DT
		,END_DT
		from casuser.PRICE
		;
	quit;
	
	proc casutil;  
		droptable casdata="&lmvOutTableName" incaslib="&lmvOutTableCLib" quiet;
	run;

	%let lmvPromoList = ('Discount', 'BOGO / 1+1', 'N+1', '1+1%', 'EVM/Set', 'Pairs');
	
	/* Временное объявление макропеременной. Будет исправлено, с появлением мэппинга промо скю и регулярных скю*/
	%let lmvPromoProductIds = (1302, 2013, 2021, 2040, 2049, 2063, 2067, 2068, 2069, 2074, 2076, 2077, 2078, 2104, 2124, 2145, 2146, 2148, 2149, 2150, 2151, 2152, 2153, 2154, 2155, 2164, 2165,
						    2166, 2167, 2168, 2172, 2173, 2175, 2176, 2178, 2189, 2190, 2191, 2193, 2194, 2198, 2199, 2235, 2243, 2244, 2245, 2246, 2248, 2251, 2253, 2254, 2304, 2317, 2318,
							2319, 2342, 2345, 2346, 2348, 2357, 2360, 2361, 2362, 2363, 2414, 2416, 2451, 2506, 2524, 2529, 3001, 3039, 3044, 3045, 3046, 3047, 3053, 3054, 3055, 3056, 3059,
							3060, 3063, 3065, 3072, 3073, 3074, 3075, 3076, 3077, 3078, 3105, 3106, 3107, 3108, 3110, 3500, 3501, 3502, 3503, 3504, 3505, 3506, 3507, 3508, 3650, 4011, 4036,
							4040, 4042, 4052, 4055, 4057, 4063, 4117, 4153, 4163, 4164, 4166, 4187, 4188, 4189, 4195, 4196, 4199, 4212, 4213, 4223, 4224, 4227, 4228, 4229, 4232, 4233, 4234,
							5060, 5061, 5062, 5064, 5065, 5125, 5126, 6566, 6567, 6568, 6569, 6570, 6571, 6572, 6573, 6574, 6579, 6580, 6581, 6582, 6583, 6584, 7023, 7024, 7032, 7037, 7041,
							7051, 7059, 7074, 7080, 7081, 7082, 7083, 7084, 7085, 7086, 7089, 7090, 7091, 7092, 7093, 7094, 7103, 7108, 7109, 7111, 7112, 7127, 7129, 7141, 7168, 7169, 7170,
							7171, 7172, 7178, 7179, 7180, 7183, 7184, 7185, 7194, 7195, 7197, 7264, 7265, 7533, 7534, 7536, 7605, 7816, 7883, 7884, 7915, 7916, 7920, 7924, 8210, 8211, 8212,
							8215, 8216, 8217, 8219, 8220, 8221, 8222, 8223, 8224, 8225, 8226, 8227, 8228, 8229, 8230, 8231, 8232, 8233, 8234, 8235, 8236, 8237, 8238, 8239, 8240, 8241, 8242,
							8243, 8246, 8247, 8248, 8249, 8250, 8251, 8252, 8253, 8254, 8697, 8700, 8711, 8715, 8716, 8717, 8718, 8719, 8720, 8721, 8722, 8723, 8738, 8739, 8740, 8741, 8749,
							8750, 8751, 8752, 8753, 8756, 8757, 8758, 8759);

	/* Джойн со справочниками. Создание промо-разметки CHANNEL_CD - SKU - ПБО - период- Флаг_промо */
	proc fedsql sessref=casauto noprint;
		create table casuser.PROMO_FILT_SKU_PBO{options replace=true} as
			select t1.CHANNEL_CD,
				t1.PROMO_ID,
				t3.PRODUCT_ID,
				t2.PBO_LOCATION_ID,
				t1.start_dt,
				t1.end_dt,
				t1.PROMO_MECHANICS,
				1 as promo_flag
		from casuser.PROMO t1
		inner join casuser.PROMO_PBO t2
			on t1.PROMO_ID = t2.PROMO_ID
		inner join casuser.PROMO_PROD t3
			on t1.PROMO_ID = t3.PROMO_ID
		where t1.PROMO_MECHANICS in &lmvPromoList
			and t1.CHANNEL_CD = 'ALL'
		;
	quit;


	/* Фильтрация цен от введенных промо товаров*/
	proc fedsql sessref=casauto noprint;
		create table casuser.PRICE_FILT{options replace=true} as
			select t1.*
		from casuser.PRICE t1
		where t1.PRODUCT_ID not in &lmvPromoProductIds
		;
	quit;

	/* Создание пустой таблицы айдишников ПБО, в которой будут храниться уже посчитанные */
	data CASUSER.PBO_USED (keep=PBO_LOCATION_ID used_flag);
		set CASUSER.PRICE_FILT;
		where PBO_LOCATION_ID < -1000;
		used_flag = 1;
	run;

	proc fedsql sessref=casauto noprint;
		create table CASUSER.pbo_list_tmp{options replace=true} as
			select distinct t1.PBO_LOCATION_ID
			from CASUSER.PRICE_FILT t1
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
				from CASUSER.PRICE_FILT t1
				left join CASUSER.PBO_USED t2
					on (t1.PBO_LOCATION_ID=t2.PBO_LOCATION_ID)
				where t2.used_flag = . /*исключение уже посчитанных*/
			;
		quit;
		data CASUSER.PBO_LIST_BATCH;
			set CASUSER.pbo_list(obs=&lmvBatchValue.);
			used_flag = 1;
		run;
		
		/* Добавление в список посчитанных айдишников ПБО */
		data CASUSER.PBO_USED;
			set CASUSER.PBO_USED CASUSER.PBO_LIST_BATCH;
		run;

		proc fedsql sessref=casauto noprint;
			create table casuser.PROMO_FILT_SKU_PBO_BATCH{options replace=true} as
				select t1.*
			from casuser.PROMO_FILT_SKU_PBO t1
			inner join CASUSER.PBO_LIST_BATCH t2
				on (t1.PBO_LOCATION_ID=t2.PBO_LOCATION_ID)
			;
		quit;
		proc fedsql sessref=casauto noprint;
			create table CASUSER.PRICE_BATCH{options replace=true} as
				select t1.*
			from CASUSER.PRICE_FILT t1
			inner join CASUSER.PBO_LIST_BATCH t2
				on (t1.PBO_LOCATION_ID=t2.PBO_LOCATION_ID)
			;
		quit;
		/* Создание батча PBO end */
		
		
		/* Переход от start_dt end_dt интеревалов к подневному списку в ПРОМО разметке*/
		data casuser.PROMO_FILT_SKU_PBO_BATCH_DAYS(rename=(start_dt=day_dt) keep=CHANNEL_CD PBO_LOCATION_ID PRODUCT_ID PROMO_ID PROMO_MECHANICS PROMO_FLAG start_dt);
			set CASUSER.PROMO_FILT_SKU_PBO_BATCH;
			output;
			do while ((start_dt < end_dt) and (start_dt < &VF_HIST_END_DT_SAS.));
				start_dt = intnx('days', start_dt, 1);
				output;
			end;
		run;
		
		/* Переход от start_dt end_dt интеревалов к подневному списку в ФАКТИЧЕСКИХ ценах */
		data casuser.PRICE_BATCH_DAYS_tmp(rename=(start_dt=day_dt) keep=product_id pbo_location_id start_dt net_price_amt gross_price_amt);
			set CASUSER.PRICE_BATCH;
			output;
			do while ((start_dt < end_dt) and (start_dt < &VF_HIST_END_DT_SAS.));
				start_dt = intnx('days', start_dt, 1);
				output;
			end;
		run;
		
		/*Устранение дублей в casuser.PRICE_BATCH_DAYS из-за дублей в начальных данных Price*/
		proc fedsql sessref=casauto noprint;
			create table CASUSER.PRICE_BATCH_DAYS{options replace=true} as
				select t1.product_id,
					   t1.pbo_location_id,
					   t1.day_dt,
					   mean(t1.net_price_amt) as net_price_amt,
					   mean(t1.gross_price_amt) as gross_price_amt
			from CASUSER.PRICE_BATCH_DAYS_tmp t1
			group by t1.product_id,
					 t1.pbo_location_id,
					 t1.day_dt
			;
		quit;		
		
		/* Джойн с промо-разметкой и проставление миссингов на цены с промо-днем = 1; замена на миссинги цены во время промо*/
		proc fedsql sessref=casauto noprint;
			create table CASUSER.PRICE_BATCH_DAYS_1{options replace=true} as
				select t1.product_id,
					   t1.pbo_location_id,
					   t1.day_dt,
						(case
							when t2.promo_flag is missing then t1.gross_price_amt
							else .
						end) as gross_price_amt_new,
						(case
							when t2.promo_flag is missing then t1.net_price_amt
							else .
						end) as net_price_amt_new,
					   t2.promo_flag
			from CASUSER.PRICE_BATCH_DAYS t1
			left join CASUSER.PROMO_FILT_SKU_PBO_BATCH_DAYS t2
				on (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
					and t1.product_id = t2.product_id
					and t1.day_dt = t2.day_dt)
			;
		quit;
		
		/* Продление каждого ВР без лидирующих и хвостовых заполнений, т.е. trimId="BOTH" */
		
		proc cas;
			timeData.timeSeries result =r /
			series={{name="net_price_amt_new", Acc="sum", setmiss="PREV"},
			{name="gross_price_amt_new", Acc="sum", setmiss="PREV"}}
			tEnd= "&VF_FC_AGG_END_DT"
			table={caslib="casuser",name="PRICE_BATCH_DAYS_1", groupby={"PBO_LOCATION_ID","product_id"},
			where="day_dt<=&VF_HIST_END_DT_SAS"}
			timeId="day_dt"
			interval="days"
			trimId="BOTH"
			casOut={caslib="casuser",name="PRICE_BATCH_DAYS_2", replace=True}
			;
			run;
		quit;
		
		/* Обработка случая, когда товар продаётся только во время промо: в этом случае регулярная цена = фактической цене START*/
		proc fedsql sessref=casauto noprint;
			create table CASUSER.ALL_DAYS_PROMO{options replace=true} as
				select t2.product_id,
					t2.pbo_location_id,
					1 as all_days_promo_flg
			from
				(select product_id,
					   pbo_location_id,
					sum(net_price_amt_new) as net_price_amt_sum,
					sum(gross_price_amt_new) as gross_price_amt_sum
				from casuser.PRICE_BATCH_DAYS_2
				group by product_id,
						 pbo_location_id) as t2
			where (t2.gross_price_amt_sum = .) or (t2.net_price_amt_sum = .)
			;
		quit;
		proc fedsql sessref=casauto noprint;
			create table casuser.ALL_DAYS_PROMO_1{options replace=true} as
				select t1.product_id,
					t1.pbo_location_id,
					t1.day_dt,
					t1.net_price_amt as net_price_amt_new,
					t1.gross_price_amt as gross_price_amt_new
			from casuser.PRICE_BATCH_DAYS t1
			inner join casuser.ALL_DAYS_PROMO t2
				on (t1.product_id = t2.product_id and
					t1.pbo_location_id = t2.pbo_location_id)
			;
		quit;
		proc fedsql sessref=casauto noprint;
			create table casuser.PRICE_BATCH_DAYS_3{options replace=true} as
				select t1.*
			from casuser.PRICE_BATCH_DAYS_2 t1
			left join casuser.ALL_DAYS_PROMO t2
				on (t1.product_id = t2.product_id
					and t1.pbo_location_id = t2.pbo_location_id)
			where t2.all_days_promo_flg = .
			;
		quit;
		data casuser.PRICE_BATCH_DAYS_4;
			set casuser.PRICE_BATCH_DAYS_3
				casuser.ALL_DAYS_PROMO_1;
		run;
		/* Обработка случая, когда товар продаётся только во время промо: в этом случае регулярная цена = фактической цене END*/
		
		
		/* Обработка случая, когда товар вводится в промо и протягивать нечем, поэтому регулярная цена равна миссинг. В этом случае, рег цена первой немиссинговой факт цене START*/
		
		/*Создание справочника с минимальной датой продажи и немиссинговой ценой */
		data casuser.PRICE_BATCH_DAYS_4_1;
			set casuser.PRICE_BATCH_DAYS_4;
			by pbo_location_id product_id day_dt;
			where (net_price_amt_new is not missing) and (gross_price_amt_new is not missing);
			if first.product_id then do;
				first_nonmiss_net_price = net_price_amt_new;
				first_nonmiss_gross_price = gross_price_amt_new;
				output;
			end;
		run;
		
		proc fedsql sessref=casauto noprint;
			create table CASUSER.PRICE_BATCH_DAYS_4_2{options replace=true} as
				select t1.product_id,
					   t1.pbo_location_id,
					   t1.day_dt,
					   t1.net_price_amt_new,
					   t1.gross_price_amt_new,
					   t2.first_nonmiss_net_price,
						(case
							when (t1.net_price_amt_new is missing) and (t1.day_dt < t2.day_dt) then t2.first_nonmiss_net_price
							else t1.net_price_amt_new
						end) as net_price_amt,
						(case
							when (t1.gross_price_amt_new is missing) and (t1.day_dt < t2.day_dt) then t2.first_nonmiss_gross_price
							else t1.gross_price_amt_new
						end) as gross_price_amt
			from CASUSER.PRICE_BATCH_DAYS_4 t1
			left join CASUSER.PRICE_BATCH_DAYS_4_1 t2
				on (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
					and t1.product_id = t2.product_id)
			;
		quit;
		
		/* Обработка случая, когда товар вводится в промо и протягивать нечем, поэтому регулярная цена равна миссинг. В этом случае, рег цена первой немиссинговой факт цене END*/
		
		/* Идентификация скачков более чем на 5% и их замена на предыдущее значение цены */
		data casuser.PRICE_BATCH_DAYS_5(keep=product_id PBO_LOCATION_ID day_dt net_price_amt gross_price_amt);
			set casuser.PRICE_BATCH_DAYS_4_2;
			by PBO_LOCATION_ID product_id day_dt;
			retain prev_net;
			retain prev_gross;
		
			net_price_old = net_price_amt;
			gross_price_old = gross_price_amt;
		
			if first.product_id then do;
				prev_net = coalesce(net_price_amt, -1000);
				prev_gross = coalesce(gross_price_amt, -1000);
			end;
		
			if (prev_gross > coalesce(gross_price_amt, 0)*(1.05)) or (prev_net > coalesce(net_price_amt, 0)*(1.05)) then do;
				alert_flag = 1;
				net_price_amt = prev_net;
				gross_price_amt = prev_gross;
			end;
		
			prev_net = max(prev_net, coalesce(net_price_amt, 0));
			prev_gross = max(prev_gross, coalesce(gross_price_amt, 0));
		run;
		
		/* Округление регулярных цен до целого числа*/
		
		data casuser.PRICE_BATCH_DAYS_6(drop=net_price_amt_tmp gross_price_amt_tmp);
			set casuser.PRICE_BATCH_DAYS_5(rename=(net_price_amt=net_price_amt_tmp gross_price_amt=gross_price_amt_tmp));
			net_price_amt = round(net_price_amt_tmp);
			gross_price_amt = round(gross_price_amt_tmp);
		run;
		
		/* Переход от подневной гранулярности к периодной */
		
		data CASUSER.REG_INTERVALS(rename=(price_net=net_price_amt price_gro=gross_price_amt));
			set CASUSER.PRICE_BATCH_DAYS_6;
			by pbo_location_id product_id day_dt;
			keep pbo_location_id product_id start_dt end_dt price_net price_gro;
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
				set CASUSER.REG_INTERVALS;
			run;
		%end;
		%else %do;
			data CASUSER.&lmvOutTableName;
				set CASUSER.&lmvOutTableName
					CASUSER.REG_INTERVALS;
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
		droptable casdata="PRICE_FILT" incaslib="CASUSER" quiet;
		droptable casdata="PBO_USED" incaslib="CASUSER" quiet;
		droptable casdata="pbo_list_tmp" incaslib="CASUSER" quiet;
		droptable casdata="PBO_USED" incaslib="CASUSER" quiet;
		droptable casdata="pbo_list" incaslib="CASUSER" quiet;
		droptable casdata="PBO_LIST_BATCH" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_FILT_SKU_PBO_BATCH" incaslib="CASUSER" quiet;
		droptable casdata="PRICE_BATCH" incaslib="CASUSER" quiet;
		droptable casdata="PROMO_FILT_SKU_PBO_BATCH_DAYS" incaslib="CASUSER" quiet;
		droptable casdata="PRICE_BATCH_DAYS" incaslib="CASUSER" quiet;
		droptable casdata="PRICE_BATCH_DAYS_1" incaslib="CASUSER" quiet;
		droptable casdata="PRICE_BATCH_DAYS_2" incaslib="CASUSER" quiet;
		droptable casdata="ALL_DAYS_PROMO" incaslib="CASUSER" quiet;
		droptable casdata="ALL_DAYS_PROMO_1" incaslib="CASUSER" quiet;
		droptable casdata="PRICE_BATCH_DAYS_3" incaslib="CASUSER" quiet;
		droptable casdata="PRICE_BATCH_DAYS_4" incaslib="CASUSER" quiet;
		droptable casdata="PRICE_BATCH_DAYS_4_1" incaslib="CASUSER" quiet;
		droptable casdata="PRICE_BATCH_DAYS_4_2" incaslib="CASUSER" quiet;
		droptable casdata="PRICE_BATCH_DAYS_5" incaslib="CASUSER" quiet;
		droptable casdata="PRICE_BATCH_DAYS_6" incaslib="CASUSER" quiet;
		droptable casdata="REG_INTERVALS" incaslib="CASUSER" quiet;
	run;

%mend price_regular_past;