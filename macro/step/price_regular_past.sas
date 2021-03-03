
%macro price_regular_past(mpOutTable=, mpBatchValue=);

	%local lmvPromoList
		   lmvPromoProductIds
		   lmvIterCounter
		   lmvPboUsedNum
		   lmvPboTotalNum
		   lmvOutTableName
		   lmvOutTableCLib
		   lmvBatchValue
		   lmvCheckNobs
		;

	%member_names (mpTable=&mpOutTable, mpLibrefNameKey=lmvOutTableCLib, mpMemberNameKey=lmvOutTableName);
	
	%let lmvBatchValue = &mpBatchValue.;
	
	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;
	
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
		create table CASUSER.PROMO_FILT_SKU_PBO{options replace=true} as
			select t1.channel_cd,
				t1.promo_id,
				t3.product_id,
				t2.pbo_location_id,
				t1.start_dt,
				t1.end_dt,
				t1.promo_mechanics,
				1 as promo_flag
		from CASUSER.PROMO t1
		inner join casuser.PROMO_PBO t2
			on t1.promo_id = t2.promo_id
		inner join CASUSER.PROMO_PROD t3
			on t1.promo_id = t3.promo_id
		where t1.promo_mechanics in &lmvPromoList
			and t1.channel_cd = 'ALL'
		;
	quit;

	/* Фильтрация цен от введенных промо товаров*/
	proc fedsql sessref=casauto noprint;
		create table CASUSER.PRICE_FILT{options replace=true} as
			select t1.product_id,
                t1.pbo_location_id,
                t1.start_dt,
                t1.end_dt,
                t1.net_price_amt,
                t1.gross_price_amt
		from CASUSER.PRICE t1
		where t1.product_id not in &lmvPromoProductIds
		;
	quit;

	/* Создание пустой таблицы айдишников ПБО, в которой будут храниться уже посчитанные */
	data CASUSER.PBO_USED(keep=pbo_location_id used_flag);
		set CASUSER.PRICE_FILT;
		where pbo_location_id < -1000;
		used_flag = 1;
	run;

	proc fedsql sessref=casauto noprint;
		create table CASUSER.PBO_LIST_TMP{options replace=true} as
			select distinct t1.pbo_location_id
			from CASUSER.PRICE_FILT t1
		;
	quit;

	data _NULL_;
		if 0 then set CASUSER.PBO_USED nobs=n;
		call symputx('lmvPboUsedNum', n);
		stop;
	run;
	data _NULL_;
		if 0 then set CASUSER.PBO_LIST_TMP nobs=n;
		call symputx('lmvPboTotalNum', n);
		stop;
	run;
	%let lmvIterCounter = 1;

	%do %while (&lmvPboUsedNum. < &lmvPboTotalNum.);

		/* Создание батча PBO start */
		proc fedsql sessref=casauto noprint;
			create table CASUSER.PBO_LIST{options replace=true} as
				select t1.pbo_location_id
				from CASUSER.PBO_LIST_TMP t1
				left join CASUSER.PBO_USED t2
					on t1.pbo_location_id=t2.pbo_location_id
				where t2.used_flag = . /*исключение уже посчитанных*/
			;
		quit;
		data CASUSER.PBO_LIST_BATCH;
			set CASUSER.PBO_LIST(obs=&lmvBatchValue.);
			used_flag = 1;
		run;

		proc casutil;droptable casdata="PBO_LIST" incaslib="CASUSER" quiet;run;

		/* Добавление в список посчитанных айдишников ПБО */
		data CASUSER.PBO_USED(append=yes);
			set CASUSER.PBO_LIST_BATCH;
		run;

		proc fedsql sessref=casauto noprint;
			create table CASUSER.PROMO_FILT_SKU_PBO_BATCH{options replace=true} as
				select t1.*
			from CASUSER.PROMO_FILT_SKU_PBO t1
			inner join CASUSER.PBO_LIST_BATCH t2
				on t1.pbo_location_id=t2.pbo_location_id
			;
		quit;

		proc fedsql sessref=casauto noprint;
			create table CASUSER.PRICE_BATCH{options replace=true} as
				select t1.*
			from CASUSER.PRICE_FILT t1
			inner join CASUSER.PBO_LIST_BATCH t2
				on t1.pbo_location_id=t2.pbo_location_id
			;
		quit;

        proc casutil;droptable casdata="PBO_LIST_BATCH" incaslib="CASUSER" quiet;run;
		/* Создание батча PBO end */
		
		
		/* Переход от start_dt end_dt интеревалов к подневному списку в ПРОМО разметке*/
		data CASUSER.PROMO_FILT_SKU_PBO_BATCH_DAYS(rename=(start_dt=day_dt) drop=end_dt);
			set CASUSER.PROMO_FILT_SKU_PBO_BATCH;
			output;
			do while ((start_dt < end_dt) and (start_dt < &VF_HIST_END_DT_SAS.));
				start_dt = intnx('days', start_dt, 1);
				output;
			end;
		run;

        proc casutil;droptable casdata="PROMO_FILT_SKU_PBO_BATCH" incaslib="CASUSER" quiet;run;

		/* Переход от start_dt end_dt интеревалов к подневному списку в ФАКТИЧЕСКИХ ценах */
		data CASUSER.PRICE_BATCH_DAYS(rename=(start_dt=day_dt) drop=end_dt);
			set CASUSER.PRICE_BATCH;
			output;
			do while ((start_dt < end_dt) and (start_dt < &VF_HIST_END_DT_SAS.));
				start_dt = intnx('days', start_dt, 1);
				output;
			end;
		run;

        proc casutil;droptable casdata="PRICE_BATCH" incaslib="CASUSER" quiet;run;
		
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
				on t1.pbo_location_id = t2.pbo_location_id
					and t1.product_id = t2.product_id
					and t1.day_dt = t2.day_dt
			;
		quit;

        proc casutil;droptable casdata="PROMO_FILT_SKU_PBO_BATCH_DAYS" incaslib="CASUSER" quiet;run;
		
		/* Продление каждого ВР без лидирующих и хвостовых заполнений, т.е. trimId="BOTH" */
		
		proc cas;
			timeData.timeSeries result =r /
			series={{name="net_price_amt_new", Acc="sum", setmiss="PREV"},
			{name="gross_price_amt_new", Acc="sum", setmiss="PREV"}}
			tEnd= "&VF_FC_AGG_END_DT"
			table={caslib="CASUSER" ,name="PRICE_BATCH_DAYS_1", groupby={"pbo_location_id","product_id"},
			where="day_dt<=&VF_HIST_END_DT_SAS"}
			timeId="day_dt"
			interval="days"
			trimId="BOTH"
			casOut={caslib="CASUSER",name="PRICE_BATCH_DAYS_2", replace=True}
			;
		run;
		quit;

	    proc casutil;droptable casdata="PRICE_BATCH_DAYS_1" incaslib="CASUSER" quiet;run;

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
    				from CASUSER.PRICE_BATCH_DAYS_2
    				group by product_id,
    					pbo_location_id) as t2
			    where t2.gross_price_amt_sum = . 
                    or t2.net_price_amt_sum = .
			;
		quit;
		proc fedsql sessref=casauto noprint;
			create table CASUSER.ALL_DAYS_PROMO_1{options replace=true} as
				select t1.product_id,
					t1.pbo_location_id,
					t1.day_dt,
					t1.net_price_amt as net_price_amt_new,
					t1.gross_price_amt as gross_price_amt_new
    			from CASUSER.PRICE_BATCH_DAYS t1
    			inner join CASUSER.ALL_DAYS_PROMO t2
    				on t1.product_id = t2.product_id 
                        and t1.pbo_location_id = t2.pbo_location_id
			;
		quit;

        proc casutil;droptable casdata="PRICE_BATCH_DAYS" incaslib="CASUSER" quiet;run;

		proc fedsql sessref=casauto noprint;
			create table CASUSER.PRICE_BATCH_DAYS_3{options replace=true} as
				select t1.*
    			from CASUSER.PRICE_BATCH_DAYS_2 t1
    			left join CASUSER.ALL_DAYS_PROMO t2
    				on t1.product_id = t2.product_id
    					and t1.pbo_location_id = t2.pbo_location_id
    			where t2.all_days_promo_flg = .
			;
		quit;

        proc casutil;droptable casdata="PRICE_BATCH_DAYS_2" incaslib="CASUSER" quiet;run;
        proc casutil;droptable casdata="ALL_DAYS_PROMO" incaslib="CASUSER" quiet;run;

		data CASUSER.PRICE_BATCH_DAYS_4;
			set CASUSER.PRICE_BATCH_DAYS_3
				CASUSER.ALL_DAYS_PROMO_1;
		run;

        proc casutil;droptable casdata="ALL_DAYS_PROMO_1" incaslib="CASUSER" quiet;run;
        proc casutil;droptable casdata="PRICE_BATCH_DAYS_3" incaslib="CASUSER" quiet;run;
        
		/* Обработка случая, когда товар продаётся только во время промо: в этом случае регулярная цена = фактической цене END*/
		
		
		/* Обработка случая, когда товар вводится в промо и протягивать нечем, поэтому регулярная цена равна миссинг. В этом случае, рег цена первой немиссинговой факт цене START*/
		
		/*Создание справочника с минимальной датой продажи и немиссинговой ценой */
		data CASUSER.PRICE_BATCH_DAYS_4_1;
			set CASUSER.PRICE_BATCH_DAYS_4;
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
    				on t1.pbo_location_id = t2.PBO_LOCATION_ID
    					and t1.product_id = t2.product_id
			;
		quit;

        proc casutil;droptable casdata="PRICE_BATCH_DAYS_4" incaslib="CASUSER" quiet;run;
        proc casutil;droptable casdata="PRICE_BATCH_DAYS_4_1" incaslib="CASUSER" quiet;run;
		
		/* Обработка случая, когда товар вводится в промо и протягивать нечем, поэтому регулярная цена равна миссинг. В этом случае, рег цена первой немиссинговой факт цене END*/
		
		/* Идентификация скачков более чем на 5% и их замена на предыдущее значение цены */
		data CASUSER.PRICE_BATCH_DAYS_5(keep=product_id PBO_LOCATION_ID day_dt net_price_amt gross_price_amt);
			set CASUSER.PRICE_BATCH_DAYS_4_2;
			by pbo_location_id product_id day_dt;
			retain prev_net;
			retain prev_gross;
		
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

        proc casutil;droptable casdata="PRICE_BATCH_DAYS_4_2" incaslib="CASUSER" quiet;run;
		
		/* Округление регулярных цен до целого числа и фильтрация дат по открытию или полному закрытию ПБО.*/

		proc fedsql sessref=casauto noprint;
			create table CASUSER.PRICE_BATCH_DAYS_6{options replace=true} as
				select t1.product_id,
					t1.pbo_location_id,
					t1.day_dt,
					t2.a_open_date,
					round(t1.net_price_amt) as net_price_amt,
					round(t1.gross_price_amt) as gross_price_amt
    			from CASUSER.PRICE_BATCH_DAYS_5 t1
    			left join CASUSER.PBO_DICTIONARY t2
    				on t1.pbo_location_id=t2.pbo_location_id
    			where t2.a_open_date is not null
                    and t1.day_dt between t2.a_open_date
                    and coalesce(t2.a_close_date, date%str(%')&VF_FC_AGG_END_DT.%str(%')) 
			;
		quit;
		
        proc casutil;droptable casdata="PRICE_BATCH_DAYS_5" incaslib="CASUSER" quiet;run;
		
		data _NULL_;
			if 0 then set CASUSER.PRICE_BATCH_DAYS_6 nobs=n;
			call symputx('lmvCheckNobs',n);
			stop;
		run;
		
		%if &lmvCheckNobs. > 0 %then %do;
			
			/* Переход от подневной гранулярности к периодной */

			data CASUSER.REG_INTERVALS(rename=(price_net=net_price_amt price_gro=gross_price_amt));
				set CASUSER.PRICE_BATCH_DAYS_6;
				by pbo_location_id product_id a_open_date day_dt;
				keep pbo_location_id product_id a_open_date start_dt end_dt price_net price_gro;
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

			proc casutil;droptable casdata="PRICE_BATCH_DAYS_6" incaslib="CASUSER" quiet;run;
			
			data WORK.REG_INTERVALS;
				set CASUSER.REG_INTERVALS;
			run;
			
			proc casutil;droptable casdata="REG_INTERVALS" incaslib="CASUSER" quiet;run;
			
			/*Обработка неоцифрованных промо во время открытия ПБО START*/
			proc sort data=WORK.REG_INTERVALS;
				by pbo_location_id a_open_date product_id start_dt end_dt;
			run;
			
			proc expand data=WORK.REG_INTERVALS out=CASUSER.REG_INTERVALS_1;
				convert gross_price_amt = lead_gross_price /transformout = (lead 1);
				convert start_dt = lead_start_dt /transformout = (lead 1);
				by pbo_location_id a_open_date product_id;
			run;

			proc sql;drop table WORK.REG_INTERVALS;quit;  

			data CASUSER.REG_INTERVALS_2(drop=a_open_date lead_gross_price lead_start_dt);
				set CASUSER.REG_INTERVALS_1;
				by pbo_location_id product_id start_dt end_dt;
				retain promo_open_flag;

				if  start_dt = a_open_date and
					end_dt - start_dt < 6 and
					missing(gross_price_amt) = 0 and
					missing(lead_gross_price) = 0 and
					missing(lead_start_dt) = 0 and
					intnx('day', lead_start_dt, -1) = end_dt and
					gross_price_amt < lead_gross_price * 0.95
					
				then promo_open_flag = 1;
						
				/*Сдвигаем начало интервала на дату открытия ПБО.*/
				if 'TIME'n = 1 and promo_open_flag = 1 then do;
					start_dt = a_open_date;
					promo_open_flag = .;
				end;
			run;

			proc casutil;droptable casdata="REG_INTERVALS_1" incaslib="CASUSER" quiet;run;
			
			data WORK.REG_INTERVALS_3(drop='TIME'n);
				set CASUSER.REG_INTERVALS_2;
				
				/*Убираем интервалы, которые были перекрыты в предыдущем степе*/
				if 'TIME'n = 0 and promo_open_flag = 1 then delete;
			run;

			proc casutil;droptable casdata="REG_INTERVALS_2" incaslib="CASUSER" quiet;run;
			
			/*Обработка неоцифрованных промо во время открытия ПБО END*/
			
			/*Обработка колебаний цен вниз окном. Если есть скачки кратковременные скачки вниз, 
				а потом возвращение на прежнюю цену, то скачок цены игнорируется START*/
			proc sort data=WORK.REG_INTERVALS_3(drop=promo_open_flag);
				by pbo_location_id product_id start_dt end_dt;
			run;

			proc expand data=WORK.REG_INTERVALS_3 out=CASUSER.REG_INTERVALS_4;
				convert gross_price_amt = lag_gross_price /transformout = (lag 1);
				convert gross_price_amt = lead_gross_price /transformout = (lead 1);
				by pbo_location_id product_id;
			run;

			proc sql;drop table WORK.REG_INTERVALS_3;quit;

			data CASUSER.REG_INTERVALS_5;
				set CASUSER.REG_INTERVALS_4;
				by pbo_location_id product_id start_dt end_dt;
				retain change_price_flag;
				
				if first.product_id then change_price_flag = 0;
				
				if missing(lag_gross_price) = 0 and 
				   missing(lead_gross_price) = 0 and
				   change_price_flag = 0 and 
				   gross_price_amt ne lag_gross_price and
				   lag_gross_price - lead_gross_price < 0.0001 and
				   end_dt - start_dt le 3
				   
				then do;
				   change_price_flag = 1;
				   gross_price_amt = lag_gross_price;
				end;
				else change_price_flag = 0;
			run;

			proc casutil;droptable casdata="REG_INTERVALS_4" incaslib="CASUSER" quiet;run;

			data CASUSER.REG_INTERVALS_DAYS(rename=(start_dt=day_dt) keep=product_id pbo_location_id start_dt net_price_amt gross_price_amt);
				set CASUSER.REG_INTERVALS_5;
				output;
				do while (start_dt < end_dt);
					start_dt = intnx('days', start_dt, 1);
					output;
				end;
			run;

			proc casutil;droptable casdata="REG_INTERVALS_5" incaslib="CASUSER" quiet;run;
			
			data CASUSER.INTERVALS(rename=(price_net=net_price_amt price_gro=gross_price_amt));
				set CASUSER.REG_INTERVALS_DAYS;
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

			proc casutil;droptable casdata="REG_INTERVALS_DAYS" incaslib="CASUSER" quiet;run;

			/*Обработка колебаний цен вниз окном. Если есть скачки кратковременные скачки вниз, 
				а потом возвращение на прежнюю цену, то скачок цены игнорируется END*/		

			data CASUSER.&lmvOutTableName(append=yes);
				set CASUSER.INTERVALS;
			run;

			proc casutil;droptable casdata="INTERVALS" incaslib="CASUSER" quiet;run;
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
		droptable casdata="PBO_LIST_TMP" incaslib="CASUSER" quiet;
		droptable casdata="pbo_list" incaslib="CASUSER" quiet;
	run;

%mend price_regular_past;
