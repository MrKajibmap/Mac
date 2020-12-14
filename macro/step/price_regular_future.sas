%macro price_regular_future(mpPriceRegPastTab=, mpOutTable=);
	%local lmvOutTableName
		   lmvOutTableCLib
		;
		
	%member_names (mpTable=&mpOutTable, mpLibrefNameKey=lmvOutTableCLib, mpMemberNameKey=lmvOutTableName);
	
	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;

	proc casutil;  
		droptable casdata="&lmvOutTableName" incaslib="&lmvOutTableCLib" quiet;
	run;
	
	proc fedsql sessref=casauto;
		create table casuser.last_reg_price{options replace=true} as
			select t1.product_id,
				t1.pbo_location_id,
				t1.net_price_amt,
				t1.gross_price_amt
			from &mpPriceRegPastTab t1
			left join (
				select product_id,
					pbo_location_id,
					max(end_dt) as max_end_dt
				from &mpPriceRegPastTab
				group by product_id,
					pbo_location_id
				) t2
				on (t1.product_id=t2.product_id and
					t1.pbo_location_id=t2.pbo_location_id)
			where t1.end_dt = t2.max_end_dt
		;
	quit;
	
	data casuser.&lmvOutTableName;
		set casuser.last_reg_price;
		format start_dt end_dt date9.;
		start_dt = &VF_FC_START_DT_SAS.;
		end_dt = &VF_FC_AGG_END_DT_SAS.;
	run;

	proc casutil;
		promote casdata="&lmvOutTableName" incaslib="casuser" outcaslib="&lmvOutTableCLib";
	run;
	
	proc casutil;  
		droptable casdata="last_reg_price" incaslib="casuser" quiet;
	run;

%mend price_regular_future;