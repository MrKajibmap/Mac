%macro load_komp_matrix;
	
	proc sql noprint;
		create table pbo_open_dt as
			select distinct pbo_location_id, 
					/* mdy(input(substr(pbo_loc_attr_value,4,2),base2.), input(substr(pbo_loc_attr_value,1,2),base2.),input(substr(pbo_loc_attr_value,7,4), base4.))*/
					input(pbo_loc_attr_value, DDMMYY10.) format=date9. as start_dt
			from etl_ia.pbo_loc_attributes
			where PBO_LOC_ATTR_NM = 'OPEN_DATE'
			;
	quit;

	data casuser.pbo_open_dt (replace=yes );
		set pbo_open_dt;
	run;
	data casuser.calendar(drop=i);
		format day month week date9.;
		do i=intnx("year", date(), 0, "b") to intnx("year", date(), 3, "e");
			day = i;
			week = intnx("week.2", day, 0, "b");
			month = intnx("month", day, 0, "b");
			output;
		end;
	run;

	proc fedsql sessref=casauto;
		create table casuser.pbo_date_list{options replace=true} as
			select distinct pbo_location_id
							, start_dt
							, month
			from casuser.pbo_open_dt t1
				cross join
			(select month
				from casuser.calendar) t2
		;
	quit;

	proc fedsql sessref=casauto;
		create table casuser.komp_matrix{options replace=true} as
			select pbo_location_id
					, start_dt
					, month 
					,case 
						when (month - start_dt) >= 365 
						then 1 else 0 
					end as komp,
					case 
						when (month - start_dt) < 365 
						then 1 else 0
					end as non_komp
			from casuser.pbo_date_list
		;
		create table casuser.komp_matrix{options replace=true} as
			select PBO_LOCATION_ID
					,start_dt
					, max(KOMP) as komp
					, max(NON_KOMP) as non_komp
			from casuser.komp_matrix 
			group by PBO_LOCATION_ID, start_dt
			;
	quit;

%mend load_komp_matrix;