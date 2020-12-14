/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки данных в PT
*		До запуска должна быть сформирована таблица work.&TableNm. с полями member_nm, member_rk...в формате PT
*
*  ПАРАМЕТРЫ:
*     Нет
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
*     load_pt_dict(mpDatetime=&mvDatetime.
*					,mpMemberTableNm = pt.product
*					);
*
****************************************************************************
*  25-08-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro load_pt_dict(mpDatetime=&mvDatetime.
					,mpMemberTableNm = pt.product
					);

	libname PT_BKP "/data/PT_BKP";

	%local lmvLibrefIn lmvTabNmIn lmvDatetime lmvReportDttm;
	%member_names (mpTable=&mpMemberTableNm, mpLibrefNameKey=lmvLibrefIn, mpMemberNameKey=lmvTabNmIn);
	%let lmvReportDttm = &ETL_CURRENT_DTTM.;
	%let lmvDatetime = &mpDatetime.;

	%if %sysfunc(SESSFOUND(casauto)) = 0 %then %do; 
		cas casauto;
		caslib _all_ assign;
	%end;

	/*calc hash for old and new batch + check for diffs */
	proc casutil ;
		droptable casdata="&lmvTabNmIn._batch" incaslib="CASUSER" quiet;
		droptable casdata="&lmvTabNmIn._old" incaslib="CASUSER" quiet;
		droptable casdata="hashjoin_config_table" incaslib="CASUSER" quiet;
		droptable casdata="&lmvTabNmIn._delta" incaslib="CASUSER" quiet;
	run;
	
	/* get new batch, calc in sas*/
	data CASUSER.&lmvTabNmIn._batch(replace=yes);
		set &lmvTabNmIn.;
		length hash_key_id hash_value_id $32;
		format hash_key_id hash_value_id $hex32.;
		valid_from_dttm = &lmvDatetime.;
		valid_to_dttm =&ETL_SCD_FUTURE_DTTM.;
		hash_key_id = SHA256HEX(catx('_',member_rk));
		hash_value_id= SHA256HEX(catx('_',member_nm));
	run;
	
	/* calc hash for old data */
	data CASUSER.&lmvTabNmIn._old(replace=yes);
		set &lmvLibrefIn..&lmvTabNmIn.;
		length hash_key_id hash_value_id $32;
		format hash_key_id hash_value_id $hex32.;
		hash_key_id = SHA256HEX(catx('_',member_rk));
		hash_value_id= SHA256HEX(catx('_',member_nm));
	run;
	
	proc fedsql SESSREF=casauto noprint;
		create table casuser.hashjoin_config_table{options replace = true} as
			select distinct n.hash_key_id, n.hash_value_id, o.valid_from_dttm, n.member_nm,
					case 
						when n.hash_value_id = o.hash_value_id
						then 2 /*same values*/
						when n.hash_value_id <> o.hash_value_id and o.hash_value_id <> ' '
						then 3 /*diff values*/
						else 1 /*new*/
					end as flag 
			from CASUSER.&lmvTabNmIn._batch n
				left join CASUSER.&lmvTabNmIn._old o 
					on n.hash_key_id = o.hash_key_id
					and o.valid_to_dttm = timestamp'5999-01-01 00:00:00'
		;
		/*extract delta*/
		create table casuser.&lmvTabNmIn._delta as
			select distinct mn.member_nm
							,mn.member_rk
							,mn.order_no
							,mn.valid_from_dttm
							,mn.valid_to_dttm
			from CASUSER.&lmvTabNmIn._batch mn
			left join casuser.hashjoin_config_table h
				on mn.hash_key_id=h.hash_key_id
			where h.flag = 1
		;
	quit;
	
	/*create table for dcl hash*/
	data work.hashjoin_table_difference(drop=flag member_nm);
		length member_nm_n $100;
		set CASUSER.HASHJOIN_CONFIG_TABLE(where=(FLAG=3) keep=flag member_nm hash_key_id);
		member_nm_n=member_nm;
	run;
	
	/* update with hash */
	/* data &lmvLibrefIn..&lmvTabNmIn.(drop=rc hash_key_id); */
	data work.&lmvTabNmIn. (drop=rc hash_value_id member_nm_n hash_key_id);
		length hash_key_id $32 member_nm_n $100;
		if _n_=1 then do;
			DECLARE HASH H (DATASET:"work.hashjoin_table_difference");
			RC=H.DEFINEKEY("hash_key_id");
			RC=H.DEFINEDATA("member_nm_n");
			RC=H.DEFINEDONE();
			call missing(hash_key_id,member_nm_n);
		end;
		set &lmvLibrefIn..&lmvTabNmIn.;
		hash_key_id = SHA256HEX(catx('_',member_rk));
		rc=h.find();
		if (rc=0) then do;
			member_nm = member_nm_n; 
			valid_from_dttm = &lmvDatetime.;
			valid_to_dttm = &ETL_SCD_FUTURE_DTTM.;
		end;
	run;

	PROC SQL NOPRINT;	
		CONNECT TO POSTGRES AS CONN (server="10.252.151.3" port=5452 user=pt password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=pt defer=yes readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=8192");
			/* truncate target table in PT PG schema */
			EXECUTE BY CONN
				(
					TRUNCATE TABLE public.&lmvTabNmIn.
				)
			;
			DISCONNECT FROM CONN;
	QUIT;

	proc append base=&lmvLibrefIn..&lmvTabNmIn. data=work.&lmvTabNmIn. force; 
	run; 

	/* add delta */
	proc append base=&lmvLibrefIn..&lmvTabNmIn. data=casuser.&lmvTabNmIn._delta force; 
	run;

%mend load_pt_dict;