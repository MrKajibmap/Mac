%macro etl_cfg_load_dicts;
	%include "/opt/sas/mcd_config/config/initialize_global.sas"; 

	cas casauto sessopts=(metrics=true);
	caslib _ALL_ ASSIGN;

	/* load schedule_rule */
	FILENAME REFFILE DISK '/data/files/static/etl_cfg/SF_CFG_SCHEDULE_RULE.csv';

	PROC IMPORT DATAFILE=REFFILE
		DBMS=CSV
		OUT=WORK.SCHEDULE_RULE;
		GETNAMES=YES;
	delimiter=";";
	RUN;
	/*
	proc sql noprint;
		create table work.SCHEDULE_RULE_except as
			select rule_cond, rule_desc, rule_nm, rule_start_hour from WORK.SCHEDULE_RULE
				except 
			select  rule_cond, rule_desc, rule_nm, rule_start_hour from etl_cfg.cfg_SCHEDULE_RULE
		;
		select max(max(rule_id),0) as max into :mvMaxRuleId
		from etl_cfg.cfg_SCHEDULE_RULE;
	quit;

	data work.schedule_rule_delta / single=yes;
		set work.SCHEDULE_RULE_except;
		rule_id = &mvMaxRuleId + _N_;
	run;
	*/
	data work.schedule_rule_delta / single=yes;
		set work.SCHEDULE_RULE;
		rule_id = monotonic();
	run;
	
	PROC SQL NOPRINT;	
		CONNECT TO POSTGRES AS CONN (server="10.252.151.3" port=5452 user=etl_cfg password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=etl defer=yes readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=8192");
			/* truncate target table in PT PG schema */
			EXECUTE BY CONN
				(
					TRUNCATE TABLE etl_cfg.cfg_schedule_rule
				)
			;
			DISCONNECT FROM CONN;
	QUIT;
	
	proc append base=etl_cfg.cfg_SCHEDULE_RULE
				data=work.schedule_rule_delta
				force;
	run;

	/* load resource */

	FILENAME REFFILE DISK '/data/files/static/etl_cfg/SF_CFG_RESOURCE.csv';

	PROC IMPORT DATAFILE=REFFILE
		DBMS=CSV
		OUT=WORK.RESOURCE;
		GETNAMES=YES;
	delimiter=";";
	RUN;
/*
	proc sql noprint;
		create table work.RESOURCE_except as
			select forced_load_flag, macro_nm, module_nm, resource_nm from WORK.RESOURCE
				except 
			select forced_load_flag, macro_nm, module_nm, resource_nm  from etl_cfg.cfg_RESOURCE
		;
		select max(max(resource_id),0) as max into :mvMaxResId
		from etl_cfg.cfg_RESOURCE;
	quit;

	data work.RESOURCE_delta / single=yes;
		set work.RESOURCE_except;
		resource_id = &mvMaxResId + _N_;
	run;
*/
	data work.RESOURCE_delta / single=yes;
		set work.RESOURCE;
		resource_id = monotonic();
	run;
	
	PROC SQL NOPRINT;	
		CONNECT TO POSTGRES AS CONN (server="10.252.151.3" port=5452 user=etl_cfg password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=etl defer=yes readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=8192");
			/* truncate target table in PT PG schema */
			EXECUTE BY CONN
				(
					TRUNCATE TABLE etl_cfg.cfg_resource
				)
			;
			DISCONNECT FROM CONN;
	QUIT;
	
	proc append base=etl_cfg.cfg_RESOURCE
				data=work.RESOURCE_delta
				force;
	run;

%mend etl_cfg_load_dicts;