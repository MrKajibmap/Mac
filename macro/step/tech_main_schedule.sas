%include "/opt/sas/mcd_config/config/initialize_global.sas"; 
%put _all_;
cas casauto authinfo="/home/sas/.authinfo" sessopts=(metrics=true);
caslib _ALL_ ASSIGN;
*cas casauto authinfo="/home/ru-nborzunov/.authinfo" sessopts=(metrics=true);
*caslib _ALL_ ASSIGN;
%macro tech_main_schedule;
/* 1. Получаем полный список по модулям */
proc sql;
   create table work.full_list_modules as
   select
      cfg_resource.resource_id length = 8   
         label = 'resource_id',
      cfg_resource.resource_nm length = 40   
         format = $40.
         informat = $40.
         label = 'resource_nm',
      cfg_resource.macro_nm length = 200   
         format = $200.
         informat = $200.
         label = 'macro_nm',
      cfg_resource.module_nm length = 32   
         format = $32.
         informat = $32.
         label = 'module_nm',
      cfg_schedule_rule.rule_cond length = 1000   
         format = $1000.
         informat = $1000.
         label = 'rule_cond',
      cfg_schedule_rule.rule_start_hour length = 8   
         format = $8.
         informat = $8.
   from
      ETL_CFG.cfg_resource as cfg_resource inner join 
      ETL_CFG.cfg_schedule_rule as cfg_schedule_rule
         on
         (
            cfg_resource.module_nm = cfg_schedule_rule.rule_nm
         )
   ;
quit;

/* 2. Проверка выполнения условий по модулям */
%M_SCHEDULE_CHECK_RULE(work.full_list_modules, work.full_list_modules_checked);

/* 3. Добавим условия запуска по ресурсам */

proc sql;
   create table work.resource_rules_list as
   select
      t1.resource_id length = 8   
         label = 'resource_id',
      t1.resource_nm length = 40   
         format = $40.
         informat = $40.
         label = 'resource_nm',
      t1.macro_nm length = 200   
         format = $200.
         informat = $200.
         label = 'macro_nm',
      t1.module_nm length = 32   
         format = $32.
         informat = $32.
         label = 'module_nm',
      t2.rule_cond length = 1000   
         format = $1000.
         informat = $1000.
         label = 'rule_cond',
      t2.rule_start_hour length = 8   
         format = $8.
         informat = $8.
   from
      work.full_list_modules_checked as t1 left join 
      ETL_CFG.cfg_schedule_rule as t2
         on
         (
            full_list_modules_checked.resource_nm = t2.rule_nm
         )
   where
      t1.macro_nm NOT IS MISSING 
   ;
quit;

/* 4. Проверка по новым правилам */
%M_SCHEDULE_CHECK_RULE(work.resource_rules_list, work.resource_rules_list_checked);

/* 5. Загруженные сегодня и загружаемые ресурсы */
proc sql;
   create table work.loaded_in_process_resources as
      select
         resource_id,
         resource_nm,
         status_cd,
         processed_dttm,
         batch_cycle_id
   from etl_cfg.cfg_status_table
      where ( 
         datepart(processed_dttm) = today()
         AND
         (
         status_cd not in ( "A" , "C"))
         )
         OR
         (
         status_cd in ( "P")
         )
   ;
quit;

/* 6. Отсеиваем ресурсы из п.5 */
proc sql;
   create table work.resources_to_start as
   select
      t1.resource_id length = 8   
         label = 'resource_id',
      t1.resource_nm length = 40   
         format = $40.
         informat = $40.
         label = 'resource_nm',
      t1.macro_nm length = 200   
         format = $200.
         informat = $200.
         label = 'macro_nm',
      t1.module_nm length = 32   
         format = $32.
         informat = $32.
         label = 'module_nm',
      t1.rule_cond length = 1000   
         format = $1000.
         informat = $1000.
         label = 'rule_cond',
      COALESCE(t2.resource_id ,1) as FLAG length = 8
   from
      work.resource_rules_list_checked as t1
	  left join 
      work.loaded_in_process_resources as t2
         on
         (
            t1.resource_id = t2.resource_id
         ) 
   where 
      COALESCE(t2.resource_id ,1) = 1
   ;
quit;

data resources_to_start;
	set resources_to_start;
	macro_nm=scan(macro_nm, -1, "/");
run;

	%let etls_jobName=tech_main_schedule;
	%etl_job_start;

	data open_res;
		set resources_to_start;
		seq = _N_;
	run;
	
	%macro m_schedule_check;

		%let seq=&seq;
		%let macro_nm = %trim(&macro_nm.);
		%let module_nm = %trim(&module_nm.);
		
		filename par_&seq temp;
			
		data _null_;
			file par_&seq;                   
			put "%nrstr(%%)&macro_nm.";
		run;
		
		systask command 
		"""/opt/sas/mcd_config/bin/start_sas.sh"" ""%sysfunc(pathname(par_&seq))"" &module_nm. &macro_nm."
		taskname=task_&seq status=rc_&seq;
		
		%if &SYSRC ne 0 %then %do;
			%put WARNING: сессия не была запущена успешно;
		%end;

	%mend m_schedule_check;


	%util_loop_data( 
	 mpLoopMacro       =  m_schedule_check,
	 mpData            =  open_res);	
	
	%macro m_schedule_check_waitfor;
		task_&seq
	%mend m_schedule_check_waitfor;

	waitfor _all_
	%util_loop_data( 
	 mpLoopMacro       =  m_schedule_check_waitfor,
	 mpData            =  open_res);
		
	%etl_job_finish;
%mend tech_main_schedule;