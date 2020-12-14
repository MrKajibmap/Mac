%macro tech_correct_sched;
	%include "/opt/sas/mcd_config/config/initialize_global.sas"; 

	%M_ETL_REDIRECT_LOG(START, tech_correct_scheduler, Tech);
	%M_LOG_EVENT(START, tech_correct_scheduler);
	
	proc sql;
			%postgres_connect (mpLoginSet=ETL_SYS);
				execute      
					(
						update etl_sys.etl_resource_registry
						set status_cd = 'L'
					) 
					by postgres;  
			disconnect from postgres;
	quit;

	%M_ETL_REDIRECT_LOG(END, tech_correct_scheduler, Tech);
	%M_LOG_EVENT(END, tech_correct_scheduler);
	
%mend tech_correct_sched;