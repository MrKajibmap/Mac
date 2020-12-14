%include "/opt/sas/mcd_config/config/initialize_global.sas"; 
%macro 600_02_load_pt;
	%let etls_jobName = 600_02_load_pt;
	%etl_job_start;

		%load_pt;
		
	%etl_job_finish;

%mend 600_02_load_pt;
%600_02_load_pt;