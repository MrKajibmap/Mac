%macro tech_002_main_schedule;
	%let etls_jobName = tech_002_main_schedule;
	%etl_job_start;

	%tech_main_schedule;
	
	%etl_job_finish;

%mend tech_002_main_schedule;
