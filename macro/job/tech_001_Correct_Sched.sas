%macro tech_001_Correct_Sched;
	%let etls_jobName = tech_001_Correct_Sched;
	%etl_job_start;

	%tech_correct_sched;
		
	%etl_job_finish;

%mend tech_001_Correct_Sched;
