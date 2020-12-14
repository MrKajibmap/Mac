/* ********************************************************************* */
/* ********************************************************************* */
/* Джоб для запуска сквозного процесса прогнозирования временными рядами */
/* ********************************************************************* */
/* ********************************************************************* */
%macro mnt_load_facts;

	%dp_load_facts(mpFactGcMnth = dm_abt.fact_gc_month,
						mpFactPmixMnth = dm_abt.fact_pmix_month,
						mpFactUptMnth = dm_abt.fact_upt_month,
						mpPath = /data/dm_rep/
						);

	%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FaM_KOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_GC_FaM_KOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FaM_KOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FaM_NONKOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_GC_FaM_NONKOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FaM_NONKOMP);
	
%mend mnt_load_facts;