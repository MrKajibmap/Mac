/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для запуска расчета проекта в VF
*	
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
*     %vf_run_project(mpProjectName=pbo_sales_v2);
*
****************************************************************************
*  07-07-2020  Борзунов     Начальное кодирование
*  11-08-2020  Борзунов		Добавлено получение ID VF-проекта по его имени + параметр mpProjectName
****************************************************************************/
%macro vf_run_project(mpProjectName=, mpLock=Y);
	option mprint mlogic;
	/*proc printto log='/data/tmp/privet_log.txt' new;*/
	/*run;*/
	
	%local lmvAPI_URL lmvOutLibrefPmixSalAbt lmvOutTabNamePmixSalAbt lmvVfPboName lmvProjectId;
	%let lmvAPI_URL = &CUR_API_URL.;
	filename resp TEMP;
	
	%member_names (mpTable=&mpPmixSalAbt, mpLibrefNameKey=lmvOutLibrefPmixSalAbt, mpMemberNameKey=lmvOutTabNamePmixSalAbt);
	/* Получение списка VF-проектов */
	%vf_get_project_list(mpOut=work.vf_project_list);
	/* Извлечение ID для VF-проекта по его имени */
	%let lmvVfPboName = &mpProjectName.;
	%let lmvProjectId = %vf_get_project_id_by_name(mpName=&lmvVfPboName., mpProjList=work.vf_project_list);
		
	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;


	/******************************************************************************
	** Acquire project lock...
	******************************************************************************/
	%if &mpLock=Y %then %do;
		%put Acquire project lock...;
		proc http
			method="POST"
			url="&lmvAPI_URL/analyticsGateway/projects/&lmvProjectId./lock?lockPeriod=300"
			oauth_bearer = sas_services
			out=resp;
			headers
				"Content-Type" = 'application/vnd.sas.project'
				"Accept" = 'application/vnd.sas.analytics.project+json';
		run;
		%put Response status: &SYS_PROCHTTP_STATUS_CODE; 

		/*Вывод в лог статуса */
		libname respjson JSON fileref=resp;
		data _NULL_;
			infile resp;
			input;
			putlog _INFILE_;
		run;

		%if not (&SYS_PROCHTTP_STATUS_CODE = 200) %then %do;
			%put ERROR: An invalid response was received.;
			%abort;
		%end;
	%end;

	/******************************************************************************
	** Fetch data specification, checking for updated input data...
	******************************************************************************/
	%put Fetch data specification, checking for updated input data...;
	proc http
		method="GET"
		url="&lmvAPI_URL/forecastingGateway/projects/&lmvProjectId./dataDefinitions/@current?checkForUpdates=true"
		oauth_bearer = sas_services
		out=resp;
		headers
			"Accept"="application/vnd.sas.analytics.forecasting.data.definition+json";
	run;
	%put Response status: &SYS_PROCHTTP_STATUS_CODE;
	/*Вывод в лог статуса */
	libname respjson JSON fileref=resp;
	data _NULL_;
		infile resp;
		input;
		putlog _INFILE_;
	run;

	%if not (&SYS_PROCHTTP_STATUS_CODE = 200) %then %do;
		%put ERROR: An invalid response was received.;
		%abort;
	%end;

	/******************************************************************************
	** If necessary, import new input data...
	******************************************************************************/
	%put If necessary, import new input data...;
	proc http
		method="POST"
		url="&lmvAPI_URL/forecastingGateway/projects/&lmvProjectId./dataDefinitions/@current/dataUpdateJobs?category=INPUT"
		oauth_bearer = sas_services
		out=resp;
		headers
			"Accept" = 'application/vnd.sas.job.execution.job+json';
	run;
	%put Response status: &SYS_PROCHTTP_STATUS_CODE;

	/*Вывод в лог статуса */
	libname respjson JSON fileref=resp;
	data _NULL_;
		infile resp;
		input;
		putlog _INFILE_;
	run;

	%if not (&SYS_PROCHTTP_STATUS_CODE = 202) %then %do;
		%put ERROR: An invalid response was received.;
		%abort;
	%end;

	/******************************************************************************
	** Run all pipelines...
	******************************************************************************/
	%put Run all pipelines...;
	proc http
		method="POST"
		url="&lmvAPI_URL/forecastingGateway/projects/&lmvProjectId./pipelineJobs"
		oauth_bearer = sas_services
		out=resp;
		headers
			"Accept"="application/vnd.sas.job.execution.job+json";
	run;
	%put Response status: &SYS_PROCHTTP_STATUS_CODE;

	/*Вывод в лог статуса */
	libname respjson JSON fileref=resp;
	data _NULL_;
		infile resp;
		input;
		putlog _INFILE_;
	run;

	%if not (&SYS_PROCHTTP_STATUS_CODE = 202) %then %do;
		%put ERROR: An invalid response was received.;
		%abort;
	%end;

	/******************************************************************************
	** Wait for pipelines to finish running...
	******************************************************************************/
	%put Wait for pipelines to finish running...;
	%global jobState;

	%do %until(&jobState ^= running);

		proc http
			method="GET"
			url="&lmvAPI_URL/forecastingGateway/projects/&lmvProjectId./pipelineJobs/@currentJob"
			oauth_bearer = sas_services
			out=resp;
			headers
				"Accept"="application/vnd.sas.job.execution.job+json";
		run;
		%put Response status: &SYS_PROCHTTP_STATUS_CODE;

		libname respjson JSON fileref=resp;
		data _null_;
			set respjson.root;
			call symput('jobState', state);
		run;
		%put jobState = &jobState;

		/*Вывод в лог статуса */
		data _NULL_;
			infile resp;
			input;
			putlog _INFILE_;
		run;

		data _null_;
			call sleep(10000);
		run;

	%end;

	%if not (&jobState = completed) %then %do;
		%put ERROR: An invalid response was received.;
		%abort;
	%end;

	/******************************************************************************
	** Check for overrides in pending or conflict state...
	******************************************************************************/
	%put Check for overrides in pending or conflict state...;
	%global pendingOverridesCount;

	proc http
		method="GET"
		url="&lmvAPI_URL/forecastingGateway/projects/&lmvProjectId./specificationDetails?start=0%nrstr(&)limit=1%nrstr(&)filter=or(eq(status,%27conflict%27),eq(status,%27pending%27))"
		oauth_bearer = sas_services
		out=resp;
		headers
			"Accept"="application/vnd.sas.collection+json";
	run;
	%put Response status: &SYS_PROCHTTP_STATUS_CODE;

	libname respjson JSON fileref=resp;
	data _null_;
		set respjson.root;
		call symputx('pendingOverridesCount', count);
	run;

	/*Вывод в лог статуса */
	data _NULL_;
		infile resp;
		input;
		putlog _INFILE_;
	run;
		
	%put pendingOverridesCount = &pendingOverridesCount;

	%if not (&pendingOverridesCount = 0) %then %do;
		%put ERROR: Overrides were found in pending or conflict state. Please submit these overrides first.;
		%abort;
	%end;

	/******************************************************************************
	** Prepare for overrides...
	******************************************************************************/
	%put Prepare for overrides...;
	proc http
		method="POST"
		url="&lmvAPI_URL/forecastingGateway/projects/&lmvProjectId./dataDefinitions/@current/dataUpdateJobs?category=FORECAST"
		oauth_bearer = sas_services
		out=resp;
		headers
			"Content-Type"="application/vnd.sas.analytics.forecasting.data.specification+json";
	run;
	%put Response status: &SYS_PROCHTTP_STATUS_CODE;

	/*Вывод в лог статуса */
	libname respjson JSON fileref=resp;
	data _NULL_;
		infile resp;
		input;
		putlog _INFILE_;
	run;

	%if not (&SYS_PROCHTTP_STATUS_CODE = 202) %then %do;
		%put ERROR: An invalid response was received.;
		%abort;
	%end;

	/******************************************************************************
	** Wait for all overrides to be prepared...
	******************************************************************************/
	%put Wait for all overrides to be prepared...;
	%global unpreparedTransactionCount;

	%do %until(&unpreparedTransactionCount = 0);

		proc http
			method="GET"
			url="&lmvAPI_URL/forecastingGateway/projects/&lmvProjectId./transactions?filter=eq(status,%27applied%27)"
			oauth_bearer = sas_services
			out=resp;
			headers
				"Accept"="application/vnd.sas.collection+json";
		run;
		%put Response status: &SYS_PROCHTTP_STATUS_CODE;

		libname respjson JSON fileref=resp;
		data _null_;
			set respjson.root;
			call symputx('unpreparedTransactionCount', count);
		run;

		/*Вывод в лог статуса */
		data _NULL_;
			infile resp;
			input;
			putlog _INFILE_;
		run;

		%put unpreparedTransactionCount = &unpreparedTransactionCount;

		data _null_;
			call sleep(3000);
		run;

	%end;

	/******************************************************************************
	** Check for overrides availability...
	******************************************************************************/
	%put Check for overrides availability...;
	%global resubmitRequiredTransactionCount;

	proc http
		method="GET"
		url="&lmvAPI_URL/forecastingGateway/projects/&lmvProjectId./transactions?filter=or(eq(status,%27resubmitPending%27),eq(status,%27resubmitConflict%27))"
		oauth_bearer = sas_services
		out=resp;
		headers
			"Accept"="application/vnd.sas.collection+json";
	run;
	%put Response status: &SYS_PROCHTTP_STATUS_CODE;

	libname respjson JSON fileref=resp;
	data _null_;
		set respjson.root;
		call symputx('resubmitRequiredTransactionCount', count);
	run;

	data _NULL_;
		infile resp;
		input;
		putlog _INFILE_;
	run;
	%put resubmitRequiredTransactionCount = &resubmitRequiredTransactionCount;

	%if not (&resubmitRequiredTransactionCount > 0) %then %do;
		%put NOTE: No overrides need to be resubmitted. Existing overrides may have become archived or expired.;
		%return;
	%end;

	/******************************************************************************
	** Resubmit overrides...
	******************************************************************************/
	%put Resubmit overrides...;
	%global transactionJobId;

	proc http
		method="POST"
		url="&lmvAPI_URL/forecastingGateway/projects/&lmvProjectId./transactionJobs"
		in="{""firstTransaction"":""@first"",""lastTransaction"":""@last"",""autoResolve"":true}"
		oauth_bearer = sas_services
		out=resp;
		headers
			"Accept"="application/vnd.sas.forecasting.overrides.transaction.collection.job+json"
			"Content-Type"="application/vnd.sas.forecasting.overrides.transaction.collection.job.request+json";
	run;
	%put Response status: &SYS_PROCHTTP_STATUS_CODE;

	libname respjson JSON fileref=resp;
	data _null_;
		set respjson.root;
		call symput('transactionJobId', id);
	run;
	data _NULL_;
		infile resp;
		input;
		putlog _INFILE_;
	run;


	%put transactionJobId = &transactionJobId;

	%if not (&SYS_PROCHTTP_STATUS_CODE = 202) %then %do;
		%put ERROR: An invalid response was received.;
		%abort;
	%end;

	/******************************************************************************
	** Wait for all overrides to be resubmitted...
	******************************************************************************/
	%put Wait for all overrides to be resubmitted...;
	%global transactionJobState;

	%do %until(&transactionJobState ^= running);

	proc http
		method="GET"
		url="&lmvAPI_URL/forecastingGateway/projects/&lmvProjectId./transactionJobs/&transactionJobId."
		oauth_bearer = sas_services
		out=resp;
		headers
			"Accept"="application/vnd.sas.forecasting.overrides.transaction.collection.job+json";
	run;
	%put Response status: &SYS_PROCHTTP_STATUS_CODE;

	libname respjson JSON fileref=resp;
	data _null_;
		set respjson.root;
		call symputx('transactionJobState', state);
	run;

	/*Вывод в лог статуса */
	data _NULL_;
		infile resp;
		input;
		putlog _INFILE_;
	run;
	%put transactionJobState = &transactionJobState;


	data _null_;
		call sleep(3000);
	run;

	%end;

	%if not (&transactionJobState = completed) %then %do;
		%put ERROR: An invalid response was received.;
		%abort;
	%end;

	/* delete lock */
	proc http
		method="DELETE"
		url="&lmvAPI_URL/analyticsGateway/projects/&lmvProjectId./lock"
		oauth_bearer = sas_services
		out=resp;
	run;
	%put Response status: &SYS_PROCHTTP_STATUS_CODE;
	
	/*proc printto;*/
	/*run;*/
%mend vf_run_project;
