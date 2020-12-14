/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для вызова процессов в DP
*
*  ПАРАМЕТРЫ:
*     mpJobName -		имя Model Process
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
*    %dp_jobexecution(mpJobName=Ruspmi Load Product Dimension);
*
****************************************************************************
*  08-09-2020  Борзунов     Начальное кодирование
****************************************************************************/

%macro dp_jobexecution(mpJobName=Ruspmi Load Product Dimension);

	%local lmvJobName lmvUrl lmvJobUrl;
	%let lmvUrl=&CUR_API_URL.;
	%let lmvJobName=&mpJobName.;

	/******************Get the processtemplate details*****************/
	filename jsn temp;
	proc http
		url="&lmvUrl./retailAnalytics/processModels/"
		method="GET"
		out=jsn 
		OAUTH_BEARER=SAS_SERVICES;
	run;

	libname posts JSON fileref=jsn ;
	title "Automap of JSON data";

	proc datasets noprint;
	   copy in= posts out=work memtype=data;
	   run; 
	quit;

	proc sql noprint;
		create table process_template as 
		select a.*,b.href,b.uri
		from ITEMS as a left join ITEMS_EXECUTE as b
		on a.ordinal_items=b.ordinal_items
		;
	quit;

	/******************Get the processtemplate url*****************/
	proc sql noprint;
		select %str(href) into: lmvJobUrl
		from process_template
		where name="&lmvJobName";
	quit;

	%put &=lmvJobUrl;

	/******************Execute the Process template*****************/
	proc http
		url="&lmvUrl.&lmvJobUrl."
		method="POST"
		OAUTH_BEARER=SAS_SERVICES;
		headers
		"Content-Type" ="application/vnd.sas.retail.process.data+json";
	run;

%mend dp_jobexecution;