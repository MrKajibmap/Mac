/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для проставления статуса A в etl_sys.etl_resource_registry
*
*  ПАРАМЕТРЫ:
*     mpResId                 -  id ресурса (при пустом значении - грузит по всем ресурсам)
*									по дефолту - пустое значение
*	  mpSt 					  - Какой статус ставить 
*									по дефолту A
*
******************************************************************
*  Использует: 
*				%resource_add
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1. должен быть обёрнут в %etl_job_start; %etl_job_finish;
*
******************************************************************
*  Пример использования:
*     %load_resource_status(mpResId=100, mpSt=N);
*
****************************************************************************
*  09-04-2020  Зотиков     Начальное кодирование
****************************************************************************/
%macro load_resource_status(
   mpResId=,
   mpSt=A
);

	%local lmvObs;

	proc sql;
		create table RESOURCES as
		select resource_id as mpResourceId
		from ETL_SYS.ETL_RESOURCE
		%if mpResId ne . %then %do;
			where resource_id = &mpResId.
		%end;
		;
	quit;
	
	%let lmvObs = %member_obs(mpData=WORK.RESOURCES);
	
	%if &lmvObs. gt 0 %then %do;
	
		proc sql;
			select mpResourceId into :mvResourceId1 %if &lmvObs. gt 1 %then %do; - :mvResourceId&lmvObs. %end; 
			from WORK.RESOURCES
			;
		quit;
	
		%do i=1 %to &lmvObs.;
			%resource_add (mpResourceId=&&mvResourceId&i., mpDate=&JOB_START_DTTM., mpStatus=&mpSt.);
		%end;
	
	%end;

%mend load_resource_status;