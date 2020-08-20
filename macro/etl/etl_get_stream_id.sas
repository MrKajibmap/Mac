/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 0befd91d5378537b90e89b6f902b4f633c22354e $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Возвращает ID родительского потока из ETL_JOB.
*
*  ПАРАМЕТРЫ:
*     mpJobID         -  JOB_ID для которого необходимо получить ID родительского потока
*     mpOutStreamID   -  имя выходной макропеременной, в которую будет помещено ID родительского потока
*
******************************************************************
*  Использует:
*     %length
*     %member_drop
*
*  Устанавливает макропеременные:
*     &mpOutStreamID
*
******************************************************************
*  Пример использования:
*     %local lmvIFRSStreamID;
*     %etl_get_stream_id(mpJobID=19313, mpOutStreamID=lmvIFRSStreamID);
*     %put &=lmvIFRSStreamID;
*
******************************************************************
*  14-01-2019  Задояный     Начальное кодирование
******************************************************************/

%macro etl_get_stream_id (
   mpJobID                   =  ,
   mpOutStreamID             =  
);
	/* Инициализация */
	%local lmvJobID;
	%let lmvJobID 		= &mpJobID;
	%put &=lmvJobID;	

	/* Проверяем корректность параметров */
	%if %length (&mpJobID) le 0 %then %do;
		%put ETLERROR: Incorrect out parameter mpJobID value.;
		%return;
	%end;
	%if %length (&mpOutStreamID) le 0 %then %do;
		%put ETLERROR: Incorrect out parameter mpOutStreamID value.;
		%return;
	%end;

	/* TODO: Сделать не BASE подход */
	data etl_job (index=(job_id / unique));
		set etl_sys.etl_job;
	run;
	%error_check;	
	
	data _null_;
		set etl_job (where=(job_id = &lmvJobID.));
		
			do i=1 to 100 until(_error_); /* recursion protection */
				job_id = parent_job_id;
				set etl_job key=job_id / unique;

				if not _error_ and job_type_cd='STREAM' then do;

					call symput("&mpOutStreamID", strip(job_id));
					output;
					put job_id;
				end;
		
			end;
			_error_ = 0;

	run;
	%error_check;
	
	%member_drop(etl_job);

%mend etl_get_stream_id;
