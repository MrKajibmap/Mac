/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 63fca960da226e8f9fe932c4333510e32f0f0a2d $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Возвращает имя текущего модуля ETL.
*
*  ПАРАМЕТРЫ:
*     mpOutNameKey         -  имя выходной макропеременной, в которую будет помещено имя текущего модуля
*
******************************************************************
*  Использует:
*     %is_blank
*
*  Устанавливает макропеременные:
*     &mpOutNameKey
*
******************************************************************
*  Пример использования:
*     %local lmvJobName;
*     %etl_job_name (mpOutNameKey=lmvJobName);
*     %job_start (mpJobName=&lmvJobName.);
*
******************************************************************
*  19-06-2015  Нестерёнок     Начальное кодирование
******************************************************************/

%macro etl_job_name (
   mpOutNameKey            =
);
   %if %is_blank(mpOutNameKey) %then %do;
      %local lmvJobName;
      %let mpOutNameKey = lmvJobName;
   %end;

   /* Проверяем исполнение из-под Loop */
   %if %symexist(handleName) %then %do;
      %let &mpOutNameKey   = &ETLS_JobName._&handleName;
   %end;
   %else %do;
      %let &mpOutNameKey   = &ETLS_JobName;
   %end;
%mend etl_job_name;
