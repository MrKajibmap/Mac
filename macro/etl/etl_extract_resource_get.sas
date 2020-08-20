/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 0168bf32897c7eabf7fca6b971313211e6477e38 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Получает список ресурсов, связанных с указанной таблицей.
*
*  ПАРАМЕТРЫ:
*     mpData                  +  имя набора данных, таблицы из источника или архива
*     mpResourceId            +  идентификатор ресурса
*                                BY_SOURCE - будет определен по источнику mpData
*                                BY_ARCH - будет определен по архиву mpData
*     mpOutResource           +  имя выходного набора, списка ресурсов, связанных с указанной таблицей
*
******************************************************************
*  Использует:
*     %error_check
*     %member_names
*
******************************************************************
*  Пример использования:
*     В трансформах transform_extract.sas, transform_extract_db2.sas
*
******************************************************************
*  16-05-2014  Нестерёнок     Начальное кодирование
******************************************************************/

%macro etl_extract_resource_get (
   mpData            =  ,
   mpResourceId      =  ,
   mpOutResource     =
);
%if (&mpResourceId = BY_SOURCE) or (&mpResourceId = BY_ARCH) %then %do;
      /* Определяем обновленный ресурс(-ы) */
      %local lmvLibref lmvMemberName;
      %member_names (mpTable=&mpData, mpLibrefNameKey=lmvLibref, mpMemberNameKey=lmvMemberName);

      proc sql noprint;
         create table &mpOutResource as select
            resource_id
         from
%if &mpResourceId = BY_SOURCE %then %do;
            ETL_SYS.ETL_RESOURCE_X_SOURCE
         where
            libref_cd = "%upcase(&lmvLibref)" and table_nm = "%upcase(&lmvMemberName)"
%end;
%if &mpResourceId = BY_ARCH %then %do;
            ETL_SYS.ETL_RESOURCE_X_ARCH
         where
            arch_nm = "%upcase(&lmvMemberName)"
%end;
         ;
      quit;
      %error_check (mpStepType=SQL);
%end;
%else %do;
      data &mpOutResource;
         if 0 then set ETL_SYS.ETL_RESOURCE (keep= resource_id);
         resource_id = &mpResourceId;
         output;
         stop;
      run;
      %error_check;
%end;
%mend etl_extract_resource_get;
