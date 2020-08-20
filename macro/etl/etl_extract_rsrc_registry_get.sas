/*****************************************************************
*  ВЕРСИЯ:
*     $Id: ba3fe28e8ae4863db7410c7ce0ec35c7c6383521 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Получает список записей реестра для ресурсов, связанных с указанной таблицей
*
*  ПАРАМЕТРЫ:
*     mpData                  +  имя набора данных, таблицы из источника или архива
*     mpResourceId            +  идентификатор ресурса
*                                BY_SOURCE - будет определен по источнику mpData
*                                BY_ARCH - будет определен по архиву mpData
*     mpVersion               +  идентификатор версии
*                                MIN - самая ранняя из открытых (A, P)
*     mpOutRegistry           +  имя выходного набора, списка записей в реестре
*
******************************************************************
*  Использует:
*     %unique_id
*     %member_drop
*     %etl_extract_resource_get
*     %etl_extract_registry_get
*
******************************************************************
*  Пример использования:
*     В макро etl_extract_common.sas, в трансформе transform_registry_get.sas
*
******************************************************************
*  19-11-2014  Кузенков       Выделено из etl_extract_common.sas
******************************************************************/

%macro etl_extract_rsrc_registry_get (
   mpData            = ,
   mpResourceId      = ,
   mpVersion         = ,
   mpOutRegistry     = 
);
   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Получаем список ресурсов */
   %local lmvResourceTable;
   %let lmvResourceTable   = work.tr_extr_res_&lmvUID.;

   %etl_extract_resource_get (
      mpData            =  &mpData,
      mpResourceId      =  &mpResourceId,
      mpOutResource     =  &lmvResourceTable
   );

   /* Получаем реестр */
   %etl_extract_registry_get (
      mpInResource   =  &lmvResourceTable,
      mpVersion      =  &mpVersion,
      mpOutRegistry  =  &mpOutRegistry
   );
   %member_drop(&lmvResourceTable);
  
%mend etl_extract_rsrc_registry_get;
