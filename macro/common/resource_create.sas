/*****************************************************************
* НАЗНАЧЕНИЕ:
*   Добавляет в системные таблицы новый ресурс, обновляет нужные форматы.
*
* ПАРАМЕТРЫ:
*   mpResourceGroup     + группа ресурса
*   mpBranchCode        + филиал ресурса
*   mpFullTableType     - тип отбора записей из архива
*
******************************************************************
* Использует:
*     %error_check
*
* Устанавливает макропеременные:
*     нет
*
******************************************************************
* Пример использования:
*
******************************************************************
* 24-05-2012   Нестерёнок  Начальное кодирование
******************************************************************/

%macro resource_create (mpResourceGroup=, mpBranchCode=, mpFullTableType=PART);
   /* Получаем подходящий идентификатор */
   %local lmvResourceId;
   %let lmvResourceId = -1;
   proc sql noprint;
      select max(resource_id)+1 into :lmvResourceId 
        from ETL_SYS.ETL_RESOURCE
       where resource_group_cd = "&mpResourceGroup."
      ;
   quit;
   %error_check (mpStepType=SQL);

   %if lmvResourceId eq -1 %then %return;

   /* Добавляем необходимые записи */
   proc sql;
      /* ETL_RESOURCE */
      insert into ETL_SYS.ETL_RESOURCE (resource_id, resource_cd, resource_group_cd, resource_desc)
      /* 011420001;DIM_CUS_INFO_A01;DIM_CUS_INFO;Файл FCC DX DIM_CUS_INFO для филиала A01 */
      values (
         &lmvResourceId,
         "&mpResourceGroup._&mpBranchCode.",
         "&mpResourceGroup.",
         "Файл &mpResourceGroup. для филиала &mpBranchCode."
      )
      ;
      /* ETL_RESOURCE_X_ARCH */
      insert into ETL_SYS.ETL_RESOURCE_X_ARCH (resource_id, arch_nm, arch_role_cd)
      /* DIM_CUS_INFO_A01;DIM_CUS_INFO_ARCH;ARCH */
      values (
         &lmvResourceId,
         "&mpResourceGroup._ARCH",
         "ARCH"
      )
      ;
      /* ETL_RESOURCE_X_SOURCE */
      insert into ETL_SYS.ETL_RESOURCE_X_SOURCE (resource_id, source_type_cd, source_role_cd, file_nm)
      /* 11420107;FILE;SRC;DIM_CUS_INFO_N02_yyyymmdd */
      values (
         &lmvResourceId,
         "FILE",
         "SRC",
         "&mpResourceGroup._&mpBranchCode._ddmmyyy"
      )
      ;
   quit;
   %error_check (mpStepType=SQL);

   /* Перестраиваем форматы */
   %format_gen (mpFmtGroup= 002_Setup_Schedules);

%mend resource_create;
