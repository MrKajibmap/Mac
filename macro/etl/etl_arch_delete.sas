/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 9b87832da06f5dd3a9ba9dbeadf8ee3f18e07cbe $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Удаляет старые записи из архива.
*
*  ПАРАМЕТРЫ:
*     mpIn                    +  имя входного набора, список для удаления из архива
*                                Должен содержать поле version_id
*     mpOut                   +  имя выходного набора, выгруженной таблицы
*
******************************************************************
*  Пример использования:
*     в трансформе transform_arch_delete
*
******************************************************************
*  02-05-2012  Пильчин        Начальное кодирование
*  12-12-2012  Нестерёнок     Оптимизация; теперь алгоритм удаляет выгрузки по одной
*  18-09-2014  Нестерёнок     Оптимизация: добавлены различные стратегии удаления
*  17-02-2015  Сазонов		  Для db2 отключено удаление по партициям
*  12-03-2015  Сазонов 		  Изменения для db2
******************************************************************/

%macro etl_arch_delete (mpIn=, mpOut=);
   /* Если ничего не надо удалять, то выход */
   %if %member_obs(mpData=&mpIn) le 0 %then %return;

   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Получаем имена архива */
   %local lmvDbmsTable lmvArchName;
   %&ETL_DBMS._table_name (mpSASTable=&mpOut, mpOutFullNameKey=lmvDbmsTable, mpOutNameKey=lmvArchName);

   /* Получаем список существующих выгрузок */
   %local lmvExistList;
   %let lmvExistList  = work.etl_arch_del_exist_&lmvUID.;
   proc sql;
      create table &lmvExistList as
      select distinct etl_extract_id from &mpOut
      ;
   quit;

   /* Получаем список удаляемых выгрузок */
   %local lmvDeleteList;
   %let lmvDeleteList  = work.etl_arch_del_todel_&lmvUID.;
   proc sql;
      create table &lmvDeleteList as
      select distinct version_id as etl_extract_id from &mpIn d
      where not missing(version_id)
      and exists (select 1 from &lmvExistList e where e.etl_extract_id = d.version_id)
      ;
   quit;

   /* Если все равно ничего не надо удалять, то выход */
   %local lmvDeleteCount lmvExistCount;
   %let lmvDeleteCount  =  %member_obs(mpData=&lmvDeleteList);
   %let lmvExistCount   =  %member_obs(mpData=&lmvExistList);
   %if &lmvDeleteCount le 0 %then %return;

   /* Если надо удалять всё */
   %if &lmvDeleteCount = &lmvExistCount %then %do;
      proc sql;
         %&ETL_DBMS._connect(mpLoginSet=ETL_STG); 
         execute (
            truncate table &lmvDbmsTable
%if &ETL_DBMS = db2 %then %do;
			immediate
%end;
         ) by &ETL_DBMS.
         ;
      quit;
      %error_check (mpStepType=SQL_PASS_THROUGH);
      %return;
   %end;

   %local lmvPartitionType lmvInterval;
%if &ETL_DBMS ne db2 %then %do;
   /* В зависимости от организации архива делаем удаление по-разному */
   proc sql noprint;
	  %&ETL_DBMS._connect(mpLoginSet=ETL_STG);      
      select
         partitioning_type,
         interval
      into
         :lmvPartitionType,
         :lmvInterval
      from connection to &ETL_DBMS. (
         select partitioning_type, interval
         from user_part_tables
         where table_name = %&ETL_DBMS._string(&lmvArchName)
      );
   quit;
%end;

%if &lmvPartitionType = RANGE and &lmvInterval = 1 and &ETL_DBMS ne db2 %then %do;
   /* Одна выгрузка - одна партиция */
   /* макрос сброса одной партиции */
   %macro _etl_arch_drop_one_partition;
      execute (
         alter table &lmvDbmsTable drop partition for(&etl_extract_id)
      ) by &ETL_DBMS.
      ;
      %error_check (mpStepType=SQL_PASS_THROUGH);
   %mend _etl_arch_drop_one_partition;

   proc sql;
      %&ETL_DBMS._connect(mpLoginSet=ETL_STG);    
      %util_loop_data (mpLoopMacro=_etl_arch_drop_one_partition, mpData=&lmvDeleteList);
   quit;
%end;
%else %do;
   /* Оперируем полной таблицей */
   /* Определяем уровень обновления архива */
   %if (&lmvDeleteCount gt &ETL_STG_CLEANUP_ABS_T) and %sysevalf(&lmvDeleteCount/&lmvExistCount gt &ETL_STG_CLEANUP_REL_T) %then %do;
      /* Удаляется много, используем временную таблицу */
      %local lmvSaveList lmvSaveTable;
      %let lmvSaveTable = etl_arch_del_save_&lmvUID.;
      proc sql noprint;
         select etl_extract_id
         into :lmvSaveList separated by ", "
         from &lmvExistList e
         where not exists (select 1 from &lmvDeleteList d where d.etl_extract_id = e.etl_extract_id)
         ;
         %&ETL_DBMS._connect(mpLoginSet=ETL_STG);    
%if &ETL_DBMS = db2 %then %do;
		 execute (
			create table &lmvSaveTable as (select * from &lmvDbmsTable) WITH NO DATA;
         ) by &ETL_DBMS.
         ;
		 execute (
			insert into &lmvSaveTable select * from &lmvDbmsTable where etl_extract_id in (&lmvSaveList);
         ) by &ETL_DBMS.
         ;
%end;
%else %do;
         execute (
            create table &lmvSaveTable as
            select * from &lmvDbmsTable
            where etl_extract_id in (&lmvSaveList)
         ) by &ETL_DBMS.
         ;
%end;
         execute (
            commit
         ) by &ETL_DBMS.
         ;
         execute (
            truncate table &lmvDbmsTable
%if &ETL_DBMS = db2 %then %do;
			immediate
%end;
         ) by &ETL_DBMS.
         ;
         execute (
            insert into &lmvDbmsTable select * from &lmvSaveTable
         ) by &ETL_DBMS.
         ;
         execute (
            commit
         ) by &ETL_DBMS.
         ;
         %error_check (mpStepType=SQL_PASS_THROUGH);
         %if &STEP_RC = 0 %then %do;
         execute (
            drop table &lmvSaveTable
         ) by &ETL_DBMS.
         ;
         %end;
      quit;
   %end;
   %else %do;
      /* Удаляется мало, удаляем по одной */
      /* макрос удаления одной выгрузки */
      %macro _etl_arch_del_one_extract;
         execute (
            delete from &lmvDbmsTable
            where
               etl_extract_id = &etl_extract_id
         ) by &ETL_DBMS.
         ;
         execute (
            commit
         ) by &ETL_DBMS.
         ;
         %error_check (mpStepType=SQL_PASS_THROUGH);
      %mend _etl_arch_del_one_extract;

      proc sql;
         %&ETL_DBMS._connect(mpLoginSet=ETL_STG); 
         %util_loop_data (mpLoopMacro=_etl_arch_del_one_extract, mpData=&lmvDeleteList);
      quit;
   %end;
%end;

%if &ETL_DBMS = oracle %then %do;
   /* Понижаем HWM */
   /*
   proc sql;
      %&ETL_DBMS._connect(mpLoginSet=ETL_STG);    
      execute (
         alter table &lmvDbmsTable enable row movement
      ) by &ETL_DBMS.;
      execute (
         alter table &lmvDbmsTable shrink space
      ) by &ETL_DBMS.;
      execute (
         alter table &lmvDbmsTable disable row movement
      ) by &ETL_DBMS.;
   quit;
   */
%end;
%mend etl_arch_delete;
