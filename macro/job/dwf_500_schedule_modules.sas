/*****************************************************************
* ВЕРСИЯ:
*   $Id$
*
******************************************************************
* НАЗНАЧЕНИЕ:
*   Планировщик постановки заданий в очередь LSF.
*   Должен работать постоянно, например, запускаясь раз в 10 мин.
*
******************************************************************
* Использует:
*     %error_check
*     %ETL_DBMS_connect
*     %job_event_reg
*     %list_expand
*     %member_obs
*     %util_loop
*     %util_loop_data
*
* Устанавливает макропеременные:
*     нет
*
******************************************************************
* 14-02-2012   Нестерёнок  Начальное кодирование
* 23-04-2012   Нестерёнок  Добавлены ограничения по ресурсам
* 30-04-2014   Нестерёнок  Добавлена защита от пересекающихся интервалов
* 17-02-2015   Сазонов     Для CMD вызов через &START_SAS_CMD
* 01-04-2015   Сазонов     Изменения для запуска без LSF
******************************************************************/
%macro dwf_500_schedule_modules;
%let etls_jobName=schdule_modules;
%etl_job_start;

/*********************************************** Ограничения по времени ****************************************************/

/* Получаем список модулей, для которых наступило окно запуска, но которые еще не были распределены */
proc sql;
   create table t_008_100_modules as
      select
         sf.schedule_cd,
         sf.frame_id,
         m.module_id,
         m.module_type_cd,
         m.module_txt
      from
         ETL_SYS.ETL_SCHEDULE_FRAME as sf,
         (select module_id, filter_schedule_cd as schedule_cd from ETL_SYS.ETL_MODULE_X_RULE
            where filter_schedule_cd is not null
         ) as mxs,
         ETL_SYS.ETL_MODULE as m
      where
         sf.schedule_cd = mxs.schedule_cd and
         mxs.module_id = m.module_id and
         sf.open_dttm le &JOB_START_DTTM le sf.close_dttm
         and not exists (
            select 1 from ETL_SYS.ETL_MODULE_X_FRAME
            where module_id = m.module_id and frame_id = sf.frame_id
         )
   ;
quit;
%error_check (mpStepType=SQL);

/* Защита от дураков */
proc sort
   data=    t_008_100_modules
   out=     t_008_100_modules (index=(module_id /unique))
   dupout=  t_008_100_modules_dup
   nodupkey
;
   by module_id;
run;

%macro validate_modules;
   %local lmvObs;
   %let lmvObs = %member_obs(mpData=t_008_100_modules_dup);
   %if &lmvObs le 0 %then %return;

   %job_event_reg (
      mpEventTypeCode=  DATA_VALIDATION_FAILED,
      mpEventValues=    %bquote(Задвоенные интервалы модулей (&lmvObs) отброшены)
   );
%mend validate_modules;
%validate_modules;

/*********************************************** Ограничения по ресурсам ****************************************************/
/* Макросы для обработки правил */

/* Макрос применяет одно правило к набору module_data */
%macro process_rule (mpCancel=);
   /* Оптимизация:  если не осталось правил, для которых есть записи в нужном состоянии, то прекратить обработку правил */
   %local lmvFutureStateCount;
   %let lmvFutureStateCount = 0;
   proc sql noprint;
      select count(*) into :lmvFutureStateCount
      from module_data
      where state_cd in (
         select
            distinct state_cd
         from
            t_008_100_rules
         where
            module_id = &module_id and rule_id ge &rule_id
      );
   quit;
   %error_check (mpStepType=SQL);

   %if &lmvFutureStateCount = 0 %then %do;
      %let &mpCancel = Y;
      %job_event_reg (mpEventTypeCode=STAT_MODULE,
                     mpEventDesc=Планировщик остановил применение правил к модулю &module_id,
                     mpEventValues= %bquote(Все записи вне области действия правил) );
      %return;
   %end;

   /* Находим записи, на которые действует правило */
   proc sql feedback;
      create table module_data_upd as
         select
            m.resource_id,
            m.version_id
         from
            module_data as m
            inner join (
               /* Получаем список подходящих групп или, если нет группировки, то единственную запись */
               select
                  distinct %list_expand(state_cd &filter_by_group, {}, mpOutDlm=%str(, ))
               from module_data
               where
                  state_cd = "&state_cd"
                  %if not %is_blank(filter_schedule_cd) %then %do;
                     and ("&filter_schedule_cd" in (
                        select distinct schedule_cd from t_008_100_modules where module_id = &module_id
                     ))
                  %end;
                  %if not %is_blank(filter_resource_group_cd) %then %do;
                     and (resource_group_cd = "&filter_resource_group_cd")
                  %end;
                  %if not %is_blank(filter_resource_cd) %then %do;
                     and (resource_cd = "&filter_resource_cd")
                  %end;
                  %if not %is_blank(filter_status_cd) %then %do;
                     and (status_cd = "&filter_status_cd")
                  %end;
               %if not %is_blank(filter_extra_txt) %then %do;
                  having (&filter_extra_txt)
               %end;
            ) as a
            on
               %list_expand(state_cd &filter_by_group, m.{}=a.{}, mpOutDlm=%str( and ))
      ;
      create unique index pk on module_data_upd (resource_id, version_id)
      ;
   quit;
   %error_check (mpStepType=SQL);

   /* Отладка */
   %if &ETL_DEBUG %then %do;
      data module_data_&rule_id.;
         set module_data;
      run;
      data module_data_upd_&rule_id.;
         set module_data_upd;
      run;
   %end;

   /* Переводим в следующее состояние */
   data module_data_next;
      set module_data end=end;

      /* Статистика */
      retain stat_unchanged_cnt stat_next_cnt stat_else_cnt 0;
      drop   stat_unchanged_cnt stat_next_cnt stat_else_cnt  ;

      if state_cd = "&state_cd" then do;
         set module_data_upd key=pk /unique;
         _error_ = 0;
         if _iorc_ = &IORC_SOK then do;
            state_cd = "&next_state_cd";
            stat_next_cnt + 1;
         end;
         else do;
            state_cd = "&else_state_cd";
            stat_else_cnt + 1;
         end;
         if state_cd ne "R" then output;
      end;
      else do;
         stat_unchanged_cnt + 1;
      end;

      if end then do;
         call symputx ("stat_unchanged_cnt", stat_unchanged_cnt);
         call symputx ("stat_next_cnt",      stat_next_cnt);
         call symputx ("stat_else_cnt",      stat_else_cnt);
      end;
   run;
   %error_check (mpStepType=DATA);

   proc append base=module_data data=module_data_next;
   run;
   %error_check (mpStepType=DATA);

   /* Отладка */
   %if &ETL_DEBUG %then %do;
      data module_data_next_&rule_id.;
         set module_data_next;
      run;
   %end;

   /* Статистика */
   %let next_state_cd = &next_state_cd;
   %let else_state_cd = &else_state_cd;
   %job_event_reg (mpEventTypeCode=STAT_MODULE,
                     mpEventDesc=Планировщик применил правило &rule_id к модулю &module_id,
                     mpEventValues= %bquote(Перешло в "&next_state_cd" &stat_next_cnt записей, в "&else_state_cd" - &stat_else_cnt, не изменилось - &stat_unchanged_cnt) );
%mend process_rule;

/* Макрос применяет все правила модуля к набору t_008_100_registry из реестра */
/* Затем выводит результат (кол-во строк, пришедших в конечное состояние успеха A (Accept)) */
%macro check_module(mpModuleId);
   data module_data;
      if 0 then set etl_sys.etl_module_x_rule (keep= module_id state_cd);
      module_id   = &mpModuleId;
      state_cd    = "S";
      set t_008_100_registry;
   run;
   %error_check (mpStepType=DATA);

   sasfile work.module_data load;
   %util_loop_data (mpData=t_008_100_rules, mpLoopMacro=process_rule, mpWhere= module_id=&mpModuleId, mpCancellable=Y);
   sasfile work.module_data close;

   proc sql;
      create table rule_ok as select
         &mpModuleId    as module_id,
         count(*)       as success_obs_cnt
      from module_data
         where state_cd = "A"
      ;
   quit;
   %error_check (mpStepType=SQL);

   proc append base=t_008_100_rule_ok data=rule_ok;
   run;
   %error_check (mpStepType=DATA);
%mend check_module;


/* Отбираем актуальные (не C) записи реестра */
/* Для запуска модуля хотя бы одна из них должна быть переведена из начального состояния S (Start) */
/* в конечное состояние успеха A (Accept) при помощи указанных для модуля правил */
data t_008_100_registry_dummy;
   if 0 then set etl_sys.etl_resource_registry;
   resource_id = -1;
   output;
run;
%error_check (mpStepType=DATA);

data t_008_100_registry;
   set
      etl_sys.etl_resource_registry (where= (status_cd ne "C"))
      t_008_100_registry_dummy (in= in_dummy)
   ;

   /* Добавляем расчетные переменные */
   length resource_cd resource_group_cd $32;
   resource_cd          = put (resource_id, res_id_cd.);
   resource_group_cd    = put (resource_id, res_id_grp.);

   /* В случае отсутствия записей в реестре добавляем виртуальную запись */
   /* Она позволит валидировать правила, рассчитанные на "все записи" */
   if not (in_dummy and _n_ gt 1) then output;
run;
%error_check (mpStepType=DATA);

/* Выполняем для каждого модуля все его правила */
%macro check_all;
   %local lmvModuleList;
   proc sql noprint;
      create table t_008_100_rules as select *
      from etl_sys.etl_module_x_rule
      order by module_id, rule_id
      ;
      select distinct module_id into :lmvModuleList separated by " " from t_008_100_rules
      ;
   quit;

	proc sql;
		create table WORK.T_008_100_RULE_OK( bufsize=65536 )
			(
			module_id num,
			success_obs_cnt num
			);
		create unique index module_id on WORK.T_008_100_RULE_OK(module_id);
	quit;

   %if not %is_blank(lmvModuleList) %then %do;
      %util_loop(mpMacroName=check_module, mpWith=&lmvModuleList);
   %end;
   %else %do;
      %job_event_reg (mpEventTypeCode=UNEXPECTED_ARGUMENT,
                      mpEventValues= %bquote(Не задано никаких правил, модули не будут запускаться) );
   %end;
%mend check_all;
%check_all;


/*********************************************** Запуск готовых модулей ****************************************************/
/* Отбираем успешные модули */
data t_008_100_modules_ready;
   merge
      t_008_100_modules (in= in_modules)
      t_008_100_rule_ok (in= in_rule_ok)
   ;
   by module_id;

   /* TODO: модули без расписания не запускаются */
   if in_modules;
   if not in_rule_ok then do;
      _error_ = 0;
       call missing(success_obs_cnt);
   end;

   /* Если у модуля нет ограничений по ресурсам, то считаем ограничения выполненными */
   if (success_obs_cnt gt 0) or missing(success_obs_cnt) then ready_flg = "Y";
run;
%error_check (mpStepType=DATA);

/* Выводим результаты в лог */
%macro log_ready;
   %local lmvEventType;
   %if &ready_flg = Y %then
      %let lmvEventType = SCHEDULER_MODULE_READY;
   %else
      %let lmvEventType = SCHEDULER_MODULE_NOT_READY;

   %let module_txt = &module_txt;
   %job_event_reg (mpEventTypeCode=&lmvEventType,
                   mpEventValues= %bquote(&module_txt (&module_id)) );
%mend log_ready;
%util_loop_data (mpData=t_008_100_modules_ready, mpLoopMacro=log_ready);


/* Исполняем готовые к распределению модули */
%macro schedule_ready;
   /* Определяем команду, которую надо выполнить */
   %local lmvCommand;
   %if &module_type_cd = LSF %then %do;
      %let lmvCommand = jtrigger &module_txt;
   %end;
   %else %if &module_type_cd = CMD %then %do;
      %let lmvCommand = &module_txt;
   %end;
   /* Workspace Server */
   %else %if &module_type_cd = WSS %then %do;
      %let lmvCommand = &SAS_START_CMD !ETL_DATA_ROOT/deployed/&module_txt..sas;
   %end;
   %else %do;
      %job_event_reg (mpEventTypeCode=ILLEGAL_ARGUMENT, mpLevel=E,
                      mpEventValues= %bquote(&module_txt: Неизвестный тип модуля &module_type_cd) );
      %return;
   %end;

   /* Выполняем, выводим лог */
   %local lmvCommandRc lmvCommandMsg;
   %sys_command (
      mpCommand= &lmvCommand,
      mpResultKey=lmvCommandRc,
      mpMsgKey=lmvCommandMsg
   );

   /* Регистрируем результат */
   %let module_txt = &module_txt;
   %if &lmvCommandRc = 0 %then %do;
      %job_event_reg (mpEventTypeCode=SCHEDULER_MODULE_KICKOFF,
                      mpEventValues= %bquote(&module_txt (&module_id)) );
   %if &module_type_cd = LSF %then %do;
      /* Получаем идентификатор задания LSF */
      %local lmvFlowId lmvRx lmvPos;
      %let lmvFlowId    = .;
      %let lmvRx        = %sysfunc(prxparse (/Flow id <(\d+)>/o));
      %let lmvPos       = %sysfunc(prxmatch (&lmvRx, &lmvCommandMsg));
      %if &lmvPos gt 0 %then
         %let lmvFlowId = %sysfunc(prxposn (&lmvRx, 1, &lmvCommandMsg));
      %syscall prxfree (lmvRx);
   %end;
   %else %if &module_type_cd = WSS %then %do;
      %local lmvFlowId;
      %let lmvFlowId  = &lmvCommandMsg;
   %end;
      /* Закрываем использованное окно */
      proc sql;
         %&ETL_DBMS._connect(mpLoginSet=ETL_SYS);

         /* Получаем имена в СУБД */
         %local lmvModuleFrameDbms;
         %&ETL_DBMS._table_name (mpSASTable=ETL_SYS.ETL_MODULE_X_FRAME,  mpOutFullNameKey=lmvModuleFrameDbms);

         execute by &ETL_DBMS (
            insert into &lmvModuleFrameDbms (
               module_id,
               frame_id,
               start_dttm,
               lsf_flow_id
            )
            values (
               %&ETL_DBMS._number(&module_id),
               %&ETL_DBMS._number(&frame_id),
               %&ETL_DBMS._timestamp(&JOB_START_DTTM),
               %&ETL_DBMS._number(&lmvFlowId)
            )
         );
         disconnect from &ETL_DBMS;
      quit;
   %end;
   %else %do;
      %job_event_reg (mpEventTypeCode=XCMD_FAILED,
                      mpEventValues= %bquote(&lmvCommandMsg) );
      %job_event_reg (mpEventTypeCode=SCHEDULER_MODULE_FAILED,
                      mpEventValues= %bquote(&module_txt (&module_id)) );
   %end;
%mend schedule_ready;
%util_loop_data (mpData=t_008_100_modules_ready, mpLoopMacro=schedule_ready, mpWhere= ready_flg = "Y");
%etl_job_finish;
%mend dwf_500_schedule_modules;