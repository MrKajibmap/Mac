/*****************************************************************
*  ВЕРСИЯ:
*     $Id: a3887fe7b6a1af05925d05ab557867de741038c1 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Обновляет целевую таблицу STG/IA/DDS при помощи дельта-набора.
*     Работает в режиме SQL.
*     Таблица журнала необязательна.
*
*  ПАРАМЕТРЫ:
*     mpIn                    +  имя входного дельта-набора
*     mpFieldsPK              +  поля первичного ключа
*     mpFieldStartDttm        -  имя поля начала временного интервала действия версии, обычно VALID_FROM_DTTM
*     mpFieldEndDttm          -  имя поля конца временного интервала действия версии, обычно VALID_TO_DTTM
*     mpFieldProcessedDttm    -  имя поля даты обновления версии.  Обязательно, если используется журнал
*     mpFieldDelta            -  имя поля для кода дельта-строки
*                                по умолчанию etl_delta_cd.
*     mpOut                   +  имя выходного набора, обновляемой таблицы STG/IA/DDS
*     mpJrnl                  -  имя выходного набора, журнала DDS
*     mpJrnlStartDttm         -  имя поля начала интервала действия версии журнала, по умолчанию JRNL_FROM_DTTM
*     mpJrnlEndDttm           -  имя поля конца интервала действия версии журнала, по умолчанию JRNL_TO_DTTM
*     mpGenericUpdate         -  оптимизация: генерировать заточенный (No) или стандартный (Yes) код для обновления
*                                по умолчанию No
*
******************************************************************
*  Использует:
*     ETL_TXN_CONNECTION
*     ETL_TXN_LOGIN_SET
*     ETL_MODULE_RC
*     %error_check
*     %ETL_DBMS_table_name
*     %list_expand
*     %member_vars_*
*     %unique_id
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1. Для mpGenericUpdate=No необходимо, чтобы ключ mpFieldsPK был заявлен в mpOut как первичный (или уникальный).
*
******************************************************************
*  Пример использования:
*     в трансформе transform_update_dds.sas
*
******************************************************************
*  24-02-2012  Нестерёнок     Начальное кодирование
*  18-07-2012  Нестерёнок     Убран mpOutDeleteClosed, дельта должна сразу строиться правильно
*  18-09-2012  Нестерёнок     Новый тип дельты P - фантомная запись для занесения в журнал
*  22-02-2013  Нестерёнок     Фантомные записи уходят в журнал, если он есть; иначе в основную таблицу
*  26-02-2013  Нестерёнок     Новый тип дельты 1 - первая запись в истории ключа (подкласс N)
*  29-05-2013  Нестерёнок     Порядок наката изменен на D, U, I
*  17-07-2014  Нестерёнок     Апдейт может делаться заточенным или стандартным кодом
******************************************************************/

%macro etl_update_dds (
   mpIn                    =  ,
   mpFieldsPK              =  ,
   mpFieldStartDttm        =  ,
   mpFieldEndDttm          =  ,
   mpFieldProcessedDttm    =  ,
   mpFieldDelta            =  etl_delta_cd,
   mpOut                   =  ,
   mpJrnl                  =  ,
   mpJrnlStartDttm         =  jrnl_from_dttm,
   mpJrnlEndDttm           =  jrnl_to_dttm,
   mpGenericUpdate         =  No
);
   /* Макросы для удобства */
   /* update-запрос - точка входа */
   %macro _etl_update_query (mpFieldsUpdate=, mpDelta=);
      %if &mpGenericUpdate = No %then
         %_etl_update_query_&ETL_DBMS. (mpFieldsUpdate=&mpFieldsUpdate, mpDelta=&mpDelta);
      %else
         %_etl_update_query_generic (mpFieldsUpdate=&mpFieldsUpdate, mpDelta=&mpDelta);
   %mend _etl_update_query;

   /* update-запрос - generic */
   %macro _etl_update_query_generic (mpFieldsUpdate=, mpDelta=);
      execute by &ETL_TXN_CONNECTION (
         update &lmvOutDbms target
         set (
            %member_vars_expand(&mpFieldsUpdate, {})
         ) = (
            select
               %member_vars_expand(&mpFieldsUpdate, {})
            from &lmvInDbms where &mpFieldDelta = %&ETL_DBMS._string(&mpDelta) and
            %list_expand(&mpFieldsPK &mpFieldStartDttm, target.{}={}, mpOutDlm=%STR( and ) )
         )
         where exists (
            select 1
            from &lmvInDbms where &mpFieldDelta = %&ETL_DBMS._string(&mpDelta) and
            %list_expand(&mpFieldsPK &mpFieldStartDttm, target.{}={}, mpOutDlm=%STR( and ) )
         )
      );
      %error_check (mpStepType=SQL_PASS_THROUGH);
   %mend _etl_update_query_generic;

    /* update-запрос - postgres */
   %macro _etl_update_query_postgres (mpFieldsUpdate=, mpDelta=);
      execute by &ETL_TXN_CONNECTION (
         update &lmvOutDbms target
         set
            %member_vars_expand(&mpFieldsUpdate, {}=source.{} )
         from &lmvInDbms source
         where
            source.&mpFieldDelta = %postgres_string(&mpDelta) and
            %member_vars_expand(&mpFieldsPK &mpFieldStartDttm, target.{}=source.{}, mpOutDlm=%STR( and ) )
      );
      %error_check (mpStepType=SQL_PASS_THROUGH);
   %mend _etl_update_query_postgres;

   /* update-запрос - заточенный под Oracle */
   %macro _etl_update_query_oracle (mpFieldsUpdate=, mpDelta=);
      /* Соединяемся через другое подключение, чтобы не мешать текущей транзакции */
      %oracle_connect(mpLoginSet=&ETL_TXN_LOGIN_SET, mpAlias=oraupdq);

      /* Создаем временную таблицу с обновляющими записями */
      %local lmvUpdateTable;
      %let lmvUpdateTable    = etl_update_&lmvUID._&mpDelta.;
      execute by oraupdq (
         create table &lmvUpdateTable as select
            %member_vars_expand(&mpFieldsPK &mpFieldStartDttm &mpFieldsUpdate, {})
         from &lmvInDbms
         where &mpFieldDelta = %oracle_string(&mpDelta)
      );
      %error_check (mpStepType=SQL_PASS_THROUGH);

      execute by oraupdq (
         alter table &lmvUpdateTable add primary key (
            %list_expand(&mpFieldsPK &mpFieldStartDttm, {}, mpOutDlm=%str(,) )
         )
      );
      %error_check (mpStepType=SQL_PASS_THROUGH);

      /* Выполняем обновление */
      execute by &ETL_TXN_CONNECTION (
         update (select
            %member_vars_expand(&mpFieldsUpdate, target.{}),
            %member_vars_expand(&mpFieldsUpdate, delta.{} as delta_{#})
         from
            &lmvOutDbms target,
            &lmvUpdateTable delta
         where
            %list_expand(&mpFieldsPK &mpFieldStartDttm, target.{}=delta.{}, mpOutDlm=%str( and ) )
         )
         set
            %member_vars_expand(&mpFieldsUpdate, {} = delta_{#})
      );
      %error_check (mpStepType=SQL_PASS_THROUGH);

      /* Удаляем временную таблицу */
      execute by oraupdq (
         drop table &lmvUpdateTable
      );
      %error_check (mpStepType=SQL_PASS_THROUGH);

      disconnect from oraupdq;
      %error_check (mpStepType=SQL);
   %mend _etl_update_query_oracle;


   /* Если уже возникла ошибка, то выход */
   %if &ETL_MODULE_RC ne 0 %then %return;

   /* Если дельта пустая, то выход */
   select 0 from &mpIn(obs=1);
   %if &SQLOBS=0 %then %return;

   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   %local lmvInDbms lmvOutDbms;
   %&ETL_DBMS._table_name (mpSASTable=&mpIn,  mpOutFullNameKey=lmvInDbms);
   %&ETL_DBMS._table_name (mpSASTable=&mpOut, mpOutFullNameKey=lmvOutDbms);

   /* Получаем списки обновляемых переменных */
   %local lmvTargetVars lmvDeltaVars lmvInsertVars lmvUpdateVars;

   /* переменные обновляемой таблицы */
   %member_vars_get(&mpOut, lmvTargetVars);
   /* переменные дельты без служебных */
   %member_vars_get(&mpIn,  lmvDeltaVars,    mpDrop=&mpFieldDelta);
   /* переменные, добавляемые из дельты (т.е. общие между дельтой и обновляемой таблицей) */
   /* TODO: UB (см. #29) */
   %member_vars_get(&mpOut, lmvInsertVars,   mpKeep=&lmvDeltaVars);
   /* переменные, обновляемые U-дельтой */
   %member_vars_get(&mpIn,  lmvUpdateVars,   mpDrop=&mpFieldsPK &mpFieldStartDttm &mpFieldDelta);

   %local lmvUseJrnl;
   %let lmvUseJrnl  = %eval( not %is_blank(mpJrnl) );

   /* Если задан журнал, переносим все обновляемые (D, С, U) записи в него */
   /* Также заносим в журнал фантомные (P) записи */
   %if &lmvUseJrnl %then %do;
      %local lmvJrnlDbms;
      %&ETL_DBMS._table_name (mpSASTable=&mpJrnl, mpOutFullNameKey=lmvJrnlDbms);

      /* Копируем записи, которые будут обновляться, в журнал */
      execute by &ETL_TXN_CONNECTION (
         insert into &lmvJrnlDbms jrnl (
           %member_vars_expand(&lmvTargetVars &mpJrnlStartDttm &mpJrnlEndDttm, {})
         )
         select
             %member_vars_expand(&lmvTargetVars &mpFieldProcessedDttm, target.{})
           , delta.&mpFieldProcessedDttm
         from &lmvOutDbms target, &lmvInDbms delta
         where
            (
               %list_expand(&mpFieldsPK &mpFieldStartDttm, target.{}=delta.{}, mpOutDlm=%STR( and ))
               and delta.&mpFieldDelta in ('D', 'C', 'U')
            )
      );
      %error_check (mpStepType=SQL_PASS_THROUGH);

      /* Добавляем фантомные записи в журнал */
      execute by &ETL_TXN_CONNECTION (
         insert into &lmvJrnlDbms jrnl (
           %member_vars_expand(&lmvInsertVars &mpJrnlStartDttm &mpJrnlEndDttm, {})
         )
         select
             %member_vars_expand(&lmvInsertVars, delta.{})
           , delta.&mpFieldProcessedDttm
           , delta.&mpFieldProcessedDttm
         from &lmvInDbms delta
         where
            delta.&mpFieldDelta = 'P'
      );
      %error_check (mpStepType=SQL_PASS_THROUGH);
   %end;

   /* Удаляем старые записи (D) */
   /* Удаление возможно, даже если не заданы интервальные поля */
   execute by &ETL_TXN_CONNECTION (
      delete from &lmvOutDbms target
      where exists (
         select 1 from &lmvInDbms delta
            where delta.&mpFieldDelta = 'D'
              and %list_expand(&mpFieldsPK &mpFieldStartDttm, target.{}=delta.{}, mpOutDlm=%STR( and ) )
      )
   );
   %error_check (mpStepType=SQL_PASS_THROUGH);

   /* Закрываем старые записи (C) */
   /* Закрытие возможно, только если заданы интервальные поля */
   %if not %is_blank(mpFieldEndDttm) %then %do;
      /* Закрываем старые записи (C + mpFieldEndDttm) */
      %_etl_update_query (
         mpFieldsUpdate =  &mpFieldEndDttm &mpFieldProcessedDttm,
         mpDelta        =  C
      );
   %end;

   /* Обновляем актуальные записи на месте (U) */
   %_etl_update_query (
      mpFieldsUpdate =  &lmvUpdateVars,
      mpDelta        =  U
   );

   /* Добавляем новые (N и 1) записи */
   /* Если нет журнала, также заносим и фантомные (P) записи */
   execute by &ETL_TXN_CONNECTION (
     insert into &lmvOutDbms
      (
         %member_vars_expand(&lmvInsertVars, {})
      )
      select
         %member_vars_expand(&lmvInsertVars, {})
      from &lmvInDbms delta
      where
         delta.&mpFieldDelta in ('N', '1'
         %if not &lmvUseJrnl %then %do;
            , 'P'
         %end;
         )
   );
   %error_check (mpStepType=SQL_PASS_THROUGH);

   /* освобождаем ресурсы */
   %member_vars_clean(&lmvTargetVars);
   %member_vars_clean(&lmvDeltaVars);
   %member_vars_clean(&lmvInsertVars);
   %member_vars_clean(&lmvUpdateVars);
%mend etl_update_dds;
