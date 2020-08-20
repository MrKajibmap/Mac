/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 665f06f6b49b6bf01022d5f355a21f3a59eb1499 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Транзакционно:
*        - отбирает уникальные дамми-записи, отсутствующие в снэпшоте, из входной таблицы с дамми-записями
*          Если указан код (или маска) снэпшота, то отбираются только подходящие дамми-записи
*        - удаляет из входной таблицы все записи, уже присутствующие в снэпшоте
*
*  ПАРАМЕТРЫ:
*     mpIn                    +  имя входного набора, порции дамми-записей
*     mpFieldsPK              +  поля первичного ключа
*     mpSnap                  +  имя входного набора, текущего состояния целевого справочника
*     mpOut                   +  имя выходного набора, уникальных и не существующих в снэпшоте дамми-записей
*     mpOutFieldsMap          -  мэппинг для выходного набора
*                                По умолчанию все поля
*     mpSnapshotCd            -  код (или маска LIKE) снэпшота, для которого отбираются дамми-записи.
*
******************************************************************
*  Использует:
*     %error_check
*     %ETL_DBMS_string
*     %etl_transaction_*
*     %job_event_reg
*     %list_expand
*     %member_names
*     %member_vars
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1. Требуется, чтобы обе таблицы были в одной библиотеке, например ETL_IA.
*     2. Трансформация поддерживает мэппинги, но поля берутся только из таблицы tpIn.
*     3. tpSnapshotCd должен быть задан пустым, если поле ETL_SNAPSHOT_CD отсутствует во входном наборе tpIn.
*        Если поле ETL_SNAPSHOT_CD присутствует, должен быть задан непустым.
*
******************************************************************
*  Пример использования:
*     В трансформе transform_get_delta_dummy.sas
*
******************************************************************
*  04-04-2014  Нестерёнок     Выделено из transform_get_delta_dummy.sas
******************************************************************/

%macro etl_get_delta_dummy (
   mpIn                       =  ,
   mpFieldsPK                 =  ,
   mpSnap                     =  ,
   mpOut                      =  ,
   mpOutFieldsMap             =  *,
   mpSnapshotCd               =
);
   /* Временные переменные */
   %local lmvLibref lmvInName lmvSnapName;
   %member_names (mpTable=&mpIn,   mpLibrefNameKey=lmvLibref, mpMemberNameKey=lmvInName);
   %member_names (mpTable=&mpSnap, mpLibrefNameKey=lmvLibref, mpMemberNameKey=lmvSnapName);

   %local lmvInFields;
   %let lmvInFields  = %member_vars (&mpIn);

   /* Проверяем корректность аргументов */
   %local lmvSnapField lmvSnapValue;
   %let lmvSnapField    =  %eval( %index(%upcase(&lmvInFields), ETL_SNAPSHOT_CD) gt 0);
   %let lmvSnapValue    =  %eval( not %is_blank(mpSnapshotCd));
   %if &lmvSnapField ne &lmvSnapValue %then %do;
      %job_event_reg (mpEventTypeCode=ILLEGAL_ARGUMENT,
         mpEventValues=%bquote(ETL_SNAPSHOT_CD: Field present=&lmvSnapField, Value requested=&lmvSnapValue) );
      %return;
   %end;

   %if %is_blank(mpOutFieldsMap) %then
      %let mpOutFieldsMap    =  *;

   %etl_transaction_start (mpLoginSet=&lmvLibref);

      /* Удаляем из входной таблицы записи, присутствующие в снэпшоте */
      execute by &ETL_DBMS (
         delete from &lmvInName tIn
         where exists (
            select 1 from &lmvSnapName tSnap
            where
               %list_expand(&mpFieldsPK, tIn.{} = tSnap.{}, mpOutDlm=%str( and ))
         )
      );
      %error_check (mpStepType=SQL_PASS_THROUGH);

      /* Отбираем из входной таблицы оставшиеся дамми-записи (возможно, сдублированные) */
      create table &mpOut as
      select &mpOutFieldsMap from connection to &ETL_DBMS (
         select * from &lmvInName
       %if not %is_blank(mpSnapshotCd) %then %do;
         where ETL_SNAPSHOT_CD like %&ETL_DBMS._string(&mpSnapshotCd)
       %end;
      );
      %error_check (mpStepType=SQL_PASS_THROUGH);

   %etl_transaction_finish;

   /* Избавляемся от потенциальных дублей */
   proc sort data=&mpOut nodupkey;
      by &mpFieldsPK;
   run;
%mend etl_get_delta_dummy;
