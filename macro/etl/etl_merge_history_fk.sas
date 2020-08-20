/*****************************************************************
*  ВЕРСИЯ:
*     $Id: aefe0ce80001a1ed7ae9972c1c249351eecf34ea $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Объединяет две таблицы, содержащих историю сущностей, связанных по FK, в одну общую историю.
*
*  ПАРАМЕТРЫ:
*     mpIn1, mpIn2            +  имена входных наборов, отдельных историй сущностей
*     mpFieldPK1              +  список полей первичного ключа tpIn1, не включает интервальные (tpField*Dttm)
*     mpFieldPK2              +  список полей первичного ключа tpIn2, не включает интервальные (tpField*Dttm)
*                                Эти же поля должны присутствовать в tpIn1 как FK
*     mpFieldStartDttm        +  имя поля начала временного интервала действия версии
*     mpFieldEndDttm          +  имя поля конца временного интервала действия версии
*     mpOut                   +  имя выходного набора, общей истории
*     mpOutOptions            -  доп. опции для выходного набора
*     mpJoinType              -  Если LEFT, то усекаются интервалы, не входящие в tpIn1
*                                Если RIGHT, то усекаются интервалы, не входящие в tpIn2
*                                По умолчанию FULL (без усечения)
*
******************************************************************
*  Использует:
*     %list_expand
*     %member_vars
*     %merge_history
*     %unique_id
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     в трансформе transform_merge_history_fk.sas
*
******************************************************************
*  19-12-2014  Нестерёнок     Начальное кодирование
*  16-03-2015  Сазонов        Изменил джоин inner на left
*  03-07-2015  Сазонов        Добавил даты в объединение
******************************************************************/

%macro etl_merge_history_fk (
   mpIn1                =  ,
   mpIn2                =  ,
   mpFieldPK1           =  ,
   mpFieldPK2           =  ,
   mpFieldStartDttm     =  ,
   mpFieldEndDttm       =  ,
   mpOut                =  ,
   mpOutOptions         =  ,
   mpJoinType           =  FULL
);
   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Получаем полный PK для обеих таблиц */
   %local lmvPKVars lmvPKTable;
   %let lmvPKVars    =  %member_vars (&mpIn1, mpKeep=&mpFieldPK1 &mpFieldPK2);
   %let lmvPKTable   =  etl_merge_pk_&lmvUID.;

   proc sort data=&mpIn1 (keep= &lmvPKVars &mpFieldStartDttm &mpFieldEndDttm) out=&lmvPKTable nodupkey;
      by &lmvPKVars &mpFieldStartDttm &mpFieldEndDttm;
   run;

   /* Расширяем ключ в подчиненной таблице */
   %local lmvIn2ExtTable lmvExtVars lmvExtVars2;
   %let lmvIn2ExtTable  =  etl_merge_in2ext_&lmvUID.;
   %let lmvExtVars      =  %member_vars (&lmvPKTable, mpDrop=&mpFieldPK2 &mpFieldStartDttm &mpFieldEndDttm);
   %let lmvExtVars2     =  %member_vars (&mpIn2, mpDrop=&mpFieldStartDttm &mpFieldEndDttm);

   proc sql;
      create table &lmvIn2ExtTable as select
         %list_expand(&lmvExtVars2, t2.{}, mpOutDlm=%str(,) ),
       max (pk.&mpFieldStartDttm, t2.&mpFieldStartDttm ) as &mpFieldStartDttm format=datetime20.,
       min (pk.&mpFieldEndDttm, t2.&mpFieldEndDttm) as &mpFieldEndDttm format=datetime20.,
         %list_expand(&lmvExtVars, pk.{}, mpOutDlm=%str(,) )
      from
         &mpIn2 t2 left join
         &lmvPKTable pk
      on
         %list_expand(&mpFieldPK2, t2.{}=pk.{}, mpOutDlm=%str( and ) )
       and (pk.&mpFieldEndDttm > t2.&mpFieldStartDttm)
       and (pk.&mpFieldStartDttm < t2.&mpFieldEndDttm)
      ;
   quit;

   /* Определяем порядок объединения */
   %local lmvMainFlg;
   %if &mpJoinType = FULL %then
      %let lmvMainFlg   = NO;
   %else
      %let lmvMainFlg   = YES;

   %local lmvIn1 lmvIn2;
   %if &mpJoinType ne RIGHT %then %do;
      %let lmvIn1       = &mpIn1;
      %let lmvIn2       = &lmvIn2ExtTable;
   %end;
   %else %do;
      %let lmvIn1       = &lmvIn2ExtTable;
      %let lmvIn2       = &mpIn1;
   %end;

   /* Формируем общую историю */
   %merge_history (
      TABLE_LIST           =  &lmvIn1 &lmvIn2,
      TARGET_TABLE         =  &mpOut,
      KEY_COLUMN           =  &lmvPKVars,
      VALID_FROM_COLUMN    =  &mpFieldStartDttm,
      VALID_TO_COLUMN      =  &mpFieldEndDttm,
      OPTION_OUT           =  %unquote(&mpOutOptions),
      MAIN_TABLE           =  &lmvMainFlg
   );
%mend etl_merge_history_fk;
