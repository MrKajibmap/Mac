/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 0b9b9a26d950b70768b299d8064590cbe5295974 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Отбирает из снэпшота записи, присутствующие во входном наборе (по ключу или кусочно).
*
*  ПАРАМЕТРЫ:
*     mpIn                    +  имя входного набора, порции новых данных
*     mpSnap                  +  имя входного набора, текущего состояния
*     mpFieldPK               +  список полей первичного ключа, не включает интервальные (mpField*Dttm)
*     mpFieldGroup            +  список полей кусочного обновления, например branch_id в пофилиальной загрузке.
*                                Также может использоваться для обновления неполным набором, в этом случае совпадает с mpFieldPK.
*                                По умолчанию не используется
*     mpOut                   +  имя выходного набора, отобранных записей снэпшота
*
******************************************************************
*  Использует:
*     %error_check
*     %list_expand
*     %member_*
*     %unique_id
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     нет
*
******************************************************************
*  Пример использования:
*     в макросах etl_get_delta_scd.sas, etl_get_delta_hist.sas
*
******************************************************************
*  22-09-2014  Нестерёнок     Начальное кодирование
******************************************************************/

%macro etl_get_delta_subset_snap (
   mpIn                       =  ,
   mpSnap                     =  ,
   mpFieldPK                  =  ,
   mpFieldGroup               =  ,
   mpOut                      =
);
   /* Получаем уникальный идентификатор для параллельного исполнения */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Получаем справочники изменяемых ключей в снэпшоте */
   %local lmvSnapLibref;
   %member_names (mpTable=&mpSnap, mpLibrefNameKey=lmvSnapLibref);
   %local lmvTmpPKTable lmvTmpGroupTable;
   %let lmvTmpPKTable      =  &lmvSnapLibref..etl_delta_pk_&lmvUID.;
   %let lmvTmpGroupTable   =  &lmvSnapLibref..etl_delta_grp_&lmvUID.;

   proc sort
      data=&mpIn (keep= &mpFieldPK)
      out=&lmvTmpPKTable (&ETL_BULKLOAD_OPTIONS)
      %if &ETL_DEBUG %then details;
      nodupkey
   ;
      by &mpFieldPK;
   run;
   %error_check;

   proc sort
      data=&mpIn (keep= &mpFieldGroup)
      out=&lmvTmpGroupTable (&ETL_BULKLOAD_OPTIONS)
      %if &ETL_DEBUG %then details;
      nodupkey
   ;
      by &mpFieldGroup;
   run;
   %error_check;

   /* Получаем необходимую часть снэпшота */
   proc sql;
      create table &mpOut as select
         s.*
      from
         &mpSnap s
      inner join
         &lmvTmpPKTable pk
      on
         %list_expand(&mpFieldPK, s.{}=pk.{}, mpOutDlm=%str( and ))
      union select
         s.*
      from
         &mpSnap s
      inner join
         &lmvTmpGroupTable grp
      on
         %list_expand(&mpFieldGroup, s.{}=grp.{}, mpOutDlm=%str( and ))
      order by
         %list_expand(&mpFieldPK, {}, mpOutDlm=%str(, ))
      ;
      %error_check (mpStepType=SQL);
   quit;

   %member_drop(&lmvTmpPKTable);
   %member_drop(&lmvTmpGroupTable);
%mend etl_get_delta_subset_snap;
