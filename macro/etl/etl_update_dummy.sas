/*****************************************************************
* ВЕРСИЯ:
*   $Id: 47187cc3e019bb2340a1a6adef7ff6de1f17269d $
*
******************************************************************
* НАЗНАЧЕНИЕ:
*   Удаляет из целевой таблицы DUMMY записи, являющиеся новыми во входном дельта-наборе SNUM.
*   Работает в режиме SQL.
*
* ПАРАМЕТРЫ:
*   mpIn                   + имя входного дельта-набора
*   mpFieldsPK             + поля первичного ключа
*   mpOut                  + имя выходного набора, обновляемой таблицы DUMMY
*
******************************************************************
* Пример использования:
*   в трансформе transform_update_dummy.sas
*
******************************************************************
* 20-05-2013   Нестерёнок     Начальное кодирование
******************************************************************/

%macro etl_update_dummy (
   mpIn=,
   mpFieldsPK=,
   mpOut=
);
   /* Если уже возникла ошибка, то выход */
   %if &ETL_MODULE_RC ne 0 %then %return;

   /* Если дельта пустая, то выход */
   select 0 from &mpIn(obs=1);
   %if &SQLOBS=0 %then %return;

   %local lmvInDbms lmvOutDbms;
   %&ETL_DBMS._table_name (mpSASTable=&mpIn,  mpOutFullNameKey=lmvInDbms);
   %&ETL_DBMS._table_name (mpSASTable=&mpOut, mpOutFullNameKey=lmvOutDbms);

   /* Удаляем только что добавленные записи (N и 1) */
   execute by &ETL_DBMS (
      delete from &lmvOutDbms dummy
      where exists (
         select 1 from &lmvInDbms snum
            where snum.etl_delta_cd in ('N', '1', 'P')
              and %list_expand(&mpFieldsPK, dummy.{}=snum.{}, mpOutDlm=%STR( and ) )
      )
   );
   %error_check (mpStepType=SQL_PASS_THROUGH);
%mend etl_update_dummy;

