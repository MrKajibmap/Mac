/*****************************************************************
*  ВЕРСИЯ:
*     $Id: d43e13fb69c003c3f98b552aeca3cce90780d7a3 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Получает открытые записи реестра, возвращает их состояние.
*
*  ПАРАМЕТРЫ:
*     mpData                  +  имя набора данных, таблицы из источника или архива
*     mpResourceId            +  идентификатор ресурса
*                                BY_SOURCE - будет определен по источнику mpData
*                                BY_ARCH - будет определен по архиву mpData
*     mpVersion               +  идентификатор версии
*                                MIN - самая ранняя из открытых (A, P)
*     mpInRegistry            -  входной набор со списком открытых записей в реестре
*     mpNoOpenAction          -  Действие при отсутствии открытых ресурсов в реестре,
*                                ERR - вызывать ошибку, NOP - ничего не делать
*     mpOutRegistry           -  имя выходного набора, списка записей в реестре
*     mpOutResourceKey        -  имя макропеременной, в которую возвращается ресурс
*     mpOutVersionKey         -  имя макропеременной, в которую возвращается версия
*     mpOutStatusKey          -  имя макропеременной, в которую возвращается статус
*
******************************************************************
*  Использует:
*     %error_check
*     %etl_extract_*
*     %member_drop
*     %unique_id
*
******************************************************************
*  Пример использования:
*     В трансформах transform_extract.sas, transform_extract_db2.sas
*
******************************************************************
*  16-05-2014  Нестерёнок     Выделено из transform_extract.sas
*  11-08-2014  Кузенков       Добавлен mpNoOpenAction
******************************************************************/

%macro etl_extract_common (
   mpData            =  ,
   mpResourceId      =  ,
   mpVersion         =  ,
   mpInRegistry      =  ,
   mpNoOpenAction    =  ,
   mpOutRegistry     =  ,
   mpOutResourceKey  =  ,
   mpOutVersionKey   =  ,
   mpOutAvailableKey =  ,
   mpOutStatusKey    =
);
   %if %is_blank(mpInRegistry) %then %do;

      %if %is_blank(mpOutRegistry) %then %do;
         /* Получаем уникальный идентификатор */
         %local lmvUID;
         %unique_id (mpOutKey=lmvUID);

         %let mpOutRegistry   = work.tr_extr_reg_&lmvUID.;
      %end;

      %etl_extract_rsrc_registry_get(
         mpData        = &mpData,
         mpResourceId  = &mpResourceId,
         mpVersion     = &mpVersion,
         mpOutRegistry = &mpOutRegistry
      );

   %end; %else %do;
      %if not %is_blank(mpOutRegistry) %then %do;
         proc sql;
           create table &mpOutRegistry as
             select * from &mpInRegistry
           ;
         quit;
      %end; %else
         %let mpOutRegistry = &mpInRegistry;
   %end;


%if not ( %is_blank(mpOutResourceKey) and %is_blank(mpOutVersionKey) and %is_blank(mpOutAvailableKey) ) %then %do;
   %local lmvResourcesQty;

   proc sql noprint;
      select
         Count(*)

         /* Получаем ресурс, если надо */
      %if not %is_blank(mpOutResourceKey) %then %do;
       , min(resource_id) FORMAT=32.
      %end;

         /* Получаем версию, если надо */
      %if not %is_blank(mpOutVersionKey) %then %do;
       , min(version_id) FORMAT=32.
      %end;

         /* Получаем available_dttm, если надо */
      %if not %is_blank(mpOutAvailableKey) %then %do;
       , min(available_dttm) FORMAT=32.
      %end;

      into
         :lmvResourcesQty

      %if not %is_blank(mpOutResourceKey) %then %do;
       , :&mpOutResourceKey TRIMMED
      %end;

      %if not %is_blank(mpOutVersionKey) %then %do;
       , :&mpOutVersionKey TRIMMED
      %end;

      %if not %is_blank(mpOutAvailableKey) %then %do;
       , :&mpOutAvailableKey TRIMMED
      %end;

      from
         &mpOutRegistry
      where
         put (resource_id, res_id_extr_role.) = "SRC"
      ;
   quit;
   %error_check (mpStepType=SQL);
%end;

   /* Проверяем реестр, если надо */
%if not %is_blank(mpOutStatusKey) %then %do;
   %etl_extract_registry_check (
      mpInRegistry   = &mpOutRegistry,
      mpLogTable     = &mpData,
      mpNoOpenAction = &mpNoOpenAction,
      mpStatusKey    = &mpOutStatusKey
   );
%end;
%mend etl_extract_common;
