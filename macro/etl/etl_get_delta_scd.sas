/*****************************************************************
*  ВЕРСИЯ:
*     $Id: d4afe61612b66e5f5e6aee2ce364767df0b4daf1 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Создает дельта-наборы для таблицы и ее прошлого состояния
*     по технике SCD1/SCD2.
*     В дельта-набор снэпшота попадают все поля снэпшота + дельта-код.
*
*     Если поле &mpFieldDelta присутствует во входном наборе, то обработка происходит в дельта-режиме,
*     т.е. записи, не вошедшие во входной набор, не изменяются.
*
*  ПАРАМЕТРЫ:
*     mpIn                    +  имя входного набора, порции новых данных
*     mpInSCD1Fields          -  список полей для расчета хэш-суммы по SCD1
*                                по умолчанию все ключевые поля (mpFieldPK)
*     mpInSCD2Fields          -  список полей для расчета хэш-суммы по SCD2
*                                по умолчанию все поля, за исключением ключевых (mpFieldPK) и временнЫх (mpField*Dttm)
*     mpSnap                  +  имя входного набора, текущего состояния
*     mpFieldPK               +  список полей первичного ключа, не включает интервальные (mpField*Dttm)
*     mpFieldDummyType        -  имя поля типа дамми, обычно x_dummy_type_cd
*     mpFieldGroup            -  список полей кусочного обновления, например branch_id в пофилиальной загрузке.
*                                Также может использоваться для обновления неполным набором, в этом случае совпадает с mpFieldPK.
*                                По умолчанию не используется
*     mpHashGroup             -  оптимизация: фильтр кусочного обновления через хэш-таблицу (Yes - да, No - нет)
*                                по умолчанию Yes.
*     mpFieldTimeFrameDt      -  имя поля даты для окна обновления, например sales_dt для продаж.
*                                Если задано, то непришедшие записи закрываются в пределах окна (иначе закрываются все).
*                                По умолчанию не используется
*     mpTimeFrameValue        -  размер окна обновления (в днях)
*     mpCreateNew             -  создавать первые версии (Yes - да, No - нет)
*                                по умолчанию Yes.
*     mpSubsetSnap            -  оптимизация: выборка только нужных ключей из снэпшота (Yes - да, No - нет)
*                                по умолчанию No.
*     mpOut                   +  имя выходного набора, дельты mpIn относительно mpSnap
*     mpSnUp                  +  имя выходного набора, дельты mpSnap относительно mpSnap
*     mpFieldDelta            -  имя поля для кода дельта-строки
*                                по умолчанию etl_delta_cd.
*                                Варианты: N - новая, U - обновление на месте, C - закрыта, V - новая версия (комбинация C+N)
*     mpFieldDigest1          -  имя поля для расчета хэш-суммы по SCD1
*                                по умолчанию etl_digest1_cd.
*     mpFieldDigest2          -  имя поля для расчета хэш-суммы по SCD2
*                                по умолчанию etl_digest2_cd.
*     mpFieldStartDttm        -  имя поля начала временного интервала действия версии
*                                по умолчанию valid_from_dttm.
*     mpFieldEndDttm          -  имя поля конца временного интервала действия версии
*                                по умолчанию valid_to_dttm.
*     mpFieldProcessedDttm    -  имя поля даты обновления версии
*                                по умолчанию processed_dttm.
*
******************************************************************
*  Использует:
*     %error_check
*     %etl_get_delta_subset_snap
*     %list_expand
*     %member_*
*     %unique_id
*     %util_digest_expr
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1. Если входной набор содержит историю, то даты интервалов должны уже быть
*        проставлены и непротиворечивы.  Кроме того, актуальная запись (если есть)
*        должна иметь концом ETL_SCD_FUTURE_DTTM.
*     2. Вне режима инициализационной загрузки за значение начала интервала новой версии
*        принимается JOB_START_DTTM, а не значение mpFieldStartDttm.
*     3. Дельта-режим исключает режим кусочного обновления.  Если заданы оба, то
*        вместо заданных полей кусочного обновления используются поля первичного ключа.
*     4. Оптимизация mpSubsetSnap возможна только в дельта-режиме или режиме кусочного обновления.
*     5. Поля интервалов не могут использоваться для окна обновления (mpFieldTimeFrameDt).
*
******************************************************************
*  Пример использования:
*     в трансформе transform_get_delta_scd.sas
*
******************************************************************
*  24-02-2012  Нестерёнок     Начальное кодирование
*  18-07-2012  Нестерёнок     Для снэпшота формируются D-, а не C-записи
*  27-07-2012  Нестерёнок     Окно обновления теперь опционально и задается параметром mpFieldTimeFrameDt
*  18-09-2012  Нестерёнок     Поддержка предзакрытых ключей
*  22-02-2013  Нестерёнок     Фантомные записи классифицируются как P
*  26-02-2013  Нестерёнок     Новый тип дельты 1 - первая запись в истории ключа (подкласс N)
*  03-06-2014  Нестерёнок     Оптимизация: режим mpSubsetSnap
*  15-07-2014  Нестерёнок     Начало интервала новой версии - JOB_START_DTTM, а не mpFieldStartDttm
*  16-07-2014  Нестерёнок     Оптимизация: режим mpHashGroup
*  27-02-2015  Сазонов        Для db2 принудительное преобразование дат в timestamp(0)
*  27-07-2020  Михайлова      mpFieldTimeFrameDttm изменено на mpFieldTimeFrameDt (теперь дата, а не дата-время), добавлен параметр размера окна (специально для проекта MCD)
******************************************************************/

%macro etl_get_delta_scd (
   mpIn                       =  ,
   mpInSCD1Fields             =  ,
   mpInSCD2Fields             =  ,
   mpSnap                     =  ,
   mpFieldPK                  =  ,
   mpFieldDummyType           =  ,
   mpFieldGroup               =  ,
   mpHashGroup                =  Yes,
   mpFieldTimeFrameDt         =  ,
   mpTimeFrameValue           =  ,
   mpOut                      =  ,
   mpSnUp                     =  ,
   mpCreateNew                =  Yes,
   mpSubsetSnap               =  No,
   mpFieldDelta               =  ETL_DELTA_CD,
   mpFieldDigest1             =  ETL_DIGEST1_CD,
   mpFieldDigest2             =  ETL_DIGEST2_CD,
   mpFieldStartDttm           =  VALID_FROM_DTTM,
   mpFieldEndDttm             =  VALID_TO_DTTM,
   mpFieldProcessedDttm       =  PROCESSED_DTTM
);

   /************************************************ Инициализация ************************************************/

   /* Получаем уникальный идентификатор для параллельного исполнения */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Поля сравнения по умолчанию */
   %if %is_blank(mpInSCD1Fields) %then
      %let mpInSCD1Fields = &mpFieldPK;
   %if %is_blank(mpInSCD2Fields) %then
      %let mpInSCD2Fields = %member_vars(&mpIn, mpDrop=&mpFieldPK &mpFieldStartDttm &mpFieldEndDttm &mpFieldProcessedDttm);
   %if %is_blank(mpInSCD2Fields) %then
      %let mpInSCD2Fields = &mpFieldPK;
   %if %is_blank(mpFieldStartDttm) %then
      %let mpFieldStartDttm = VALID_FROM_DTTM;
   %if %is_blank(mpFieldEndDttm) %then
      %let mpFieldEndDttm = VALID_TO_DTTM;
   %if %is_blank(mpFieldDigest1) %then
      %let mpFieldDigest1 = ETL_DIGEST1_CD;
   %if %is_blank(mpFieldDigest2) %then
      %let mpFieldDigest2 = ETL_DIGEST2_CD;

   /* Получаем режимы работы */
   %local lmvUseDummy lmvUseProcessedDttm lmvUseDeltaMode lmvUseGroupMode lmvUseTimeFrame;
   %let lmvUseDummy           = %eval( not %is_blank(mpFieldDummyType) );
   %let lmvUseProcessedDttm   = %eval( not %is_blank(mpFieldProcessedDttm) );
   %let lmvUseDeltaMode       = %member_vars_exist (mpData=&mpIn, mpVars=&mpFieldDelta);
   %let lmvUseGroupMode       = %eval( not %is_blank(mpFieldGroup) );
   %let lmvUseTimeFrame       = %eval( not %is_blank(mpFieldTimeFrameDt) );

   /* Дельта-режим интерпретируется как кусочное обновление по первичному ключу */
   %if &lmvUseDeltaMode %then %do;
      %let mpFieldGroup       = &mpFieldPK;
      %let lmvUseGroupMode    = 1;
   %end;
   /* Оптимизация mpSubsetSnap возможна только в дельта-режиме или режиме кусочного обновления */
   %if not &lmvUseGroupMode %then
      %let mpSubsetSnap       = No;

   /* Проверка аргументов */
   %if &lmvUseTimeFrame %then %do;
      %if (%upcase(&mpFieldTimeFrameDt) = %upcase(&mpFieldStartDttm)) or
          (%upcase(&mpFieldTimeFrameDt) = %upcase(&mpFieldEndDttm)) %then %do;
         %job_event_reg (mpEventTypeCode=ILLEGAL_ARGUMENT,
                         mpEventValues= %bquote(Поле окна обновления не может совпадать с интервальным) );
         %return;
      %end;
   %end;

   %local lmvInTestVars;
   %let lmvInTestVars      = %util_list(&mpFieldPK &mpFieldStartDttm &mpFieldEndDttm
                                                &mpFieldGroup &mpFieldTimeFrameDt &mpFieldDummyType,
                                        mpOutDlm=%str( ), mpUnique=Y );
   %if not %member_vars_exist (mpData=&mpIn, mpVars=&lmvInTestVars) %then %do;
      %job_event_reg (
         mpEventTypeCode   =  ILLEGAL_ARGUMENT,
         mpEventDesc       =  %bquote(Структура таблицы &mpIn некорректна),
         mpEventValues     =  %bquote(Должны присутствовать поля &lmvInTestVars) );
      %return;
   %end;

   %if %member_vars_exist (mpData=&mpIn, mpVars=&mpFieldDigest1) or
       %member_vars_exist (mpData=&mpIn, mpVars=&mpFieldDigest2)
   %then %do;
      %job_event_reg (
         mpEventTypeCode   =  ILLEGAL_ARGUMENT,
         mpEventDesc       =  %bquote(Структура таблицы &mpIn некорректна),
         mpEventValues     =  %bquote(Полей &mpFieldDigest1, &mpFieldDigest2 не должно быть) );
      %return;
   %end;

   %local lmvSnapTestVars lmvSnapVars;
   %let lmvSnapVars        = %member_vars(&mpSnap);
   %let lmvSnapTestVars    = %util_list(&mpFieldPK &mpFieldStartDttm &mpFieldEndDttm
                                                &mpFieldGroup &mpFieldTimeFrameDt &mpFieldDigest1 &mpFieldDigest2 &mpFieldDummyType,
                                        mpOutDlm=%str( ), mpUnique=Y );
   %if not %member_vars_exist (mpData=&mpSnap, mpVars=&lmvSnapTestVars) %then %do;
      %job_event_reg (
         mpEventTypeCode   =  ILLEGAL_ARGUMENT,
         mpEventDesc       =  %bquote(Структура таблицы &mpSnap некорректна),
         mpEventValues     =  %bquote(Должны присутствовать поля &lmvSnapTestVars) );
      %return;
   %end;

   /* Получаем имена временных переменных */
   %local lmvFieldStartDttmNew lmvFieldEndDttmNew;
   %let lmvFieldStartDttmNew  = &mpFieldStartDttm._&lmvUID;
   %let lmvFieldEndDttmNew    = &mpFieldEndDttm._&lmvUID;
   %local lmvFieldDigest1New lmvFieldDigest2New;
   %let lmvFieldDigest1New    = &mpFieldDigest1._&lmvUID;
   %let lmvFieldDigest2New    = &mpFieldDigest2._&lmvUID;
   %local lmvFieldDummyTypeNew;
   %let lmvFieldDummyTypeNew  = &mpFieldDummyType._&lmvUID;

   %local lmvPKIndex;
   %let lmvPKIndex = &mpFieldPK;
   %if %sysfunc(countw(&mpFieldPK, , s)) gt 1 %then
      %let lmvPKIndex = etl_pk_&lmvUID.;

   /************************************************ Подготовка данных ************************************************/

   /* Упорядочение входного набора */
   %local lmvInSorted;
   %let lmvInSorted  = work.etl_delta_&lmvUID._ins;
   %member_drop (&lmvInSorted);

   proc sort
      data=&mpIn
      out=&lmvInSorted
      %if &ETL_DEBUG %then details;
   ;
      by &mpFieldPK &mpFieldStartDttm;
   run;
   %error_check (mpStepType=DATA);

   /* Приводим входной набор в удобную для сравнения со снэпшотом форму */
   %local lmvIn;
   %let lmvIn        = work.etl_delta_&lmvUID._in;
   %member_drop (&lmvIn);

   %local lmvSCD1MD5Exp lmvSCD2MD5Exp;
   %util_digest_expr (mpIn=&lmvInSorted, mpDigestFields=&mpInSCD1Fields, mpOutKey=lmvSCD1MD5Exp);
   %util_digest_expr (mpIn=&lmvInSorted, mpDigestFields=&mpInSCD2Fields, mpOutKey=lmvSCD2MD5Exp);

   data &lmvIn (sortedby= &mpFieldPK);
      set &lmvInSorted;

      /* Добавление новых переменных */
      attrib
         &mpFieldDelta           length=$1
         &lmvFieldStartDttmNew   length=8    format=datetime20.
         &lmvFieldEndDttmNew     length=8    format=datetime20.
         &lmvFieldDigest1New     length=$16  format=$hex32.
         &lmvFieldDigest2New     length=$16  format=$hex32.
      ;

      call missing(&mpFieldDelta);
%if &ETL_TYPE eq INIT %then %do;
      &lmvFieldStartDttmNew = coalesce(&mpFieldStartDttm, &JOB_START_DTTM);
%end;
%else %do;
      &lmvFieldStartDttmNew = &JOB_START_DTTM;
%end;
      &lmvFieldEndDttmNew   = coalesce(&mpFieldEndDttm,   &ETL_SCD_FUTURE_DTTM);
      drop &mpFieldStartDttm &mpFieldEndDttm;

      &lmvFieldDigest1New = &lmvSCD1MD5Exp;
      &lmvFieldDigest2New = &lmvSCD2MD5Exp;

      %if &lmvUseDummy %then %do;
         &lmvFieldDummyTypeNew = &mpFieldDummyType;
         drop &mpFieldDummyType;
      %end;
   run;
   %error_check (mpStepType=DATA);

   /* Получаем упорядоченную таблицу снэпшота */
   %local lmvSnap;
   %let lmvSnap = work.etl_delta_&lmvUID._snap;
%if &mpSubsetSnap = Yes %then %do;
   /* Получаем справочники изменяемых ключей в снэпшоте */
   %etl_get_delta_subset_snap (
      mpIn                       =  &lmvIn,
      mpSnap                     =  &mpSnap,
      mpFieldPK                  =  &mpFieldPK,
      mpFieldGroup               =  &mpFieldGroup,
      mpOut                      =  &lmvSnap
   );
   %let lmvUseGroupMode    = 0;
%end;
%else %do;
   /* Упорядочение снэпшота */
   proc sort
      data=&mpSnap
      out=&lmvSnap
      %if &ETL_DEBUG %then details;
   ;
      by &mpFieldPK;
   run;
   %error_check (mpStepType=DATA);
%end;

   /* Получаем справочник кусочного обновления */
%if &lmvUseGroupMode %then %do;
   %local lmvGroup lmvGroupIndex;
   %let lmvGroup      = work.etl_delta_&lmvUID._group;
   %let lmvGroupIndex = &mpFieldGroup;
   %if %sysfunc(countw(&mpFieldGroup, , s)) gt 1 %then
      %let lmvGroupIndex = etl_group_&lmvUID.;

   proc sort
      data=&lmvIn (keep= &mpFieldGroup)
      out=&lmvGroup
      %if &ETL_DEBUG %then details;
      nodupkey
   ;
      by &mpFieldGroup;
   run;
   %error_check;

%if &mpHashGroup = Yes %then %do;
   %local lmvHashGroup;
   %let lmvHashGroup = hash_grp_&lmvUID.;
%end;
%else %do;
   proc sql;
      create unique index &lmvGroupIndex on &lmvGroup ( %list_expand(&mpFieldGroup, {}, mpOutDlm=%str(,)) );
   quit;
   %error_check (mpStepType=SQL);
%end;
%end;

   /************************************************ Ищем дельту ************************************************/
   %local lmvInputVars;
   %let lmvInputVars = %member_vars(&mpIn);
   %local lmvCheck1;
   %let lmvCheck1 = work.etl_delta_&lmvUID._ck1;
   %local lmvLibref;
   %member_names (mpTable=&mpOut, mpLibrefNameKey=lmvLibref);

   data
      &mpOut (
         keep=
            &lmvInputVars
            &mpFieldStartDttm &mpFieldEndDttm
            &mpFieldDelta
            &mpFieldProcessedDttm
%if &ETL_DBMS = db2 and %upcase(&lmvLibref) ne WORK %then %do;
       dbtype=(
       &mpFieldStartDttm='timestamp(0)'
       &mpFieldEndDttm='timestamp(0)'
       )
%end;
     )
      &mpSnUp (
         keep=
            &lmvSnapVars
            &mpFieldDelta
%if &ETL_DBMS = postgres and %upcase(&lmvLibref) ne WORK %then %do;
       dbtype=(
       &lmvFieldDigest1='bytea'
       &lmvFieldDigest2='bytea'
       )
%end;
      )
      &lmvCheck1 (
         keep=
            &mpFieldPK
            &lmvFieldStartDttmNew &lmvFieldEndDttmNew
            &mpFieldStartDttm &mpFieldEndDttm
      )
   ;
      merge
         &lmvSnap    (in= in_base)
         &lmvIn      (in= in_data)
      ;
      by &mpFieldPK;

      /* Объявление переменных */
%if &lmvUseGroupMode and &mpHashGroup = Yes %then %do;
      if _n_ = 1 then do;
         if 0 then set &lmvGroup;
         declare hash &lmvHashGroup(dataset:"&lmvGroup");
         &lmvHashGroup..defineKey( %list_expand(&mpFieldGroup, "{}", mpOutDlm=%str(,)) );
         &lmvHashGroup..defineDone();
      end;
%end;

      /* Сравнение */
      /* в оба набора выводятся только несовпавшие записи */

      /* Все изменения датируются текущим временем */
      %if &lmvUseProcessedDttm %then %do;
         &mpFieldProcessedDttm = &JOB_START_DTTM;
      %end;

      /* Варианты пронумерованы по сумме флагов: */
      /* in_data=1      дает +4, in_data=0         дает 0 */
      /* in_base=1      дает +2, in_base=0         дает 0 */
      /* mpFieldDelta=D дает +1, mpFieldDelta ne D дает 0 */
      /* Всего 8 вариантов, из них V0, V1, V3 вырождены (in_data=0 и in_base=0, либо in_data=0 и mpFieldDelta ne D) */

      /* Вариант V6 */
      if in_data and in_base and &mpFieldDelta ne "D" then do;
         /* Дамми-записи не обновляют текущие, но текущая дамми-запись может быть обновлена обычной */
         %if &lmvUseDummy %then %do;
            if not missing(&lmvFieldDummyTypeNew) then return;
            if not missing(&mpFieldDummyType) then do;
               /* Обновляем актуальную версию */
               &mpFieldDelta     = "U";
               &mpFieldDigest1   = &lmvFieldDigest1New;
               &mpFieldDigest2   = &lmvFieldDigest2New;
               &mpFieldDummyType = &lmvFieldDummyTypeNew;
               output &mpOut &mpSnUp;
               return;
            end;
         %end;

         /* Если не совпали SCD2-хэши, то возникает новая версия */
         if &lmvFieldDigest2New ne &mpFieldDigest2 then do;
            /* Если начало новой записи раньше старой, то это ошибка */
            if &lmvFieldStartDttmNew le &mpFieldStartDttm then do;
               output &lmvCheck1;
               delete;
            end;

            /* Удаляем старую версию из снэпшота */
            &mpFieldEndDttm      = &lmvFieldStartDttmNew;
            &mpFieldDelta        = "D";
            output &mpSnUp;

            /* Закрываем старую версию */
            &mpFieldEndDttm   = &lmvFieldStartDttmNew;
            &mpFieldDelta     = "C";
            output &mpOut;

            /* Добавляем новую версию */
            &mpFieldDigest1      = &lmvFieldDigest1New;
            &mpFieldDigest2      = &lmvFieldDigest2New;
            %if &lmvUseDummy %then %do;
               &mpFieldDummyType    = &lmvFieldDummyTypeNew;
            %end;

            &mpFieldStartDttm    = &lmvFieldStartDttmNew;
            &mpFieldEndDttm      = &lmvFieldEndDttmNew;
            &mpFieldDelta        = "N";
            output &mpOut;

            /* Добавляем новую версию в снэпшот */
            &mpFieldDelta        = "N";
            output &mpSnUp;
         end;
         /* Иначе, если не совпали SCD1-хэши, то обновляется актуальная версия */
         else if &lmvFieldDigest1New ne &mpFieldDigest1 then do;
            /* Обновляем актуальную версию */
            &mpFieldDelta        = "U";
            &mpFieldDigest1      = &lmvFieldDigest1New;
            output &mpOut &mpSnUp;
         end;
      end;
   %if &mpCreateNew = Yes %then %do;
      /* Вариант V4 */
      else if in_data and not in_base and &mpFieldDelta ne "D" then do;
            /* Добавляем первую версию */
            &mpFieldDelta        = "1";
            &mpFieldStartDttm    = &lmvFieldStartDttmNew;
            &mpFieldEndDttm      = &lmvFieldEndDttmNew;
            &mpFieldDigest1      = &lmvFieldDigest1New;
            &mpFieldDigest2      = &lmvFieldDigest2New;
%if &lmvUseDummy %then %do;
            &mpFieldDummyType    = &lmvFieldDummyTypeNew;
            if &lmvFieldDummyTypeNew in ("D1", "D2") then
               &mpFieldEndDttm   = &lmvFieldStartDttmNew;
%end;
            output &mpOut;

%if &lmvUseDummy %then %do;
            if &lmvFieldDummyTypeNew in ("D1", "D2") then delete;
%end;
            /* В режиме инициализации в снэпшот попадают только актуальные записи */
            %if &ETL_TYPE eq INIT %then %do;
               if &mpFieldEndDttm = &ETL_SCD_FUTURE_DTTM then
                  output &mpSnUp;
            %end;
            %else %do;
               output &mpSnUp;
            %end;
      end;
   %end;
      /* Варианты V2, V7 */
      else if (not in_data and in_base and &mpFieldDelta ne "D")
           or (in_data     and in_base and &mpFieldDelta = "D") then do;
         /* Закрываем все предзакрытые ключи и часть обычных */
         if &mpFieldDelta ne "D" then do;
            /* Игнорируем записи, не входящие в кусочное обновление */
%if &lmvUseGroupMode %then %do;
%if &mpHashGroup = Yes %then %do;
            if &lmvHashGroup..find() ne 0 then delete;
%end;
%else %do;
            set &lmvGroup key=&lmvGroupIndex /unique;
            if _iorc_ ne &IORC_SOK then do;
               _error_ = 0;
               delete;
            end;
%end;
%end;
            /* Игнорируем записи, не входящие в окно обновления */
            %if &lmvUseTimeFrame %then %do;
               if &mpFieldTimeFrameDt < (&ETL_CURRENT_DT - &mpTimeFrameValue) then delete;
            %end;

            /* D3 могут быть только обновлены */
            %if &lmvUseDummy %then %do;
               if &mpFieldDummyType = "D3" then delete;
            %end;
         end;

         /* Закрываем актуальную версию */
         &mpFieldDelta        = "C";
         &mpFieldEndDttm      = &JOB_START_DTTM;
         output &mpOut;
         &mpFieldDelta        = "D";
         output &mpSnUp;
      end;
      /* Вариант V5 */
      else if in_data and not in_base and &mpFieldDelta = "D" then do;
         /* Добавляем фантомную запись */
         &mpFieldDelta        = "P";
         &mpFieldStartDttm    = &JOB_START_DTTM;
         &mpFieldEndDttm      = &JOB_START_DTTM;
         output &mpOut;
      end;
   run;
   %error_check (mpStepType=DATA);

   /* Анализ ошибок */
   %if %member_obs (mpData=&lmvCheck1) gt 0 %then %do;
      %job_event_reg (mpEventTypeCode  =  DATA_VALIDATION_FAILED,
                      mpEventDesc      =  %bquote(Обнаружены обновления задним числом в таблице &mpIn),
                      mpEventValues    =  %bquote(См. выборку в таблице &lmvCheck1) );
   %end;
   %else %do;
      %member_drop (&lmvCheck1);
   %end;

   /************************************************ Завершение ************************************************/

   %if not &ETL_DEBUG %then %do;
      %member_drop (&lmvIn);
      %member_drop (&lmvInSorted);
      %member_drop (&lmvSnap);
      %if &lmvUseGroupMode %then
         %member_drop (&lmvGroup);
   %end;
%mend etl_get_delta_scd;
