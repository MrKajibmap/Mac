/*****************************************************************
*  ВЕРСИЯ:
*     $Id: f2a68489f92e3c4397e7995c2dddca0c573fb6d1 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Создает дельта-наборы для таблицы и ее прошлого состояния
*     по технике историчности (т.е. с отбросом "хвоста" истории, начиная с первого расхождения).
*     Решение о переносе измененных/удаляемых записей в журнал принимается позже, на шаге применения дельты.
*
*     Если поле &mpFieldDelta присутствует во входном наборе, то обработка происходит в дельта-режиме,
*     т.е. записи, не вошедшие во входной набор, не изменяются.
*
*  ПАРАМЕТРЫ:
*     mpIn                    +  имя входного набора, порции новых данных
*     mpInChangedFields       -  список полей для контроля расхождений
*                                по умолчанию все поля, за исключением ключевых (mpFieldPK) и
                                 технических (mpFieldStartDttm, mpFieldEndDttm, mpFieldProcessedDttm, mpFieldDelta)
*     mpSnap                  +  имя входного набора, текущего состояния
*     mpFieldPK               +  список полей первичного ключа, не включает интервальные (mpFieldStartDttm, mpFieldEndDttm)
*     mpFieldDummyType        -  имя поля типа дамми, обычно x_dummy_type_cd
*     mpFieldGroup            -  список полей кусочного обновления, например branch_id в пофилиальной загрузке.
*                                Также может использоваться для обновления неполным набором, в этом случае совпадает с mpFieldPK.
*                                По умолчанию не используется.
*     mpHashGroup             -  оптимизация: фильтр кусочного обновления через хэш-таблицу (Yes - да, No - нет)
*                                по умолчанию Yes.
*     mpFieldTimeFrameDttm    -  имя поля даты для окна обновления, обычно mpFieldEndDttm (режим исторического окна).
*                                Если задано, то в рамках одного пришедшего ключа
*                                непришедшие интервалы закрываются в пределах окна (иначе не закрываются).
*                                По умолчанию не используется.
*     mpTimeFrameDttm         -  дата начала окна обновления, обычно &ETL_TIME_FRAME_DTTM.
*                                Также возможно задать значение BY_MIN_START_DTTM,
*                                тогда граница окна будет определена по минимальному mpFieldStartDttm в mpIn.
*                                По умолчанию не используется.
*     mpTimeFrameConflict     -  В режиме исторического окна обновления - действие над исторической записью, пересекающей границу окна.
*                                Допустимые значения: CLOSE (закрывается на границе окна) и DELETE (удаляется целиком).
*     mpSubsetSnap            -  оптимизация: выборка только нужных ключей из снэпшота (Yes - да, No - нет)
*                                по умолчанию No.
*     mpOut                   +  имя выходного набора, дельты mpIn относительно mpSnap
*     mpSnUp                  +  имя выходного набора, дельты mpSnap относительно mpSnap
*     mpFieldDelta            +  имя поля для кода дельта-строки
*                                по умолчанию etl_delta_cd.
*                                Варианты: 1/N - новая, С - закрывается, D - удалена, P - фантомная.
*     mpFieldDigest           -  имя поля для расчета хэш-суммы
*                                по умолчанию etl_digest_cd.
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
*     1. Входной набор должен быть уникален по первичному ключу и полю начала интервала.
*     2. Поля mpFieldStartDttm, mpFieldEndDttm обязаны быть заполнены.
*     3. Дельта-режим исключает режим кусочного обновления.  Если заданы оба, то
*        вместо заданных полей кусочного обновления используются поля первичного ключа.
*     4. В дельта-режиме совместно с режимом окна обновления удаляются только записи внутри окна.
*        При этом дельта-режим, как и в п. 3, замещается кусочным обновлением.
*     5. В режиме окна обновления с конечной глубиной дельта-коды 1 не создаются.
*     6. Оптимизация mpSubsetSnap возможна только в дельта-режиме или режиме кусочного обновления.
*     7. Неисторическое окно обновления требует, чтобы mpFieldTimeFrameDttm был неизменным для каждого ключа.
*
******************************************************************
*  Пример использования:
*     в трансформе transform_get_delta_hist.sas
*
******************************************************************
*  24-02-2012  Нестерёнок     Начальное кодирование
*  27-07-2012  Нестерёнок     Окно обновления теперь опционально и задается параметром mpFieldTimeFrameDttm
*  18-09-2012  Нестерёнок     Поддержка предзакрытых ключей
*  24-10-2012  Нестерёнок     Добавлен mpTimeFrameDttm
*  08-11-2012  Нестерёнок     Улучшен дельта-режим
*  25-02-2013  Герасимов      Добавлен дельта-код 1 для выделения первой записи в истории ключа (подкласс N)
*  09-07-2014  Нестерёнок     Переход к двойному сравнению
*  22-09-2014  Нестерёнок     Оптимизация: режим mpSubsetSnap
*  01-04-2015  Сазонов        Проверка на пустую таблицу для BY_MIN_START_DTTM
******************************************************************/

%macro etl_get_delta_hist (
   mpIn                       =  ,
   mpInChangedFields          =  ,
   mpSnap                     =  ,
   mpFieldPK                  =  ,
   mpFieldDummyType           =  ,
   mpFieldGroup               =  ,
   mpHashGroup                =  Yes,
   mpFieldTimeFrameDttm       =  ,
   mpTimeFrameDttm            =  ,
   mpTimeFrameConflict        =  ,
   mpSubsetSnap               =  No,
   mpOut                      =  ,
   mpSnUp                     =  ,
   mpFieldDelta               =  ETL_DELTA_CD,
   mpFieldDigest              =  ETL_DIGEST_CD,
   mpFieldStartDttm           =  VALID_FROM_DTTM,
   mpFieldEndDttm             =  VALID_TO_DTTM,
   mpFieldProcessedDttm       =  PROCESSED_DTTM
);

   /************************************************ Инициализация ************************************************/

   /* Получаем уникальный идентификатор для параллельного исполнения */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Получаем режимы работы */
   %local lmvUseVersions lmvUseDummy lmvUseProcessedDttm lmvUseDeltaMode lmvUseGroupMode;
   %let lmvUseVersions        = %eval( not %is_blank(mpFieldStartDttm) and not %is_blank(mpFieldEndDttm) );
   %let lmvUseDummy           = %eval( not %is_blank(mpFieldDummyType) );
   %let lmvUseProcessedDttm   = %eval( not %is_blank(mpFieldProcessedDttm) );
   %let lmvUseDeltaMode       = %member_vars_exist (mpData=&mpIn, mpVars=&mpFieldDelta);
   %let lmvUseGroupMode       = %eval( not %is_blank(mpFieldGroup) );

   %local lmvUseTimeFrame lmvTimeFrameType lmvTimeFrameWhere;
   %let lmvUseTimeFrame       = %eval( (not %is_blank(mpFieldTimeFrameDttm)) and (not %is_blank(mpTimeFrameDttm)) );
   %if &lmvUseTimeFrame %then %do;
      %if %upcase(&mpFieldTimeFrameDttm) eq %upcase(&mpFieldEndDttm) %then %do;
         %let lmvTimeFrameType   = HISTORIC;
         %if %is_blank(mpTimeFrameConflict) %then
            %let mpTimeFrameConflict = CLOSE;
      %end;
      %else %do;
         %let lmvTimeFrameType   = GROUP;
      %end;
   %end;

   /* Дельта-режим интерпретируется как кусочное обновление по первичному ключу */
   %if &lmvUseDeltaMode %then %do;
      %let mpFieldGroup       = &mpFieldPK;
      %let lmvUseGroupMode    = 1;
   %end;
   /* Оптимизация mpSubsetSnap возможна только в дельта-режиме или режиме кусочного обновления */
   %if not &lmvUseGroupMode %then
      %let mpSubsetSnap       = No;

   /* Получаем имена временных переменных */
   %local lmvFieldStartDttmNew lmvFieldEndDttmNew;
   %if &lmvUseVersions %then %do;
      %let lmvFieldStartDttmNew = &mpFieldStartDttm._&lmvUID;
      %let lmvFieldEndDttmNew   = &mpFieldEndDttm._&lmvUID;
   %end;
   %else %do;
      %let lmvFieldStartDttmNew = ;
      %let lmvFieldEndDttmNew   = ;
   %end;

   %local lmvFieldDigestNew lmvChangeFlag lmvLastPKField;
   %let lmvFieldDigestNew       = &mpFieldDigest._&lmvUID;
   %let lmvChangeFlag           = etl_change_flg_&lmvUID;
   %let lmvLastPKField          = %sysfunc(scan (&mpFieldPK, -1));

   %local lmvFirstValidFrom lmvLastValidFrom lmvLastValidTo lmvLastDigest lmvCloseFlag lmv1stVersionFlg;
   %let lmvFirstValidFrom       = etl_first_vfrom_&lmvUID;
   %let lmvLastValidFrom        = etl_last_vfrom_&lmvUID;
   %let lmvLastValidTo          = etl_last_vto_&lmvUID;
   %let lmvLastDigest           = etl_last_digest_&lmvUID;
   %let lmvCloseFlag            = etl_close_flg_&lmvUID;
   %let lmv1stVersionFlg        = etl_1st_ver_flg_&lmvUID;

   %local lmvPKIndex;
   %let lmvPKIndex = &mpFieldPK;
   %if %sysfunc(countw(&mpFieldPK, , s)) gt 1 %then
      %let lmvPKIndex = etl_pk_&lmvUID.;

   %local lmvFullIndex;
   %let lmvFullIndex = &lmvPKIndex;
   %if &lmvUseVersions %then
      %let lmvFullIndex = etl_full_&lmvUID.;

   /* Поля сравнения по умолчанию */
   %if %is_blank(mpInChangedFields) %then
      %let mpInChangedFields = %member_vars(&mpIn, mpDrop=&mpFieldPK &mpFieldStartDttm &mpFieldEndDttm &mpFieldProcessedDttm &mpFieldDelta);
   %if %is_blank(mpInChangedFields) %then
      %let mpInChangedFields = &mpFieldPK;

   /* Проверка аргументов */
   %local lmvInTestVars;
   %let lmvInTestVars      = %util_list(&mpFieldPK &mpFieldStartDttm &mpFieldEndDttm
                                        &mpFieldGroup &mpFieldTimeFrameDttm &mpFieldDummyType,
                                        mpOutDlm=%str( ), mpUnique=Y );
   %if not %member_vars_exist (mpData=&mpIn, mpVars=&lmvInTestVars) %then %do;
      %job_event_reg (
         mpEventTypeCode   =  ILLEGAL_ARGUMENT,
         mpEventDesc       =  %bquote(Структура таблицы &mpIn некорректна),
         mpEventValues     =  %bquote(Должны присутствовать поля &lmvInTestVars) );
      %return;
   %end;

   %if %member_vars_exist (mpData=&mpIn, mpVars=&mpFieldDigest) %then %do;
      %job_event_reg (
         mpEventTypeCode   =  ILLEGAL_ARGUMENT,
         mpEventDesc       =  %bquote(Структура таблицы &mpIn некорректна),
         mpEventValues     =  %bquote(Поля &mpFieldDigest не должно быть) );
      %return;
   %end;

   %local lmvSnapVars lmvSnapTestVars;
   %let lmvSnapVars        = %member_vars(&mpSnap);
   %let lmvSnapTestVars    = %util_list(&mpFieldPK &mpFieldStartDttm &mpFieldEndDttm
                                        &mpFieldGroup &mpFieldTimeFrameDttm &mpFieldDigest &mpFieldDummyType,
                                        mpOutDlm=%str( ), mpUnique=Y );
   %if not %member_vars_exist (mpData=&mpSnap, mpVars=&lmvSnapTestVars) %then %do;
      %job_event_reg (
         mpEventTypeCode   =  ILLEGAL_ARGUMENT,
         mpEventDesc       =  %bquote(Структура таблицы &mpSnap некорректна),
         mpEventValues     =  %bquote(Должны присутствовать поля &lmvSnapTestVars) );
      %return;
   %end;

   /************************************************ Подготовка данных ************************************************/

   /* Определяем окно обновления, если запрошено */
%if &lmvUseTimeFrame %then %do;
   /* Находим минимальную дату в наборе */
   %local lmvTimeFrameDttm;
   %if &mpTimeFrameDttm eq BY_MIN_START_DTTM %then %do;
      proc sql noprint;
         select min(&mpFieldStartDttm) format=best20. into :lmvTimeFrameDttm trimmed from &mpIn;
      quit;
      %error_check (mpStepType=SQL);

      /* Проверка на пустую таблицу или незаполненное &mpFieldStartDttm */
      %if &lmvTimeFrameDttm = . %then %do;
         %job_event_reg (
            mpEventTypeCode   =  ILLEGAL_ARGUMENT,
            mpEventValues     =  %bquote(Невозможно определить минимальное значение &mpIn..&mpFieldStartDttm) );
         %return;
      %end;

      %let mpTimeFrameDttm = &lmvTimeFrameDttm;
   %end;

   %if &lmvTimeFrameType = HISTORIC %then %do;
         %let lmvTimeFrameWhere  = (&mpFieldEndDttm gt &mpTimeFrameDttm);
   %end;
   %else %if &lmvTimeFrameType = GROUP %then %do;
         %let lmvTimeFrameWhere  = (&mpFieldTimeFrameDttm ge &mpTimeFrameDttm);
   %end;
%end;

   /* Упорядочение входного набора */
   %local lmvInSorted lmvDupOut;
   %let lmvInSorted  = work.etl_delta_&lmvUID._ins;
   %let lmvDupOut    = work_stg.etl_delta_&lmvUID._dup;
   %member_drop (&lmvInSorted);
   %member_drop (&lmvDupOut);

   proc sort
      data=&mpIn
      out=&lmvInSorted
      dupout=&lmvDupOut
      %if &ETL_DEBUG %then details;
      nodupkey
   ;
      by &mpFieldPK &mpFieldStartDttm;
%if &lmvUseTimeFrame %then %do;
      where &lmvTimeFrameWhere;
%end;
   run;
   %error_check (mpStepType=DATA);

   /* Анализ ошибок */
   %if %member_obs (mpData=&lmvDupOut) gt 0 %then %do;
      %job_event_reg (mpEventTypeCode  =  DATA_VALIDATION_FAILED,
                      mpEventDesc      =  %bquote(Обнаружены дубликатные интервалы в таблице &mpIn),
                      mpEventValues    =  %bquote(См. выборку в таблице &lmvDupOut) );
   %end;
   %else %do;
      %member_drop (&lmvDupOut);
   %end;


   /* Приводим входной набор в удобную для сравнения со снэпшотом форму */
   %local lmvIn lmvInErr lmvDelRef;
   %let lmvIn     =  work.etl_delta_&lmvUID._in;
   %let lmvInErr  =  work.etl_delta_&lmvUID._inerr;
   %let lmvDelRef =  work.etl_delta_&lmvUID._delref;
   %member_drop (&lmvIn);
   %member_drop (&lmvInErr);
   %member_drop (&lmvDelRef);

   %local lmvChangeMD5Exp;
   %util_digest_expr (mpIn=&mpIn, mpDigestFields=&mpInChangedFields, mpOutKey=lmvChangeMD5Exp);

   data
      &lmvIn (sortedby= &mpFieldPK &mpFieldStartDttm)
      &lmvInErr
      &lmvDelRef (keep= &mpFieldPK sortedby= &mpFieldPK)
   ;
      set &lmvInSorted;
      by &mpFieldPK &mpFieldStartDttm;

      /* Добавление новых переменных */
      length &mpFieldDelta $1;
      &mpFieldDelta = &mpFieldDelta;

%if &lmvUseVersions %then %do;
      format &lmvFieldStartDttmNew &lmvFieldEndDttmNew datetime20.;
%if &lmvTimeFrameType = HISTORIC %then %do;
      &mpFieldStartDttm = max (&mpFieldStartDttm, &mpTimeFrameDttm);
%end;
      &lmvFieldStartDttmNew = &mpFieldStartDttm;
      &lmvFieldEndDttmNew   = &mpFieldEndDttm;
      drop &mpFieldEndDttm;

      if missing (&lmvFieldStartDttmNew) or missing (&lmvFieldEndDttmNew) then do;
         output &lmvInErr;
         delete;
      end;
%end;

      attrib
        &lmvFieldDigestNew length=$16 format=$hex32.
      ;
      &lmvFieldDigestNew = &lmvChangeMD5Exp;

%if &lmvUseDeltaMode %then %do;
      if &mpFieldDelta = "D" then output &lmvDelRef;
%end;
      output &lmvIn;
   run;
   %error_check (mpStepType=DATA);
   %member_drop (&lmvInSorted);

   /* Анализ ошибок */
   %if %member_obs (mpData=&lmvInErr) gt 0 %then %do;
      %job_event_reg (mpEventTypeCode  =  DATA_VALIDATION_FAILED,
                      mpEventDesc      =  %bquote(Не заполнены интервалы в таблице &mpIn),
                      mpEventValues    =  %bquote(См. выборку в таблице &lmvInErr) );
   %end;
   %else %do;
      %member_drop (&lmvInErr);
   %end;

   /* Получаем справочник предзакрытых ключей */
   /* Применимо только для дельта-режима */
%if &lmvUseDeltaMode %then %do;
   proc sort
      data=&lmvDelRef
      %if &ETL_DEBUG %then details;
      nodupkey
   ;
      by &mpFieldPK;
   run;
   %error_check;
%end;

   /* Упорядочение снэпшота */
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
   %let mpSnap             = &lmvSnap;
%end;

   proc sort
      data=&mpSnap
      out=&lmvSnap
      %if &ETL_DEBUG %then details;
   ;
      by &mpFieldPK &mpFieldStartDttm;

%if &lmvUseTimeFrame %then %do;
      where (&mpFieldEndDttm gt &mpTimeFrameDttm)
%if &lmvUseDummy %then %do;
         or not missing(&mpFieldDummyType)
%end;
      ;
%end;
   run;
   %error_check;

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
   data
      &mpOut (
         drop=
            &lmvFieldStartDttmNew &lmvFieldEndDttmNew
            &lmvFieldDigestNew
            &lmvChangeFlag &lmvCloseFlag &lmv1stVersionFlg
            &lmvFirstValidFrom &lmvLastValidFrom &lmvLastValidTo &lmvLastDigest
            &mpFieldDigest
      )
      &mpSnUp (
         keep=
            &lmvSnapVars
            &mpFieldDelta
      )
   ;
      /* Определение переменных */
      if 0 then set &lmvIn;
      if 0 then set &lmvSnap;

      /* Определяем сохраняемые в течение обработка сущности атрибуты */
      /* lmvChangeFlag:
                            0 - обычный ключ, точка расхождения еще не найдена,
                            1 - обычный ключ, точка расхождения найдена,
                            2 - предзакрытый ключ, искать не надо */
      /* lmvCloseFlag:      [только для lmvChangeFlag = 1] 0 - не нужно закрывать прошлый интервал, 1 - нужно */
      /* lmv1stVersionFlg:  [только для lmvChangeFlag = 1] 1 - первая версия в истории ключа, 0 - не первая */
      /* lmvFirstValidFrom: значение начала первого интервала из входного набора */
      /* lmvLastValidFrom:  значение начала прошлого интервала из снэпшота */
      /* lmvLastValidTo:    значение конца прошлого интервала из снэпшота */
      /* lmvLastDigest:     значение прошлой контрольной суммы из снэпшота */

      length &lmvChangeFlag &lmvCloseFlag &lmv1stVersionFlg 8;
      length &lmvFirstValidFrom &lmvLastValidFrom &lmvLastValidTo 8 &lmvLastDigest $16;

      call missing(&lmvFirstValidFrom, &lmvLastValidFrom, &lmvLastValidTo);
      &lmvChangeFlag    = 0;

%if &lmvUseGroupMode and &mpHashGroup = Yes %then %do;
      if _n_ = 1 then do;
         if 0 then set &lmvGroup;
         declare hash &lmvHashGroup(dataset:"&lmvGroup");
         &lmvHashGroup..defineKey( %list_expand(&mpFieldGroup, "{}", mpOutDlm=%str(,)) );
         &lmvHashGroup..defineDone();
      end;
%end;

      /* Сравнение по ключам */
      do until(last.&lmvLastPKField);
         merge
            &lmvSnap    (
               keep= &mpFieldPK
               in=   exist_base
            )
            &lmvIn      (
               keep= &mpFieldPK
%if &lmvTimeFrameType = HISTORIC %then %do;
                     &lmvFieldStartDttmNew
%end;
               in=   exist_data
            )
%if &lmvUseDeltaMode %then %do;
            &lmvDelRef  (
               in=   exist_del
            )
%end;
         ;
         by &mpFieldPK;

%if &lmvTimeFrameType = HISTORIC %then %do;
         if exist_data and first.&lmvLastPKField then
            &lmvFirstValidFrom = &lmvFieldStartDttmNew;
%end;
      end;

      /* Сравнение по интервалам */
      do until(last.&lmvLastPKField);
         merge
            &lmvSnap    (in= in_base)
            &lmvIn      (in= in_data)
         ;
         by &mpFieldPK &mpFieldStartDttm;

         /* Флаг закрытия предыдущей записи работает только в точке расхождения */
         &lmvCloseFlag     = 0;

         /* Определяем точку расхождения для предзакрытых ключей */
%if &lmvUseDeltaMode %then %do;
         if &lmvChangeFlag = 0 and exist_del then do;
            /* Если есть окно обновления, то сохраняем предысторию до его границы */
%if &lmvTimeFrameType = HISTORIC %then %do;
            /* Фиксируем расхождение */
            &lmvChangeFlag = 2;
            if in_base and &mpFieldStartDttm lt &mpTimeFrameDttm then do;
               /* Ключ перед окном */
               /* Если допускается закрытие, закрываем на границе окна */
%if &mpTimeFrameConflict = CLOSE %then %do;
               &lmvFieldStartDttmNew = &mpTimeFrameDttm;
               &lmvCloseFlag = 1;
%end;
            end;
%end;
%else %if &lmvTimeFrameType = GROUP %then %do;
            if &lmvTimeFrameWhere then
               &lmvChangeFlag = 2;
%end;
%else %do;
            /* Без окна обновления определять точку расхождения для предзакрытых ключей не надо */
            &lmvChangeFlag = 2;
%end;
         end;
%end;

         /* Определяем точку расхождения для обычных ключей */
         /* Если сущность присутствует в снэпшоте, но отсутствует во входном наборе, то она закрывается (кроме кусочного обновления и дельта-режима) */
         /* Если сущность присутствует во входном наборе, но отсутствует в снэпшоте, то она добавляется.
            При этом прошлый интервал должен быть закрыт */
         /* Если сущность присутствует в обоих наборах, то до времени начала записей из входного набора изменения не производятся,
            а с этого времени определяется точка первого расхождения (либо по времени, либо по хэш-сумме), после которой записи заменяются */
         if &lmvChangeFlag = 0 then do;
            /* Для дамми:  пришедшие дамми-записи не меняют уже существующие в снэпшоте */
%if &lmvUseDummy %then %do;
            if in_data and not missing(&mpFieldDummyType) and exist_base then continue;
%end;

            if in_data and in_base then do;
               /* Если не совпали хэш-суммы или концы интервалов действия, то возникает расхождение */
               &lmv1stVersionFlg    = ifn (&lmv1stVersionFlg, 0, 0, 1);
               if (&lmvFieldDigestNew ne &mpFieldDigest)
%if &lmvUseVersions %then %do;
                  or (&lmvFieldEndDttmNew ne &mpFieldEndDttm)
%end;
               then do;
                  &lmvChangeFlag    = 1;
               end;
            end;
            else if in_data and not in_base then do;
               /* Для дамми:  здесь либо имеем случай новой дамми-записи, либо перезапись обычной сущностью дамми */

               /* Запись, которой нет в снэпшоте, вызывает расхождение, если ее хэш-сумма не совпала с прошлой суммой из снэпшота */
               /* либо не совпали концы интервалов */
               &lmv1stVersionFlg    = ifn (&lmv1stVersionFlg, 0, 0, 1);
               if missing(&lmvLastDigest) or (&lmvFieldDigestNew ne &lmvLastDigest)
%if &lmvUseVersions %then %do;
                  or (&lmvFieldEndDttmNew ne &lmvLastValidTo)
%end;
               then do;
                  &lmvChangeFlag    = 1;

%if &lmvUseVersions %then %do;
                  /* Только в этом случае возможно пересечение интервалов */
                  if not missing(&lmvLastValidTo) then do;
                     /* м.б. нужно закрыть предыдущую запись из снэпшота */
                     if (&lmvLastValidTo gt &lmvFieldStartDttmNew) then
                        &lmvCloseFlag = 1;

%if &lmvTimeFrameType = HISTORIC and &mpTimeFrameConflict = DELETE %then %do;
                     /* а м.б. и удалить */
                     if (&lmvLastValidFrom lt &mpTimeFrameDttm) then do;
                        &mpFieldStartDttm    = &lmvLastValidFrom;
                        &mpFieldEndDttm      = &lmvLastValidTo;
                        &mpFieldDigest       = &lmvLastDigest;
                        &lmvCloseFlag        = 0;
                        &lmv1stVersionFlg    = 1;
                     end;
%end;
                  end;
%end;
               end;
            end;
            else if not in_data and in_base then do;
               /* Игнорируем записи, не входящие в кусочное обновление */
%if &lmvUseGroupMode %then %do;
%if &mpHashGroup = Yes %then %do;
               if &lmvHashGroup..find() ne 0 then go to wait_in_data;
%end;
%else %do;
               set &lmvGroup key=&lmvGroupIndex /unique;
               if _iorc_ ne &IORC_SOK then do;
                  _error_ = 0;
                  go to wait_in_data;
               end;
%end;
%end;
               /* Игнорируем D3, отсутствующие во входном наборе */
%if &lmvUseDummy %then %do;
               /* ключ D3 и отсутствует во входном наборе */
               if &mpFieldDummyType = "D3" and not exist_data then
                  go to end_in_base;
%end;

               /* Закрываем историю с этого момента, если */
               /* 3) ключ дамми (в т.ч. D3) */
%if &lmvUseDummy %then %do;
               if not missing(&mpFieldDummyType) then do;
                  &lmvChangeFlag = 1;
                  go to end_in_base;
               end;
%end;

               /* 1) окно обновления задано, и очередной интервал его пересекает */
%if &lmvTimeFrameType = HISTORIC %then %do;
            if &mpFieldStartDttm lt &mpTimeFrameDttm then do;
               /* ключ перед окном */
               /* Если ключ есть во входном наборе, причем не внутри окна, то подождем */
               if exist_data and (&lmvFirstValidFrom le &mpTimeFrameDttm) then
                  go to wait_in_data;

               /* Иначе фиксируем расхождение */
               &lmvChangeFlag = 1;
               /* и, если допускается закрытие, закрываем на границе окна */
%if &mpTimeFrameConflict = CLOSE %then %do;
               &lmvFieldStartDttmNew = &mpTimeFrameDttm;
               &lmvCloseFlag = 1;
%end;
            end;
            else do;
               /* ключ внутри окна */
               &lmvChangeFlag = 1;
            end;
%end;
%else %if &lmvTimeFrameType = GROUP %then %do;
               if &lmvTimeFrameWhere then
                  &lmvChangeFlag = 1;
%end;
%else %do;
               /* Закрываем историю с этого момента, если */
               /* 2) окно обновления не задано, и ключ отсутствует во входном наборе */
               if not exist_data then
                  &lmvChangeFlag = 1;
%end;

               /* Ожидание первой записи из входного набора, предыстория остается как есть */
               wait_in_data:
%if &lmvUseVersions %then %do;
               &lmvLastValidFrom    = &mpFieldStartDttm;
               &lmvLastValidTo      = &mpFieldEndDttm;
%end;
               &lmvLastDigest       = &mpFieldDigest;
               &lmv1stVersionFlg    = ifn (&lmvChangeFlag, ., 0);

               end_in_base:
            end;
         end;

         /* В случае расхождения, в оба набора выводятся несовпавшие записи (т.е. "хвост") */
         /* Все изменения датируются текущим временем */
%if &lmvUseProcessedDttm %then %do;
         if &lmvChangeFlag in (1, 2) then do;
            &mpFieldProcessedDttm = &JOB_START_DTTM;
         end;
%end;

         /* Удаляем версию из хвоста */
         if &lmvChangeFlag in (1, 2) then do;
            if in_base and &lmvCloseFlag = 0 then do;
               &mpFieldDelta            = "D";
               output &mpOut &mpSnUp;
            end;
         end;

         /* Добавляем новую версию для обычных ключей */
         if &lmvChangeFlag = 1 then do;
            if in_data then do;
%if &lmvUseTimeFrame and &mpTimeFrameDttm ne &ETL_MIN_DTTM %then %do;
               &mpFieldDelta        = "N";
%end;
%else %do;
               &mpFieldDelta        = ifc (&lmv1stVersionFlg ne 0, "1", "N");
%end;
%if &lmvUseDummy %then %do;
               if &mpFieldDummyType in ("D1", "D2") then
                  &mpFieldDelta     = "P";
%end;
               &lmv1stVersionFlg    = 0;
%if &lmvUseVersions %then %do;
               &mpFieldStartDttm    = &lmvFieldStartDttmNew;
               &mpFieldEndDttm      = &lmvFieldEndDttmNew;
%end;
               &mpFieldDigest       = &lmvFieldDigestNew;

               output &mpOut;
               if &mpFieldDelta ne "P" then
                  output &mpSnUp;
            end;
         end;

%if &lmvUseVersions %then %do;
         if &lmvChangeFlag in (1, 2) then do;
            /* Закрываем старую версию на пересечении с новым хвостом */
            if &lmvCloseFlag = 1 then do;
               &mpFieldDelta     = "C";
               if &lmvChangeFlag = 1 then do;
                  &mpFieldStartDttm = &lmvLastValidFrom;
                  &mpFieldDigest    = &lmvLastDigest;
               end;
               &mpFieldEndDttm   = &lmvFieldStartDttmNew;
               output &mpOut &mpSnUp;
            end;
         end;
%end;

         /* Добавляем фантомные версии для ранее не существовавших предзакрытых ключей */
         /* Применимо только для дельта-режима */
%if &lmvUseDeltaMode %then %do;
         if &lmvChangeFlag = 2 then do;
            if not exist_base and &mpFieldDelta = "D" then do;
               &mpFieldDelta        = "P";
%if &lmvUseVersions %then %do;
               &mpFieldStartDttm    = &lmvFieldStartDttmNew;
               &mpFieldEndDttm      = &lmvFieldEndDttmNew;
%end;
               &mpFieldDigest       = &lmvFieldDigestNew;
               output &mpOut;
            end;
         end;
%end;

      end;
   run;
   %error_check;

   /************************************************ Завершение ************************************************/

   /* Сброс временных таблиц */
%if not &ETL_DEBUG %then %do;
   %member_drop (&lmvIn);
   %member_drop (&lmvDelRef);
   %member_drop (&lmvSnap);
%if &lmvUseGroupMode %then
   %member_drop (&lmvGroup);
%end;
%mend etl_get_delta_hist;
