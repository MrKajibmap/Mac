/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 730ce4cc7b734eb2ff46140e3715f7b70b59eaeb $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Генерирует или подставляет суррогатный ключ для бизнес-ключа по таблице BK-RK (BK-CD).
*     Обновляет таблицу BK-RK (BK-CD) новыми парами соответствий.
*
*     В методе mpSKMethod=ASCII используется 63 знака: 0-9, A-Z, a-z, и _
*
*  ПАРАМЕТРЫ:
*     mpIn                 +  имя входного набора
*     mpInFieldBK          +  поле бизнес-ключа во входном наборе
*     mpBKType             +  тип сущностей, передаваемых в бизнес-ключе (например, FINANCIAL_ACCOUNT)
*     mpBKRef              +  тип генерируемой ссылочной целостности
*                             PK  - mpInFieldBK это primary key
*                             FK  - mpInFieldBK это foreign key
*                             FK0 - mpInFieldBK это foreign key, но вместо D3 генерится D0
*     mpOut                +  имя выходного набора
*     mpOutDummy           -  имя выходного набора для D2/D3-записей (только при генерации FK)
*     mpOutFieldSK         +  поле, в которое будет помещен сгенерированный суррогатный ключ
*     mpOutFieldDummyType  -  поле, в которое будет помещен тип дамми сгенерированного суррогатного ключа
*                             По умолчанию X_DUMMY_TYPE_CD
*     mpSKMethod           -  метод генерации суррогатных ключей (RK, ASCII, HEX)
*                             По умолчанию RK
*     mpLookup             +  имя таблицы соответствий BK-RK (BK-CD)
*     mpLkupFieldBK        -  поле бизнес-ключа в таблице соответствий
*                             по умолчанию равно mpInFieldBK
*     mpLkupFieldSK        -  поле суррогатного ключа в таблице соответствий
*                             по умолчанию равно mpOutFieldSK
*     mpSubsetLkup         -  оптимизация: выборка только нужных ключей из таблицы соответствий (Yes - да, No - нет)
*                             по умолчанию No.
*     mpHashDecode         -  оптимизация: перекодировка входного набора через хэш-таблицу (Yes - да, No - нет)
*                             по умолчанию Yes.
*     mpGridFieldBy        -  для исполнения на гриде - поле во входном наборе и таблице соответствий, используемое для разбиения
*                             Обычно ETL_PARTITION_ID, по умолчанию не используется.
*     mpGridValues         -  для исполнения на гриде - значения mpGridFieldBy, позволяющие отобрать нужную часть
*
******************************************************************
*  Использует:
*     %error_check
*     %etl_generate_bk_rx
*     %etl_generate_sk_merge_new
*     %member_drop
*     %member_obs
*     %member_vars_*
*     %unique_id
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*  %macro etl_generate_sk (mpIn=  WORK_IA.COUNTERPARTY_BK, mpInFieldBK=COUNTERPARTY_BK,
*                          mpOut= WORK_IA.COUNTERPARTY_RK, mpOutFieldSK=COUNTERPARTY_RK,
*                          mpLookup=ETL_IA.COUNTERPARTY_BK_RK);
*
******************************************************************
*  28-02-2012  Нестерёнок     Начальное кодирование
*  22-05-2012  Нестерёнок     Добавил поддержку генерации CD
*  29-08-2012  Кузенков       Атрибуты поля CD берутся из Lookup.  Для CD также выбирается максимальный.
*  30-08-2012  Нестерёнок     Метод генерации по умолчанию заменен на BASE64
*  31-10-2012  Кузенков       Метод BASE64 заменен на ASCII
*  02-11-2012  Нестерёнок     Метод ASCII использует 63 символа букв, цифр и _
*  05-12-2012  Нестерёнок     Если загружаем не справочник, то пустые BK ключи переходят в пустые RK
*  27-05-2014  Нестерёнок     Функции генерации по методу ASCII вынесены
*  04-06-2014  Нестерёнок     Добавлена оптимизация mpSubsetLkup
*  15-07-2014  Нестерёнок     Добавлена оптимизация mpHashDecode
*  24-07-2014  Нестерёнок     Поддержка исполнения на гриде (mpGridFieldBy, mpGridValues)
******************************************************************/

%macro etl_generate_sk (
   mpIn                       =  ,
   mpInFieldBK                =  ,
   mpBKType                   =  ,
   mpBKRef                    =  ,
   mpOut                      =  ,
   mpOutFieldSK               =  ,
   mpOutFieldDummyType        =  X_DUMMY_TYPE_CD,
   mpOutDummy                 =  ,
   mpSKMethod                 =  RK,
   mpLookup                   =  ,
   mpLkupFieldBK              =  &mpInFieldBK,
   mpLkupFieldSK              =  &mpOutFieldSK,
   mpSubsetLkup               =  No,
   mpHashDecode               =  Yes,
   mpGridFieldBy              =  ,
   mpGridValues               =
);
   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Получаем режимы работы */
   %local lmvGridMode;
   %let lmvGridMode     =  %eval( (not %is_blank(mpGridFieldBy)) and (not %is_blank(mpGridValues)) );

   /* Получаем список уникальных ключей для перекодирования */
   %local lmvKeysTable;
   %let lmvKeysTable    = work.etl_sk_&lmvUID._keys;
   proc sort
      data=&mpIn (
         keep= &mpInFieldBK &mpGridFieldBy
%if &lmvGridMode %then %do;
         where= (&mpGridFieldBy in (&mpGridValues))
%end;
      )
      out=&lmvKeysTable (
         sortedby= _null_
      )
      %if &ETL_DEBUG %then details;
      nodupkey
   ;
      by &mpInFieldBK;
   run;
   %error_check (mpStepType=DATA);

   /* Упорядочиваем глобальную таблицу соответствия */
   %local lmvLookupTable lmvLookupD0Table lmvLookupMainTable lmvOutFieldSK;
   %let lmvLookupTable     = work.etl_sk_&lmvUID._lkup;
   %let lmvLookupD0Table   = work.etl_sk_&lmvUID._lkupd0;
   %let lmvLookupMainTable = work.etl_sk_&lmvUID._lkupm;
   %let lmvOutFieldSK      = etl_sk_&lmvUID;

   /* Создаем явную запись о D0 */
   data &lmvLookupD0Table;
      if 0 then set &mpLookup(keep= &mpLkupFieldBK &mpLkupFieldSK);

      &mpLkupFieldBK = &ETL_D0_ID;
%if &mpSKMethod=RK %then
      &mpLkupFieldSK = &ETL_D0_RK;
%else
      &mpLkupFieldSK = &ETL_D0_CD;
;
      output;
      stop;
   run;

%if &mpSubsetLkup = Yes %then %do;
   /* Поднимаем справочник искомых ключей */
   %local lmvLkupLibref;
   %member_names (mpTable=&mpLookup, mpLibrefNameKey=lmvLkupLibref);
   %local lmvTmpBKTable;
   %let lmvTmpBKTable   =  &lmvLkupLibref..etl_sk_bk_&lmvUID.;

   proc append
      base=&lmvTmpBKTable (&ETL_BULKLOAD_OPTIONS)
      data=&lmvKeysTable (
         keep=  &mpInFieldBK
         where= (not missing(&mpInFieldBK))
      )
      ;
   run;
   %error_check;

   /* Получаем эти ключи из таблицы соответствия */
   proc sql;
      create table &lmvLookupMainTable as select
         lkup.&mpLkupFieldBK,
         lkup.&mpLkupFieldSK
      from
         &mpLookup lkup
      inner join
         &lmvTmpBKTable bk
      on
         lkup.&mpLkupFieldBK = bk.&mpInFieldBK
%if &lmvGridMode %then %do;
         and lkup.&mpGridFieldBy in (&mpGridValues)
%end;
      ;
      %error_check (mpStepType=SQL);
   quit;

   %member_drop(&lmvTmpBKTable);
%end;
%else %do;
   %let lmvLookupMainTable = &mpLookup;
%end;

   data &lmvLookupTable;
      set
         &lmvLookupD0Table
         &lmvLookupMainTable (
            keep=
               &mpLkupFieldBK
               &mpLkupFieldSK
%if &lmvGridMode and (&mpSubsetLkup ne Yes) %then %do;
               &mpGridFieldBy
%end;
            where= (
               (&mpLkupFieldBK ne &ETL_D0_ID)
%if &lmvGridMode and (&mpSubsetLkup ne Yes) %then %do;
               and (&mpGridFieldBy in (&mpGridValues))
%end;
            )
         )
      end=_end;

      rename
         &mpLkupFieldBK = &mpInFieldBK
         &mpLkupFieldSK = &lmvOutFieldSK
      ;
      keep &mpLkupFieldBK &mpLkupFieldSK;
   run;

   proc sort data=&lmvLookupTable
      %if &ETL_DEBUG %then details;
   ;
      by &mpInFieldBK;
   run;
   %error_check (mpStepType=DATA);

   proc sql;
      create unique index &mpInFieldBK on &lmvLookupTable;
   quit;
   %error_check (mpStepType=SQL);

   /* Получаем маски для определения дамми */
   %local lmvBkFormat lmvBkRx lmvDummyRx lmvOutFieldDummyType;
   %let lmvBkFormat     = %quote(%sysfunc(putc(&mpBKType, bkt_cd_fmt.)));
   %let lmvBkRx         = %etl_generate_bk_rx(mpTemplate= &lmvBkFormat);
   %let lmvDummyRx      = %trim(&ETL_BK_INVALID._(\d+));
   %let lmvOutFieldDummyType  = etl_dummy_type_&lmvUID.;

   %local lmvTmpFieldD0ErrCount lmvTmpFieldRC lmvD0ErrMax;
   %let lmvTmpFieldD0ErrCount = etl_d0_cnt_&lmvUID.;
   %let lmvTmpFieldRC         = etl_rc_&lmvUID.;
   %let lmvD0ErrMax           = %sysfunc(getoption(ERRORS));

   /* Создаем локальную таблицу соответствия и список новых ключей */
   %local lmvBKSKTable lmvBKSKNewTable;
   %let lmvBKSKTable    = work.etl_sk_&lmvUID._bksk;
   %let lmvBKSKNewTable = work.etl_sk_&lmvUID._bksk_new;

   data
      &lmvBKSKTable (
        keep  =
          &mpInFieldBK
          &lmvOutFieldSK
          &lmvOutFieldDummyType
      )
      &lmvBKSKNewTable (
        keep =
          &mpInFieldBK
          &lmvOutFieldSK
          &lmvOutFieldDummyType
          &mpGridFieldBy
      )
   ;
      set &lmvKeysTable;
      if 0 then set &lmvLookupTable;

      length &lmvOutFieldDummyType $3;
      retain &lmvTmpFieldD0ErrCount 0;

      /* Если загружаем не справочник, то пустые BK ключи переходят в пустые RK */
      %if &mpBKRef ne PK %then %do;
         if missing(&mpInFieldBK) then delete;
      %end;

      /* Проверяем: дамми (D1, D2) или нет */
      etl_bk_rx      = prxparse("/\b&lmvBkRx\b/o");
      etl_dummy_rx   = prxparse("/\b&lmvDummyRx\b/o");
      if not prxmatch(etl_bk_rx, &mpInFieldBK) then do;
         /* Должен быть дамми */
         if &mpInFieldBK = &ETL_D0_ID then do;
            /* D0 всегда есть в таблице перекодировки */
            &lmvOutFieldDummyType = "D0";
%if &mpSKMethod=RK %then %do;
            &lmvOutFieldSK        = &ETL_D0_RK;
%end;
%else %do;
            &lmvOutFieldSK        = &ETL_D0_CD;
%end;
            output &lmvBKSKTable;
         end;
         else do;
            /* Проверяем D1, D2 */
            if prxmatch(etl_dummy_rx, &mpInFieldBK) then do;
               /* Заполняем X_DUMMY_TYPE_CD */
  %if &mpBKRef eq PK %then %do;
              &lmvOutFieldDummyType = "D1";
  %end; %else %do;
              &lmvOutFieldDummyType = "D2";
  %end;
               /* Проверяем таблицу перекодировки */
               set &lmvLookupTable key=&mpInFieldBK /unique;
               if _iorc_ = &IORC_SOK then do;
                  output &lmvBKSKTable;
               end;
               else do;
                  _error_ = 0;

                  /* Делаем перекодировку без доп. ключа RK (берется из BK)... */
%if &mpSKMethod=RK %then %do;
                  &lmvOutFieldSK = -input (prxposn(etl_dummy_rx, 1, &mpInFieldBK), best.);
%end;
%else %do;
                  call missing(&lmvOutFieldSK);
%end;
                  output &lmvBKSKNewTable;
               end;
            end;
            else do;
               /* Ошибка, заменяем на D0 */
               /* D0 всегда есть в таблице перекодировки */
               &lmvTmpFieldD0ErrCount + 1;
               if &lmvTmpFieldD0ErrCount le &lmvD0ErrMax then
                  &lmvTmpFieldRC = log4sas_error ("dwf.macro.etl_generate_sk", catx (" ", "Business key &mpInFieldBK=", &mpInFieldBK,
                     "does not conform to type &mpBKType, resetting to D0"));
               if &lmvTmpFieldD0ErrCount = &lmvD0ErrMax then
                  &lmvTmpFieldRC = log4sas_error ("dwf.macro.etl_generate_sk", "Limit set by ERRORS=&lmvD0ErrMax reached, further errors will not be printed");
               &lmvOutFieldDummyType = "D0";
%if &mpSKMethod=RK %then %do;
               &lmvOutFieldSK        = &ETL_D0_RK;
%end;
%else %do;
               &lmvOutFieldSK        = &ETL_D0_CD;
%end;
               output &lmvBKSKTable;
            end;
         end;

         /* Обработка дамми-ключа завершена */
         return;
      end;

      /* Для не дамми используем таблицу перекодировки */
      set &lmvLookupTable key=&mpInFieldBK /unique;
      if _iorc_ = &IORC_SOK then do;
         output &lmvBKSKTable;
      end;
      else do;
         _error_ = 0;

         /* Если загружаем не справочник, то ненайденный ключ - это дамми (D3), в случае FK,
            и дамми (D0), в случае FK0 */
%if &mpBKRef = FK0 %then %do;
         &lmvOutFieldDummyType = "D0";
%if &mpSKMethod=RK %then %do;
         &lmvOutFieldSK        = &ETL_D0_RK;
%end;
%else %do;
         &lmvOutFieldSK        = &ETL_D0_CD;
%end;
         output &lmvBKSKTable;
%end;
%else %do;
    %if &mpBKRef = PK %then %do;
         &lmvOutFieldDummyType = "";
    %end; %else %do;
         &lmvOutFieldDummyType = "D3";
    %end;

         /* Выделяем новый ключ */
         call missing(&lmvOutFieldSK);
         output &lmvBKSKNewTable;
%end;
      end;
   run;
   %error_check (mpStepType=DATA);

   /* Если новых ключей нет, то начинаем перекодировку */
   %if %member_obs(mpData=&lmvBKSKNewTable) le 0 %then %goto decode;

   /* Получаем новые ключи, обновляем глобальную таблицу соответствия */
   %local lmvBKSKMergedTable lmvMergeStatus;
   %let lmvBKSKMergedTable = work.etl_sk_&lmvUID._bksk_mrg;
   %etl_generate_sk_merge_new (
      mpIn                 =  &lmvBKSKNewTable,
      mpInFieldBK          =  &mpInFieldBK,
      mpInFieldSK          =  &lmvOutFieldSK,
      mpOut                =  &lmvBKSKMergedTable,
      mpSKMethod           =  &mpSKMethod,
      mpLookup             =  &mpLookup,
      mpLkupFieldBK        =  &mpLkupFieldBK,
      mpLkupFieldSK        =  &mpLkupFieldSK,
%if &lmvGridMode %then %do;
      mpGridFieldBy        =  &mpGridFieldBy,
      mpGridValues         =  &mpGridValues,
%end;
      mpConflict1Level     =  ,
      mpStatusKey          =  lmvMergeStatus
   );
   %if &lmvMergeStatus lt 0 %then %goto cleanup;

   /* Добавляем все новые ключи */
   proc append
      base= &lmvBKSKTable
      data= &lmvBKSKMergedTable (
         keep=
            &mpInFieldBK
            &lmvOutFieldSK
            &lmvOutFieldDummyType
     );
   run;
   %error_check (mpStepType=DATA);

   /* Очистка */
   %member_drop(&lmvBKSKMergedTable);

%decode:
   /* Перекодируем BK -> SK */
%if &mpHashDecode = Yes %then %do;
   %local lmvHashMap;
   %let lmvHashMap = hash_&lmvUID.;
%end;
%else %do;
   proc sql;
      create unique index &mpInFieldBK on &lmvBKSKTable;
   quit;
   %error_check (mpStepType=SQL);

   sasfile &lmvBKSKTable load;
%end;

   %local lmvInputVars lmvInputNoDummyVars;
   %member_vars_get(&mpIn, lmvInputVars,        mpDrop=&mpGridFieldBy);
   %member_vars_get(&mpIn, lmvInputNoDummyVars, mpDrop=&mpOutFieldDummyType &mpGridFieldBy);

%if &mpBKRef eq PK %then %do;
   data &mpOut(
      keep =
         %member_vars_expand(&lmvInputNoDummyVars, {})
         &lmvOutFieldSK
         &lmvOutFieldDummyType
      rename = (
         &lmvOutFieldSK = &mpOutFieldSK
         &lmvOutFieldDummyType = &mpOutFieldDummyType
      )
   );
      set &mpIn (
         keep = %member_vars_expand(&lmvInputNoDummyVars, {})
      );
      /* Перекодируем &mpInFieldBK -> &lmvOutFieldSK &lmvOutFieldDummyType */
%if &mpHashDecode = Yes %then %do;
      if _n_ = 1 then do;
         if 0 then set &lmvBKSKTable;
         declare hash &lmvHashMap(dataset:"&lmvBKSKTable", hashexp:10);
         &lmvHashMap..defineKey("&mpInFieldBK");
         &lmvHashMap..defineData(all:"Y");
         &lmvHashMap..defineDone();
      end;
      &lmvHashMap..find();
%end;
%else %do;
      set &lmvBKSKTable key=&mpInFieldBK /unique;
%end;

      if &lmvOutFieldDummyType ne "D0";
   run;
%end;
%else %do;
   data
      &mpOut (
         keep =
           %member_vars_expand(&lmvInputVars, {})
           &lmvOutFieldSK
         rename = (
           &lmvOutFieldSK = &mpOutFieldSK
         )
       )
       &mpOutDummy(
         keep =
           %member_vars_expand(&lmvInputNoDummyVars, {})
           &lmvOutFieldSK
           &lmvOutFieldDummyType
         rename = (
           &lmvOutFieldSK = &mpOutFieldSK
           &lmvOutFieldDummyType = &mpOutFieldDummyType
         )
      )
   ;
      set &mpIn;
%if &lmvGridMode %then %do;
      where (&mpGridFieldBy in (&mpGridValues));
%end;

      /* Перекодируем &mpInFieldBK -> &lmvOutFieldSK &lmvOutFieldDummyType */
%if &mpHashDecode = Yes %then %do;
      if _n_ = 1 then do;
         if 0 then set &lmvBKSKTable;
         declare hash &lmvHashMap(dataset:"&lmvBKSKTable", hashexp:10);
         &lmvHashMap..defineKey("&mpInFieldBK");
         &lmvHashMap..defineData(all:"Y");
         &lmvHashMap..defineDone();
      end;
%end;
      if not missing(&mpInFieldBK) then do;
%if &mpHashDecode = Yes %then %do;
         &lmvHashMap..find();
%end;
%else %do;
         set &lmvBKSKTable key=&mpInFieldBK /unique;
%end;
         if &lmvOutFieldDummyType in ("D2", "D3") then
            output &mpOutDummy;
      end;
      else do;
         call missing (&lmvOutFieldDummyType, &lmvOutFieldSK);
      end;

      output &mpOut;
   run;
%end;
   %error_check;

%if &mpHashDecode ne Yes %then %do;
   sasfile &lmvBKSKTable close;
%end;

   %member_vars_clean(&lmvInputNoDummyVars);
   %member_vars_clean(&lmvInputVars);

%cleanup:
   /* Очистка */
   %member_drop(&lmvKeysTable);
   %member_drop(&lmvLookupTable);
   %member_drop(&lmvLookupD0Table);
   %member_drop(&lmvBKSKTable);
   %member_drop(&lmvBKSKNewTable);
%mend etl_generate_sk;
