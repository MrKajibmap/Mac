/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 901b14f158aa2ea2bac7420482c128e7819bcaf6 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Генерирует или подставляет суррогатный ключ для бизнес-ключа по таблице BK-RK (BK-CD).
*     Обновляет таблицу BK-RK/CD новыми парами соответствий.
*
*     В методе mpSKMethod=ASCII используется 63 знака: 0-9, A-Z, a-z, и _
*
*  ПАРАМЕТРЫ:
*     mpIn                    +  имя входного набора
*     mpInFieldBK[N]          +  поле бизнес-ключа во входном наборе
*     mpBKType[N]             +  тип сущностей, передаваемых в бизнес-ключе (например, FINANCIAL_ACCOUNT)
*     mpOut                   +  имя выходного набора
*     mpOutFieldSK            +  поле, в которое будет помещен сгенерированный суррогатный ключ
*     mpOutFieldDummyType     -  поле, в которое будет помещен тип дамми сгенерированного суррогатного ключа
*                                По умолчанию X_DUMMY_TYPE_CD
*     mpSKMethod              -  метод генерации суррогатных ключей (RK, ASCII, HEX)
*                                По умолчанию RK
*     mpLookup                +  имя таблицы соответствий BK-RK (BK-CD)
*     mpLkupFieldBK           -  поле бизнес-ключа в таблице соответствий
*                                по умолчанию равно mpInFieldBK
*     mpLkupFieldSK           -  поле суррогатного (числового или строкового) ключа в таблице соответствий
*                                по умолчанию равно mpOutFieldSK
*
******************************************************************
*  Использует:
*     %job_event_reg
*     %member_drop
*     %member_obs
*     %member_vars_*
*     %error_check
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1. Не более 3 бизнес-ключей (2 <= N <= 3).
*     2. mpInFieldBK[N] должны быть заданы в ASCII-7.
*
******************************************************************
*  Пример использования:
*  %macro etl_generate_sk_2 (mpIn=  WORK_IA.COUNTERPARTY_BK, mpInFieldBK=COUNTERPARTY_BK,
*                            mpOut= WORK_IA.COUNTERPARTY_RK, mpOutFieldSK=COUNTERPARTY_RK,
*                            mpLookup=ETL_IA.COUNTERPARTY_BK_RK);
*
******************************************************************
*  27-03-2014  Нестерёнок     Начальное кодирование
*  27-05-2014  Нестерёнок     Функции генерации по методу ASCII вынесены
*  30-05-2014  Нестерёнок     Убрано требование блокировки по RK
******************************************************************/

%macro etl_generate_sk_2 (
   mpIn                 =  ,
   mpInFieldBK1         =  ,
   mpInFieldBK2         =  ,
   mpInFieldBK3         =  ,
   mpBKType1            =  ,
   mpBKType2            =  ,
   mpBKType3            =  ,
   mpOut                =  ,
   mpOutFieldSK         =  ,
   mpOutFieldDummyType  =  X_DUMMY_TYPE_CD,
   mpSKMethod           =  RK,
   mpLookup             =  ,
   mpLkupFieldBK        =  &mpInFieldBK,
   mpLkupFieldSK        =  &mpOutFieldSK
);
   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Получаем кол-во ключей */
   %local lmvI lmvN lmvAllBK;
   %let lmvAllBK = &mpInFieldBK1 &mpInFieldBK2 &mpInFieldBK3;
   %let lmvN = %sysfunc(countw (&lmvAllBK, , s));

   /* Создаем исходный набор пар */
   %local lmvInTable;
   %let lmvInTable            = work.etl_sk2_&lmvUID._in;
   %local lmvTmpFieldPairBK lmvTmpFieldPairNo lmvTmpFieldPairIdx;
   %let lmvTmpFieldPairBK     = etl_pair_bk_&lmvUID;
   %let lmvTmpFieldPairNo     = etl_pair_no_&lmvUID;
   %let lmvTmpFieldPairIdx    = etl_pair_ix_&lmvUID;
   data &lmvInTable(
      keep= &lmvTmpFieldPairBK &lmvTmpFieldPairNo &lmvTmpFieldPairIdx
   );
      set &mpIn;

      /* временные переменные */
      &lmvTmpFieldPairNo = _n_;
      if 0 then set &mpLookup(
         keep= &mpLkupFieldBK rename= (&mpLkupFieldBK=&lmvTmpFieldPairBK)
      );

      %do lmvI=1 %to &lmvN;
         if not missing(&&mpInFieldBK&lmvI) then do;
            &lmvTmpFieldPairBK   = &&mpInFieldBK&lmvI;
            &lmvTmpFieldPairIdx  = &lmvI;
            output;
         end;
      %end;
   run;
   %error_check;

   /* Получаем список уникальных ключей для перекодирования */
   %local lmvKeysTable;
   %let lmvKeysTable    = work.etl_sk2_&lmvUID._keys;
   %let lmvCheckTable1  = work_ia.etl_chk_1_&lmvUID.;
   proc sort
      data=&lmvInTable
      nodupkey
      %if &ETL_DEBUG %then details;
   ;
      by &lmvTmpFieldPairBK &lmvTmpFieldPairNo;
   run;
   %error_check;

   proc sort
      data=&lmvInTable
      uniqueout=&lmvKeysTable
      out=&lmvCheckTable1
      nouniquekey
      %if &ETL_DEBUG %then details;
   ;
      by &lmvTmpFieldPairBK;
   run;
   %error_check;

   /* Проверка: все непустые ключи должны входить только в одну пару */
   %local lmvCheckTable1Obs;
   %let lmvCheckTable1Obs  = %member_obs(mpData=&lmvCheckTable1);
   %if &lmvCheckTable1Obs gt 0 %then %do;
      %job_event_reg (mpEventTypeCode  =  ILLEGAL_ARGUMENT,
                      mpEventDesc      =  %bquote(В таблице &mpIn &lmvCheckTable1Obs ключей-дубликатов),
                      mpEventValues    =  %bquote(См. выборку в таблице &lmvCheckTable1) );
      %return;
   %end;

   /* Упорядочиваем глобальную таблицу соответствия */
   %local lmvLookupTable lmvLookupD0Table lmvTmpFieldSK;
   %let lmvLookupTable     = work.etl_sk2_&lmvUID._lkup;
   %let lmvLookupD0Table   = work.etl_sk2_&lmvUID._lkupd0;
   %let lmvTmpFieldSK      = etl_sk_&lmvUID;

   /* Создаем явную запись о D0 */
   data &lmvLookupD0Table;
      if 0 then set &mpLookup(keep= &mpLkupFieldBK &mpLkupFieldSK);

      &mpLkupFieldBK    = &ETL_D0_ID;
      %if &mpSKMethod=RK %then
         &mpLkupFieldSK = &ETL_D0_RK;
      %else
         &mpLkupFieldSK = &ETL_D0_CD;
      ;
      output;
      stop;
   run;

   %local lmvLoopCount;
   %let lmvLoopCount = 0;
%conflict_loop:
   %let lmvLoopCount = %eval(&lmvLoopCount + 1);
   %if &lmvLoopCount gt 3 %then %do;
      %job_event_reg (mpEventTypeCode=DATA_CONFLICT,
                      mpLevel=E,
                      mpEventText= %bquote(Превышен лимит конфликтов при генерации ключей для таблицы &mpIn));
      %goto cleanup;
   %end;

   DATA &lmvLookupTable;
      SET
         &lmvLookupD0Table
         &mpLookup(
            keep= &mpLkupFieldBK &mpLkupFieldSK
            where= (&mpLkupFieldBK ne &ETL_D0_ID)
         )
      END=_end;

      RENAME
         &mpLkupFieldBK = &lmvTmpFieldPairBK
         &mpLkupFieldSK = &lmvTmpFieldSK
      ;
      KEEP &mpLkupFieldBK &mpLkupFieldSK;
   RUN;

   proc sort data=&lmvLookupTable
      %if &ETL_DEBUG %then details;
   ;
      by &lmvTmpFieldPairBK;
   run;
   %error_check;

   proc sql;
      create unique index &lmvTmpFieldPairBK on &lmvLookupTable;
   quit;
   %error_check (mpStepType=SQL);

   /* Создаем взвешенную таблицу ключей для определения RK */
   %local lmvBKWgtTable;
   %let lmvBKWgtTable   = work.etl_sk2_&lmvUID._bkrk_wgt;

   %local lmvTmpFieldDummyType lmvTmpFieldPairType lmvTmpFieldRC;
   %let lmvTmpFieldDummyType  = etl_dummy_type_&lmvUID.;
   %let lmvTmpFieldPairType   = etl_pair_type_&lmvUID.;
   %let lmvTmpFieldRC         = etl_rc_&lmvUID.;

   %local lmvTmpFieldD0ErrCount lmvD0ErrMax;
   %let lmvTmpFieldD0ErrCount = etl_d0_cnt_&lmvUID.;
   %let lmvD0ErrMax           = %sysfunc(getoption(ERRORS));

   %local lmvTmpArrayBKType lmvTmpArrayBKRx lmvTmpArrayDummyRx;
   %let lmvTmpArrayBKType     = etl_bk_type_&lmvUID.;
   %let lmvTmpArrayBKRx       = etl_bk_rx_&lmvUID.;
   %let lmvTmpArrayDummyRx    = etl_dummy_rx_&lmvUID.;

   data &lmvBKWgtTable;
      /* Определение переменных */
      set &lmvKeysTable;
      if 0 then set &lmvLookupTable;
      length &lmvTmpFieldDummyType &lmvTmpFieldPairType $3;
      array &lmvTmpArrayBKType[1:&lmvN]   $%member_var_attr(mpData=etl_sys.etl_bk_type, mpVar=bk_type_cd, mpAttr=VARLEN)   _temporary_;
      array &lmvTmpArrayBKRx[1:&lmvN]     8 _temporary_;
      array &lmvTmpArrayDummyRx[1:&lmvN]  8 _temporary_;
      retain &lmvTmpFieldD0ErrCount 0;
      drop &lmvTmpFieldD0ErrCount &lmvTmpFieldRC;

      /* Инициализация */
      if _N_ = 1 then do;
%do lmvI=1 %to &lmvN;
         /* Получаем маски для определения дамми */
         %local lmvBkRx lmvDummyRx;
         %let lmvBkRx      = %sysfunc(putc(&&mpBKType&lmvI, bkt_cd_rx.));
         %let lmvDummyRx   = %sysfunc(putc(&&mpBKType&lmvI, bkt_cd_dummy_rx.));

         &lmvTmpArrayBKType[&lmvI]  = "&&mpBKType&lmvI";
         &lmvTmpArrayBKRx[&lmvI]    = prxparse("/\b&lmvBkRx\b/o");
         &lmvTmpArrayDummyRx[&lmvI] = prxparse("/\b&lmvDummyRx\b/o");
%end;
      end;

      /* Проверяем: дамми (D1) или нет */
      if prxmatch(&lmvTmpArrayBKRx[&lmvTmpFieldPairIdx], &lmvTmpFieldPairBK) then do;
         /* Либо известный, либо новый ключ: используем таблицу перекодировки */
         &lmvTmpFieldDummyType = "";
         set &lmvLookupTable key=&lmvTmpFieldPairBK /unique;
         if _iorc_ = &IORC_SOK then do;
            &lmvTmpFieldPairType = "XK";
         end;
         else do;
            _error_ = 0;
            &lmvTmpFieldPairType = "NK";
            call missing(&lmvTmpFieldSK);
         end;
      end;
      else do;
         /* Должен быть дамми */
         if &lmvTmpFieldPairBK = &ETL_D0_ID then do;
            /* D0 всегда есть в таблице перекодировки */
            delete;
         end;
         else do;
            /* Проверяем D1 */
            if prxmatch(&lmvTmpArrayDummyRx[&lmvTmpFieldPairIdx], &lmvTmpFieldPairBK) then do;
               /* Заполняем X_DUMMY_TYPE_CD */
               &lmvTmpFieldDummyType = "D1";

               /* Проверяем таблицу перекодировки */
               set &lmvLookupTable key=&lmvTmpFieldPairBK /unique;
               if _iorc_ = &IORC_SOK then do;
                  &lmvTmpFieldPairType    = "XD1";
               end;
               else do;
                  _error_ = 0;
                  &lmvTmpFieldPairType    = "ND1";

                  /* Делаем перекодировку без доп. ключа RK (берется из BK)... */
%if &mpSKMethod=RK %then %do;
                  &lmvTmpFieldSK = -input (prxposn(&lmvTmpArrayDummyRx[&lmvTmpFieldPairIdx], 2, &lmvTmpFieldPairBK), best.);
%end;
%else %do;
                  call missing(&lmvTmpFieldSK);
%end;
               end;
            end;
            else do;
               /* Ошибка, заменяем на D0 */
               /* D0 всегда есть в таблице перекодировки */
               &lmvTmpFieldD0ErrCount + 1;
               if &lmvTmpFieldD0ErrCount le &lmvD0ErrMax then
                  &lmvTmpFieldRC = log4sas_error ("dwf.macro.etl_generate_sk_2", catx (" ", "Business key", &lmvTmpFieldPairBK,
                     "does not conform to type", &lmvTmpArrayBKType[&lmvTmpFieldPairIdx], ", resetting to D0"));
               if &lmvTmpFieldD0ErrCount = &lmvD0ErrMax then
                  &lmvTmpFieldRC = log4sas_error ("dwf.macro.etl_generate_sk", "Limit set by ERRORS=&lmvD0ErrMax reached, further errors will not be printed");
               delete;
            end;     /* если не соотв. типу BK */
         end;        /* если не D0 */
      end;           /* если дамми */
   run;
   %error_check;

   proc sort data=&lmvBKWgtTable;
      by &lmvTmpFieldPairNo descending &lmvTmpFieldPairType;
   run;
   %error_check;

   /* Создаем локальную таблицу соответствия */
   %local lmvBKSKTable lmvBKSKNewTable lmvBKSKAddTable lmvBKSKErrTable;
   %let lmvBKSKTable       = work.etl_sk2_&lmvUID._bksk;
   %let lmvBKSKNewTable    = work.etl_sk2_&lmvUID._bksk_new;
   %let lmvBKSKAddTable    = work.etl_sk2_&lmvUID._bksk_add;
   %let lmvBKSKErrTable    = work_ia.etl_sk2_&lmvUID._bksk_err;
   data
      &lmvBKSKTable (
         keep=
            &lmvTmpFieldPairNo
            &lmvTmpFieldPairBK
            &lmvTmpFieldSK
            &lmvTmpFieldDummyType
         index= (
            &lmvTmpFieldPairNo /unique
            &lmvTmpFieldPairBK /unique
         )
      )
      &lmvBKSKNewTable (
         keep=
            &lmvTmpFieldPairBK
            &lmvTmpFieldSK
      )
      &lmvBKSKAddTable (
         keep=
            &lmvTmpFieldPairNo
            &lmvTmpFieldPairBK
      )
      &lmvBKSKErrTable
   ;
      set &lmvBKWgtTable;
      by &lmvTmpFieldPairNo descending &lmvTmpFieldPairType;

      /* инициализация */
      /* определяем RK/CD для каждой пары BK */
      %local lmvTmpFieldPairSK;
      %let lmvTmpFieldPairSK  = etl_pair_sk_&lmvUID;
      retain &lmvTmpFieldPairSK;

      select (&lmvTmpFieldPairType);
         when ("XK", "XD1") do;
            if first.&lmvTmpFieldPairNo then
               &lmvTmpFieldPairSK = &lmvTmpFieldSK;
            else
               /* Проверяем, что все RK/CD одинаковы */
               if &lmvTmpFieldSK ne &lmvTmpFieldPairSK then do;
                  output &lmvBKSKErrTable;
                  delete;
               end;
         end;
         when ("NK") do;
            if first.&lmvTmpFieldPairNo then do;
               /* Создаем новый ключ */
               call missing(&lmvTmpFieldPairSK);
               &lmvTmpFieldSK = &lmvTmpFieldPairSK;
               output &lmvBKSKNewTable;
            end;
            else do;
               &lmvTmpFieldSK = &lmvTmpFieldPairSK;
               output &lmvBKSKAddTable;
            end;
         end;
         when ("ND1") do;
            if first.&lmvTmpFieldPairNo then do;
               /* Ставим этот ключ для всех остальных */
               &lmvTmpFieldPairSK = &lmvTmpFieldSK;
               output &lmvBKSKNewTable;
            end;
            else do;
               &lmvTmpFieldSK = &lmvTmpFieldPairSK;
               output &lmvBKSKAddTable;
            end;
         end;
         otherwise do;
            output &lmvBKSKErrTable;
            delete;
         end;
      end;

      /* Оставляем лучшую перекодировку */
      if first.&lmvTmpFieldPairNo then output &lmvBKSKTable;
   run;
   %error_check;

   /* Проверка: нет пар с разными ключами */
   %local lmvCheckBKSKErrObs;
   %let lmvCheckBKSKErrObs  = %member_obs(mpData=&lmvBKSKErrTable);
   %if &lmvCheckBKSKErrObs gt 0 %then %do;
      %job_event_reg (mpEventTypeCode  =  ILLEGAL_ARGUMENT,
                      mpEventDesc      =  %bquote(В таблице &mpLookup &lmvCheckBKSKErrObs пар с разными ключами),
                      mpEventValues    =  %bquote(См. выборку в таблице &lmvBKSKErrTable) );
      %return;
   %end;

   /* Если новых ключей нет, то переходим к добавлению */
   %if %member_obs(mpData=&lmvBKSKNewTable) le 0 %then %goto add;

   /* Получаем новые ключи, обновляем глобальную таблицу соответствия */
   %local lmvBKSKMergedTable lmvMergeStatus;
   %let lmvBKSKMergedTable = work.etl_sk2_&lmvUID._bksk_mrg;
   %etl_generate_sk_merge_new (
      mpIn                 =  &lmvBKSKNewTable,
      mpInFieldBK          =  &lmvTmpFieldPairBK,
      mpInFieldSK          =  &lmvTmpFieldSK,
      mpOut                =  &lmvBKSKMergedTable,
      mpSKMethod           =  &mpSKMethod,
      mpLookup             =  &mpLookup,
      mpLkupFieldBK        =  &mpLkupFieldBK,
      mpLkupFieldSK        =  &mpLkupFieldSK,
      mpConflict1Level     =  W,
      mpStatusKey          =  lmvMergeStatus
   );
   %if &lmvMergeStatus ne 0 %then %goto conflict_loop;

   /* Обновляем локальную таблицу соответствия */
   data &lmvBKSKTable;
      modify &lmvBKSKTable;
      set &lmvBKSKMergedTable (keep= &lmvTmpFieldPairBK &lmvTmpFieldSK) key=&lmvTmpFieldPairBK /unique;
      if _iorc_ = &IORC_SOK then
         replace;
      else do;
         _error_ = 0;
         output;
      end;
   run;

   /* Очистка */
   %member_drop(&lmvBKSKMergedTable);

%add:
   /* Если добавляемых ключей нет, то начинаем перекодировку */
   %if %member_obs(mpData=&lmvBKSKAddTable) le 0 %then %goto decode;

   /* Добавляем новые ключи к новым связям, обновляем глобальную таблицу соответствия */
   data &lmvBKSKAddTable (
      keep= &lmvTmpFieldPairBK &lmvTmpFieldSK
   );
      set &lmvBKSKAddTable;
      set &lmvBKSKTable (keep= &lmvTmpFieldPairNo &lmvTmpFieldSK) key=&lmvTmpFieldPairNo /unique;
   run;

   %let lmvMergeStatus = ;
   %etl_generate_sk_merge_add (
      mpIn                 =  &lmvBKSKAddTable,
      mpInFieldBK          =  &lmvTmpFieldPairBK,
      mpInFieldSK          =  &lmvTmpFieldSK,
      mpLookup             =  &mpLookup,
      mpLkupFieldBK        =  &mpLkupFieldBK,
      mpLkupFieldSK        =  &mpLkupFieldSK,
      mpConflict1Level     =  W,
      mpConflict2Level     =  W,
      mpStatusKey          =  lmvMergeStatus
   );
   %if &lmvMergeStatus ne 0 %then %goto conflict_loop;

%decode:
   /* Перекодируем BK -> RK */
   %local lmvInputNoDummyVars;
   %member_vars_get(&mpIn, lmvInputNoDummyVars, mpDrop=&mpOutFieldDummyType);

   data &mpOut(
      keep=
         %member_vars_expand(&lmvInputNoDummyVars, {})
         &lmvTmpFieldSK
         &lmvTmpFieldDummyType
      rename= (
         &lmvTmpFieldSK          = &mpOutFieldSK
         &lmvTmpFieldDummyType   = &mpOutFieldDummyType
       )
   );
      set &mpIn (
         keep= %member_vars_expand(&lmvInputNoDummyVars, {})
      );

      /* Ищем лучшую перекодировку */
%do lmvI=1 %to &lmvN;
      if not missing(&&mpInFieldBK&lmvI) then do;
         set &lmvBKSKTable (
            keep=   &lmvTmpFieldPairBK &lmvTmpFieldSK &lmvTmpFieldDummyType
            rename= (&lmvTmpFieldPairBK=&&mpInFieldBK&lmvI)
            )
            key=&&mpInFieldBK&lmvI /unique;

         if _iorc_ = &IORC_SOK then do;
            output;
            return;
         end;
         else do;
            _error_ = 0;
         end;
      end;
%end;
   run;
   %error_check;

   %member_vars_clean(&lmvInputNoDummyVars);

%cleanup:
   /* Очистка */
   %member_drop(&lmvInTable);
   %member_drop(&lmvKeysTable);
   %member_drop(&lmvCheckTable1);
   %member_drop(&lmvLookupTable);
   %member_drop(&lmvLookupD0Table);
   %member_drop(&lmvBKWgtTable);
   %member_drop(&lmvBKSKTable);
   %member_drop(&lmvBKSKNewTable);
   %member_drop(&lmvBKSKAddTable);
   %member_drop(&lmvBKSKErrTable);
%mend etl_generate_sk_2;
