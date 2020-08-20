/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 260bdcb84bcae79877b79865605e16c96fb5d1fb $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Генерирует бизнес-ключ по шаблону и сохраняет его в новом/обновляемом поле.
*
*  ПАРАМЕТРЫ:
*     mpFieldBK               +  поле, в которое будет помещен сгенерированный бизнес-ключ
*     mpColumnList            +  список полей, составляющих бизнес-ключ, разделенный пробелами
*     mpFormat                +  шаблон создания бизнес-ключа
*     mpFieldDummy            -  временное поле, в которое будет помещен номер дамми этого ключа (или 0, если не дамми)
*                                Если не задано, дамми не создаются, а бизнес-ключ генерируется пустым
*     mpFieldDummyCount       -  временное поле, в котором находится кол-во выданных номеров дамми
*     mpLengthBK              -  длина всего бизнес-ключа
*                                по умолчанию 32
*
******************************************************************
*  Использует:
*     %error_check
*     %etl_generate_bk_fmt
*     %etl_generate_bk_rx
*     %is_blank
*     %unique_id
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1.  Формат G будет корректно работать только в SBCS-окружении.
*
******************************************************************
*  Пример использования:
*     data t1;
*        set sashelp.class (keep= NAME AGE);
*
*        %etl_generate_1_bk (
*           mpFieldBK                  =  TEST_BK,
*           mpColumnList               =  NAME AGE,
*           mpFormat                   =  TEST_{C5:10}_{N4}_SET
*        );
*
*        %etl_degenerate_1_bk (
*           mpFieldBK                  =  TEST_BK,
*           mpColumnList               =  NAME_OUT AGE_OUT,
*           mpFormat                   =  TEST_{C5:10}_{N4}_SET
*        );
*     run;
*
******************************************************************
*  24-02-2012  Нестерёнок     Начальное кодирование
*  04-05-2012  Нестерёнок     Добавлены D1/D2
*  01-06-2012  Нестерёнок     Добавлен формат T
*  31-08-2012  Нестерёнок     Рефактор mpMode
*  17-09-2012  Нестерёнок     mpFieldDummy теперь необязателен
*  29-03-2013  Нестерёнок     Добавлена проверка соответствия формату типа ключа (mpTypeRX)
*  08-11-2013  Нестерёнок     Добавлен формат X
*  17-07-2014  Нестерёнок     Переименован в etl_generate_1_bk
*  01-11-2015  Нестерёнок     RX-формат определяется из mpFormat
******************************************************************/

%macro etl_generate_1_bk (
   mpFieldBK                  =  ,
   mpColumnList               =  ,
   mpFormat                   =  ,
   mpFieldDummy               =  ,
   mpFieldDummyCount          =  ,
   mpLengthBK                 =  32
);
   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Получаем количество полей */
   %local lmvFieldCount;
   %let lmvFieldCount = %sysfunc(countw(&mpColumnList, , s));

   /* проверим, что в формате есть столько полей */
   %local lmvTouchType;
   %etl_generate_bk_fmt (
      mpFormat       =  &mpFormat,
      mpIndex        =  &lmvFieldCount,
      mpOutTypeKey   =  lmvTouchType
   );
   %if %is_blank(lmvTouchType) %then %do;
      %log4sas_error (dwf.macro.etl_generate_bk, Pattern &mpFormat. does not contain &lmvFieldCount fields, or is incorrect);
      %return;
   %end;
   /* проверим, что в формате не больше полей */
   %etl_generate_bk_fmt (
      mpFormat       =  &mpFormat,
      mpIndex        =  %eval(&lmvFieldCount + 1),
      mpOutTypeKey   =  lmvTouchType
   );
   %if not %is_blank(lmvTouchType) %then %do;
      %log4sas_error (dwf.macro.etl_generate_bk, Pattern &mpFormat. is not complete with fields [&mpColumnList]);
      %return;
   %end;

   /* Для вычисления признака дамми */
   %local lmvErrCountKey lmvDummyRequired;
   %let lmvErrCountKey     = etl_err_&lmvUID.;
   drop &lmvErrCountKey;
   length &lmvErrCountKey 8;
   &lmvErrCountKey = 0;
   %let lmvDummyRequired = %eval (not %is_blank(mpFieldDummy));

   /* Инициализация общих переменных */
   length &mpFieldBK $&mpLengthBK;
   %if &lmvDummyRequired %then %do;
      length &mpFieldDummy 8;
   %end;

   /* Проходим по всем компонентам формата, определяя значения полей */
   %local lmvBKExpr lmvFirstArg;
   %let lmvFirstArg     =  1;
   %let lmvBKExpr       =  ;

   %local lmvFieldIndex;
   %do lmvFieldIndex = 1 %to &lmvFieldCount;
      /* Определяем параметры очередного компонента */
      %local lmvFormatType lmvFormatMinWidth lmvFormatMaxWidth lmvFormatPrefix lmvFormatSuffix;
      %etl_generate_bk_fmt (
         mpFormat          =  &mpFormat,
         mpIndex           =  &lmvFieldIndex,
         mpOutTypeKey      =  lmvFormatType,
         mpOutMinWidthKey  =  lmvFormatMinWidth,
         mpOutMaxWidthKey  =  lmvFormatMaxWidth,
         mpOutPrefixKey    =  lmvFormatPrefix,
         mpOutSuffixKey    =  lmvFormatSuffix
      );

      /* Конвертируем значение поля согласно формату */
      %local lmvFieldValue;
      %let lmvFieldValue   = etl_value_&lmvFieldIndex._&lmvUID.;
      length &lmvFieldValue $&lmvFormatMaxWidth;
      drop &lmvFieldValue;

      %let lmvFieldName = %scan(&mpColumnList, &lmvFieldIndex, %str( ));
      if missing(&lmvFieldName) then do;
         &lmvFieldValue = repeat("&ETL_BK_INVALID", %eval(&lmvFormatMaxWidth.-1));
         &lmvErrCountKey = &lmvErrCountKey + 1;
      end;
      else do;
         %if &lmvFormatType eq N %then %do;
            if vtype(&lmvFieldName) = "N" then do;
               &lmvFieldValue = put(&lmvFieldName, z&lmvFormatMaxWidth..);
            end;
            else do;
               &lmvFieldValue = repeat("&ETL_BK_INVALID", %eval(&lmvFormatMaxWidth.-1));
               &lmvErrCountKey = &lmvErrCountKey + 1;
               /* TODO: fire event WRONG_FIELD_TYPE */
            end;
         %end;
         %if &lmvFormatType eq C %then %do;
            if vtype(&lmvFieldName) = "C" then do;
               &lmvFieldValue = substrn(&lmvFieldName, 1, &lmvFormatMaxWidth.);
            end;
            else do;
               &lmvFieldValue = repeat("&ETL_BK_INVALID", %eval(&lmvFormatMaxWidth.-1));
               &lmvErrCountKey = &lmvErrCountKey + 1;
               /* TODO: fire event WRONG_FIELD_TYPE */
            end;
         %end;
         %if &lmvFormatType eq G %then %do;
            if vtype(&lmvFieldName) = "C" then do;
               &lmvFieldValue = substrn(&lmvFieldName, 1, &lmvFormatMaxWidth.);
            end;
            else do;
               &lmvFieldValue = repeat("&ETL_BK_INVALID", %eval(&lmvFormatMaxWidth.-1));
               &lmvErrCountKey = &lmvErrCountKey + 1;
               /* TODO: fire event WRONG_FIELD_TYPE */
            end;
         %end;
         %if &lmvFormatType eq Z %then %do;
            if vtype(&lmvFieldName) = "C" and (verify (trim(&lmvFieldName), "0123456789") = 0) then do;
               if lengthn(&lmvFieldName) < &lmvFormatMaxWidth. then do;
                  &lmvFieldValue = repeat("0", %eval(&lmvFormatMaxWidth.-1)-lengthn(&lmvFieldName)) || &lmvFieldName;
                  &lmvFieldValue = substrn(&lmvFieldValue, 1, &lmvFormatMaxWidth.);
               end;
               else do;
                  &lmvFieldValue = substrn(&lmvFieldName, 1, &lmvFormatMaxWidth.);
               end;
            end;
            else do;
               &lmvFieldValue = repeat("&ETL_BK_INVALID", %eval(&lmvFormatMaxWidth.-1));
               &lmvErrCountKey = &lmvErrCountKey + 1;
               /* TODO: fire event WRONG_FIELD_TYPE */
            end;
         %end;
         %if &lmvFormatType eq D %then %do;
            if vtype(&lmvFieldName) = "N" then do;
               &lmvFieldValue = put(&lmvFieldName, yymmddn&lmvFormatMaxWidth..);
            end;
            else do;
               &lmvFieldValue = repeat("&ETL_BK_INVALID", %eval(&lmvFormatMaxWidth.-1));
               &lmvErrCountKey = &lmvErrCountKey + 1;
               /* TODO: fire event WRONG_FIELD_TYPE */
            end;
         %end;
         %if &lmvFormatType eq T %then %do;
            if vtype(&lmvFieldName) = "N" then do;
               &lmvFieldValue = put(datepart(&lmvFieldName), yymmddn&lmvFormatMaxWidth..);
            end;
            else do;
               &lmvFieldValue = repeat("&ETL_BK_INVALID", %eval(&lmvFormatMaxWidth.-1));
               &lmvErrCountKey = &lmvErrCountKey + 1;
               /* TODO: fire event WRONG_FIELD_TYPE */
            end;
         %end;
         %if &lmvFormatType eq X %then %do;
            if vtype(&lmvFieldName) = "C" then do;
               &lmvFieldValue = put(md5(cats(&lmvFieldName)), $hex&lmvFormatMaxWidth..);
            end;
            else do;
               &lmvFieldValue = repeat("&ETL_BK_INVALID", %eval(&lmvFormatMaxWidth.-1));
               &lmvErrCountKey = &lmvErrCountKey + 1;
               /* TODO: fire event WRONG_FIELD_TYPE */
            end;
         %end;
      end;

      /* Строим выражение для BK */
      /* Добавляем префикс форматной строки перед компонентом, если есть */
      %if not %is_blank(lmvFormatPrefix) %then %do;
         %if not &lmvFirstArg %then
            %let lmvBKExpr = &lmvBKExpr ||;
         %let lmvBKExpr = &lmvBKExpr "&lmvFormatPrefix";
         %let lmvFirstArg  =  0;
      %end;

      /* Добавляем значение компонента */
      %if not &lmvFirstArg %then
         %let lmvBKExpr = &lmvBKExpr ||;
      %let lmvBKExpr = &lmvBKExpr strip(&lmvFieldValue);
      %let lmvFirstArg  =  0;

      /* Добавляем суффикс форматной строки после последнего компонента, если есть */
      %if (&lmvFieldIndex = &lmvFieldCount) and not %is_blank(lmvFormatSuffix) %then %do;
         %let lmvBKExpr = &lmvBKExpr || "&lmvFormatSuffix";
      %end;
   %end;    /* do lmvFieldIndex */

   /* Генерируем ключ */
   &mpFieldBK = &lmvBKExpr;

   /* Проверка соответствия ключа его RX-формату */
   %local lmvRxTypeKey lmvRxTemplate;
   %let lmvRxTypeKey    =  etl_rxt_&lmvUID.;
   %let lmvRxTemplate   =  %etl_generate_bk_rx(mpTemplate= &mpFormat);
   &lmvRxTypeKey        =  prxparse("/^&lmvRxTemplate *$/o");
   drop &lmvRxTypeKey;
   if not prxmatch(&lmvRxTypeKey, &mpFieldBK) then do;
      &lmvErrCountKey = &lmvErrCountKey + 1;
   end;

   /* Если были ошибки, и генерация ключа обязательна, то это дамми-ключ (типа D1 или D2) */
   %if &lmvDummyRequired %then %do;
      if (&lmvErrCountKey gt 0) then do;
            &mpFieldDummyCount = &mpFieldDummyCount + 1;
            &mpFieldDummy = &mpFieldDummyCount;
      end;
      else
         &mpFieldDummy = 0;
   %end;
   /* Если были ошибки, а генерация ключа не требуется, то бизнес-ключ становится пустым */
   %else %do;
      if (&lmvErrCountKey gt 0) then
         call missing (&mpFieldBK);
   %end;
%mend etl_generate_1_bk;
