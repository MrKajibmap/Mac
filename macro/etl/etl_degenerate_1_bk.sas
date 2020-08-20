/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 6bd880244bf397aa3728e4d97820b3fc519fabd8 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Разбирает бизнес-ключ по шаблону и сохраняет результаты в новых полях.
*     В случае несоответствия ключа шаблону поля останутся пустыми.
*
*  ПАРАМЕТРЫ:
*     mpFieldBK               +  поле сгенерированного ранее бизнес-ключа
*     mpColumnList            +  список полей, на которые разбирается бизнес-ключ, разделенный пробелами
*                                Эти поля будут созданы с типами и длинами, соответствующими своему формату
*     mpFormat                +  шаблон разбора бизнес-ключа
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
*     1.  В общем случае корректный разбор BK может быть невозможен.
*         В частности, исходные значения формата X не могут быть восстановлены и остаются как в ключе.
*         Восстановленные значения форматов Z и T могут отличаться от исходных.
*     2.  Формат G будет корректно работать только в SBCS-окружении.
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
*  03-11-2015  Нестерёнок     Начальное кодирование
******************************************************************/

%macro etl_degenerate_1_bk (
   mpFieldBK                  =  ,
   mpColumnList               =  ,
   mpFormat                   =
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
      %log4sas_error (dwf.macro.etl_generate_bk, Pattern &mpFormat. implies more columns than [&mpColumnList]);
      %return;
   %end;

   /* Проверка соответствия ключа его RX-формату */
   %local lmvRxTypeKey lmvRxTemplate;
   %let lmvRxTypeKey    =  etl_rxt_&lmvUID.;
   %let lmvRxTemplate   =  %etl_generate_bk_rx(mpTemplate= &mpFormat);
   &lmvRxTypeKey        =  prxparse("/\b&lmvRxTemplate\b/o");
   drop &lmvRxTypeKey;
   if prxmatch(&lmvRxTypeKey, &mpFieldBK) then do;
      /* Проходим по всем компонентам формата, определяя значения полей */
      %local lmvFieldIndex;
      %do lmvFieldIndex = 1 %to &lmvFieldCount;
         /* Определяем параметры очередного компонента */
         %local lmvFormatType lmvFormatMinWidth lmvFormatMaxWidth;
         %etl_generate_bk_fmt (
            mpFormat          =  &mpFormat,
            mpIndex           =  &lmvFieldIndex,
            mpOutTypeKey      =  lmvFormatType,
            mpOutMinWidthKey  =  lmvFormatMinWidth,
            mpOutMaxWidthKey  =  lmvFormatMaxWidth
         );

         /* Конвертируем значение поля согласно формату */
         %local lmvFieldName lmvFieldValue;
         %let lmvFieldName = %scan(&mpColumnList, &lmvFieldIndex, %str( ));
         %let lmvFieldValue = prxposn(&lmvRxTypeKey, &lmvFieldIndex, &mpFieldBK);

         %if &lmvFormatType eq N %then %do;
            length &lmvFieldName 8;
            &lmvFieldName = input (&lmvFieldValue, &lmvFormatMaxWidth..);
         %end;
         %if &lmvFormatType eq C %then %do;
            length &lmvFieldName $&lmvFormatMaxWidth;
            &lmvFieldName = &lmvFieldValue;
         %end;
         %if &lmvFormatType eq G %then %do;
            length &lmvFieldName $&lmvFormatMaxWidth;
            &lmvFieldName = &lmvFieldValue;
         %end;
         %if &lmvFormatType eq Z %then %do;
            length &lmvFieldName $&lmvFormatMaxWidth;
            &lmvFieldName = cats(input (&lmvFieldValue, &lmvFormatMaxWidth..));
         %end;
         %if &lmvFormatType eq D %then %do;
            length &lmvFieldName 8;
            format &lmvFieldName yymmddn&lmvFormatMaxWidth.;
            &lmvFieldName = input(&lmvFieldValue, yymmddn&lmvFormatMaxWidth..);
         %end;
         %if &lmvFormatType eq T %then %do;
            length &lmvFieldName 8;
            format &lmvFieldName datetime20.;
            &lmvFieldName = dhms (input(&lmvFieldValue, yymmddn&lmvFormatMaxWidth..), 0, 0, 0);
         %end;
         %if &lmvFormatType eq X %then %do;
            length &lmvFieldName $&lmvFormatMaxWidth;
            &lmvFieldName = &lmvFieldValue;
         %end;
      %end;    /* do lmvFieldIndex */
   end;        /* if BK matched */
   else do;
      call missing (of &mpColumnList);
   end;
%mend etl_degenerate_1_bk;
