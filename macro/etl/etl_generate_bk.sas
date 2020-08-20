/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 3a4cc4744391c115bb3e37551fab1e682e687a04 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Генерирует бизнес-ключи в таблице по списку.
*
*  ПАРАМЕТРЫ:
*     mpIn                    +  имя входного набора
*     mpInOptions             -  дополнительные опции входного набора
*     mpBK                    +  список кодов обязательных для генерации бизнес-ключей, разделенный пробелами
*                                Такой бизнес-ключ станет дамми, если заполнены не все поля, его составляющие
*                                FK к ETL_SYS.ETL_BK.BK_CD
*     mpOptionalBK            +  список кодов НЕобязательных для генерации бизнес-ключей, разделенный пробелами
*                                Такой бизнес-ключ останется пустым, если заполнены не все поля, его составляющие
*     mpOut                   +  имя выходного набора
*
******************************************************************
*  Использует:
*     ETL_DUMMY_SEQ
*     %error_check
*     %unique_ids
*     %util_loop_data
*     %util_quote
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1. mpBK/mpOptionalBK должны быть заданы в ASCII-7.
*
******************************************************************
*  Пример использования:
*     В трансформации transform_generate_bk.sas
*
******************************************************************
*  17-07-2014  Нестерёнок     Выделен из transform_generate_bk.sas (678735d05cb1 2014-07-16 08:31:20Z)
******************************************************************/

%macro etl_generate_bk (
   mpIn                       =  ,
   mpInOptions                =  ,
   mpBK                       =  ,
   mpOptionalBK               =  ,
   mpOut                      =
);
   /* Инициализация */
   %local lmvBKCount lmvOptionalBKCount;
   %let lmvBKCount         = %sysfunc(countw(&mpBK, , s));
   %let lmvOptionalBKCount = %sysfunc(countw(&mpOptionalBK, , s));

   %local lmvDummyRows lmvFieldDummyCount lmvDummyRequired;
   %let lmvDummyRows       = work.tr_gen_bk_dummy_rows;
   %let lmvFieldDummyCount = etl_dummy_count;
   %let lmvDummyRequired   = %eval (&lmvBKCount gt 0);

   %member_drop (&mpOut);

   /* Макросы для генерации одного ключа */
   %macro sm_trans_gen_bk_required;
      %etl_generate_1_bk (
         mpFieldBK=&bk_field_nm, mpLengthBK=%sysfunc(inputn(&bk_type_cd, bkt_cd_len.)),
         mpColumnList=&bk_column_list_txt, mpFormat=&bk_format_txt,
         mpFieldDummy=&lmvFieldDummy, mpFieldDummyCount=&lmvFieldDummyCount
      );
   %mend;
   %macro sm_trans_gen_bk_optional;
      %etl_generate_1_bk (
         mpFieldBK=&bk_field_nm, mpLengthBK=%sysfunc(inputn(&bk_type_cd, bkt_cd_len.)),
         mpColumnList=&bk_column_list_txt, mpFormat=&bk_format_txt,
         mpFieldDummy=
      );
   %mend;

   /* Макрос для подстановки в дамми-ключ уникального номера */
   %macro sm_trans_gen_bk_dummy;
      if &lmvFieldDummy gt 0 then do;
         set &lmvDummyIds (keep=OBJECT_ID rename=(OBJECT_ID=&lmvFieldDummy._no)) point=&lmvFieldDummy;
         &bk_field_nm = cats ("&ETL_BK_INVALID._", &lmvFieldDummy._no);

         %error_check;
      end;
   %mend;


   /* Создаем выходной набор итерацией по всем заявленным бизнес-ключам */
   /* Считаем необходимое кол-во дамми-ключей */
   %local lmvBkIndex lmvFieldDummy lmvDummyCount;
   %let lmvDummyCount   = 0;
   data
      %if &lmvDummyRequired %then %do;
         &mpOut (drop= etl_dummy_:)
         &lmvDummyRows
      %end;
      %else %do;
         &mpOut
      %end;
   ;
      set &mpIn (&mpInOptions) end=ds_end;

      /* Генерация необязательных ключей */
      %do lmvBkIndex=1 %to &lmvOptionalBKCount;
         %util_loop_data (mpLoopMacro=sm_trans_gen_bk_optional, mpData=ETL_SYS.ETL_BK,
                          mpWhere=BK_CD eq %util_quote(%scan(&mpOptionalBK, &lmvBkIndex))
         );
      %end;

      %if &lmvDummyRequired %then %do;
         length &lmvFieldDummyCount 8;
         drop &lmvFieldDummyCount;
         retain &lmvFieldDummyCount 0;

         /* Генерация обязательных ключей */
         %do lmvBkIndex=1 %to &lmvBKCount;
            %let lmvFieldDummy = etl_dummy_&lmvBkIndex.;

            %util_loop_data (mpLoopMacro=sm_trans_gen_bk_required, mpData=ETL_SYS.ETL_BK,
                             mpWhere=BK_CD eq %util_quote(%scan(&mpBK, &lmvBkIndex))
            );
         %end;

         /* Если в какой-то записи есть дамми, то не выводим ее сразу */
         if sum (of etl_dummy_1-etl_dummy_&lmvBKCount) = 0 then
            output &mpOut;
         else
            output &lmvDummyRows;

         if ds_end then call symputx ("lmvDummyCount", &lmvFieldDummyCount);
      %end;
      %else %do;
         output &mpOut;
      %end;
   run;
   %error_check;

   /* Если дамми нет, то всё */
   %if (not &lmvDummyRequired) or (&lmvDummyCount eq 0) %then %return;


   /* Для каждого дамми генерируем уникальный номер */
   %local lmvDummyIds;
   %let lmvDummyIds = work.tr_gen_bk_dummy_ids;
   %unique_ids (mpIdCount=&lmvDummyCount, mpOut=&lmvDummyIds, mpSequenceName=ETL_DUMMY_SEQ);

   /* Добавляем номер к дамми */
   data &lmvDummyRows._id (drop= etl_dummy_:);
      set &lmvDummyRows;

      %do lmvBkIndex=1 %to &lmvBKCount;
         %let lmvFieldDummy = etl_dummy_&lmvBkIndex.;

         %util_loop_data (mpLoopMacro=sm_trans_gen_bk_dummy, mpData=ETL_SYS.ETL_BK,
                          mpWhere=BK_CD eq %util_quote(%scan(&mpBK, &lmvBkIndex))
         );
      %end;
   run;
   %error_check;

   /* Добавляем дамми-строки к выходному набору */
   proc append base=&mpOut data=&lmvDummyRows._id;
   run;
   %error_check;
%mend etl_generate_bk;
