/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 9c8d959b4daeb6692809f350ba5d38d538003923 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Сжатие интервального набора данных.
*     Предполагается, что в "дырках" между интервалами значения сохраняются.
*     Поддерживаются только dttm-интервалы.
*
*  ПАРАМЕТРЫ:
*     mpIn                    +  имя входного набора
*     mpFieldDigest           x  имя поля хэш-суммы в mpIn
*                                По умолчанию etl_digest_cd.
*     mpDigestFields          x  список полей mpIn для расчета хэш-суммы, если mpFieldDigest не задан
*     mpBK                    +  поля бизнес-ключа
*     mpOut                   +  имя выходного набора
*     mpOutOptions            -  доп. опции выходного набора
*     mpOutFieldDigest        -  имя поля хэш-суммы в mpOut
*                                По умолчанию mpFieldDigest
*                                Если не задано, то не выводится
*     mpFieldStartDate        -  имя поля начала интервала
*                                По умолчанию start_dttm
*     mpStartDateExpression   -  выражение для вычисления mpFieldStartDate
*                                По умолчанию mpFieldStartDate (1:1)
*     mpFieldEndDate          -  имя поля конца интервала
*                                По умолчанию end_dttm
*     mpEndDateExpression     -  выражение для вычисления mpFieldEndDate
*                                По умолчанию mpFieldEndDate (1:1)
*     mpLastEndDate           -  значение конца последнего сжатого интервала
*                                по умолчанию &ETL_SCD_FUTURE_DTTM (+бесконечность SCD)
*                                Также возможно указать ASIS (не менять)
*     mpNonLastEndDate        -  значение конца остальных сжатых интервалов
*                                по умолчанию EXTEND (расширить до начала следующего интервала)
*                                Также возможно указать ASIS (не менять)
*     mpEquals                -  сохранять (EQUALS) или нет (NOEQUALS) относительный порядок записей при равных mpFieldStartDate
*                                По умолчанию не задано, т.е. согласно системной опции EQUALS
*
******************************************************************
*  Использует:
*     %member_drop
*     %unique_id
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %etl_compress_intervals(
*        mpIn=work_stg.tr_exhist_4759587019_cmp_all,
*        mpOut=work_stg.W41W8T96,
*        mpBK=CUSTOMER_NO,
*        mpFieldDigest=CRC
*     );
*
******************************************************************
*  20-02-2012  Нестерёнок     Начальное кодирование
*  08-04-2013  Нестерёнок     Поддержка параллельного запуска, оптимизация
*  10-10-2014  Нестерёнок     Добавлен mpLastEndDate
*  20-11-2014  Нестерёнок     Добавлен mpEquals
*  26-11-2014  Нестерёнок     Добавлены mpOutFieldDigest, mpDigestFields
*  27-11-2014  Нестерёнок     Добавлен mpNonLastEndDate
*  18-03-2015  Нестерёнок     Добавлены mpStartDateExpression, mpEndDateExpression, mpOutOptions
******************************************************************/

%macro etl_compress_intervals (
   mpIn                       =  ,
   mpFieldDigest              =  etl_digest_cd,
   mpDigestFields             =  ,
   mpBK                       =  ,
   mpOut                      =  ,
   mpOutOptions               =  ,
   mpOutFieldDigest           =  &mpFieldDigest,
   mpFieldStartDate           =  start_dttm,
   mpStartDateExpression      =  &mpFieldStartDate,
   mpFieldEndDate             =  end_dttm,
   mpEndDateExpression        =  &mpFieldEndDate,
   mpLastEndDate              =  &ETL_SCD_FUTURE_DTTM,
   mpNonLastEndDate           =  EXTEND,
   mpEquals                   =
);
   %local lmvLastBKField;
   %let lmvLastBKField  = %scan(&mpBK, -1);

   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   %local lmvSourceCRC lmvTargetByCRC;
   %if &ETL_DEBUG %then %do;
      %let lmvSourceCRC    = work_stg.etl_cmp2_&lmvUID._src_crc;
      %let lmvTargetByCRC  = work_stg.etl_cmp2_&lmvUID._tgt_crc;
   %end;
   %else %do;
      %let lmvSourceCRC    = work.etl_cmp2_&lmvUID._src_crc;
      %let lmvTargetByCRC  = work.etl_cmp2_&lmvUID._tgt_crc;
   %end;

   /* Запоминаем переменные входного набора без ключевых */
   %local lmvInExtraVars;
   %let lmvInExtraVars = %member_vars(&mpIn, mpDrop=&mpBK &mpFieldStartDate &mpFieldEndDate &mpFieldDigest);

   /* Готовим расчет хэш-суммы, если требуется */
   %local lmvDigestMD5Exp;
%if %is_blank(mpFieldDigest) %then %do;
   %let mpFieldDigest   = etl_digest_&lmvUID.;
   %util_digest_expr (mpIn=&mpIn, mpDigestFields=&mpDigestFields, mpOutKey=lmvDigestMD5Exp);
%end;

   %local lmvFieldRowid;
   %let lmvFieldRowid   = etl_row_&lmvUID.;
   data &lmvSourceCRC (
      keep= &mpBK &mpFieldStartDate &mpFieldEndDate &mpFieldDigest &lmvFieldRowid
      compress= no
   );
      set &mpIn;

      &mpFieldStartDate = &mpStartDateExpression;
      &mpFieldEndDate   = &mpEndDateExpression;
      format &mpFieldStartDate &mpFieldEndDate datetime.;
      &lmvFieldRowid = _n_;

%if not %is_blank(mpFieldDigest) and not %is_blank(mpDigestFields) %then %do;
      attrib
        &mpFieldDigest length = $16 format = $hex32.
      ;
      &mpFieldDigest = &lmvDigestMD5Exp;
%end;
   run;

   proc sort data=&lmvSourceCRC &mpEquals;
      by &mpBK &mpFieldStartDate;
   run;

   %local lmvNewStartDate lmvNewEndDate lmvNewStartRow;
   %let lmvNewStartDate = etl_start_dttm_&lmvUID.;
   %let lmvNewEndDate   = etl_end_dttm_&lmvUID.;
   %let lmvNewStartRow  = etl_nn_&lmvUID.;

   data &lmvTargetByCRC (
      compress= no
      keep=     &mpBK &lmvNewStartDate &lmvNewEndDate &lmvNewStartRow
      rename=   (&lmvNewStartDate=&mpFieldStartDate
                 &lmvNewEndDate=&mpFieldEndDate
                 &lmvNewStartRow=&lmvFieldRowid)
%if not %is_blank(mpOutFieldDigest) %then %do;
      keep=     &mpFieldDigest
      rename=   (&mpFieldDigest=&mpOutFieldDigest)
%end;
   );
      set &lmvSourceCRC;
      by &mpBK &mpFieldDigest notsorted;

      if first.&mpFieldDigest then do;
         retain &lmvNewStartDate;
         retain &lmvNewEndDate;
         retain &lmvNewStartRow;

         /* Сохраняем начало интервала */
         &lmvNewStartDate  =  &mpFieldStartDate;
         call missing (&lmvNewEndDate);
         &lmvNewStartRow   =  &lmvFieldRowid;
      end;

      /* Если нет продолжения интервалов, и произошел разрыв в истории */
%if &mpNonLastEndDate ne EXTEND %then %do;
      if not first.&mpFieldDigest and &mpFieldStartDate > &lmvNewEndDate then do;
%if &mpNonLastEndDate ne ASIS %then %do;
         &lmvNewEndDate    =  &mpNonLastEndDate;
%end;
         output;

         /* Сохраняем начало интервала */
         &lmvNewStartDate  =  &mpFieldStartDate;
         &lmvNewStartRow   =  &lmvFieldRowid;
      end;
      &lmvNewEndDate       =  &mpFieldEndDate;
%end;

      /* Если произошло изменение хэш-суммы */
      if last.&mpFieldDigest then do;
         if last.&lmvLastBKField then do;
%if &mpLastEndDate = ASIS %then %do;
            &lmvNewEndDate = &mpFieldEndDate;
%end;
%else %do;
            /* Устанавливаем последний интервал открытым до заданной даты */
            &lmvNewEndDate = &mpLastEndDate;
%end;
         end;
         else do;
%if &mpNonLastEndDate = EXTEND %then %do;
            /* Устанавливаем конец интервала на начало следующего интервала */
            n1 = _n_ + 1;
            drop n1;
            set &lmvSourceCRC (keep=&mpFieldStartDate rename=(&mpFieldStartDate=&lmvNewEndDate)) point=n1;
%end;
%else %if &mpNonLastEndDate = ASIS %then %do;
            &lmvNewEndDate = &mpFieldEndDate;
%end;
%else %do;
            &lmvNewEndDate = &mpNonLastEndDate;
%end;
         end;

         output;
      end;
   run;

   %if not %is_blank(lmvInExtraVars) %then %do;
      /* Оптимизация с целью облегчить попадание pointa в ту же страницу */
      proc sort data=&lmvTargetByCRC;
         by &lmvFieldRowid;
      run;

      data &mpOut (&mpOutOptions);
         set &lmvTargetByCRC;
         set &mpIn (keep= &lmvInExtraVars) point=&lmvFieldRowid;
         drop &lmvFieldRowid;
      run;
   %end;
   %else %do;
      data &mpOut (&mpOutOptions);
         set &lmvTargetByCRC;
         drop &lmvFieldRowid;
      run;
   %end;

   %member_drop(&lmvSourceCRC);
   %member_drop(&lmvTargetByCRC);
%mend etl_compress_intervals;
