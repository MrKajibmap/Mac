/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 8bfd25c9fdbb52dcad6497a1fd7132d29c1730ff $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Выгружает набор из исторического источника, рассчитывает MD5-суммы строк.
*     Выгружаются все поля, для которых определен мэппинг.
*
*  ПАРАМЕТРЫ:
*     mpIn                    +  имя входного набора, таблицы из источника
*     mpOut                   +  имя выходного набора, выгруженной таблицы
*                                Будет содержать все поля, для которых определен мэппинг,
*                                а также поля SOURCE_SYSTEM_CD и mpFieldDigest (если задано)
*     mpHistoryStartDt        +  дата начала интервала выгрузки
*     mpHistoryEndDt          +  дата конца интервала выгрузки
*     mpInterval              -  размер куска выгрузки, в неделях
*                                по умолчанию 4
*     mpFieldDate             +  поле, задающее дату
*     mpFieldDigest           -  поле хэш-суммы, если требуется
*                                Как правило, ETL_DIGEST_CD
*     mpDigestFields          -  список полей для расчета хэш-суммы
*                                По умолчанию все, для которых определен мэппинг, кроме mpFieldDate
*     mpSrcSystem             +  код источника
*     mpWhere                 -  дополнительное условие отбора из mpIn
*
******************************************************************
*  Использует:
*     %error_check
*     %etl_extract
*     %etl_get_input_columns
*     %member_drop
*     %member_vars
*     %unique_id
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
* Пример использования:
*     в трансформе transform_extract_history.sas
*
******************************************************************
* 20-11-2014   Нестерёнок     Адаптация из transform_extract_history.sas 3027:f906310546fe
******************************************************************/

%macro etl_extract_history (
   mpIn                       =  ,
   mpOut                      =  ,
   mpHistoryStartDt           =  ,
   mpHistoryEndDt             =  ,
   mpInterval                 =  4,
   mpFieldDate                =  ,
   mpFieldDigest              =  ,
   mpDigestFields             =  ,
   mpSrcSystem                =  ,
   mpWhere                    =
);
   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   %local lmvExhistData;
   %if &ETL_DEBUG %then %do;
      %let lmvExhistData       = work_stg.tr_exhist_&lmvUID._data;
   %end;
   %else %do;
      %let lmvExhistData       = work.tr_exhist_&lmvUID._data;
   %end;

   %member_drop(&lmvExhistData);

   /* Получаем список полей, для которых определен мэппинг */
   %local lmvInCols;
   %etl_get_input_columns(mpTableMacroName=tpOut, mpOutKey=lmvInCols);

   %if %is_blank(mpDigestFields) %then %do;
      %etl_get_input_columns(mpTableMacroName=tpOut, mpOutKey=mpDigestFields, mpDrop=&mpFieldDate);
   %end;

   %local dtbeg dtend;
   %let dtbeg = %sysfunc(dhms(&mpHistoryStartDt, 0, 0, 0));
   %let dtend = %sysfunc(dhms(&mpHistoryEndDt, 0, 0, 0));

   %local pbeg pend;
   %do pbeg = &dtbeg %to &dtend;
      %let pend = %sysfunc(intnx(DTWEEK, &pbeg, &mpInterval, SAME));
      %if &pend > &dtend %then
         %let pend = %sysfunc(intnx(DTDAY, &dtend, 1, SAME));

      %error_check;

      %etl_extract (
         mpIn           =  &mpIn,
         mpOut          =  &lmvExhistData,
         mpWhere        =  (&mpFieldDate >= &pbeg) and (&mpFieldDate < &pend)
%if not %is_blank(mpWhere) %then %do;
                           and (&mpWhere)
%end;
         ,
         mpKeep         =  &lmvInCols,
         mpSrcSystem    =  "&mpSrcSystem",
         mpFieldDigest  =  &mpFieldDigest,
         mpDigestFields =  &mpDigestFields,
         mpFieldSrcSystem  =  SOURCE_SYSTEM_CD,
         mpExtractId    =  ,
         mpResourceId   =
      );

      proc append base=&mpOut data=&lmvExhistData;
      run;

      %error_check;

      %let pbeg = %eval (&pend - 1);
   %end;

   %if not &ETL_DEBUG %then %do;
      %member_drop(&lmvExhistData);
   %end;
%mend etl_extract_history;
