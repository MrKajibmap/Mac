/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 96bdf5ab46450981d01ab93e7565dde79b64ef12 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Выгружает набор из источника, рассчитывает MD5-суммы строк.
*
*  ПАРАМЕТРЫ:
*     mpIn                 +  имя входного набора, таблицы из источника
*     mpOut                +  имя выходного набора, выгруженной таблицы
*     mpWhere              -  условие отбора из входного набора
*     mpKeep               -  список полей для выгрузки, по умолчанию все
*     mpSrcSystem          -  код источника.  Если код источника не нужен, указать пустое значение
*     mpExtractId          -  уникальный идентификатор выгрузки (ETL_VERSION_SEQ)
*                             Если идентификатор выгрузки не нужен, указать пустое значение
*     mpResourceId         -  идентификатор ресурса. Если идентификатор ресурса не нужен, указать пустое значение
*     mpDigestFields       -  список полей для расчета хэш-суммы
*                             по умолчанию равно mpKeep.  Если хэш-сумма не нужна, указать пустое значение
*     mpFieldDigest        -  имя поля для добавления хэш-суммы
*                             по умолчанию etl_digest_cd.
*     mpFieldSrcSystem     -  имя поля для кода источника
*                             по умолчанию source_system_cd
*     mpFieldExtractId     -  имя поля для идентификатора выгрузки
*                             по умолчанию etl_extract_id
*     mpFieldResourceId    -  имя поля для идентификатора ресурса
*                             по умолчанию etl_resource_id
*
******************************************************************
*  Использует:
*     %member_vars
*     %util_digest_expr
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %etl_extract (mpIn=RSS.DACCOUNT, mpOut=sas.daccount, mpSrcSystem="RSS");
*
******************************************************************
*  09-02-2012  Нестерёнок     Начальное кодирование
******************************************************************/

%macro etl_extract (
   mpIn                    =  ,
   mpOut                   =  ,
   mpWhere                 =  ,
   mpKeep                  =  ,
   mpSrcSystem             =  ,
   mpExtractId             =  ,
   mpDigestFields          =  &mpKeep,
   mpResourceId            =  ,
   mpFieldDigest           =  etl_digest_cd,
   mpFieldSrcSystem        =  source_system_cd,
   mpFieldExtractId        =  etl_extract_id,
   mpFieldResourceId       =  etl_resource_id
);

   %if %is_blank(mpKeep) %then %do;
      %let mpKeep = %member_vars(&mpIn);
   %end;

   %if not %is_blank(mpFieldDigest) and not %is_blank(mpDigestFields) %then %do;
      %local lmvDigestMD5Exp;
      %util_digest_expr (mpIn=&mpIn, mpDigestFields=&mpDigestFields, mpOutKey=lmvDigestMD5Exp);
   %end;

   data &mpOut (
      keep= &mpKeep
      %if not %is_blank(mpFieldDigest) and not %is_blank(mpDigestFields) %then %do;
         &mpFieldDigest
      %end;
      %if not %is_blank(mpFieldSrcSystem) and not %is_blank(mpSrcSystem) %then %do;
         &mpFieldSrcSystem
      %end;
      %if not %is_blank(mpFieldExtractId) and not %is_blank(mpExtractId) %then %do;
         &mpFieldExtractId
      %end;
      %if not %is_blank(mpFieldResourceId) and not %is_blank(mpResourceId) %then %do;
         &mpFieldResourceId
      %end;

      %if not %is_blank(mpExtractId) %then %do;
         label= &mpExtractId
      %end;
   );

      /* copy source */
      set &mpIn (
		 keep= &mpKeep
%if not %is_blank(mpWhere) %then %do;
         where= (%unquote(&mpWhere))    
%end;
      );

      /* add digest if requested */
      %if not %is_blank(mpFieldDigest) and not %is_blank(mpDigestFields) %then %do;
         attrib
           &mpFieldDigest length = $16 format = $hex32.
         ;
         &mpFieldDigest = &lmvDigestMD5Exp;
      %end;

      /* add source system if requested */
      %if not %is_blank(mpFieldSrcSystem) and not %is_blank(mpSrcSystem) %then %do;
         &mpFieldSrcSystem = &mpSrcSystem;
      %end;

      /* add extract id if requested */
      %if not %is_blank(mpFieldExtractId) and not %is_blank(mpExtractId) %then %do;
         &mpFieldExtractId = &mpExtractId;
      %end;

      /* add resource id if requested */
      %if not %is_blank(mpFieldResourceId) and not %is_blank(mpResourceId) %then %do;
         &mpFieldResourceId = &mpResourceId;
      %end;
   run;
%mend etl_extract;
