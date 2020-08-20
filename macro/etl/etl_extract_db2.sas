/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 18c922274da52a1842fcc5f58072f8caefc5a8fb $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Выгружает набор из DB2, рассчитывает MD5-суммы строк.
*
*  ПАРАМЕТРЫ:
*     mpIn                 +  имя входного набора, таблицы из источника
*     mpOut                +  имя выходного набора, выгруженной таблицы
*     mpWhere              -  условие отбора из входного набора
*     mpKeep               -  список полей для выгрузки, по умолчанию все
*     mpFieldsRu           x  список полей в русской кодировке
*     mpFieldsBinary       x  список бинарных полей, передающихся без перекодировки
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
*     %db2_connect
*     %db2_table_name
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %etl_extract_db2 (mpIn=MID.ACCNTAB, mpSrcSystem=MID, mpFieldsRu=ANAME, mpOut=sas.accntab);
*
******************************************************************
*  15-03-2012  Нестерёнок     Начальное кодирование (etl_extract_midas)
*  29-08-2012  Кузенков       Переход на новый md5
*  31-08-2012  Нестерёнок     Добавлен функционал битовых полей (mpFieldsBinary)
*  05-09-2012  Кузенков       Переход на util_md5_get_exp
******************************************************************/

%macro etl_extract_db2 (
   mpIn                    =  ,
   mpOut                   =  ,
   mpWhere                 =  ,
   mpKeep                  =  ,
   mpFieldsRu              =  ,
   mpFieldsBinary          =  ,
   mpSrcSystem             =  ,
   mpExtractId             =  ,
   mpDigestFields          =  &mpKeep,
   mpResourceId            =  ,
   mpFieldDigest           =  etl_digest_cd,
   mpFieldSrcSystem        =  source_system_cd,
   mpFieldExtractId        =  etl_extract_id,
   mpFieldResourceId       =  etl_resource_id
);
   /* Макросы для удобства */
   /* Получить длину поля */
   %macro _etl_get_column_length (var, mpDefaultLength=100);
      %local i lmvLen;
      %let lmvLen = &mpDefaultLength;
      %if %symexist(_OUTPUT_col_count) %then %do;
         %do i=0 %to %eval(&_OUTPUT_col_count - 1);
            %if &var eq &&_OUTPUT_col&i._name %then
               %let lmvLen = &&_OUTPUT_col&i._length;
         %end;
      %end;
      %do;
         &lmvLen
      %end;
   %mend _etl_get_column_length;
   /* Конвертация полей внутри DB2 */
   %macro _etl_extract_db2_loop (var);
      %local lmvLen;
      %if %index(&mpFieldsRu, &var) %then %do;
         %let lmvLen = %_etl_get_column_length (&var, mpDefaultLength=100);

         %quote(cast(cast(&var as char(&lmvLen) for bit data) as char(&lmvLen) ccsid 1025)) &var
      %end;
      %else %if %index(&mpFieldsBinary, &var) %then %do;
         %let lmvLen = %_etl_get_column_length (&var, mpDefaultLength=1);

         %quote(hex(&var)) &var
      %end;
      %else %do;
         &var
      %end;
   %mend _etl_extract_db2_loop;
   /* Обратная конвертация полей в SAS */
   %macro _etl_extract_sas_loop (var);
      %local lmvLen lmvHexLen;
      %if %index(&mpFieldsBinary, &var) %then %do;
         %let lmvLen = %_etl_get_column_length (&var, mpDefaultLength=1);
         %let lmvHexLen = %eval(&lmvLen*2);

         %quote(input(&var, $hex&lmvHexLen..)) as &var
      %end;
      %else %do;
         &var
      %end;
   %mend _etl_extract_sas_loop;

   /* Получаем одноуровневые имена таблиц и схемы */
   %local lmvLoginSet lmvSource;
   %db2_table_name(mpSASTable=&mpIn, mpOutFullNameKey=lmvSource, mpOutLoginSetKey=lmvLoginSet);

   %if %is_blank(mpKeep) %then %do;
      %let mpKeep = %member_vars(&mpIn);
   %end;

   %if not %is_blank(mpFieldDigest) and not %is_blank(mpDigestFields) %then %do;
      %local lmvDigestMD5Exp;
      %util_digest_expr (mpIn=&mpIn, mpDigestFields=&mpDigestFields, mpOutKey=lmvDigestMD5Exp, mpProc=SQL);
   %end;

   proc sql;
      %db2_connect(mpLoginSet=&lmvLoginSet)
      ;
      create table &mpOut
         %if not %is_blank(mpExtractId) %then %do;
            (label= &mpExtractId)
         %end;
      as select
         %util_loop (mpMacroName=_etl_extract_sas_loop, mpWith=
            &mpKeep
            , mpOutDlm=%str(, )
         )

         /* Расчетные переменные */
         /* add digest if requested */
         %if not %is_blank(mpFieldDigest) and not %is_blank(mpDigestFields) %then %do;
            , &lmvDigestMD5Exp as &mpFieldDigest length=16 format=$hex32.
         %end;

         /* add source system if requested */
         %if not %is_blank(mpFieldSrcSystem) and not %is_blank(mpSrcSystem) %then %do;
            , &mpSrcSystem as &mpFieldSrcSystem
         %end;

         /* add extract id if requested */
         %if not %is_blank(mpFieldExtractId) and not %is_blank(mpExtractId) %then %do;
            , &mpExtractId as &mpFieldExtractId
         %end;

         /* add resource id if requested */
         %if not %is_blank(mpFieldResourceId) and not %is_blank(mpResourceId) %then %do;
            , &mpResourceId as &mpFieldResourceId
         %end;

      from connection to db2 (select
         /* Выгружаемые переменные */
         %util_loop (mpMacroName=_etl_extract_db2_loop, mpWith=
            &mpKeep
            , mpOutDlm=%str(, )
         )
         from &lmvSource
         %if not %is_blank(mpWhere) %then %do;
            where (%unquote(&mpWhere))
         %end;
      )
      ;
   quit;
%mend etl_extract_db2;
