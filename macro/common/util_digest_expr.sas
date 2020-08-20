/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 6b32a797640a19a2e815e7d73985b1177d4e55ce $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Создает строку для расчета хэш-суммы переданных полей таблицы.
*     Может использовать методы MD5 и SHA256.
*
*  ПАРАМЕТРЫ:
*     mpIn                    +  таблица, для полей которой вычисляется хэш-сумма
*     mpDigestFields          +  список полей входной таблицы, разделенных пробелами, для которых вычисляется хэш-сумма
*     mpOutKey                +  имя макропеременной, в которую возвращается результат
*     mpProc                  -  {DATA|SQL} шаг, для которого строится строка расчета
*     mpMethod                -  метод вычисления хэш-суммы (MD5, SHA256, BASE64)
*                                По умолчанию MD5
*     mpSalt                  -  соль
*
******************************************************************
*  Использует:
*     %list_expand
*     %util_sasver_ge
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1. Метод SHA256 поддерживается, начиная с 9.4M1, и входит в пакет SAS/SECURE.
*
******************************************************************
*  Пример использования:
*     %local lmvDigestExpr;
*     %util_digest_expr (mpIn=&mpIn, mpDigestFields=&mpInChangedFields, mpOutKey=lmvDigestExpr);
*
*     data _null_;
*        set &mpIn;
*        ATTRIB
*           my_digest LENGTH = $16 FORMAT = $HEX32.
*        ;
*        my_digest = &lmvDigestExpr;
*        ...
*
******************************************************************
*  31-08-2012   Кузенков    Начальное кодирование
*  20-10-2012   Кузенков    Добавлен mpProc
*  25-10-2012   Нестерёнок  Добавлена проверка кол-ва полей
*  14-12-2012   Нестерёнок  Метаданные таблицы берутся через proc contents
*  09-07-2014   Нестерёнок  Поддержка SHA256
*  20-09-2016   Сазонов     Добавил соль
*  14-12-2016   Сазонов     Добавил BASE64
******************************************************************/

%macro util_digest_expr (mpIn=, mpDigestFields=, mpOutKey=, mpProc=DATA, mpMethod=MD5, mpSalt=);
   /* Получаем уникальный идентификатор */
   %local lmvUID lmvMethodOrig;
   %unique_id (mpOutKey=lmvUID);

   %let lmvMethodOrig=&mpMethod;
   /* Проверка аргументов */
   %if &mpMethod = SHA256 and not %util_sasver_ge (mpMajor=9, mpMinor=4, mpTSLevel=M1) %then %do;
      %let mpMethod = MD5;
   %end;
   %if &mpMethod = BASE64 %then %do;
     %let mpMethod = MD5;
   %end;

   /* Временные переменные */
   %local lmvColumnsMetaTable;
   %let lmvColumnsMetaTable   = work.util_digest_&lmvUID.;

   /* Получаем список доступных переменных */
   proc contents data=&mpIn out=&lmvColumnsMetaTable nodetails noprint;
   run;
   %error_check;

   %local lmvOut lmvCount;
   %let lmvCount = 0;
   proc sql noprint
      %if &ETL_DEBUG %then feedback;
   ;
      select
         /* Строковые переменные */
         case type when 2
   %if &mpProc=DATA %then %do;
            then name
   %end;
   %else %do;
            then catt('trimn(ktrim(', name, '))')
   %end;
         /* Числовые переменные */
            else catt('put(', name, ',hex16.)')
         end
      into
         :lmvOut separated by ' '
      from
         &lmvColumnsMetaTable
      where
         upcase(name) in (
            %upcase(
               %list_expand(&mpDigestFields, '{}', mpOutDlm=%str(, ))
            )
         )
      order by upcase(name)
      ;
   quit;
   %let lmvCount = &SQLOBS;

   /* Проверка кол-ва полей */
   %local lmvTargetCount;
   %let lmvTargetCount = %sysfunc(countw(&mpDigestFields, , s));
   %if &lmvCount ne &lmvTargetCount %then %do;
      %job_event_reg (mpEventTypeCode=ILLEGAL_ARGUMENT,
         mpEventValues=%bquote(&mpMethod called for &lmvTargetCount vars, but only &lmvCount were present.) );
      %return;
   %end;

   %local lmvVarList;
   %if %is_blank(mpSalt) %then %do;
      %let lmvVarList=&lmvOut;
   %end;
   %else %do;
      %let lmvVarList="&mpSalt" &lmvOut;
   %end;

   %if &mpProc=DATA %then %do;
      %let &mpOutKey = &mpMethod( catq('dmt','01'x, %util_list(&lmvVarList) ) );
   %end;
   %else %do;
      %let &mpOutKey = &mpMethod( %util_list(&lmvVarList, mpOutDlm=%str(||'01'x||)) );
   %end;

   %if &lmvMethodOrig = BASE64 %then %do;
     %let &mpOutKey = put(&&&mpOutKey,$base64x24.);
   %end;

   %member_drop(&lmvColumnsMetaTable);
%mend util_digest_expr;
