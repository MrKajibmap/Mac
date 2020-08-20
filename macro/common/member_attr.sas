/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 9f211c98fa84243275d896f833686e14200783f4 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Выдает атрибут набора.
*
*  ПАРАМЕТРЫ:
*     mpData         +  имя набора
*     mpAttr         +  имя атрибута, согласно документации на ATTRC/ATTRN
*
******************************************************************
*  Использует:
*     нет
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %let ds_row_count = %member_attr (mpData=sashelp.class, mpAttr=NLOBS);
*
******************************************************************
* 24-02-2012   Нестерёнок  Начальное кодирование
******************************************************************/

%macro member_attr (mpData=, mpAttr=);
   %local dsid lmvAttrType lmvAttrValue;

   /* Проверяем корректность имени атрибута, заодно определяя его тип */
   %let lmvAttrType = ;
   %let lmvAttrValue = ;
   %if not %is_blank(mpAttr) %then %do;
      %if %index (ALTERPW ANOBS ANY ARAND ARWU AUDIT AUDIT_BEFORE AUDIT_DATA AUDIT_ERROR
                  CRDTE ICONST INDEX ISINDEX ISSUBSET LRECL LRID MAXGEN MAXRC MODTE NDEL
                  NEXTGEN NLOBS NLOBSF NOBS NVARS PW RADIX RANDOM READPW TAPE VAROBS
                  WHSTMT WRITEPW
                  , %upcase(&mpAttr)) %then %let lmvAttrType = N;
      %if %index (CHARSET ENCRYPT ENGINE LABEL LIB MEM MODE MTYPE SORTEDBY SORTLVL SORTSEQ
                  , %upcase(&mpAttr)) %then %let lmvAttrType = C;
   %end;
   %if %is_blank(lmvAttrType) %then %do;
      %log4sas_error (cwf.macro.member_attr, Unknown attribute name &mpAttr.);
      %goto exit;
   %end;

   /* Открываем набор */
   %let dsid = %sysfunc(open(&mpData, I));
   %if &dsid eq 0 %then %do;
      %log4sas_error (cwf.macro.member_attr, Cannot open data set &mpData. );
      %goto exit;
   %end;

   /* Получаем результат */
   %let lmvAttrValue = %sysfunc(attr&lmvAttrType(&dsid, &mpAttr));
   %let dsid = %sysfunc(close(&dsid));

   /* Выход */
   %exit:

   /* Возвращаем результат */
   %do; &lmvAttrValue %end;
%mend member_attr;

