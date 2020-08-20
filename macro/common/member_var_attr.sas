/*****************************************************************
* ВЕРСИЯ:
*   $Id: d14f2001ba14dfee6b131da2005f993a12a8f21c $
*
******************************************************************
* НАЗНАЧЕНИЕ:
*   Выдает атрибут переменной набора.
*
* ПАРАМЕТРЫ:
*   mpData                 +  имя набора
*   mpVar                  +  имя переменной набора
*   mpAttr                 +  имя атрибута (VARFMT, VARINFMT, VARLABEL, VARLEN, VARNUM, VARTYPE)
*
******************************************************************
* Использует:
*     нет
*
* Устанавливает макропеременные:
*     нет
*
******************************************************************
* Пример использования:
*   %let name_len = %member_var_attr (mpData=sashelp.class, mpVar=name, mpAttr=VARLEN);
*
******************************************************************
* 19-10-2012   Нестерёнок  Начальное кодирование
******************************************************************/

%macro member_var_attr (mpData=, mpVar=, mpAttr=);
   %local dsid lmvVarNum lmvAttrValue;
   %let lmvAttrValue = ;

   /* Проверяем корректность имени атрибута */
   %if %is_blank(mpAttr) or
       %index (VARFMT VARINFMT VARLABEL VARLEN VARNUM VARTYPE
               , %upcase(&mpAttr)) = 0
   %then %do;
      %log4sas_error (cwf.macro.member_var_attr, Unknown attribute name &mpAttr.);
      %goto exit;
   %end;

   /* Открываем набор */
   %let dsid = %sysfunc(open(&mpData, I));
   %if &dsid eq 0 %then %do;
      %log4sas_error (cwf.macro.member_var_attr, Cannot open data set &mpData. );
      %goto exit;
   %end;

   /* Получаем номер переменной */
   %let lmvVarNum = %sysfunc(varnum(&dsid, &mpVar));
   %if &lmvVarNum eq 0 %then %do;
      %log4sas_error (cwf.macro.member_var_attr, Variable &mpVar is not found in data set &mpData. );
      %goto exit;
   %end;

   /* Получаем результат */
   %let lmvAttrValue = %sysfunc(&mpAttr(&dsid, &lmvVarNum));
   %let dsid = %sysfunc(close(&dsid));

   /* Выход */
   %exit:

   /* Возвращаем результат */
   %do; &lmvAttrValue %end;
%mend member_var_attr;

