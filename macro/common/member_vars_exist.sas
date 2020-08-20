/*****************************************************************
*  ВЕРСИЯ:
*     $Id: f73144f758d2adab5d66212969c876a1647fe53f $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Возвращает 1, если все указанные переменные есть в наборе, или 0 если нет.
*
*  ПАРАМЕТРЫ:
*     mpData                 +  имя набора
*     mpVars                 +  список имен переменных набора, разделенный пробелами
*
******************************************************************
*  Использует:
*     %is_blank
*     %list_expand
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %if %member_vars_exist (sashelp.class, name age) %then ...
*
******************************************************************
*  08-11-2012   Нестерёнок  Начальное кодирование
*  22-03-2016   Нестерёнок  Добавлена проверка по списку
******************************************************************/

%macro member_vars_exist (
   mpData      ,
   mpVars
);
   %local dsid lmvVarNum lmvVarsExist;
   %let lmvVarsExist = 0;

   /* Проверяем корректность аргументов */
   %if %is_blank(mpData) or %is_blank(mpVars) %then %do;
      %log4sas_error (cwf.macro.member_vars_exist, Arguments are missing: mpData=&mpData mpVars=&mpVars );
      %goto exit;
   %end;

   /* Открываем набор */
   %let dsid = %sysfunc(open(&mpData, I));
   %if &dsid eq 0 %then %do;
      %log4sas_error (cwf.macro.member_vars_exist, Cannot open data set &mpData. );
      %goto exit;
   %end;

   /* Получаем номера переменных */
   %let lmvVarsExist = %eval( %unquote(
       %list_expand(mpWith=&mpVars, mpPattern= (%nrbquote(%)sysfunc(varnum(&dsid, {})) gt 0), mpOutDlm=%str( and ))
   ));
   %let dsid = %sysfunc(close(&dsid));

   /* Выход */
   %exit:

   /* Возвращаем результат */
   %do;&lmvVarsExist%end;
%mend member_vars_exist;
