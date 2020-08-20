/*****************************************************************
*  ВЕРСИЯ:
*     $Id: d04e896beb9794a96f74cd501759651fe7b21d65 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Выдает список переменных набора.
*
*  ПАРАМЕТРЫ:
*     mpIn                    +  имя входного набора
*     mpType                  -  фильтр по типу переменных, по умолчанию все
*                                N - только числовые
*                                C - только символьные
*     mpDlm                   -  символ-разделитель, по умолчанию пробел
*     mpKeep                  -  список полей, которые нужно включать в результат
*     mpDrop                  -  список полей, которые не нужно включать в результат
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
*     %let dsvars = %member_vars (sashelp.class);
*
******************************************************************
*  09-02-2012  Нестерёнок     Начальное кодирование
*  08-07-2014  Нестерёнок     Добавлен mpKeep
******************************************************************/

%macro member_vars (mpIn, mpType=, mpDlm=%str( ), mpKeep=, mpDrop=);
   %local i dsid nvars varlst varname vartype varadd;

   %let varlst = ;
   %let dsid = %sysfunc(open(&mpIn, i));
   %if &dsid eq 0 %then %do;
      %log4sas_error (cwf.macro.member_vars, Не удалось открыть таблицу &mpIn. на чтение);
      %return;
   %end;

   %* Convert parameter to uppercase. ;
   %let mpType = %upcase(&mpType);

   %* Retrieve the variable names from the dataset. ;
   %let nvars = %sysfunc(attrn(&dsid,nvars));
   %do i=1 %to &nvars;
      %let varname = %sysfunc(varname(&dsid,&i));
      %let varadd = 1;

      /* исключаем, если не тот тип */
      %let vartype = %sysfunc(vartype(&dsid,&i));
      %if (&mpType eq N or &mpType eq C) and &mpType ne &vartype %then
         %let varadd = 0;

      /* исключаем, если не указан в списке mpKeep */
      %if not %is_blank(mpKeep) %then %do;
         %if %sysfunc(indexw(%upcase(&mpKeep), %upcase(&varname))) = 0 %then %let varadd = 0;
      %end;

      /* исключаем, если указан в списке mpDrop */
      %if not %is_blank(mpDrop) %then %do;
         %if %sysfunc(indexw(%upcase(&mpDrop), %upcase(&varname))) %then %let varadd = 0;
      %end;

      /* если не исключен, добавляем к результату */
      %if &varadd %then
         %if not %is_blank(varlst) %then
            %let varlst = &varlst.&mpDlm.&varname.;
         %else
            %let varlst = &varname.;
   %end;

   %* Set "exit"-label. ;
   %exit:
   %let dsid = %sysfunc(close(&dsid));

   %* Return the variable list. ;
   %do; &varlst %end;
%mend member_vars;



