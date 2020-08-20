﻿/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 6a1a17b676ee291a1ef48efb9131105e0dac77c5 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Возвращает 1, если набор указанного типа существует, или 0 в противном случае.
*
*  ПАРАМЕТРЫ:
*     mpTarget                +  имя набора или view
*     mpMemberTypes           -  список проверяемых типов, разделенный пробелами
*                                По умолчанию DATA VIEW
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
*     %if %member_exists (sashelp.class) %then %do...
*
******************************************************************
*  28-10-2015  Нестерёнок     Начальное кодирование
******************************************************************/

%macro member_exists (
   mpTarget             ,
   mpMemberTypes        =  DATA VIEW
);
   %local lmvExists;
   %let lmvExists = 0;

   /* Из-за специфики реализации внутренних макро повторяется код из util_loop */
   %local lmvI lmvType;
   %do lmvI = 1 %to 10;
      %let lmvType = %scan(&mpMemberTypes, &lmvI, %str( ) ) ;
      %if %is_blank(lmvType) %then %goto results;

      %if %sysfunc(exist(&mpTarget, &lmvType)) %then %do;
         %let lmvExists = 1;
         %goto results;
      %end;
   %end;

%results:
   %do;&lmvExists%end;
%mend member_exists;