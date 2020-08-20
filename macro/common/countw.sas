/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 216d0cc88c100bcdec38a32e0be010a635423575 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для замены функции COUNTW с учетом MBCS.
*
*  ПАРАМЕТРЫ:
*     mpString                -  входной список значений
*     mpDlm                   -  разделитель значений
*                                По умолчанию совпадает с дефолтными значениями ф-й COUNTW (NODBCS) или KSCAN(DBCS)
*     mpMaxWords              -  макс. кол-во слов (имеет значение только для DBCS)
*                                По умолчанию 1000
*
******************************************************************
*  Использует:
*     %is_blank
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %let count = %countw();
*     %let count = %countw(&myVar);
*     %let count = %countw(&myVar, %str(,));
*     %let count = %countw(каждый+охотник+желает+знать, mpMaxWords=2000);
*
****************************************************************************
*  01-04-2016  Нестерёнок     Начальное кодирование
****************************************************************************/

%macro countw(
   mpString,
   mpDlm,
   mpMaxWords     =  1000
);
   /* Проверка аргументов */
   %if %is_blank(mpString) %then %do;
      %do;0%end;
      %return;
   %end;

   %if not &ETL_DBCS %then %do;
      %if %is_blank(mpDlm) %then %do;%sysfunc(countw(&mpString))%end;
      %else %do;%sysfunc(countw(&mpString, &mpDlm))%end;
      %return;
   %end;
   %else %do;
      /* as of 9.4m3, there's only KSCAN function to get DBCS words */
      %local i item result;
      %let result = 0;
      %do i = 1 %to &mpMaxWords;
         %if %is_blank(mpDlm) %then %do;
            %let item   =  %sysfunc(kscan(&mpString, &i, %str( )));
         %end;
         %else %do;
            %let item   =  %sysfunc(kscan(&mpString, &i, &mpDlm));
         %end;
         %if %is_blank(item) %then %goto mLeave;
         %let result = &i;
      %end;
   %end;
%mLeave:
   %do;&result%end;
   %goto mExit;
%mExit:
%mend countw;
