/*****************************************************************
*  ВЕРСИЯ:
*     $Id: b93c98d3219c1a4bf56764f45eb6bc9bb039d3e4 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для трансформации списка значений.
*
*  ПАРАМЕТРЫ:
*     mpWith                  +  входной список значений
*     mpWithDlm               -  разделитель значений во входном списке, по умолчанию пробел
*     mpOutDlm                -  разделитель значений в результате
*                                По умолчанию ", "
*     mpUnique                -  оставить (Y) или нет (N) в результате только уникальные значения
*                                По умолчанию N
*
******************************************************************
*  Использует:
*     %countw
*     %util_crc32
*     %util_sasver_ge
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1. Начиная с 9.4M5, для определения уникальности элементов используется CRC-32, а не MD5 (S1381633).
*
******************************************************************
*  Пример использования:
*    proc sql;
*       select %util_list (var1 var2 var3) from ...
*
****************************************************************************
*  07-03-2012  Нестерёнок     Начальное кодирование
*  08-07-2014  Нестерёнок     Добавлен mpUnique
****************************************************************************/

%macro util_list(
   mpWith,
   mpWithDlm         =  %str( ),
   mpOutDlm          =  %str(, ),
   mpUnique          =  N
);

   %local i count item result itemadd itemvar;

   %let count = %countw(&mpWith, &mpWithDlm);
   %do i = 1 %to &count;
      %let item   = %sysfunc(scan(&mpWith, &i, &mpWithDlm));
      %let itemadd = 1;

      %if &mpUnique = Y %then %do;
         %if %util_sasver_ge (mpMajor=9, mpMinor=4, mpTSLevel=M5) %then %do;
            %let itemvar = v_%util_crc32(&item);
         %end;
         %else %do;
            %let itemvar = v_%sysfunc(md5(&item), $hex30.);
         %end;

         %if %symexist(&itemvar) %then %do;
            %let itemadd = 0;
         %end;
         %else %do;
            %local &itemvar;
         %end;
      %end;

      %if &itemadd %then
         %if not %is_blank(result) %then
            %let result = &result.&mpOutDlm.&item.;
         %else
            %let result = &item.;
   %end;

   %do;&result%end;

%mend util_list;
