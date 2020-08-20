/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 6b970276c0c21a5f33be83cf28a1e2a371b2065a $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для исполнения других макросов в цикле.
*
*  ПАРАМЕТРЫ:
*     mpLoopMacro             +  имя макроса итерации (должен иметь ровно 1 позиционный параметр)
*     mpWith                  -  список значений цикла
*     mpWithDlm               -  разделитель значений mpWith
*                                по умолчанию пробел
*     mpOutDlm                -  разделитель значений результата
*                                по умолчанию пробел
*     mpMacroName             D  имя макроса для исполнения
******************************************************************
*  Использует:
*     %is_blank
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1. Если mpWith пуст, то макрос ничего не делает.
*
******************************************************************
*  Пример использования:
*     %macro inner (par1);
*        %put &par1;
*     %mend inner;
*     %util_loop (mpLoopMacro=inner, mpWith=AAA BBB CCC);
*
******************************************************************
*  19-12-2011  Нестерёнок     Начальное кодирование
*  16-03-2012  Нестерёнок     Добавлен mpOutDlm
*  06-02-2017  Нестерёнок     Deprecated: mpMacroName, в пользу mpLoopMacro
*  06-04-2018  Нестерёнок     mpWith необязателен
******************************************************************/

%macro util_loop(
   mpLoopMacro          =  ,
   mpWith               =  ,
   mpWithDlm            =  %str( ),
   mpOutDlm             =  %str( ),
   /* DEPRECATED */
   mpMacroName          =
);
   %if %is_blank(mpLoopMacro) and not %is_blank(mpMacroName) %then %do;
      %let mpLoopMacro = &mpMacroName;
   %end;
   %if %is_blank(mpLoopMacro) %then %goto mError;
   %if %is_blank(mpWith) %then %return;

   %local lmvI lmvJ lmvDlm;
   %do lmvI = 1 %to 32000;
      %let lmvJ = %scan( &mpWith, &lmvI, &mpWithDlm ) ;
      %if %is_blank(lmvJ) %then %goto mExit;

      %do;&lmvDlm%&mpLoopMacro.( &lmvJ )%end;
      %let lmvDlm = &mpOutDlm;
   %end;
   %goto mExit;

%mError:
   %log4sas_error (cwf.macro.util_loop, Не указано значение mpLoopMacro);
%mExit:
%mend util_loop;
