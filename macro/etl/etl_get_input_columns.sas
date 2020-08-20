/*****************************************************************
*  ВЕРСИЯ:
*     $Id: a4465d303cfd86de4fb2c5593b7f09b4f2158444 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Получает список используемых колонок из input таблицы.
*
*  ПАРАМЕТРЫ:
*     mpTableMacroName        +  имя макропеременной для выходного набора
*     mpOutKey                +  имя выходной макропеременной
*     mpDrop                  -  список полей, которые не нужно включать в результат
*     mpDlm                   -  разделитель значений результата
*                                По умолчанию пробел
*
******************************************************************
*  ПРИМЕР ИСПОЛЬЗОВАНИЯ:
*     %etl_get_input_columns (mpTableMacroName=_OUTPUT, mpOutKey=mvOut);
*
******************************************************************
*  20-08-2012  Кузенков       Начальное кодирование
*  30-08-2012  Нестерёнок     Счет полей идет с 0, используемые поля имеют вид table_colX_inputY
*  07-09-2012  Нестерёнок     Добавлен mpDrop
*  04-04-2014  Нестерёнок     Добавлен mpDlm
******************************************************************/

%macro etl_get_input_columns(mpTableMacroName=, mpOutKey=, mpDrop=, mpDlm=%str( ));
   %local lmvColCount lmvInputCount lmvI lmvJ;
   %let &mpOutKey=;

   %let mpTableMacroName = %upcase(&mpTableMacroName);
   %let lmvColCount = &&&mpTableMacroName._col_count;

   proc sql noprint;
      select distinct value into :&mpOutKey separated by "&mpDlm" from dictionary.macros
      where name in (
         ""
         %do lmvI=0 %to %eval(&lmvColCount - 1);
            %let lmvInputCount = &&&mpTableMacroName._col&lmvI._input_count;
            %do lmvJ=0 %to %eval(&lmvInputCount - 1);
               "&mpTableMacroName._COL&lmvI._INPUT&lmvJ."
            %end;
         %end;
      )
      %if not %is_blank(mpDrop) %then %do;
         and upcase(value) not in (
            %upcase(
               %list_expand(&mpDrop, "{}", mpOutDlm=%str(, ))
            )
         )
      %end;
      ;
   quit;
%mend etl_get_input_columns;