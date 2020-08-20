/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 61cf9894992c0418f06be7dd61b39b5864a743a8 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для работы с динамическим списком колонок.
*     Формирует динамический код на основе списка колонок.
*
*  ПАРАМЕТРЫ:
*     mpWith               +  список элементов (элемент - либо имя колонки либо ссылка на файл с именами колонок
*     mpWithDlm            -  разделитель значений mpWith
*                             по умолчанию пробел
*     mpPattern            +  шаблон для генерации кода.  В шаблоне заменяются:
*                             {} на имена колонок
*                             {#} на порядковые номера колонок, начиная с 1
*     mpOutDlm             -  символ-разделитель, по умолчанию ", " внутри SQL и пробел вне SQL
*
******************************************************************
*  Использует:
*     нет
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1. В отличие от list_expand, апкейсит переменные списка.
*     2. В отличие от list_expand, не допускает значения со спец. символами.
*     3. В отличие от list_expand, mpOutDlm зависит от контекста исполнения.
*
******************************************************************
*  Пример использования:
*     %global lmvSaveVars;
*     %member_vars_get(SASHELP.CLASS,  lmvSaveVars, mpDrop=age height);
*     DATA WORK.CLASS;
*        SET SASHELP.CLASS;
*        KEEP
*           %member_vars_expand(&lmvSaveVars age, {})
*        ;
*     RUN;
*     %member_vars_clean(&lmvSaveVars);
*
******************************************************************
*  22-08-2012  Кузенков    Начальное кодирование
*  11-07-2014  Нестерёнок  В шаблоне {#} заменяются на порядковые номера колонок
*  19-10-2016  Сазонов     Добавлен mpWithDlm
*  25-04-2016  Нестерёнок  Разделитель назван mpOutDlm и зависит от контекста
******************************************************************/

%MACRO member_vars_expand (
   mpWith,
   mpPattern,
   mpOutDlm       =  ,
   mpWithDlm      =  %str( )
);
   %if %is_blank(mpOutDlm) %then %do;
      %if &SYSPROCNAME eq SQL %then
         %let mpOutDlm = %str(, );
      %else
         %let mpOutDlm = %str( );
   %end;

   %LOCAL mvI mvItem mvFID mvRC mvCol mvColNo mvOut mvDlm;
   %LET mvItem = %SCAN(&mpWith, 1, &mpWithDlm);
   %LET mvI = 1;
   %LET mvColNo = 1;
   %DO %WHILE(not %is_blank(mvItem));
      %IF %SUBSTR(&mvItem,1,1)=# %THEN %DO;
         %LET mvFID = %SYSFUNC(FOpen(&mvItem));
         %IF &mvFID>0 %THEN %DO;
            %DO %WHILE(%SYSFUNC(FRead(&mvFID)) = 0);
               %LET mvRC   = %SYSFUNC(FGet(&mvFID,mvCol,32));
               %LET mvOut = %SYSFUNC(PrxChange(s/\{\}/%UPCASE(&mvCol)/,-1,&mpPattern));
               %LET mvOut = %SYSFUNC(PrxChange(s/\{#\}/&mvColNo/,-1,&mvOut));
%do;&mvDlm&mvOut%end;
               %LET mvDlm = &mpOutDlm;
               %LET mvColNo = %EVAL(&mvColNo + 1);
            %END;
            %LET mvFID = %SYSFUNC(FClose(&mvFID));
         %END;
      %END; %ELSE %DO;
         %LET mvOut = %SYSFUNC(PrxChange(s/\{\}/%UPCASE(&mvItem)/,-1,&mpPattern));
         %LET mvOut = %SYSFUNC(PrxChange(s/\{#\}/&mvColNo/,-1,&mvOut));
%do;&mvDlm&mvOut%end;
         %LET mvDlm = &mpOutDlm;
         %LET mvColNo = %EVAL(&mvColNo + 1);
      %END;

      %LET mvI = %EVAL(&mvI + 1);
      %LET mvItem = %SCAN(&mpWith, &mvI, &mpWithDlm);
   %END;
%MEND member_vars_expand;
