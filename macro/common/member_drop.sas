/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 675ac42b165b538cc84bffb6e63059c523974ed0 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Удаляет набор или view.
*     Таблицы СУБД пока не поддерживаются (TODO).
*
*  ПАРАМЕТРЫ:
*     mpTarget       +  имя набора или view
*     mpOnlyEmpty    -  удалять ли набор, если он наполнен (N), или только если пуст (Y)
*                       По умолчанию N
*
******************************************************************
*  Использует:
*     member_names
*     member_obs
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %member_drop (sashelp.class);
*
******************************************************************
*  20-02-2012  Нестерёнок     Начальное кодирование
*  04-06-2013  Нестерёнок     Добавлен mpOnlyEmpty
*  09-02-2015  Сазонов        Добавлен upcase для db2
******************************************************************/

%macro member_drop (mpTarget, mpOnlyEmpty=N);
   /* check if delete needed */
   %if %is_blank(mpTarget) %then %return;
   %if not %sysfunc(exist(&mpTarget, DATA)) and not %sysfunc(exist(&mpTarget, VIEW)) %then %return;

   %if &mpOnlyEmpty = Y %then %do;
      %if %member_obs(mpData=&mpTarget) ne 0 %then %return;
   %end;

   /* do delete */
   %local lmvLibref lmvMemberName;
   %member_names (mpTable=&mpTarget, mpLibrefNameKey=lmvLibref, mpMemberNameKey=lmvMemberName);

   proc datasets lib = &lmvLibref nolist nowarn memtype = (data view);
%if &ETL_DBMS = db2 %then %do;
	 delete %upcase(&lmvMemberName);
%end;
%else %do;
	 delete &lmvMemberName;
%end;
   quit;
%mend member_drop;