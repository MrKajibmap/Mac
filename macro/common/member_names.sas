/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 6c10f89d6b66e93cf18b0807c85869b4e98aaf0c $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Разделяет двух- или одноуровневое имя набора на libref и имя набора.
*     В случае одноуровневого имени в libref возвращается user или work,
*     в зависимости от настроек системы.
*
*  ПАРАМЕТРЫ:
*     mpTable              +  имя набора для разделения
*     mpLibrefNameKey      -  имя макропеременной, в которую возвращается libref
*     mpMemberNameKey      -  имя макропеременной, в которую возвращается имя набора
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
*     %local lmvLibref lmvMemberName;
*     %member_names (mpTable=sashelp.class, mpLibrefNameKey=lmvLibref, mpMemberNameKey=lmvMemberName);
*
******************************************************************
*  09-02-2012  Нестерёнок     Начальное кодирование
******************************************************************/

%macro member_names (mpTable=, mpLibrefNameKey=, mpMemberNameKey=);
   /* Если переданы пустые имена, то результат сохраняется в локальных переменных */
   %if %is_blank(mpLibrefNameKey) %then %do;
      %local lmvLibrefNameKey;
      %let mpLibrefNameKey = lmvLibrefNameKey;
   %end;
   %if %is_blank(mpMemberNameKey) %then %do;
      %local lmvMemberNameKey;
      %let mpMemberNameKey = lmvMemberNameKey;
   %end;

   /* get target libref */
   %if %sysfunc (indexc (&mpTable, %str(.))) gt 0 %then %do;
      /* double-name */
      %let &mpLibrefNameKey = %scan (&mpTable, 1, %str(.));
      %let &mpMemberNameKey = %scan (&mpTable, 2, %str(.));
   %end;
   %else %do;
      /* single-name */
      %if %sysfunc(libref(USER)) eq 0 %then
         %let &mpLibrefNameKey = user;
      %else
         %let &mpLibrefNameKey = work;
      %let &mpMemberNameKey = &mpTable;
   %end;
%mend member_names;
