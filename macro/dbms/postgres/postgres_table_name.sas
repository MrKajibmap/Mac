/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 5def5fdaaa6d8f4899dcdc3d61a1d4f361f3fb38 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Получает Postgres-специфичную информацию об именовании таблицы
*     (схема, наименование, имя подключения) по имени в SAS ([LIBNAME.]TABNAME).
*     Может вызываться в глобальном режиме или внутри proc sql.
*
*  ПАРАМЕТРЫ:
*     mpSASTable              +  имя таблицы в SAS
*     mpOutFullNameKey        -  имя выходной макропеременной, в которую будет помещено полное имя таблицы в Postgres в формате SCHEMA.TABNAME
*     mpOutSchemaKey          -  имя выходной макропеременной, в которую будет помещено имя схемы в Postgres
*     mpOutNameKey            -  имя выходной макропеременной, в которую будет помещено имя таблицы в Postgres
*     mpOutLoginSetKey        -  имя выходной макропеременной, в которую будет помещено имя подключения к Postgres
*
******************************************************************
*  Пример использования:
*     %postgres_table_name(mpSASTable=&mpOutJrnl,  mpOutFullNameKey=mvJrnlOra);
*     proc sql;
*        ...
*        execute by postgres (
*           insert into &mvJrnlOra jrnl (...
*
******************************************************************
*  31-07-2012  Кузенков       Начальное кодирование
*  02-10-2013  Нестерёнок     Добавлен mpOutLoginSetKey
*  10-10-2013  Нестерёнок     Добавлены mpOutSchemaKey, mpOutNameKey
******************************************************************/

%macro postgres_table_name(mpSASTable=, mpOutFullNameKey=, mpOutSchemaKey=, mpOutNameKey=, mpOutLoginSetKey=);
   %local lmvSchema lmvLibref lmvMemberName;
   %member_names (mpTable=&mpSASTable, mpLibrefNameKey=lmvLibref, mpMemberNameKey=lmvMemberName);

   /* Открываем proc sql, если он еще не открыт */
   %local lmvIsNotSQL;
   %let lmvIsNotSQL = %eval (&SYSPROCNAME ne SQL);
   %if &lmvIsNotSQL %then %do;
      proc sql noprint;
   %end;
   %else %do;
         reset noprint;
   %end;
         select sysvalue into :lmvSchema from dictionary.libnames
         where libname="%upcase(&lmvLibref)"
         ;
         select memname into :lmvMemberName from dictionary.tables
         where libname="%upcase(&lmvLibref)" and upcase(memname)=%upcase("&lmvMemberName")
         ;

   %if &lmvIsNotSQL %then %do;
      quit;
   %end;

   %let lmvSchema    = &lmvSchema;
   %if not %is_blank(mpOutFullNameKey) %then
      %let &mpOutFullNameKey  = &lmvSchema..&lmvMemberName;;
   %if not %is_blank(mpOutSchemaKey) %then
      %let &mpOutSchemaKey    = &lmvSchema;;
   %if not %is_blank(mpOutNameKey) %then
      %let &mpOutNameKey      = &lmvMemberName;;
   %if not %is_blank(mpOutLoginSetKey) %then
      %let &mpOutLoginSetKey  = &lmvLibref;;
%mend postgres_table_name;
