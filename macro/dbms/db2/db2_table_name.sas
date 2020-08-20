/*****************************************************************
*  ВЕРСИЯ:
*     $Id: db902e744334d65800ab3d9ae7f17f1c91ed447d $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Получает DB2-специфичную информацию об именовании таблицы
*     (схема, наименование, имя подключения) по имени в SAS ([LIBNAME.]TABNAME).
*     Может вызываться в глобальном режиме или внутри proc sql.
*
*  ПАРАМЕТРЫ:
*     mpSASTable              +  имя таблицы в SAS
*     mpOutFullNameKey        -  имя выходной макропеременной, в которую будет помещено полное имя таблицы в DB2 в формате SCHEMA.TABNAME
*     mpOutSchemaKey          -  имя выходной макропеременной, в которую будет помещено имя схемы в DB2
*     mpOutNameKey            -  имя выходной макропеременной, в которую будет помещено имя таблицы в DB2
*     mpOutLoginSetKey        -  имя выходной макропеременной, в которую будет помещено имя подключения к DB2
*
******************************************************************
*  Пример использования:
*     %db2_table_name(mpSASTable=&mpOutJrnl,  mpOutFullNameKey=mvJrnlDB2);
*     proc sql;
*        ...
*        execute by db2 (
*           insert into &mvJrnlDB2 jrnl (...
*
******************************************************************
*  31-07-2012  Кузенков       Начальное кодирование
*  02-10-2013  Нестерёнок     Добавлен mpOutLoginSetKey
*  10-10-2013  Нестерёнок     Добавлены mpOutSchemaKey, mpOutNameKey
******************************************************************/

%macro db2_table_name(mpSASTable=, mpOutFullNameKey=, mpOutSchemaKey=, mpOutNameKey=, mpOutLoginSetKey=);
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
%mend db2_table_name;
