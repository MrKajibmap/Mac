/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 073cae1405ebcb720df6c89fbe73a899548c0331 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Получает DB2-специфичную информацию о текущем пользователе и схеме.
*     Может вызываться в глобальном режиме или внутри proc sql.
*
*  ПАРАМЕТРЫ:
*     mpLoginSet              -  Имя набора параметров подключения к БД
*     mpConnection            -  Если указано, использовать это подключение вместо установки нового (db2user)
*     mpOutUserKey            -  имя макропеременной, в которую возвращается имя пользователя
*     mpOutSchemaKey          -  имя макропеременной, в которую возвращается наименование схемы
*
******************************************************************
*  Использует:
*     %ETL_DBMS_*
*
*  Устанавливает макропеременные:
*     mpOutUserKey
*     mpOutSchemaKey
*
******************************************************************
*  Пример использования:
*     %global db2user;
*     %db2_user (mpLoginSet=ETL_SYS, mpOutUserKey=db2user);
*     %put &=db2user;
*
*     proc sql;
*        ... создано соединение conn
*        %db2_user (mpConnection=conn, mpOutUserKey=db2user);
*
******************************************************************
*  13-08-2019  Нестерёнок     Начальное кодирование
******************************************************************/

%macro db2_user (
   mpLoginSet     =  ,
   mpConnection   =  ,
   mpOutUserKey   =  ,
   mpOutSchemaKey =
);
   /* Открываем proc sql, если он еще не открыт */
   %local lmvIsNotSQL;
   %let lmvIsNotSQL = %eval (&SYSPROCNAME ne SQL);
   %if &lmvIsNotSQL %then %do;
      proc sql noprint;
   %end;
   %else %do;
         reset noprint;
   %end;

      /* Устанавливаем соединение, если требуется */
      %local lmvNotConnected;
      %let lmvNotConnected = %eval (&lmvIsNotSQL or %is_blank(mpConnection));
      %if &lmvNotConnected %then %do;
         %let mpConnection = db2user;
         %&ETL_DBMS._connect(mpLoginSet=&mpLoginSet, mpAlias=&mpConnection);
      %end;

      /* Получаем имена из СУБД */
      %local lmvUser lmvSchema;
      select
         current_user,
         current_schema
      into
         :lmvUser trimmed,
         :lmvSchema trimmed
      from connection to &mpConnection (
         select
            current user as "current_user",
            current schema as "current_schema"
         from dual
      );
      %error_check (mpStepType=SQL_PASS_THROUGH);

      /* Возвращаем значения */
      %if not %is_blank(mpOutUserKey) %then
         %let &mpOutUserKey   = &lmvUser;
      %if not %is_blank(mpOutSchemaKey) %then
         %let &mpOutSchemaKey = &lmvSchema;

      /* Закрываем новое соединение */
      %if &lmvNotConnected %then %do;
         disconnect from &mpConnection;
      %end;
   %if &lmvIsNotSQL %then %do;
      quit;
   %end;
%mend db2_user;
