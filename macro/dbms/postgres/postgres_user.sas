/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 98c85a5ef8ed988a8efa30b4513621dc6ed7b820 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Получает Postgres-специфичную информацию о текущем пользователе и схеме.
*     Может вызываться в глобальном режиме или внутри proc sql.
*
*  ПАРАМЕТРЫ:
*     mpLoginSet              -  Имя набора параметров подключения к БД
*     mpConnection            -  Если указано, использовать это подключение вместо установки нового (pguser)
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
*     %global pguser;
*     %postgres_user (mpLoginSet=ETL_SYS, mpOutUserKey=pguser);
*     %put &=pguser;
*
*     proc sql;
*        ... создано соединение conn
*        %postgres_user (mpConnection=conn, mpOutUserKey=pguser);
*
******************************************************************
*  13-08-2019  Нестерёнок     Начальное кодирование
******************************************************************/

%macro postgres_user (
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
         %let mpConnection = pguser;
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
            current_user,
            current_schema
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
%mend postgres_user;
