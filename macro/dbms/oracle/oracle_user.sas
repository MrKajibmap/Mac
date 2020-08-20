/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 3ba9d87f53428d7eec33000332889192aa926725 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Получает Oracle-специфичную информацию о текущем пользователе и схеме.
*     Может вызываться в глобальном режиме или внутри proc sql.
*
*  ПАРАМЕТРЫ:
*     mpLoginSet              -  Имя набора параметров подключения к БД
*     mpConnection            -  Если указано, использовать это подключение вместо установки нового (orauser)
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
*     %global orauser;
*     %oracle_user (mpLoginSet=ETL_SYS, mpOutUserKey=orauser);
*     %put &=orauser;
*
*     proc sql;
*        ... создано соединение conn
*        %oracle_user (mpConnection=conn, mpOutUserKey=orauser);
*
******************************************************************
*  13-08-2019  Нестерёнок     Начальное кодирование
******************************************************************/

%macro oracle_user (
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
         %let mpConnection = orauser;
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
            sys_context('USERENV', 'SESSION_USER') as "current_user",
            sys_context('USERENV', 'CURRENT_SCHEMA') as "current_schema"
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
%mend oracle_user;
