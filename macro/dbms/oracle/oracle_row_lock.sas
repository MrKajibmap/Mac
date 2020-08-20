/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 7aa6707fd2fa9415c8b316e062fc03e2e3edf496 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Блокирует (делает лок) строку в таблице &mpTable
*     Для снятия блокировки(лока) необходимо выполнить: libname &mpLibname. clear;
*
*  ПАРАМЕТРЫ:
*     mpLoginSet        +   Имя набора параметров подключения к БД
*     mpLibname         +   Имя создаваемой библиотеки
*     mpTable           +   Таблица для лока
*     mpWhere           +   Условие для лока (для оператора sql where)
*     mpLockSec         +   Значение (в сек.) таймаута для проверки таблицы, 0 - опция nowait, -1 - skip locked
*     mpOut             -   Таблица для вывода
*     mpOutResultVar    -   mv для установки статуса. 1 - блокировка установлена, 0 - нет
*
******************************************************************
*  Использует:
*       %error_check
*       %is_blank
*
*  Устанавливает макропеременные:.
*       mpOutResultVar
*
*  Ограничения:
*
******************************************************************
*  Пример использования:
*       %oracle_row_lock(
*           mpLoginSet=&mpLoginSet,
*           mpLibname=REGC,
*           mpSchema=REGC,
*           mpTable=REG_RDM_TO_RWA_TABLE,
*           mpWhere=RRTRT_SNAP_ID eq &mpSnapId,
*           mpLockSec=60,
*           mpOutResultVar=lmvLockResult);
*
*       <... обработка ...>
*
*       libname REGC clear;
*
******************************************************************
*  20-09-2016  Колосов     Начальное кодирование
*  27-09-2016  Кузнеченков Добавлены mpOut, исправлен функционал
******************************************************************/
%macro oracle_row_lock(
   mpLoginSet   =  , /* Коннект */
   mpLibname    =  , /* Имя создаваемой библиотеки */
   mpSchema     =  , /* Схема блокируемой таблицы */
   mpTable      =  , /* Блокируемая таблица */
   mpWhere      =  , /* Условие для лока */
   mpLockSec    =  , /* Таймаут */
   mpOut        =  , /* Таблица для вывода */
   mpOutResultVar  =
);

    %if %symexist(&mpOutResultVar) %then %let &mpOutResultVar=0;

    %if (not %symexist(&mpLoginSet._CONNECT_OPTIONS)) %then
    %do;
      %log4sas_error (dwf.macro.oracle_row_lock, Login credentials for set &mpLoginSet are not defined);
      %return;
    %end;

    %if %is_blank(mpSchema) %then %let mpSchema=&&&mpLoginSet._CONNECT_SCHEMA;

    %if %is_blank(mpWhere) %then
    %do;
      %log4sas_error (dwf.macro.oracle_row_lock, Where condition is not defined);
      %return;
    %end;

    %local lmvLockStmt;
    %if &mpLockSec=0 %then
        %let lmvLockStmt = nowait ;
    %else
    %if &mpLockSec>0 %then
        %let lmvLockStmt = wait &mpLockSec ;
    %else
    %if &mpLockSec=-1 %then
        %let lmvLockStmt = skip locked ;

    libname &mpLibname. oracle &&&mpLoginSet._CONNECT_OPTIONS
      dbconinit="
              begin execute immediate '
                  select *
                  from &mpSchema..&mpTable.
                  where %unquote(&mpWhere)
                  for update &lmvLockStmt ';
              end;"
      dbconterm="";
    %error_check;

    proc sql noprint feedback;

        %if ^%is_blank(mpOut) %then
          %do;
            create table &mpOut as
          %end;
            select * from &mpLibname..&mpTable.
                where %unquote(&mpWhere);

        %error_check (mpStepType=SQL);

    quit;
    %error_check (mpStepType=SQL);

    /* ORA-30006 */
    /* если не было ошибок - результат 1*/
    %if &STEP_RC eq 0 %then %let &mpOutResultVar=1;

%mend oracle_row_lock;
