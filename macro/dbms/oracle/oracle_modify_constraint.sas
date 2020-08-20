/*****************************************************************
*  ВЕРСИЯ:
*     $Id: a4419441ead1563e97837615a0d7b39ae215323d $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Изменяет constraint в таблице.
*
*  ПАРАМЕТРЫ:
*     mpTable                 +  имя таблицы в SAS, в которую добавляется партиция
*     mpConstrNm              +  имя constraint'а
*     mpAction                +  enable/disable/rename/drop и т.д.
*
******************************************************************
*  Использует:
*     %error_check
*     %oracle_connect
*     %oracle_table_name
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %oracle_modify_constraint (mpTable=ETL_STG.ACCNTAB, mpConstrNm=PK_ACCNTAB, mpAction=enable);
*
******************************************************************
*  04-07-2017  Могилёв     Начальное кодирование
******************************************************************/

%macro oracle_modify_constraint (mpTable=, mpConstrNm=, mpAction=, mpLoginSet=);
    %local lmvTable lmvLoginSet;
    %if %is_blank(mpLoginSet) %then %do;
        %oracle_table_name (mpSASTable=&mpTable,  mpOutFullNameKey=lmvTable,  mpOutLoginSetKey=lmvLoginSet);
    %end;
    %else %do;
        %let lmvTable=&mpTable;
        %let lmvLoginSet=&mpLoginSet;
    %end;

    %local lmvIsNotSQL;
    %let lmvIsNotSQL   = %eval (&SYSPROCNAME ne SQL);

    /* Открываем proc sql, если он еще не открыт */
    %if &lmvIsNotSQL %then %do;
        proc sql;
            %oracle_connect (mpLoginSet=&lmvLoginSet);
    %end;
            execute (
                alter table &lmvTable &mpAction constraint &mpConstrNm
            ) by oracle;
    %if &lmvIsNotSQL %then %do;
            %error_check (mpStepType=SQL_PASS_THROUGH);
            disconnect from oracle;
        quit;
    %end;
%mend oracle_modify_constraint;
