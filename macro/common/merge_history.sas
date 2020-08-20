/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 94a0cbd5102769a33e972fe9f9c9273879ca5d1e $
*
*************************************************************************
*  DESCRIPTION:
*     Макрос объединения истории из нескольких таблиц
*     Подробности см. в документе "Спецификация макроса объединения истории.doc"
*
*  PARAMS:
*     TABLE_LIST - (REQ)         - Список таблиц участвующих в объединении разделенных знаком пробела
*                                            (более одной). Название таблицы указывается в нотации libref.table_name
*     TARGET_TABLE - (REQ)       - Название таблицы, в которую должна записаться объединенная история.
*                                            Название таблицы указывается в нотации libref.table_name
*     KEY_COLUMN - (REQ)         - Список колонок обеспечивающий идентификацию сущности
*     VALID_FROM_COLUMN (REQ) - Название колонки задающей начало периода в истории изменения атрибутов
*     VALID_TO_COLUMN - (REQ) - Название колонки задающей конец периода в истории изменения атрибутов
*     MAIN_TABLE - (по умолчанию NO) - для значения YES усекаются интервалы не входящие в главную таблицу.
*     Главной считается первая таблица в списке.
*     OPTION_OUT - опции datasets накладываемые на выходной набор
*  EXTERNAL MACRO USED:
*     member_vars_exist
*     drop_member
*
*  SAMPLE OF USAGE:
*     %merge_history (
*        TABLE_LIST = WRK.LOANS WRK.LOAN_RATES,
*        TARGET_TABLE = WRK.LOAN_HISTORY,
*        KEY_COLUMN     = ACCOUNT_RK,
*        VALID_FROM_COLUMN = VALID_FROM,
*        VALID_TO_COLUMN   = VALID_TO,
*        MAIN_NABLE=YES
*     );
*
*************************************************************************
*  INPUT TABLES: &TABLE_LIST
*  OUTPUT TABLES: &TARGET_TABLE
**************************************************************************
* June-2007, Vasiliev  - Initial coding
* Sep-2007, Khilkevich - исправлен алгоритм генерации минимальнвх интервалов из дат
* Okt-2007, Khilkevich - переписан алгоритм, удалено proc sql
* Jun-2008, Khilkevich - добавлено понятие главной таблицы
* Jul-2008, Khilkevich - изменен порядок сортировкм с целью оптимизации
* Jul-2008, Khilkevich - option_out
* Jul-2008, Khilkevich - ошибка убрана
* 04-AUG_2012, Khilkevich - изменен алгоритм сортировки отобранных дат,
теперь в случае пересечения интервалов в таблице возникает ошибка.
* 29-NOV-2013, Morozov - добавлен proc sort для пункта 4.1.
**************************************************************************/
%macro merge_history (
   TABLE_LIST=,
   TARGET_TABLE=,
   KEY_COLUMN=,
   VALID_FROM_COLUMN=,
   VALID_TO_COLUMN=,
   MAIN_TABLE=NO,
   OPTION_OUT=
);

/* Kuzenkov: Disable compression */
%LOCAL mvComress;
%LET mvCompress=%SYSFUNC(GetOption(COMPRESS,KEYWORD));
OPTION COMPRESS=NO;


%LOCAL
  mvErrMsg
;

%LET mvErrMsg=;

%log4sas_debug (dwf.macro.merge_history, Started at %sysfunc(DateTime(),datetime.));

/* 1. проверка заполнения обязательных параметров */
%IF %is_blank(TABLE_LIST) %THEN %DO;
  %etl_rcSet(&USER_ERROR);
  %LET mvErrMsg=Не указан обязательный параметр TABLE_LIST;
  %GOTO macro_end;
%END;

%IF %is_blank(TARGET_TABLE) %THEN %DO;
  %etl_rcSet(&USER_ERROR);
  %LET mvErrMsg=Не указан обязательный параметр TARGET_TABLE;
  %GOTO macro_end;
%END;

%IF %is_blank(KEY_COLUMN) %THEN %DO;
  %etl_rcSet(&USER_ERROR);
  %LET mvErrMsg=Не указан обязательный параметр KEY_COLUMN;
  %GOTO macro_end;
%END;

%IF %is_blank(VALID_FROM_COLUMN) %THEN %DO;
  %etl_rcSet(&USER_ERROR);
  %LET mvErrMsg=Не указан обязательный параметр VALID_FROM_COLUMN;
  %GOTO macro_end;
%END;

%IF %is_blank(VALID_TO_COLUMN) %THEN %DO;
  %etl_rcSet(&USER_ERROR);
  %LET mvErrMsg=Не указан обязательный параметр VALID_TO_COLUMN;
  %GOTO macro_end;
%END;

/* 2.проверка входных наборов */
%local DS DS_COUNT;
%LET DS = %SYSFUNC(COMPBL(&TABLE_LIST));
%LET DS_COUNT = %EVAL(%SYSFUNC(COUNT(&DS,%STR( )))+1);

%IF &DS_COUNT lt 2 %THEN %DO;
  %etl_rcSet(&USER_ERROR);
  %LET mvErrMsg=Входных наборов меньше, чем 2;
  %GOTO macro_end;
%END;

/* получение списка переменных, которые должны присутствовать во входных наборах */
%local INPUT_LIST;
%let INPUT_LIST = %sysfunc(LOWCASE(&KEY_COLUMN &VALID_FROM_COLUMN &VALID_TO_COLUMN));

/* проверка существования библиотек, таблиц, столбцов, указанных в параметрах */
%local I DS_NAME;
%DO I=&DS_COUNT %TO 1 %BY -1;
   %LET DS_NAME = %SCAN(&DS, &I, %str( ));

   %let RESULT = ;

   %if not %member_vars_exist(&DS_NAME, &INPUT_LIST) %then %do;
      %etl_rcSet(&USER_ERROR);
      %LET mvErrMsg=Переменные &INPUT_LIST отсутствуют в наборе &DS_NAME;
      %GOTO macro_end;
   %end;
%END;

/* 3. подготовка входных наборов к обработке */
%local Keys LASTKEY;
%LET Keys = %SYSFUNC(COMPBL(&KEY_COLUMN));
%LET LASTKEY = %SCAN(&KEY_COLUMN,%EVAL(%SYSFUNC(COUNT(&Keys,%STR( )))+1));

/* 3.1 сохранение имен наборов и формирование списков переменных каждого из входных наборов */
%local VARLIST FULL_VARLIST dsid;
%LET FULL_VARLIST=;
%DO I=&DS_COUNT %TO 1 %BY -1;
  %LET DS_NAME = %SCAN(&DS, &I, %str( ));
  %LET VARLIST =;

  %let dsid=%sysfunc(open(&DS_NAME,i));
  %if &dsid le 0 %then %do;
      %etl_rcSet(&USER_ERROR);
     %LET mvErrMsg=Входной набор &DS_NAME не существует или недоступен для использования;
      %GOTO macro_end;
  %end;
  %else %do;

    %local j VAR_NAME;
    %do j=1 %to %sysfunc(attrn(&dsid,nvars));
      %let VAR_NAME =%sysfunc(lowcase(%sysfunc(varname(&dsid,&j))));

      %IF %INDEX(&INPUT_LIST, &VAR_NAME) eq 0 %THEN %DO;

        %if %INDEX(&FULL_VARLIST, &VAR_NAME) %then %do;
          %log4sas_warn (dwf.macro.merge_history, Переменная &VAR_NAME из датасета &DS_NAME уже существует в другом датасете );
        %end;
        %else %do;
          %LET VARLIST = &VARLIST &VAR_NAME;
        %end;

      %END;
    %end;

    %local DS_&I FLDS_&I;
    %LET FULL_VARLIST = &FULL_VARLIST &VARLIST;
    %LET DS_&I = &DS_NAME;
    %LET FLDS_&I = &VARLIST;
  %end;

  %if &dsid > 0 %then
    %let rc=%sysfunc(close(&dsid));
%END;

%local MAIN_TABLE_NM;
%LET MAIN_TABLE_NM=%SCAN(&DS, 1, %str( ));

%IF "&MAIN_TABLE" eq "YES" %THEN %DO;
proc sort data=&MAIN_TABLE_NM (keep=&KEYS) out = MAIN_TABLE nodupkey;
by &KEYS;
run;
%END;

%etl_rcSet(&syserr);
%IF &syserr > 4 %THEN %DO;
  %LET mvErrMsg = Ошибка создания MAIN_TABLE;
  %GOTO macro_end;
%END;

/* 4. начало реализации основного назначения макроса */

/* 4.1. формирование таблицы со всевозможными диапазонами из входных наборов */
   %DO I=&DS_COUNT %TO 1 %BY -1;
     %LET DS_NAME = %SCAN(&DS, &I, %str( ));

      proc sort data = &DS_NAME /*nodupkey dupout=&DS_NAME._DUP*/ out =&DS_NAME._SRT;
      by &Key_COLUMN &VALID_FROM_COLUMN descending &VALID_TO_COLUMN;
      run;

      data Date&I (keep=&Keys date)
      %IF "&MAIN_TABLE" eq "YES" %THEN %DO;
         &DS_NAME.&I (drop=date);
      %END;
      %IF "&MAIN_TABLE" eq "YES" AND &I ne 1 %THEN %DO;
         merge &DS_NAME._SRT (in=a) MAIN_TABLE (keep = &Keys in=b);
         by &Keys;
         %end;
      %ELSE
      %DO;
      ;
      set &DS_NAME._SRT;
      %end;

      %IF "&MAIN_TABLE" eq "YES" %THEN %DO;
         %IF &I ne 1 %THEN %DO;
            if a and b;
         %END;
      output &DS_NAME.&I;
      %end;

      date = &VALID_FROM_COLUMN;
      output Date&I;
      date = &VALID_TO_COLUMN;
      output Date&I;

      run;

   %etl_rcSet(&syserr);
   %IF &syserr > 4 %THEN %DO;
      %LET mvErrMsg = Ошибка создания &DS_NAME.&I;
      %GOTO macro_end;
   %END;
   %END;

/*Добавлена сортировка для наборов*/
%DO I=&DS_COUNT %TO 1 %BY -1;
   proc sort data = Date&I out = Date&I;
      by &Keys date;
   run;
%END;

data dates(keep=&Keys date);
   format date ddmmyy10.;
   set
   %DO I=&DS_COUNT %TO 1 %BY -1;
      Date&I
   %END;
   ;
   by &Keys date;
   if first.date;
run;
%etl_rcSet(&syserr);

%DO I=1 %TO &DS_COUNT;
  %member_drop(DATE&I.);
%END;

/*Индекс. Определить количество полей, создать имя мндекса*/
%LOCAL INDEX_NAME INDEX_STR;
%IF %EVAL(%SYSFUNC(COUNT(&KEYS,%STR( )))+1) gt 1 %THEN
   %DO;
   %LET INDEX_NAME=Date_Index;
   %LET INDEX_STR=%STR(index=(&INDEX_NAME=(&KEYS)));
   %END;
%else
   %DO;
   %LET INDEX_NAME=&KEYS;
   %LET INDEX_STR=%STR(index=(&INDEX_NAME));
   %END;
%log4sas_debug (dwf.macro.merge_history, INDEX_NAME=&INDEX_NAME INDEX_STR=&INDEX_STR );


data dates_to (drop=PREV_DT &INDEX_STR);
    format &VALID_FROM_COLUMN  datetime20. &VALID_TO_COLUMN datetime20.;
   set dates  (rename=(date=&VALID_TO_COLUMN)) nobs=last;
   by &Keys;

   retain PREV_DT;

   if first.&LASTKEY then do;
      PREV_DT=.;
      if last.&LASTKEY then do;
         &VALID_FROM_COLUMN = &VALID_TO_COLUMN;
      output;
      end;
   end;
   else do;
      &VALID_FROM_COLUMN = PREV_DT;
      output;
   end;
   PREV_DT=&VALID_TO_COLUMN;
run;
%etl_rcSet(&syserr);

%member_drop(work.dates);

%DO I=1 %TO &DS_COUNT;
   %LET DS_NAME  =  &&DS_&I;

   %etl_rcSet(&syserr);
   %IF &syserr > 4 %THEN %DO;
     %LET mvErrMsg = Ошибка сортировки &DS_NAME;
     %GOTO macro_end;
   %END;

   data DS_&I._DATES;
      set
      %IF "&MAIN_TABLE" eq "YES" %THEN %DO; &DS_NAME.&I %END;
      %ELSE %DO; &DS_NAME._SRT %END;
      ;
      by &Keys;

      FLG      =  1;
      START    =  &VALID_FROM_COLUMN;
      FINISH   =  &VALID_TO_COLUMN;
      do while (FLG ne 0 and _iorc_ = &IORC_SOK);
         set DATES_TO  key = &INDEX_NAME;
         if ( _iorc_ = &IORC_SOK ) then do;
            if   &VALID_FROM_COLUMN ge START and &VALID_TO_COLUMN le FINISH then output;
            if    /* VALID_FROM_DTTM lt START or*/ &VALID_TO_COLUMN ge FINISH then FLG=0;
         end;
         else
         do;
            FLG      =  0;
            _ERROR_  =  0;
            _iorc_   =  &IORC_SOK;
         end;
      end;
      drop flg start finish;
   run;
   %etl_rcSet(&syserr);

   %IF &syserr > 4 %THEN %DO;
     %LET mvErrMsg = Ошибка создания DS_&I._DATES;
     %GOTO macro_end;
   %END;

%end;

%member_drop(work.dates_to);

%DO I=1 %TO &DS_COUNT;
    %LET DS_NAME=DS_&I;

   %member_drop(&&&DS_NAME.._SRT);
   %IF "&MAIN_TABLE" eq "YES" %THEN %DO;
      %member_drop (&&&DS_NAME..&I);
   %END;

%END;

/* 4.4. объединение промежуточных наборов и получение окончательного результата */
data &TARGET_TABLE &OPTION_OUT;
   merge
   %DO I=1 %TO &DS_COUNT;
      DS_&I._DATES
      %IF "&MAIN_TABLE" eq "YES" AND &I eq 1 %THEN %DO;
         (in=main)
      %END;
  %END;
   ;
   by &Keys &VALID_FROM_COLUMN &VALID_TO_COLUMN;
   %IF "&MAIN_TABLE" eq "YES" %THEN %DO;
   if main;
   %END;
run;
%etl_rcSet(&syserr);


%DO I=1 %TO &DS_COUNT;
   %member_drop(DS_&I._DATES);
%END;

%IF "&MAIN_TABLE" eq "YES" %THEN %DO;
   %member_drop(MAIN_TABLE);
%END;

/* 5. обработка ошибок */
%macro_end:

%IF not %is_blank(mvErrMsg) %THEN
  %log4sas_error (dwf.macro.merge_history, &mvErrMsg );
%ELSE
  %log4sas_debug (dwf.macro.merge_history, Completed successfully at %sysfunc(DateTime(),datetime.) );

/* Kuzenkov: Disable compression: Restore */
OPTION &mvCompress;

%mend merge_history;