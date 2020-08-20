/*****************************************************************
*  ВЕРСИЯ:
*     $Id: c68c128a57d41b9bfeb1588f0683729d40e19149 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макросы для работы с динамическим списком колонок
*        member_vars_get - сохраняет список колонок таблицы во временный текстовый файл и возвращает ссылку на файл
*        member_vars_clean - освобождает ресурсы
*
*  ПАРАМЕТРЫ:
*     member_vars_get
*     mpInTab                 +  имя входного набора
*     mpOutHandle             +  имя макропеременной для сохранения ссылки на файл с именами колонок
*     mpType                  -  фильтр по типу переменных, по умолчанию все
*                                N - только числовые
*                                C - только символьные
*     mpKeep                  -  список полей, которые нужно включать в результат
*     mpDrop                  -  список полей, которые не нужно включать в результат
*
*     member_vars_clean
*     mpHandle                +  ссылка на файл с именами колонок
******************************************************************
*  Использует:
*     member_vars_expand
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1. Порядок полей не специфицирован и может отличаться при повторных вызовах.
*     2. Параметры mpKeep, mpDrop гарантированно работают только со списком полей, поведение при
*        передаче в них файловой ссылки не специфицировано.  См. также #29.
*
******************************************************************
*  Пример использования:
*     %global lmvSaveVars;
*     %member_vars_get(SASHELP.CLASS,  lmvSaveVars, mpDrop=age height);
*     DATA WORK.CLASS;
*        SET SASHELP.CLASS;
*        KEEP
*           %member_vars_expand(&lmvSaveVars age, {})
*        ;
*     RUN;
*     %member_vars_clean(&lmvSaveVars);
*
******************************************************************
*  22-08-2012  Кузенков       Начальное кодирование
*  25-12-2012  Нестерёнок     Добавлен mpKeep
******************************************************************/

%MACRO member_vars_get(mpInTab, mpOutHandle, mpType=, mpKeep=, mpDrop=);
  %LOCAL mvFR mvRC mvFID mvWhere mvI mvCol mvN mvIsNotSQL;

  %LET mvIsNotSQL = %EVAL(&SYSPROCNAME^=SQL);

  %IF not %is_blank(mpType) %THEN %DO;
    %IF &mpType=n OR &mpType=N %THEN
      %LET mvWhere = AND %STR(type = 'num');
    %ELSE %IF &mpType=c OR &mpType=C %THEN
      %LET mvWhere = AND %STR(type = 'char');
  %END;

  %LET mvWhere = libname %STR(=) "%UPCASE(%SCAN(WORK.&mpInTab,-2,.))" AND memname %STR(=) "%UPCASE(%SCAN(&mpInTab,-1,.))" &mvWhere;

  %LET mvFR = %STR( );
  %LET mvRC = %SYSFUNC(Filename(mvFR,,TEMP));

  %IF &mvIsNotSQL %THEN %DO;
  PROC SQL NOPRINT;
  %END;
  %else %do;
     reset noprint;
  %end;
    SELECT FOpen("&mvFR","O", 32) INTO :mvFID FROM (select count(*) from dictionary.DICTIONARIES) dual;
    SELECT Min(IfN(FPut(&mvFID, TrimN(UpCase(name))), FWrite(&mvFID),0)), Count(*) INTO :mvRC, :mvN FROM DICTIONARY.COLUMNS
      WHERE &mvWhere
  %IF not %is_blank(mpKeep) %THEN %DO;
        AND Upper(name) IN (
            %member_vars_expand(&mpKeep, '{}', mpOutDlm=%str(,))
        )
  %END;
  %IF not %is_blank(mpDrop) %THEN %DO;
        AND Upper(name) NOT IN (
            %member_vars_expand(&mpDrop, '{}', mpOutDlm=%str(,))
        )
  %END;
    ;
    SELECT FClose(&mvFID) INTO :mvRC FROM (select count(*) from dictionary.DICTIONARIES) dual;
  %IF &mvIsNotSQL %THEN %DO;
  QUIT;
  %END;

  %IF &mvN=0 %THEN %DO;
    %LET mvRC = %SYSFUNC(Filename(mvFR));
    %LET mvFR=;
  %END;

  %LET &mpOutHandle = &mvFR;
%MEND member_vars_get;


%MACRO member_vars_clean(mpHandle);
  %LOCAL mvRC;
  %LET mvRC = %SYSFUNC(Filename(mpHandle));
%MEND member_vars_clean;