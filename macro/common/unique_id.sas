/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 658bcce7821ace1cb05372c43d87884f6eca146a $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Генерирует 1 уникальное числовое значение (идентификатор) и возвращает его в макропеременную
*     Работает в глобальном режиме, внутри PROC SQL, или внутри DATA STEP.
*
*  ПАРАМЕТРЫ:
*     mpOutKey          +  имя макропеременной, в которую возвращается идентификатор
*     mpSequenceName    -  имя ETL_DBMS sequence для получения очередного идентификатора
*     mpLoginSet        -  может использоваться только если указано mpSequenceName.
*     mpOutLength       -  длина идентификатора (до 10 знаков)
*                          По умолчанию 10
*
******************************************************************
*  Использует:
*     %ETL_DBMS_connect (если задан mpSequenceName)
*     mpSequenceName (если задан)
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     В режиме внутри DATA STEP mpSequenceName игнорируется.
*
******************************************************************
*  Пример использования:
*     * получить уникальный идентификатор в макропеременную mvNextId;
*     %local mvNextId;
*     %unique_id (mpOutKey=mvNextId);
*     proc sql;
*        * получаем очередное значение;
*        %unique_id (mpOutKey=mvNextId);
*     quit;
*
******************************************************************
*  17-01-2012  Нестерёнок     Начальное кодирование
*  31-08-2012  Нестерёнок     Рефактор mpMode
*  15-04-2014  Нестерёнок     Добавлен режим внутри DATA STEP
*  15-04-2014  Нестерёнок     Изменен способ генерации UID
*  30-01-2015  Сазонов        SQL вызов вынесен в dbms specific
******************************************************************/

%macro unique_id (
   mpOutKey                   =  ,
   mpSequenceName             =  ,
   mpLoginSet                 =  ETL_SYS,
   mpOutLength                =  10
);
   /* Проверяем корректность параметров */
   %if %is_blank(mpOutKey) %then %do;
      %log4sas_error (cwf.macro.unique_id, Incorrect parameter mpOutKey value.);
      %return;
   %end;

   /* Проверка среды */
   %local lmvIsDataStep;
   %let lmvIsDataStep = %eval (&SYSPROCNAME eq DATASTEP);

   %if (not %is_blank(mpSequenceName)) and not &lmvIsDataStep %then %do;
     /* Генерируем ID для новых объектов через sequence */
      %&ETL_DBMS._get_seq_val(mpOutKey=&mpOutKey, mpSequenceName=&mpSequenceName, mpLoginSet=&mpLoginSet);
   %end;
   %else %do;
      /* Генерируем ID */
      %let &mpOutKey = %substr(%sysfunc(ranuni(0))123456789, 3, &mpOutLength);
   %end;
%mend unique_id;