/*****************************************************************
*  НАЗНАЧЕНИЕ:
*     Исполняет SQL-скрипт как ряд отдельных pass-through stmts.
*     Точка с запятой в комментариях не поддерживается.
*     Длина одной строки скрипта ограничена параметром mpLrecl, длина одного stmt - 32K.
*
*  ПАРАМЕТРЫ:
*     mpScriptName            +  имя файла (в кавычках) или fileref, содержащего скрипт
*     mpLoginSet              +  имя набора параметров подключения к БД [ETL_SYS, ETL_STG и т.п.]
*     mpLrecl                 -  наибольшая длина строки файла в байтах.
*                                По умолчанию 1000, т.е. 500 русских букв в UTF-8.
*
******************************************************************
*  ИСПОЛЬЗУЕТ:
*     %error_check
*     %ETL_DBMS_connect
*     %member_drop
*     %unique_id
*     %util_loop_data
*
*  УСТАНАВЛИВАЕТ МАКРОПЕРЕМЕННЫЕ:
*     нет
*
******************************************************************
*  ПРИМЕР ИСПОЛЬЗОВАНИЯ:
*     %sql_exec (mpScriptName="&ETL_ROOT./setup/sql/etl_sys_create.sql", mpLoginSet=ETL_SYS);
*
******************************************************************
*  12-09-2012  Нестерёнок     Начальное кодирование
******************************************************************/

%macro sql_exec (mpScriptName=, mpLoginSet=, mpLrecl=1000);
   /* Получаем уникальный идентификатор для параллельного исполнения */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Разбираем входной файл */
   %local lmvStmt;
   %let lmvStmt = work.sql_stmt_&lmvUID;

   data &lmvStmt;
      infile &mpScriptName lrecl=&mpLrecl;
      length i1 8 txt stmt $32000;
      input;

      retain txt;
      txt = catx (' ', txt, _infile_);

      do until (i1 = 0);
         i1 = indexc (txt, ';');
         if i1 gt 0 then do;
            stmt = catt (stmt, substrn(txt, 1, i1-1));
            output;
            txt = substrn(txt, i1+1);
         end;
      end;
      keep stmt;
   run;
   %error_check;

   /* Исполняеи все stmts */
   %macro _sql_exec_stmt;
      execute (&stmt) by &ETL_DBMS;
      %error_check (mpStepType=SQL_PASS_THROUGH);
   %mend _sql_exec_stmt;

   proc sql;
      %&ETL_DBMS._connect(mpLoginSet=&mpLoginSet);
      %util_loop_data (mpLoopMacro=_sql_exec_stmt, mpData=&lmvStmt);
   quit;

   %if not &ETL_DEBUG %then
      %member_drop (&lmvStmt);
%mend sql_exec;
