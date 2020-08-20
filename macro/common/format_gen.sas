/*****************************************************************
* ВЕРСИЯ:
*   $Id: 6b9d3de1085af9d2409ff7bc08f60866242545df $
*
******************************************************************
* НАЗНАЧЕНИЕ:
*   Создает или обновляет табличный (ин)формат.
*   Параллельный запуск не поддерживается.
*
* ПАРАМЕТРЫ:
*   mpMeta     - таблица, содержащая метаданные форматов
*   mpFmtLib   - библиотека, в которую будет записан формат
*   mpFmtName  - имя нужного формата, если не задано, то будут созданы все
*   mpFmtGroup - группа нужных форматов, если не задана, то будут созданы все
*   mpWhere    - необязательное where-условие на таблицу данных для формата
*
******************************************************************
* Пример использования:
*    %format_gen (mpFmtGroup=ETL_INIT);
*
******************************************************************
* 26-12-2011   Нестерёнок  Начальное кодирование
* 30-08-2012   Нестерёнок  Добавлена защита от одновременного чтения-записи каталога форматов
******************************************************************/

%macro format_gen(
  mpMeta=ETL_SYS.ETL_FORMAT,
  mpFmtLib=ETL_FMT,
  mpFmtName=,
  mpFmtGroup=,
  mpWhere=
);

   filename _fmtgen temp lrecl=1024;

   data _null_;
      set &mpMeta end=ds_end;
      by format_nm notsorted;

      %if not %is_blank(mpFmtName) %then %do;
         where format_nm="&mpFmtName";
      %end;
      %else %if not %is_blank(mpFmtGroup) %then %do;
         where format_group_cd="&mpFmtGroup";
      %end;

      length cmd $2048 fmttab $32 effective_other $100;
      file _fmtgen;
      fmttab = cats (table_nm, "_", _n_);

      /* Копируем исходный каталог форматов для обновления, если он существует */
      %local lmvInit lmvFmtUpd;
      %let lmvInit = not %sysfunc(exist(&mpFmtLib..formats, CATALOG));
      %unique_id(mpOutKey=lmvFmtUpd);
      %let lmvFmtUpd = fmt_upd_&lmvFmtUpd;
      %if not &lmvInit %then %do;
         if _n_ = 1 then do;
            cmd = catt ("proc datasets lib=work mt=(catalog) noprint;");
            put cmd;
            cmd = catt ("copy in=&mpFmtLib out=work;   select formats;");
            put cmd;
            cmd = catt ("delete &lmvFmtUpd;");
            put cmd;
            cmd = catt ("change formats=&lmvFmtUpd;");
            put cmd;
            cmd = cats ("quit;");
            put cmd;
         end;
      %end;

      /* Строим все требуемые форматы */
      cmd = cats ("proc sql;");
      put cmd;

      /* Build base format table */
      cmd = catx (" ", "create table", fmttab, "as select distinct");
      cmd = catx (" ", cmd, start_col_nm,    " as start, ");
      if lengthn(end_col_nm) gt 0 then
         cmd = catx (" ", cmd, end_col_nm,   " as end, ");
      else
         cmd = catx (" ", cmd, start_col_nm, " as end, ");

      cmd = catx (" ", cmd, label_col_nm,    " as label, ");
      cmd = catx (" ", cmd, quote(cats(format_nm)),       " as fmtname, ");
      cmd = catx (" ", cmd, quote(cats(format_type_cd)),  " as type, ");
      cmd = catx (" ", cmd, quote(cats(hlo_cd)), " as hlo ");

      cmd = catx (" ", cmd, "from ", cats (library_nm, ".", table_nm), "as fmt_src");

      %if not %is_blank(mpWhere) %then %do;
         if lengthn(where_txt) gt 0 then
            cmd = catx (" ", cmd, "where (", where_txt, ") and (&mpWhere)");
         else
            cmd = catx (" ", cmd, "where (&mpWhere)");
      %end;
      %else %do;
         if lengthn(where_txt) gt 0 then
            cmd = catx (" ", cmd, "where (", where_txt, ")");
      %end;

      cmd = cats (cmd, ";");
      put cmd;

      /* Other processing */
      /* remove space chars */
      effective_other = strip(other_value_txt);
      if lengthn(effective_other) gt 0 then do;
         /* double quotes are not allowed in Other */
         /* in addition, wiping them out supports missing character values */
         effective_other = kcompress (effective_other, """");
         cmd = catx (" ", "insert into", fmttab, "(start, end, label, fmtname, type, hlo)");

         if (format_type_cd = "N") then
            cmd = catx (" ", cmd, "values (-1, -1,");
         else do;
            cmd = catx (" ", cmd, "values (""*"", ""*"",");
         end;
         if (format_type_cd = "I") then
            cmd = catx (" ", cmd, cats(effective_other), ",");
         else do;
            cmd = catx (" ", cmd, quote(cats(effective_other)), ",");
         end;
         cmd = catx (" ", cmd, quote(cats(format_nm)),       ",");
         cmd = catx (" ", cmd, quote(cats(format_type_cd)),  ",");
         cmd = catx (" ", cmd, quote("O"), ");");

         put cmd;
      end;

      cmd = cats ("quit;");
      put cmd;

      /* Format generation */
      cmd = catt ("proc format lib=work.&lmvFmtUpd cntlin=", fmttab, "; run;");
      put cmd;

      if ds_end then do;
         /* Меняем местами новый и старый каталоги */
         cmd = catt ("proc datasets lib=&mpFmtLib mt=(catalog) noprint;");
         put cmd;
         cmd = catt ("copy in=work out=&mpFmtLib;   select &lmvFmtUpd;");
         put cmd;
         %if &lmvInit %then %do;
            cmd = catt ("change &lmvFmtUpd=formats;");
            put cmd;
         %end;
         %else %do;
            cmd = catt ("exchange &lmvFmtUpd=formats;");
            put cmd;
            cmd = catt ("delete &lmvFmtUpd;");
            put cmd;
         %end;
         cmd = cats ("quit;");
         put cmd;
      end;
   run;

   %include _fmtgen;
   filename _fmtgen clear;

%mend format_gen;
