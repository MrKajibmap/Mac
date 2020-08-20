/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 313b4a42a27ee1ccbcdd0622790ad9cc153961d8 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Генерирует номера инстансов для таблицы, ведущейся с переиспользованием внешнего ключа.
*     Номера начинаются с 1.
*
*  ПАРАМЕТРЫ:
*     mpIn                    +  имя входного набора
*     mpInFieldPK             +  список полей внешнего ключа во входном наборе
*     mpInFieldWith           +  поле (дата-время), определяющее, в который отрезок времени жизни попадает ключ
*                                значение показывает любую дату, в которую действует инстанс
*     mpInFieldBy             +  поле (дата-время), задающее разбиение времени жизни ключа на инстансы
*                                значение показывает дату, с которой инстанс не действует
*     mpFieldByInst           -  Если THIS, то дата mpInFieldBy является последней датой жизни текущего инстанса,
*                                а следующий начинается датой mpInFieldBy + 1 день
*                                Если NEXT, то дата mpInFieldBy является первой датой жизни следующего инстанса
*                                по умолчанию THIS (mpInFieldBy + 1 день)
*     mpSnap                  +  имя входного набора, текущего состояния
*                                обязан содержать поля: mpInFieldPK, mpSnapFieldInst*
*     mpSnapFieldInstNo       -  поле, в котором хранится номер инстанса
*                                По умолчанию INST_NO
*     mpSnapFieldInstStart    -  поле, в котором хранится дата начала времени жизни этого инстанса
*                                По умолчанию INST_START_DTTM
*     mpSnapFieldInstFinish   -  поле, в котором хранится дата конца времени жизни этого инстанса
*                                По умолчанию INST_FINISH_DTTM
*     mpOut                   +  имя выходного набора
*                                будет содержать все поля mpIn, а также mpSnapFieldInst*
*
******************************************************************
*  Использует:
*     %error_check
*     %job_event_reg
*     %list_expand
*     %member_drop
*     %member_obs
*     %unique_id
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1. Поля mpInFieldBy и mpInFieldWith обязаны быть заполнены.
*     2. Изменение отрезка жизни инстанса допускается только для последнего из них.
*        Попытка изменить время жизни прошлых инстансов вызовет ошибку.
*
******************************************************************
*  Пример использования:
*     %etl_generate_inst_no (
*        mpIn                       =  work.data1,
*        mpInFieldPK                =  INST_SRC_ID,
*        mpInFieldBy                =  DTEX,
*        mpInFieldWith              =  DTAP,
*        mpSnap                     =  ETL_IA.FINANCIAL_ACCOUNT_CRED_SNAP,
*        mpOut                      =  work.data2
*     );
*
******************************************************************
*  20-10-2014  Нестерёнок     Начальное кодирование
*  26-01-2015  Нестерёнок     Добавлен mpFieldByInst
******************************************************************/

%macro etl_generate_inst_no (
   mpIn                       =  ,
   mpInFieldPK                =  ,
   mpInFieldWith              =  ,
   mpInFieldBy                =  ,
   mpFieldByInst              =  THIS,
   mpSnap                     =  ,
   mpSnapFieldInstNo          =  INST_NO,
   mpSnapFieldInstStart       =  INST_START_DTTM,
   mpSnapFieldInstFinish      =  INST_FINISH_DTTM,
   mpOut                      =
);
   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Получаем интервалы существующих инстансов */
   %local lmvInstPrev;
   %let lmvInstPrev     = work.etl_inst_&lmvUID._prev;

   proc sort
      data= &mpSnap (keep= &mpInFieldPK &mpSnapFieldInstNo &mpSnapFieldInstStart &mpSnapFieldInstFinish)
      out=  &lmvInstPrev
      ;
      by &mpInFieldPK &mpSnapFieldInstNo;
   run;
   %error_check;

   /* Добавляем интервалы будущих инстансов */
   %local lmvInstAll lmvPermanentFlg;
   %let lmvInstAll      = work.etl_inst_&lmvUID._all;
   %let lmvPermanentFlg = etl_&lmvUID._perm_flg;

   data &lmvInstAll;
      set &lmvInstPrev;
      by &mpInFieldPK &mpSnapFieldInstNo;
      length &lmvPermanentFlg 3;

      if not last.&mpInFieldPK then do;
         &lmvPermanentFlg = 1;
         output;
      end;
      else do;
         &lmvPermanentFlg = 0;
         output;
         if &mpSnapFieldInstFinish < &ETL_MAX_DTTM then do;
            &mpSnapFieldInstNo      =  &mpSnapFieldInstNo + 1;
            &mpSnapFieldInstStart   =  &mpSnapFieldInstFinish;
            &mpSnapFieldInstFinish  =  &ETL_MAX_DTTM;
            output;
         end;
      end;
   run;
   %error_check;

   /* Подготовка обновляющей информации */
   %local lmvInstUpd;
   %let lmvInstUpd      = work.etl_inst_&lmvUID._upd;

   proc sort
      data= &mpIn (keep= &mpInFieldPK &mpInFieldWith &mpInFieldBy)
      out=  &lmvInstUpd
      nodupkey
      ;
      by &mpInFieldPK &mpInFieldWith;
   run;
   %error_check;

   /* Проверка 1: &mpInFieldWith < &mpInFieldBy */
   %local lmvCheckTable1 lmvDateFormat;
   %let lmvCheckTable1  = work_ia.etl_inst_&lmvUID._check1;

   data &lmvInstUpd &lmvCheckTable1;
      set &lmvInstUpd;
      if _n_ = 1 then call symput("lmvDateFormat", vformat(&mpInFieldBy));

%if &mpFieldByInst = THIS %then %do;
      &mpInFieldBy = intnx ("DTDAY", &mpInFieldBy, 1, "BEGINNING");
%end;

      if &mpInFieldWith lt &mpInFieldBy then output &lmvInstUpd;
      else output &lmvCheckTable1;
   run;
   %error_check;

   %if %member_obs(mpData=&lmvCheckTable1) gt 0 %then %do;
      %job_event_reg (mpEventTypeCode  =  DATA_VALIDATION_FAILED,
                      mpEventDesc      =  %bquote(В таблице &mpIn есть записи, у которых &mpInFieldWith >= &mpInFieldBy),
                      mpEventValues    =  %bquote(См. выборку в таблице &lmvCheckTable1) );
      %return;
   %end;

   /* Получаем обновленные интервалы */
   %let lmvInstAllNew   = work.etl_inst_&lmvUID._allnew;
   proc sql;
      create table &lmvInstAllNew as select
         %list_expand(&mpInFieldPK, t.{}, mpOutDlm=%str(, )),
         t.&mpInFieldWith,
         t.&mpInFieldBy,
         coalesce (i.&mpSnapFieldInstNo,     1)                as &mpSnapFieldInstNo,
         coalesce (i.&mpSnapFieldInstStart,  &ETL_MIN_DTTM)    as &mpSnapFieldInstStart   format=&lmvDateFormat,
         coalesce (i.&mpSnapFieldInstFinish, &mpInFieldBy)     as &mpSnapFieldInstFinish  format=&lmvDateFormat,
         coalesce (i.&lmvPermanentFlg,       0)                as &lmvPermanentFlg
      from
         &lmvInstUpd t
      left join
         &lmvInstAll i
      on
         %list_expand(&mpInFieldPK, t.{}=i.{}, mpOutDlm=%str( and )),
         and t.&mpInFieldWith ge i.&mpSnapFieldInstStart
         and t.&mpInFieldWith lt i.&mpSnapFieldInstFinish
      ;
   quit;
   %error_check (mpStepType=SQL);

   /* Проверка 2: старые интервалы не обновились */
   %local lmvInstAllUpd lmvCheckTable2;
   %let lmvInstAllUpd   = work.etl_inst_&lmvUID._allupd;
   %let lmvCheckTable2  = work_ia.etl_inst_&lmvUID._check2;

   data &lmvInstAllUpd &lmvCheckTable2;
      set &lmvInstAllNew;

      if &mpSnapFieldInstFinish ne &mpInFieldBy then do;
         if &lmvPermanentFlg = 1 then do;
            output &lmvCheckTable2;
            return;
         end;
         else &mpSnapFieldInstFinish = &mpInFieldBy;
      end;
      output &lmvInstAllUpd;

      keep &mpInFieldPK &mpInFieldWith &mpSnapFieldInstNo &mpSnapFieldInstStart &mpSnapFieldInstFinish;
   run;
   %error_check;

   %if %member_obs(mpData=&lmvCheckTable2) gt 0 %then %do;
      %job_event_reg (mpEventTypeCode  =  DATA_VALIDATION_FAILED,
                      mpEventDesc      =  %bquote(Старые интервалы не могут быть обновлены),
                      mpEventValues    =  %bquote(См. выборку в таблице &lmvCheckTable2) );
      %return;
   %end;

   /* Создаем выходной набор */
   %local lmvHashGroup lmvHashRc;
   %let lmvHashGroup = hash_grp_&lmvUID.;
   %let lmvHashRc    = hash_rc_&lmvUID.;
   data &mpOut;
      set &mpIn;
      if _n_ = 1 then do;
         if 0 then set &lmvInstAllUpd;
         declare hash &lmvHashGroup(dataset:"&lmvInstAllUpd");
         &lmvHashGroup..defineKey( %list_expand(&mpInFieldPK &mpInFieldWith, "{}", mpOutDlm=%str(, )) );
         &lmvHashGroup..defineData( %list_expand(&mpSnapFieldInstNo &mpSnapFieldInstStart &mpSnapFieldInstFinish, "{}", mpOutDlm=%str(, )) );
         &lmvHashGroup..defineDone();
      end;

      &lmvHashRc = &lmvHashGroup..find();
      if &lmvHashRc ne 0 then error;
      drop &lmvHashRc;
   run;
   %error_check;

   /* Очистка */
   %member_drop(&lmvInstPrev);
   %member_drop(&lmvInstAll);
   %member_drop(&lmvInstUpd);
   %member_drop(&lmvCheckTable1);
   %member_drop(&lmvInstAllNew);
   %member_drop(&lmvInstAllUpd);
   %member_drop(&lmvCheckTable2);
%mend etl_generate_inst_no;
