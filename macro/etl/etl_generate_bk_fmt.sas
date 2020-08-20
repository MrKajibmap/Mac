/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 60edc514b4f5d77f46afff74a981810bc558d5ee $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Генерирует RX-выражение для разбора формата части бизнес-ключа.
*
*  ПАРАМЕТРЫ:
*     mpFormat                +  шаблон создания бизнес-ключа
*                                Должен состоять из знаков ASCII и следующих частей:
*                                {[CG]n} - см. {[CG]1:n}
*                                {Cm:n} - строка длиной от m до n знаков ASCII
*                                {Gm:n} - строка длиной от m до n печатаемых знаков
*                                {Xn} - hex-строка длиной n знаков
*                                {[NDZT]n} - число длиной n знаков
*     mpIndex                 +  номер части, от 1
*                                Если такого номера не существует, все mpOut*Key возвращаются пустыми
*     mpOutTypeKey            -  имя макропеременной, в которую возвращается тип части
*     mpOutMinWidthKey        -  имя макропеременной, в которую возвращается мин. длина части
*     mpOutMaxWidthKey        -  имя макропеременной, в которую возвращается макс. длина части
*     mpOutPrefixKey          -  имя макропеременной, в которую возвращается префикс части
*     mpOutSuffixKey          -  имя макропеременной, в которую возвращается суффикс части
*
******************************************************************
*  Использует:
*     %is_blank
*     %unique_id
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1.  mpOutSuffixKey корректно считается только для последней части.
*     2.  Формат G будет корректно работать только в SBCS-окружении.
*
******************************************************************
*  Пример использования:
*     %local type min max;
*     %etl_generate_bk_fmt (
*        mpFormat          =  TEST_{C3:5}_{N6}_SAMPLE,
*        mpIndex           =  2,
*        mpOutTypeKey      =  type,
*        mpOutMinWidthKey  =  min,
*        mpOutMaxWidthKey  =  max
*     );
*
*     %put &=type &=min &=max;
*     выводит TYPE=N MIN=6 MAX=6
*
******************************************************************
*  03-11-2015  Нестерёнок     Начальное кодирование
******************************************************************/

%macro etl_generate_bk_fmt (
   mpFormat                =  ,
   mpIndex                 =  ,
   mpOutTypeKey            =  ,
   mpOutMinWidthKey        =  ,
   mpOutMaxWidthKey        =  ,
   mpOutPrefixKey          =  ,
   mpOutSuffixKey          =
);
   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Проверка параметров */
   %if %is_blank(mpOutTypeKey) %then %do;
      %let mpOutTypeKey = mpOutTypeKey_&lmvUID;
      %local &mpOutTypeKey;
   %end;
   %if %is_blank(mpOutMinWidthKey) %then %do;
      %let mpOutMinWidthKey = mpOutMinWidthKey_&lmvUID;
      %local &mpOutMinWidthKey;
   %end;
   %if %is_blank(mpOutMaxWidthKey) %then %do;
      %let mpOutMaxWidthKey = mpOutMaxWidthKey_&lmvUID;
      %local &mpOutMaxWidthKey;
   %end;
   %if %is_blank(mpOutPrefixKey) %then %do;
      %let mpOutPrefixKey = mpOutPrefixKey_&lmvUID;
      %local &mpOutPrefixKey;
   %end;
   %if %is_blank(mpOutSuffixKey) %then %do;
      %let mpOutSuffixKey = mpOutSuffixKey_&lmvUID;
      %local &mpOutSuffixKey;
   %end;
   %if &mpIndex lt 1 %then %goto leave;

   /* Приводим к полному виду */
   %local lmvFormat;
   %let lmvFormat  =  &mpFormat;
   %let lmvFormat  =  %sysfunc(prxchange(%str(s/\{([CG])(\d+)\}/{${1}1:$2}/),    -1, %superq(lmvFormat) ));
   %let lmvFormat  =  %sysfunc(prxchange(%str(s/\{([NDZTX])(\d+)\}/{$1$2:$2}/),  -1, %superq(lmvFormat) ));

   /* Ищем нужную часть */
   %local lmvRx lmvStart lmvStop lmvPosition lmvLength;
   %let lmvRx        =  %sysfunc(prxparse( /\{([NCGDZTX])(\d+)\:(\d+)\}/ ));
   %let lmvStart     = 1;
   %let lmvStop      = -1;
   %let lmvPosition  = 0;
   %let lmvLength    = 0;

   %local lmvFormatCount;
   %do lmvFormatCount=1 %to &mpIndex;
      %local lmvLastStart;
      %let lmvLastStart = &lmvStart;
      %syscall prxnext(lmvRx, lmvStart, lmvStop, lmvFormat, lmvPosition, lmvLength);
      %if (&lmvPosition le 0) %then %goto leave_and_free;
   %end;
   %if (&lmvFormatCount le &mpIndex) %then %goto leave_and_free;

   /* Выводим результат */
   %let &mpOutTypeKey      =  %sysfunc(prxposn(&lmvRx, 1, &lmvFormat));
   %let &mpOutMinWidthKey  =  %sysfunc(prxposn(&lmvRx, 2, &lmvFormat));
   %let &mpOutMaxWidthKey  =  %sysfunc(prxposn(&lmvRx, 3, &lmvFormat));
   %let &mpOutPrefixKey    =  %sysfunc(substrn(&lmvFormat, &lmvLastStart, %eval(&lmvPosition - &lmvLastStart)));
   %let &mpOutSuffixKey    =  %sysfunc(substrn(&lmvFormat, &lmvStart));

   %syscall prxfree(lmvRx);
   %return;

   /* Результат отсутствует */
%leave_and_free:
   %syscall prxfree(lmvRx);
%leave:
   %let &mpOutTypeKey      =  ;
   %let &mpOutMinWidthKey  =  ;
   %let &mpOutMaxWidthKey  =  ;
   %let &mpOutPrefixKey    =  ;
   %let &mpOutSuffixKey    =  ;
%mend etl_generate_bk_fmt;
