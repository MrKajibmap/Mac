/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 6e7c1d84790a644dc69d90874fa210d2ef45ab52 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Возвращает 1, если версия SAS выше или равна заявленной,
*     или 0 в любом другом случае.
*
*  ПАРАМЕТРЫ:
*     mpMajor           +  старшая версия (9 для 9.4M1)
*     mpMinor           -  младшая версия (4 для 9.4M1)
*     mpTSLevel         -  уровень поддержки (M1 для 9.4M1)
*
******************************************************************
*  Использует:
*     нет
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %if %util_sasver_ge (mpMajor=9, mpMinor=4, mpTSLevel=M1) %then %do...
*
******************************************************************
*  09-07-2014  Нестерёнок     Начальное кодирование
******************************************************************/

%macro util_sasver_ge (
   mpMajor                 =  ,
   mpMinor                 =  ,
   mpTSLevel               =
);
   /* Получаем актуальную версию */
   %local lmvMajor lmvMinor lmvBuild lmvTSLevel;
   %let lmvMajor     =  %scan(&SYSVLONG4, 1, %str(.));
   %let lmvMinor     =  %scan(&SYSVLONG4, 2, %str(.));
   %let lmvBuild     =  %scan(&SYSVLONG4, 3, %str(.));
   %let lmvTSLevel   =  %substr(&lmvBuild, 3, 4);

   %if %sysevalf(&lmvMajor lt &mpMajor) %then %do;
      0
      %return;
   %end;

%if not %is_blank(mpMinor) %then %do;
   %if %sysevalf(&lmvMinor lt &mpMinor) %then %do;
      0
      %return;
   %end;
%end;

%if not %is_blank(mpMinor) %then %do;
   %if %sysevalf(&lmvTSLevel lt &mpTSLevel) %then %do;
      0
      %return;
   %end;
%end;

   %do;1%end;
%mend util_sasver_ge;