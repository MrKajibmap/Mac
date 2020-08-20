/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 6e76dde4f5f1e0a1743b73079b1973b092fb1569 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Исполняет другой макрос в цикле с итерацией по набору данных.
*     Все переменные набора устанавливаются как локальные макропеременные для макроса итерации.
*
*     В режиме mpCancellable=Y вызываемый макрос должен объявить именованный параметр mpCancel.
*     Для остановки цикла нужно присвоить переменной &mpCancel значение Y.
*
*  ПАРАМЕТРЫ:
*     mpLoopMacro          +  имя макроса итерации
*     mpData               +  имя входного набора для итераций
*     mpWhere              -  условие отбора из входного набора
*     mpCancellable        -  может (Y) или нет (N) вызываемый макрос остановить цикл
*                             по умолчанию N
*
******************************************************************
*  Использует:
*     %error_check
*     %job_event_reg
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1. Макрос итерации не должен иметь позиционных параметров.
*     2. Макрос итерации не должен быть задан с пустым списком параметров
*        (например, %macro inner(); - не допускается).
*     3. Макрос итерации может иметь именованные параметры, но их значения будут приняты
*        за пустые (кроме mpCancel=cancel_var_name=N, если затребован режим mpCancellable=Y).
*
******************************************************************
* Пример использования:
*    %macro inner;
*        %put &name &age;
*    %mend inner;
*    %util_loop_data (mpData=sashelp.class, mpLoopMacro=inner);
*
*    %macro inner_cancellable(mpCancel=);
*        %put &name &age;
*        %if &name = Judy %then %let &mpCancel=Y;;
*    %mend inner_cancellable;
*    %util_loop_data (mpData=sashelp.class, mpLoopMacro=inner_cancellable, mpCancellable=Y);
*
******************************************************************
*  27-02-2012  Нестерёнок     Начальное кодирование
*  26-02-2014  Нестерёнок     Добавлен mpCancellable
******************************************************************/

%macro util_loop_data(
   mpLoopMacro       =  ,
   mpData            =  ,
   mpWhere           =  ,
   mpCancellable     =  N
);
   /* Готовим список опций */
   %local dsid dsoptions;
   %let dsoptions = ;
   %if not %is_blank(mpWhere) %then %do;
      %let dsoptions = (where=(&mpWhere));
   %end;

   /* Открываем набор */
   %let dsid = %sysfunc(open(&mpData &dsoptions, I));
   %if &dsid eq 0 %then %do;
      %job_event_reg (mpEventTypeCode=DATA_NOT_AVAILABLE,
                      mpEventValues= %bquote(Не удалось открыть таблицу &mpData. (&dsoptions.) на чтение) );
      %return;
   %end;

   /* Итерация по набору */
   %syscall set(dsid);
   %do %while (%sysfunc(fetch(&dsid)) eq 0);
      %if &mpCancellable ne Y %then %do;
         %do;%&mpLoopMacro.%end;
      %end;
      %else %do;
         %local lmvCancelVarName;
         %let lmvCancelVarName   = lmvUtilLoopDataCancel%sysmexecdepth;
         %let &lmvCancelVarName  = N;

         %do;%&mpLoopMacro.(mpCancel=&lmvCancelVarName);%end;

         %if &&&lmvCancelVarName = Y %then %goto leave;
      %end;
   %end;
   %leave:
   %let dsid = %sysfunc(close(&dsid));
%mend util_loop_data;
