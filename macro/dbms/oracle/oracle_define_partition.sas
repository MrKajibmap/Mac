/*****************************************************************
*  ВЕРСИЯ:
*     $Id: a4ce322626d5301c00734bd20697f7ae4a239ae3 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Создает скрипт для определения партиций по дате.
*     Партиции строятся по заданному интервалу (по умолчанию по месяцу).
*
*  ПАРАМЕТРЫ:
*     mpFieldPartition        +  поле, по которому строятся партиции
*     mpMinDate               +  мин. дата, от которой начинаются отдельные партиции
*     mpMaxDate               +  макс. дата, для которой строится отдельная партиция
*     mpEnableRowMovement     -  разрешается ли изменение поля mpFieldPartition
*                                по умолчанию Yes
*     mpPeriod                -  размер партиций (константа из интервалов дат SAS)
*                                по умолчанию MONTH
*
******************************************************************
*  Пример использования:
*
*     %let min_date = %sysfunc(putn('01jan2001'd, best.));
*     %let max_date = %sysfunc(putn('01jan2005'd, best.));
*     %put %oracle_define_partition (mpFieldPartition=sqldate, mpMinDate=&min_date, mpMaxDate=&max_date);
*
******************************************************************
*  23-08-2012  Нестерёнок     Начальное кодирование
*  14-11-2012  Нестерёнок     Добавлен mpEnableRowMovement
*  23-05-2014  Нестерёнок     Добавлен mpPeriod
******************************************************************/

%macro oracle_define_partition (mpFieldPartition=, mpMinDate=, mpMaxDate=, mpEnableRowMovement=Yes, mpPeriod=MONTH);
   partition by range (&mpFieldPartition)
   (
      partition &mpFieldPartition._past values less than (%oracle_date(&mpMinDate)),

   %local i lmvDate lmvPartitionName;
   %let lmvDate = &mpMinDate;
   %do i=1 %to 1000;
      %let lmvPartitionName = &mpFieldPartition._%sysfunc(putn(&lmvDate, yymm7.));
      %let lmvDate = %sysfunc(intnx(&mpPeriod, &lmvDate, 1, BEGINNING));

      partition &lmvPartitionName values less than (%oracle_date(&lmvDate)),

      %if &lmvDate ge &mpMaxDate %then %goto exit;
   %end;

   %exit:
      partition &mpFieldPartition._future values less than (maxvalue)
   )

   %if %upcase(&mpEnableRowMovement) = YES %then %do;
      enable row movement
   %end;
%mend oracle_define_partition;
