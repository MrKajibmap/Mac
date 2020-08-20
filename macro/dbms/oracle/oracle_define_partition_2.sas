/*****************************************************************
* ВЕРСИЯ:
*   $Id: d5e917d5ba874241488f4eac7eb3efae5229170d $
*
******************************************************************
* НАЗНАЧЕНИЕ:
*   Создает скрипт для определения (суб)партиций по паре дат.
*   Партиции строятся по кварталу.
*
* ПАРАМЕТРЫ:
*   mpFieldPartition1      +     первое поле, по которому строятся партиции, например VALID_FROM_DTTM
*   mpFieldPartition2      +     второе поле, по которому строятся партиции, например VALID_TO_DTTM
*   mpMinDate              +     мин. дата, от которой начинаются отдельные партиции
*   mpMaxDate              +     макс. дата, для которой строится отдельная партиция
*   mpEnableRowMovement    -     разрешается ли изменение поля mpFieldPartition
*                                по умолчанию Yes
*
******************************************************************
* Пример использования:
*
*  %let min_date = %sysfunc(putn('01jan2001'd, best.));
*  %let max_date = %sysfunc(putn('01jan2005'd, best.));
*  %put %oracle_define_partition (mpFieldPartition1=VALID_FROM_DTTM, mpFieldPartition2=VALID_TO_DTTM, mpMinDate=&min_date, mpMaxDate=&max_date);
*
******************************************************************
* 16-04-2013   Нестерёнок  Начальное кодирование
******************************************************************/

%macro oracle_define_partition_2 (mpFieldPartition1=, mpFieldPartition2=, mpMinDate=, mpMaxDate=, mpEnableRowMovement=Yes);

   %local i lmvDate lmvPartitionName;

   partition by range (&mpFieldPartition1)
   subpartition by range (&mpFieldPartition2)
   subpartition template(

      %let lmvDate = &mpMinDate;
      %do i=1 %to 1000;
         %let lmvPartitionName = S%sysfunc(putn(&lmvDate, yyq6.));

         subpartition &lmvPartitionName values less than (%oracle_date(&lmvDate)),

         %let lmvDate = %sysfunc(intnx(QTR, &lmvDate, 1, BEGINNING));
         %if &lmvDate gt &mpMaxDate %then %goto exit2;
      %end;

      %exit2:
         subpartition SFUTURE values less than (maxvalue)
   )
   (

      %let lmvDate = &mpMinDate;
      %do i=1 %to 1000;
         %let lmvPartitionName = &mpFieldPartition1._%sysfunc(putn(&lmvDate, yyq6.));

         partition &lmvPartitionName values less than (%oracle_date(&lmvDate)),

         %let lmvDate = %sysfunc(intnx(QTR, &lmvDate, 1, BEGINNING));
         %if &lmvDate gt &mpMaxDate %then %goto exit1;
      %end;

      %exit1:
         partition &mpFieldPartition1._future values less than (maxvalue)
   )

   %if %upcase(&mpEnableRowMovement) = YES %then %do;
      enable row movement
   %end;
%mend oracle_define_partition_2;
