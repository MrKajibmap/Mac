/*****************************************************************
*  ВЕРСИЯ:
*     $Id: f67cc8aca68f20718e6e47d70fcced3b95dd74ed $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Генерирует поле с уникальным идентификатором.
*     Если указаны mpLoginSet+mpSequenceName, то идентификаторы берутся из указанного sequence.
*
*  ПАРАМЕТРЫ:
*     mpIn                    +  имя входного набора
*     mpLoginSet              -  имя DBMS схемы, в которой находится mpSequence
*     mpSequence              -  имя DBMS sequence для получения очередного идентификатора
*     mpOut                   +  имя выходного набора
*                                Структура идентична tpIn + tpOutFieldId
*     mpOutFieldId            +  поле (numeric), в которое будет помещен сгенерированный уникальный идентификатор
*     mpKeyType               +  поле (numeric), определяет тип ключа (для записи или для таблицы)
*
******************************************************************
*  Использует:
*     %error_check
*     %unique_ids
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     1. Если не указан mpLoginSet или mpSequenceName, то идентификаторы уникальны только в пределах таблицы.
*
******************************************************************
*  Пример использования:
*     в трансформе transform_generate_unique_ids.sas
*
******************************************************************
*  18-06-2015  Нестерёнок     Начальное кодирование
*  03-11-2017  Задояный       Добавлен mpKeyType
******************************************************************/

%macro etl_generate_unique_ids (
      mpIn                 =  ,
      mpLoginSet           =  ,
      mpSequence           =  ,
      mpOut                =  ,
      mpOutFieldId         =  ,
      mpKeyType            = 
);
   /* Получаем уникальный идентификатор */
   %local lmvUID;
   %unique_id (mpOutKey=lmvUID);

   /* Обработка параметров */
   %if %is_blank(mpLoginSet) or %is_blank(mpSequence) %then %do;
      %let mpLoginSet = ;
      %let mpSequence = ;
   %end;

   /* Получаем кол-во записей */
   %local lmvInCount;
   %let lmvInCount = %member_obs (mpData=&mpIn);

   /* Отбрасываем вырожденные случаи */
   %if %is_blank(lmvInCount) %then %return;
   %if &lmvInCount = 0 %then %do;
      data &mpOut;
         set &mpIn;
         length &mpOutFieldId 8;
         call missing (&mpOutFieldId);
      run;
      %error_check;
      %return;
   %end;

   /* Получаем список идов */
   %local lmvNewIdsTable lmvOutFieldValue;
   %let lmvNewIdsTable  =  work.tr_gen_uids_&lmvUID;
   
   %if &mpKeyType = 1 %then %do;
   		%unique_id (mpOutKey=lmvOutFieldValue, mpLoginSet=&mpLoginSet, mpSequenceName=&mpSequence);
   %end;
   %else %do;
		%unique_ids (mpIdCount=&lmvInCount, mpOut=&lmvNewIdsTable, mpLoginSet=&mpLoginSet, mpSequenceName=&mpSequence);
   %end;

   /* Создаем выходной набор */
   data &mpOut;
      set &mpIn;
	  
   %if &mpKeyType = 1 %then %do; 
		&mpOutFieldId = &lmvOutFieldValue;
   %end;
   %else %do;	
      set &lmvNewIdsTable (
         keep= OBJECT_ID
         rename= (OBJECT_ID = &mpOutFieldId)
      );
   %end;
	  
   run;
   %error_check;

   /* Очистка */
   %member_drop(&lmvNewIdsTable);
%mend etl_generate_unique_ids;
