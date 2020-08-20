/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 9b36e6680cbf48c9ba49763c432adc9f1bcfbb3a $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Генерирует mpIdCount уникальных числовых значений (идентификаторов), и сохраняет их в таблицу mpOut.
*     Если указан дополнительный параметр mpSequenceName, то идентификаторы берутся из одноименного ETL_DBMS sequence.
*     Если mpOut существует, он перезаписывается.
*     Выходная таблица mpOut содержит следующие поля:
*        OBJECT_ID (INTEGER) - уникальный идентификатор
*        ORDER_NO (INTEGER) - порядковый номер идентификатора, начиная с 1
*     и отсортирована по возрастанию ORDER_NO.
*     Работает в глобальном режиме или внутри PROC SQL.
*
*  ПАРАМЕТРЫ:
*     mpIdCount               +  требуемое кол-во идентификаторов, должно быть целым и больше 0
*     mpOut                   +  имя выходной таблицы
*     mpSequenceName          -  необязателен, имя ETL_DBMS sequence для получения очередного идентификатора
*     mpLoginSet              -  необязателен, может использоваться только если указано mpSequenceName.
*                                По умолчанию ETL_SYS
*
******************************************************************
*  Использует:
*     %ETL_DBMS_connect (в случае указания mpSequenceName)
*     указанный sequence
*     %member_drop
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*      1) генерирует 20 идентификаторов и кладет их в таблицу work.aaa
*      %unique_ids (mpIdCount=20, mpOut=work.aaa);
*      2) берет 5 идентификаторов sequence из ETL_SYS.ETL_DUMMY_SEQ и кладет их в таблицу work.bbb
*      %unique_ids (mpSequenceName=ETL_DUMMY_SEQ, mpIdCount=5, mpOut=work.bbb);
*
******************************************************************
*  10-05-2012  Нестерёнок     Начальное кодирование
*  31-08-2012  Нестерёнок     Рефактор mpMode
*  15-04-2014  Нестерёнок     Изменен способ генерации UID
*  30-01-2015  Сазонов        SQL вызов вынесен в dbms specific
******************************************************************/

%macro unique_ids (mpIdCount=, mpOut=, mpSequenceName=, mpLoginSet=ETL_SYS);
  /* Проверяем корректность параметров */
   %if %is_blank(mpOut) %then %do;
      %log4sas_error (cwf.macro.unique_ids, Incorrect parameter mpOut value.);
      %return;
   %end;
   %if %is_blank(mpIdCount) %then %do;
      %log4sas_error (cwf.macro.unique_ids, Incorrect parameter mpIdCount value.);
      %return;
   %end;
   %if &mpIdCount le 0 %then %do;
      %log4sas_error (cwf.macro.unique_ids, Specify positive id count.);
      %return;
   %end;

   /* Инициализация */
   %member_drop (&mpOut);

   %if not %is_blank(mpSequenceName) %then %do;
     /* Генерируем ID для новых объектов через sequence */
     %&ETL_DBMS._get_seq_vals(mpIdCount=&mpIdCount, mpOut=&mpOut, mpSequenceName=&mpSequenceName, mpLoginSet=&mpLoginSet);
   %end;
   %else %do;
      data &mpOut (sortedby= ORDER_NO);
         length OBJECT_ID ORDER_NO 8;
         do ORDER_NO = 1 to &mpIdCount;
            OBJECT_ID = %substr(%sysfunc(ranuni(0))123456789, 3, 10) + ORDER_NO - 1;
            output;
         end;
      run;
   %end;
%mend unique_ids;