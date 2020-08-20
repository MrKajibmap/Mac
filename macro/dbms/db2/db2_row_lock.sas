/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 0297bac00cda42f110ae9b324c9a5b3ab11d86a4 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Открывает блокировку строки.
*     Может вызываться только внутри транзакции.
*
*  ПАРАМЕТРЫ:
*     mpTable                 +  имя DB2-таблицы в SAS
*     mpWhere                 +  уловие на отбор строки для лока
*
******************************************************************
*  Пример использования:
*     %db2_row_lock (mpTable=DWH_DDS.X_DDS_INVENTORY, mpWhere= table_nm='COUNTERPARTY');
*
******************************************************************
*  22-04-2015  Сазонов     Начальное кодирование
*  13-08-2019  Нестерёнок  Исключен mpLoginSet
******************************************************************/

%macro db2_row_lock (
   mpTable                 =  ,
   mpWhere                 =
);
   /* Получаем имена в СУБД */
   %local lmvTableDbms lmvLoginSet;
   %&ETL_DBMS._table_name (mpSASTable=&mpTable, mpOutFullNameKey=lmvTableDbms, mpOutLoginSetKey=lmvLoginSet);

   /* Создаем блокировку библиотекой */
   %db2_libref (mpLoginSet=&lmvLoginSet, mpLibref=lck,
      mpEngineOptions=
         defer=no
         dbconinit="select *
               from &lmvTableDbms
               where %unquote(&mpWhere)
               for read only with rs use and keep exclusive locks"
   );
%mend db2_row_lock;
