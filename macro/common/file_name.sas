﻿/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 04df09ef9361cf13a2333eec0941fb7b3502e544 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Возвращает имя файла из полного пути.
*
*  ПАРАМЕТРЫ:
*     mpFullPath              +  полное имя файла
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
*     %put Log file name is: %file_name (mpFullPath=%log_location);
*
******************************************************************
*  18-06-2015  Нестерёнок     Начальное кодирование
******************************************************************/

%macro file_name (
   mpFullPath=
);
   %do;%scan(%superq(mpFullPath), -1, %str(/\))%end;
%mend file_name;