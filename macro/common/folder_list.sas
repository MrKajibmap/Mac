/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 20765ee7aa3a3790b1c90919470cc66f1883e59e $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Возвращает список файлов в папке.
*     Порядок произвольный.
*
*  ПАРАМЕТРЫ:
*     mpInFolderName          -  имя папки поиска
*                                Если не указано, mpInFolderRef должен быть уже назначен
*     mpInFolderRef           +  fileref папки поиска
*                                по умолчанию _fin
*     mpFilter                -  фильтр отбора файлов, regexp
*                                по умолчанию .*
*     mpOut                   +  выходная таблица, содержит поле file_nm ($200)
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
*     %folder_list(mpInFolderName="/", mpOut=root_list);
*     %folder_list(mpInFolderName="&ETL_FILE_INPUT_ROOT/CF1", mpFilter=".csv$", mpOut=cf1_list);
*
******************************************************************
*  15-08-2014  Нестерёнок     Начальное кодирование
******************************************************************/

%macro folder_list (
   mpInFolderName          =  "",
   mpInFolderRef           =  "_fin",
   mpFilter                =  ".*",
   mpOut                   =
);
   data &mpOut;
      /* Инициализация */
      length did i 8 file_nm $200;
      keep file_nm;

      if lengthn(&mpInFolderName) gt 0 then do;
         rc = filename(&mpInFolderRef,  &mpInFolderName,  "DISK");
         if rc ne 0 then do;
            rc = log4sas_error ("cwf.macro.folder_list", catx (" ", "Не удалось открыть папку", &mpInFolderName));
            goto exit;
         end;
      end;

      /* Чтение файлов */
      did = dopen(&mpInFolderRef);
      if did > 0 then do;
         do i=1 to dnum(did);
            file_nm = dread(did, i);
            if prxmatch(cats("/", &mpFilter, "/o"), strip(file_nm)) then output;
         end;
         did = dclose(did);
      end;
      else do;
         rc = log4sas_error ("cwf.macro.folder_list", sysmsg());
      end;

   exit:
        stop;
   run;
%mend folder_list;