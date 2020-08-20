/*****************************************************************
Описание:
Макрос производит проверку существования заданого файла, и возвращает результат в заданную макропеременную.

Параметры:
   mpFile         -  файл, существование которого необходимо проверить (fileref или "Путь в кавычках")
   mpResultVar    -  название макропеременной, в которую будет возвращен результат проверки.
                     Значение по умолчанию=RESULT

Возвращаемое значение:
   1 - файл существует
   0 - файл не существует

Примеры использования:
%mCheckFileExist(mpFile=reptmpl, mpResultVar=MYRESULT);
%mCheckFileExist(mpFile="C:\Project\Uryupina\Data\Templates\xsl\XSLExcelExtractTableBySheetName.xsl");

История:
05-03-2015  Урюпина        Начальное кодирование
*****************************************************************/

%macro mCheckFileExist(mpFile=, mpResultVar=RESULT) / minoperator;
   %local mvErrmsg;
   %let mvErrmsg=;

   %IF %klength(&mpFile.)=0 %THEN %DO;
      %let mvErrmsg=%str(ERROR:Не задан параметр mpFile);
      %goto EXIT_MACRO;
   %END;

   %IF not(%symexist(&mpResultVar.)) %THEN %DO;
      %GLOBAL &mpResultVar.;
   %END;

   %IF not(%kindex(&mpFile., %str(%")) or %kindex(&mpFile., %str(%')))
      and %klength(&mpFile.)<=8 %THEN %DO;
      *считаем, что это fileref;
      %let &mpResultVar.=%sysfunc(fexist(&mpFile.));
   %END;
   %ELSE %DO;
      *считаем, что это путь;
      %let &mpResultVar.=%sysfunc(fileexist(&mpfile.));
   %END;

   %EXIT_MACRO: %put &mvErrmsg.;
%mend;
