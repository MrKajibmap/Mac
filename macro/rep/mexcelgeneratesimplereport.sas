/**********************************************************************
Описание:
Макрос предназначен для генерации простого Excel-отчета на основе xml-шаблона отчета

Отчет обычно состоит из статической части (заголовки, легенда, шапка таблицы), и динамической части.
Динамической называем только ту часть отчета, в которой кол-во столбцов/строк может меняться в зависимости от данных
(обычно это тело таблицы). Ячейки, в которых необходимо только поменять значения, не размножая их,
относятся к статической части.

Простым называется тот отчет, динамическая часть которого получается простым размножением одной строки шаблона
(без объединения и дополнительного форматирования ячеек, т.е. все ячейки одного столбца будут иметь такое же
форматирование, как и соответствующая ячейка шаблонной строки).

Макрос принимает на вход шаблон, таблицу с данными для динамической части и таблицу с данными для замены значений
статических ячеек. Преобразовывает таблицу с данными к виду, принимаемому на вход макросом
%mExcelGenerateComplicatedReport и производит его вызов.

ПАРАМЕТРЫ:
   mpReportTemplateFile       -  файл шаблона отчета в формате XML Spreadsheet 2003 (либо путь в кавычках, либо fileref)
   mpSheetName                -  название листа отчета, на котором содержится шаблон, и на котором будет генерироваться отчет
   mpTemplateRowName          -  название строки в файле шаблона отчета, которая подлежит размножению
   mpReportDataTable          -  название таблицы, содержащей данные для динамической части
   mpParamDataTable           -  название таблицы, содержащей значения для замены значений статических полей
   mpOutfile                  -  выходной файл (либо fileref, либо путь в двойных кавычках)
   mpOutfileType              -  тип выходного файла: xml|xls. Значение по умолчанию: xls
   mpDebugFolder              -  путь к папке, в которой будут сохраняться промежуточные файлы (для отладки)
                                 Если не указан, промежуточные файлы сохраняются во временной директории
   mpDebugLib                 -  путь к библиотеке, в которой будут сохраняться промежуточные таблицы (для отладки).
                                 Если не указан, промежуточные таблицы формируются в библиотеке WORK, и удаляются
                                 по завершении работы макроса
   mpDeleteNamesFlg           -  Флаг удаления именований ячеек в конечном файле отчета (Y – удалять, N/пустое значение – не удалять)
                                 Значения по умолчанию:
                                    - Для mpOutfileType=xml: N
                                    - Для mpOutFileType=xls: Y
   mpDeleteWorksheets         -  Список листов, подлежащих удалению в конечном файле отчета (через ‘|’, без пробелов между названиями. Пример: TEMPLATE_WORK|Sheet 2).
                                 Если задано пустое значение, листы не удаляются
                                 Значения по умолчанию:
                                    - Для mpOutfileType=xml: пустое значение
                                    - Для mpOutFileType=xls: TEMPLATE_WORK
   mpResultVar                -  название макропеременной, в которой будет лежать код результата выполнения макроса
                                 Значение по умолчанию: RESULT
                                 Макропеременная может принимать следующие значения:
                                    -1 - некорректно заданы параметры
                                    -2 - не существуют файлы, заданные в параметрах
                                    -3 - при выполнении xsl не создался выходной файл
                                    -4 - отсутствует шаблон xsl
                                    значения>0 - системные ошибки SAS

**********************************************************************
ТРЕБОВАНИЯ К ШАБЛОНУ ОТЧЕТА mpReportTemplateFile:
**********************************************************************

Шаблон должен содержать:

 - все статические части отчета. Ячейками статической части, значения которых необходимо заменить при генерации отчета,
должны быть присвоены имена с помощью Named Manager. Эти ячейки должны обязательно содержать пример заполнения (главное,
чтобы они были заполнены, и значение было того же типа - строка/число/дата, что и значение в реальном отчета)

 - пример одной заполненной строки таблицы, расположенный с том месте, куда необходимо вставить динамическую часть.
Все ячейки строки должны быть отформатированы так, как должны быть отформатированы соответствующие генерируемые ячейки отчета,
и должны обязательно содержать примеры заполнения, тип которых совпадает со значением соответствующих ячеек отчета.
Каждой из этих ячеек должно быть присвоено имя с помощью Named Manager.
Всему шаблону динамической строки также должно быть присвоено имя, совпадающее со значением передаваемого в макрос параметра
mpTemplateRowName

**********************************************************************
ТРЕБОВАНИЯ К ТАБЛИЦЕ ДАННЫХ ДЛЯ ДИНАМИЧЕСКОЙ ОБЛАСТИ mpReportDataTable
**********************************************************************

Зерно таблицы - одна строка листа Excel.
Таблица должна содержать все поля, из которых состоит шаблон строки таблицы. Название полей таблицы
должно совпадать с названиями соответствующих ячеек Excel, заданных в шаблоне строки.

- числовые поля должны иметь формат вида 123456.025 (для пустого значения - пробел)
- поля типов "дата" и "время" должны быть преобразованы в DateTime (для времени при этом можно задать любой день)
- поля типа DateTime должны иметь формат вида 2015-03-05T00:00:00.000, который можно создать следующим кодом:

   picture xmldttm
      low-high = '%Y-%0m-%0dT%0H:%0M:%0S'  (DATATYPE=DATETIME)
      . = ' '
   ;

**********************************************************************
ТРЕБОВАНИЯ К ТАБЛИЦЕ ЗНАЧЕНИЙ СТАТИЧЕСКИХ ЯЧЕЕК
**********************************************************************

Таблица должна иметь структуру:
   attrib
      param_name  length=$64
      param_value length=$32000
   ;
где
 - param_name - название ячейки (Name), в которой необходимо заменить значение (регистр не важен)
 - param_value - значение, которое необходимо положить в ячейку, в формате:
   для строкового типа - обычная строка
   для числового типа - число в формате 1234.034
   для даты/времени - 2015-03-05T00:00:00.000 (для создания формата можно использовать picture: low-high = '%Y-%0m-%0dT%0H:%0M:%0S'  (DATATYPE=DATETIME))

*********************************************************************

Использует (внешние макросы):
   mExcelGenerateComplicatedReport
   mCheckFileExist
   mExcelGenerateComplicatedAttrib
   mXML2XlsDoc

*********************************************************************

Использует (xsl, xmlmap):
   см. вызываемый mExcelGenerateComplicatedReport

*********************************************************************

История:
05.03.2015 - Initial coding (Светлана Урюпина)
19.06.2015 - добавлена поддержка xlsx
22.06.2015 - добавлен параметр mpDebugLib

**********************************************************************/

%macro mexcelgeneratesimplereport(
    mpReportTemplateFile=
   ,mpSheetName=
   ,mpTemplateRowName=
   ,mpReportDataTable=
   ,mpParamDataTable=
   ,mpOutfile=
   ,mpOutfileType=xls
   ,mpDebugFolder=
   ,mpDebugLib=
   ,mpDeleteNamesFlg=
   ,mpDeleteWorksheets=
   ,mpResultVar=RESULT
);

   %let mpLib=%kupcase(%kscan(&mpReportDataTable., -2));
   %IF %klength(&mpLib.)=0 %then %do;
      %let mpLib=WORK;
   %end;
   %let mpTable=%kupcase(%kscan(&mpReportDataTable., -1));

   %IF (%klength(&mpDebugLib.)=0) %THEN %DO;
      %let mvWorkLib=work;
   %END;
   %ELSE %DO;
      %let mvWorkLib=&mpDebugLib.;
   %END;

   proc sql noprint;;
      select ktrim(kleft(put(count(name), 10.)))
      into :var_n
      from dictionary.columns
      where
         libname="&mpLib."
         and memname = "&mpTable."
      ;
      select
         name
         ,type
         ,ifc(type='char', format, coalescec(format, 'best20.'))
      into
         :varName1-:varName&var_n.
         ,:varType1-:varType&var_n.
         ,:varFormat1-:varFormat&var_n.
      from dictionary.columns
      where
         libname="&mpLib."
         and memname = "&mpTable."
      ;
   quit;

   data &mvWorkLib.._report_data_table_t (keep=
      Row_Ordinal
      Row_Index
      Cell_ORDINAL
      Cell_StyleName
      Cell_MergeDown
      Cell_MergeAcross
      Cell_Index
      Cell_ArrayRange
      Cell_Formula
      Cell_Href
      Cell_HRefScreenTip
      Comment_Author
      Comment_ShowAlways
      Comment_Data
      Data
      NamedCell_Name
   );

      %mExcelGenerateComplicatedAttrib;


      set &mpReportDataTable.;

      Row_Ordinal=_N_;

      retain Cell_ORDINAL 0;

      %do i=1 %to &var_n.;
         NamedCell_name="&&varName&i.";

         %IF (&&varType&i.=char) %THEN %DO;
            Data=&&varName&i.;
         %END;
         %ELSE %DO;
            Data=strip(put(&&varName&i., &&varFormat&i.));
         %END;

         Cell_ORDINAL+1;
         output;
      %end;
   run;

   %mExcelGenerateComplicatedReport(
      mpReportTemplateFile=&mpReportTemplateFile.
      ,mpSheetName=&mpSheetName.
      ,mpTemplateAreaName=&mpTemplateRowName.
      ,mpStaticCellsName=
      ,mpReportDataTable=&mvWorkLib.._report_data_table_t
      ,mpParamDataTable=&mpParamDataTable.
      ,mpOutfile=&mpOutfile.
      ,mpOutfileType=&mpOutfileType.
      ,mpDebugFolder=&mpDebugFolder.
      ,mpDebugLib=&mpDebugLib.
      ,mpDeleteNamesFlg=&mpDeleteNamesFlg.
      ,mpDeleteWorksheets=&mpDeleteWorksheets.
      ,mpResultVar=&mpResultVar.
   );

   %IF (%klength(&mpDebugLib.)=0) %THEN %DO;
      proc datasets lib=&mvWorkLib. nowarn nolist;
         delete
            _report_data_table_t
         ;
      run;
      quit;
   %END;
%mend;