/**********************************************************************
Описание:
Макрос предназначен для генерации сложного Excel-отчета на основе xml-шаблона отчета.

Отчет обычно состоит из статической части (заголовки, легенда, шапка таблицы), и динамической части.
Динамической называем только ту часть отчета, в которой кол-во столбцов/строк может меняться в зависимости от данных
(обычно это тело таблицы). Ячейки, в которых необходимо только поменять значения, не размножая их,
относятся к статической части.

Сложным называется тот отчет, динамическую часть которого невозможно сгенерировать простым размножением строки.
Например, она содержит объединенные по горизонтали ячейки, и кол-во объединенных ячеек не является константой,
а зависит от данных. Либо содержит объединенные по вертикали ячейки. Либо форматирование ячейки зависит от данных.

Макрос
1) принимает на вход шаблон, таблицу с данными для динамической части, таблицу с данными для замены значений
статических ячеек
2) вырезает из шаблона строки, содержащие шаблонные ячейки динамической части
3) генерирует динамическую часть на основе заданной таблицы с данными, используя форматирование, заданное в шаблоне
4) вставляет динамическую часть в шаблон
5) заменяет значения статических ячеек на основе соответствующей заданной таблицы.

ПАРАМЕТРЫ:
   mpReportTemplateFile    -  файл шаблона отчета в формате XML Spreadsheet 2003 (либо путь в кавычках, либо fileref)
   mpSheetName             -  название листа отчета, на котором содержится шаблон, и на котором будет генерироваться отчет
   mpTemplateAreaName      -  название, которое присвоено динамической области отчета в шаблоне
   mpReportDataTable       -  название таблицы с данными для динамической области. Может быть незаполненным,
                              но в этом случае должен быть заполнен параметр mpParamDataTable. Описание структуры таблицы см. в ТП
   mpColumnTable           -  используется в случае, если необходимо изменить значение атрибутов колонок (доступные атрибуты: width, hidden)
                              Содержит путь к таблице, в которой заданы значения соответствующих атрибутов. Если не задан,
                              ширины столбцов берутся напрямую из шаблона (т.е 1-ый столбец отчета имеет ширину, равную ширине 1-ого столбца шаблона,
                              10-ый столбец отчета - ширину, равную ширине 10ого столбца шаблона и т.д.). Описание структуры таблицы см. в ТП
   mpParamDataTable        -  название таблицы для замены значений статических полей. Может быть незаполненным,
                              но в этом случае должен быть заполнен параметр mpReportDataTable. Описание структуры таблицы см. в ТП
   mpOutfile               -  выходной файл (либо fileref, либо путь в двойных кавычках)
   mpOutfileType           -  тип выходного файла: xml|xls|xlsx. Значение по умолчанию: xlsx
   mpDeleteNamesFlg        -  Флаг удаления именований ячеек в конечном файле отчета (Y – удалять, N/пустое значение – не удалять)
                              Значения по умолчанию:
                                 - Для mpOutfileType=xml: N
                                 - Для mpOutFileType=xls/xlsx: Y
   mpDeleteWorksheets      -  Список листов, подлежащих удалению в конечном файле отчета (через ‘|’, без пробелов между названиями.
                              Пример: TEMPLATE_WORK|Sheet 2).
                              Если задано пустое значение, листы не удаляются
                              Значения по умолчанию:
                                 - Для mpOutfileType=xml: пустое значение
                                 - Для mpOutFileType=xls/xlsx: TEMPLATE_WORK
   mpDebugFolder           -  путь к папке, в которой будут сохраняться промежуточные файлы (для отладки)
                              Если не указан, промежуточные файлы сохраняются во временной директории
   mpDebugLib              -  путь к библиотеке, в которой будут сохраняться промежуточные таблицы (для отладки).
                              Если не указан, промежуточные таблицы формируются в библиотеке WORK, и удаляются
                              по завершении работы макроса
   mpResultVar             -  название макропеременной, в которой будет лежать код результата выполнения макроса
                              Значение по умолчанию: RESULT
                              Макропеременная может принимать следующие значения:
                                  0 - отчет сформирован корректно
                                 -1 - некорректно заданы параметры (не заданы обязательные, или значение не входит в спискок возможных значений)
                                 -2 - не существуют файлы, заданные в параметрах
                                 -3 - ошибка выполнения xsl (не создался выходной файл)
                                 -4 - отсутствует шаблон xsl/xml
                                 -5 - некорректно задана входная таблица (таблица физически отсутствует либо имеет неверную структуру)
                                 -6 - заданный лист отсутствует в шаблоне отчета
                                 -7 - ячейки с заданной меткой отсутствуют на указанном листе шаблона отчета
                                 -8 - ошибка при преобразовании xml в xls/xlsx
                                 -9 - ошибка при копировании файла отчета в указанный выходной файл
                                 значения>0 - системные ошибки SAS

Обязательные параметры:
   mpReportTemplateFile
   mpSheetName
   mpOutfile
   либо mpParamDataTable, либо mpReportDataTalbe + mpTemplateAreaName

*********************************************************************

Использует (внешние макросы):
   member_vars_exist
   mCheckFileExist
   mExcelGenerateComplicatedAttrib
   mXML2XlsDoc

*********************************************************************

Использует (xsl, xmlmap):
   Макрос предполагает, что задана макропеременная TEMPLATE_PATH, в которой
   лежит путь к директории, содержащей следующие папки и файлы:

   Папка xmlmap с файлом:
      - xmlmapExcelTable.map

   Папка xsl c файлами:
      - XSLExcelGetStyles
      - XSLExcelGetCondFormatting

*********************************************************************

ОГРАНИЧЕНИЯ:
   1.    Если кодировка сессии - не UTF-8, то ни шаблон, ни данные не могут содержать
         символы UTF-8 (при формировании отчета возникнет ошибка)

*********************************************************************

История изменений:
31.03.2015 initial coding (Светлана Урюпина)
19.05.2015 исправление багов
10.06.2015 добавлена возможность использования статических ячеек внутри динамической области
         (доп.параметр mpStaticCellsName),
         также возможность удаления списка указанных листов (параметр mpDeleteElements
         заменен на mpDeleteNamesFlg + mpDeleteWorksheets
19.06.2015 добавлена возможность создания отчета в формате xlsx
22.06.2015 добавлен вывод более информативного сообщения при ошибке в расчете cell_index:
           выводятся координаты ячейки в исходных данных/в шаблоне, а не координаты в промежуточной таблице макроса
           Добавлен параметр mpDebugLib, и удаление временных таблиц при его отсутствии.
21.07.2015 основной proc sql заменен на merge и hash из-за нехватки памяти при сравнительно небольших кол-вах ячеек
31.07.2015 - изменено поведение при отсутствии в шаблоне ячейки с именем, заданным в данных (ранее выводилась пустая ячейка,
         теперь выводится ячейка с заданными данными, ячейке по умолчанию присваивается тип String)
         - Изменен принцип обработки ошибок при конвертации в xls/xlsx (ранее успешность отработки определялась
         наличием/отсутствием файла, теперь определяется по возвращаемому макросом конвертации коду)

12.08.2015 - Неверно учитывались пустые строки шаблона, не содержащие ни одной ячейки. Добавлено соответствующее
         условие в расчет таблицы _CELL_DATA
01.09.2015 - добавлен case insensitive на именования ячеек
22.09.2015 - добавлена возможность задания ширины колонок отчета и номеров скрываемых колонок через параметр mpColumnTable

*********************************************************************/

%macro mExcelGenerateComplicatedReport(
   mpReportTemplateFile=
   ,mpSheetName=
   ,mpTemplateAreaName=
   ,mpStaticCellsName=
   ,mpReportDataTable=
   ,mpColumnTable=
   ,mpParamDataTable=
   ,mpOutfile=
   ,mpOutfileType=xlsx
   ,mpDebugFolder=
   ,mpDebugLib=
   ,mpResultVar=RESULT
   ,mpDeleteNamesFlg=
   ,mpDeleteWorksheets=
) / minoperator;

%local mvErrMsg;
%let mvErrMsg=;

%IF not(%symexist(&mpResultVar.)) %THEN %DO;
   %GLOBAL &mpResultVar.;
%END;
%let &mpResultvar.=0;

%let mpTemplateAreaName=%kupcase(&mpTemplateAreaName.);
%let mpOutFileType=%kupcase(&mpOutFileType.);
%let mpDeleteNamesFlg = %kupcase(&mpDeleteNamesFlg.);
%let mpStaticCellsName = %kupcase(&mpStaticCellsName.);

%IF (&mpOutFiletype. IN (XLS XLSX)) %THEN %DO;
   %IF %kLENGTH(&mpDeleteNamesFlg.)=0 %THEN %DO;
      %let mpDeleteNamesFlg=Y;
   %END;
   %IF %kLENGTH(&mpDeleteWorksheets.)=0 %THEN %DO;
      %let mpDeleteWorksheets=TEMPLATE_WORK;
   %END;
%END;

%IF (%klength(&mpDebugLib.)=0) %THEN %DO;
   %let mvWorkLib=work;
%END;
%ELSE %DO;
   %let mvWorkLib=&mpDebugLib.;
%END;

/*************** проверка корректности задания параметров ***********/

%IF %klength(&mpReportTemplateFile.)=0 %THEN %DO;
   %let mvErrMsg=%bquote(ОШИБКА: Не задано значение входного параметра mpReportTemplateFile);
   %let &mpResultVar.=-1;
    %goto EXIT_MACRO;
%END;
%ELSE %DO;
   %mCheckFileExist(mpFile=&mpReportTemplateFile., mpResultVar=templateFileExist);
   %IF (&templateFileExist.=0) %THEN %DO;
      %let mvErrMsg=%bquote(ОШИБКА: Файл &mpReportTemplateFile. не существует);
      %let &mpResultVar.=-2;
      %goto EXIT_MACRO;
   %END;
%END;

%IF %klength(&mpSheetName.)=0 %THEN %DO;
   %let mvErrMsg=%bquote(ОШИБКА: Не задано значение входного параметра mpSheetName);
   %let &mpResultVar.=-1;
    %goto EXIT_MACRO;
%END;

%IF (%klength(&mpReportDataTable.)=0 and %klength(&mpParamDataTable.)=0) %THEN %DO;
   %let mvErrMsg=%str(ОШИБКА: Не заданы значения параметров mpReportDataTable и mpParamDataTable. Хотя бы один из них должен быть задан.);
   %let &mpResultVar.=-1;
   %goto EXIT_MACRO;
%END;

%IF (%klength(&mpReportDataTable.)>0 and %klength(&mpTemplateAreaName.)=0) %THEN %DO;
   %let mvErrMsg=%str(ОШИБКА: Параметр mpReportDataTable задан, а параметр mpTemplateAreaName не задан);
   %let &mpResultVar.=-1;
   %goto EXIT_MACRO;
%END;
%ELSE %IF (%klength(&mpReportDataTable.)=0 and %klength(&mpTemplateAreaName.)>0) %THEN %DO;
   %let mvErrMsg=%str(ОШИБКА: Параметр mpTemplateAreaName задан, а параметр mpReportDataTable не задан);
   %let &mpResultVar.=-1;
   %goto EXIT_MACRO;
%END;

%IF (%klength(&mpStaticCellsName.)>0 and %klength(&mpTemplateAreaName.)=0) %THEN %DO;
   %let mvErrMsg=%str(ОШИБКА: Параметр mpStaticCellsName задан, а параметр mpTemplateAreaName не задан);
   %let &mpResultVar.=-1;
   %goto EXIT_MACRO;
%END;

%IF (%klength(&mpOutfile.)=0) %THEN %DO;
   %let mvErrMsg=%str(ОШИБКА: Не задано значение входного параметра mpOutfile);
   %let &mpResultVar.=-1;
   %goto EXIT_MACRO;
%END;

%IF not(&mpOutFileType. in XLSX XLS XML) %THEN %DO;
   %let mvErrMsg=%bquote(ОШИБКА: Неверно задано значение параметра mpOutfileType=&mpOutfileType.. Параметр может принимать только следующие значения: xml, xls, xlsx);
   %let &mpResultVar.=-1;
    %goto EXIT_MACRO;
%END;

%IF %klength(&mpDebugFolder.)>0 %then %DO;
   %local debugFolderExist; %let debugFolderExist=;
   %mCheckFileExist(mpFile="&mpDebugFolder.", mpResultVar=debugFolderExist);
   %IF (&debugFolderExist.=0) %THEN %DO;
      %let mvErrMsg=%bquote(ОШИБКА: Директория &mpDebugFolder. не существует);
      %let &mpResultVar.=-2;
       %goto EXIT_MACRO;
   %END;
%END;
%ELSE %DO;
   %let mpDebugFolder=%sysfunc(pathname(work));
%END;

/***************** Проверка структуры входных таблиц **********************/

%IF (%klength(&mpReportDataTable.)>0) %THEN %DO;
   %IF not %member_vars_exist(
      mpData=  &mpReportDataTable.,
      mpVars=  Row_Ordinal Cell_ORDINAL Cell_StyleName Cell_MergeDown Cell_MergeAcross Cell_Index Cell_ArrayRange Cell_Formula Cell_Href Cell_HRefScreenTip
               Comment_Author Comment_ShowAlways Comment_Data Data NamedCell_Name
   ) %THEN %DO;
      %let mvErrMsg=%str(ОШИБКА: Неверная структура &mpReportDataTable.);
      %let &mpResultVar.=-5;
   %END;
%END;
%ELSE %DO;
   %let mpReportDataTable=work._reportDataTable;
   data &mpReportDataTable.;
      %mExcelGenerateComplicatedAttrib;
      stop;
   run;
%END;

%IF (%klength(&mpParamDataTable.)>0) %THEN %DO;
   %IF not %member_vars_exist(
      mpData=  &mpParamDataTable.,
      mpVars=  param_name param_value
   ) %THEN %DO;
      %let mvErrMsg=%str(ОШИБКА: Неверная структура &mpParamDataTable.);
      %let &mpResultVar.=-5;
   %END;
%END;

%IF (%klength(&mpColumnTable.)>0) %THEN %DO;
   %IF not %member_vars_exist(
      mpData=  &mpColumnTable.,
      mpVars=  Column_Index Column_Width Column_Hidden
   ) %THEN %DO;
      %let mvErrMsg=%str(ОШИБКА: Неверная структура &mpColumnTable.);
      %let &mpResultVar.=-5;
   %END;
%END;
/********************* ВСПОМОГАТЕЛЬНЫЕ МАКРОСЫ **********************/

/*Вытаскивает из xml-шаблона спискок id и названий стилей и записывает их в таблицу*/
%macro mExcelGetStyles (mpReportTemplateFile=, mpOutTable=);

   filename _styles TEMP;
   filename _xsltplt "&TEMPLATE_PATH/xsl/XSLExcelGetStyles.xsl";

   %IF not %sysfunc(fexist(_xsltplt)) %THEN %DO;
      %let &mpResultVar.=-4;
      %let mvErrMsg=%bquote(ОШИБКА: файл &TEMPLATE_PATH/xsl/XSLExcelGetStyles.xsl не существует);
      %goto EXIT_MACRO;
   %END;

   proc xsl
      in=&mpReportTemplateFile.
      xsl=_xsltplt
      out=_styles
   ;
   run;

   %IF not %sysfunc(fexist(_styles)) %THEN %DO;
      %let &mpResultVar.=-3;
      %let mvErrMsg=%str(ERROR in XSL: файл styles не был создан);
      %goto EXIT_MACRO;
   %END;

   data &mpOutTable.;
      attrib
         style_ID    length=$64
         style_Name  length=$64
      ;
      infile _styles encoding='UTF-8' dlm=';' missover;

      input style_ID $ style_Name $;

      if klength(style_Name)>0 then do;
         style_Name=kupcase(style_Name);
         output;
      end;
   run;

   %put Таблица стилей &mpOutTable. была успешно создана;

   %EXIT_MACRO:
   filename _styles clear;
   filename _xsltplt clear;
%mend;

/*вытаскивает из листа шаблона кол-во строк и колонок, к которым применен conditional formatting*/
%macro mExcelGetCondFormatting(mpReportTemplateFile=, mpWorkSheetName=, mpColumnCountVar=, mpRowCountVar=);
   %if not(%symexist(&mpColumnCountVar.)) %THEN %DO;
      %GLOBAL &mpColumnCountVar.;
   %END;
   %if not(%symexist(&mpRowCountVar.)) %THEN %DO;
      %GLOBAL &mpRowCountVar.;
   %END;

   filename _cond TEMP;
   filename _xsltplt "&TEMPLATE_PATH/xsl/XSLExcelGetCondFormatting.xsl";
   filename _xsltmp TEMP;

   %IF not %sysfunc(fexist(_xsltplt)) %THEN %DO;
      %let &mpResultVar.=-4;
      %let mvErrMsg=%bquote(ОШИБКА: файл &TEMPLATE_PATH/xsl/XSLExcelGetCondFormatting.xsl не существует);
      %goto EXIT_MACRO;
   %END;

   data _null_;
      infile _xsltplt _infile_=cur_str encoding='utf-8' lrecl=32000;
      file _xsltmp encoding='utf-8' lrecl=32000;

      input;

      cur_str=tranwrd(cur_str, '&mpWorkSheetName.', "&mpWorkSheetName.");
      put cur_str;
   run;

   proc xsl
      in=&mpReportTemplateFile.
      xsl=_xsltmp
      out=_cond
   ;
   run;

   %IF not %sysfunc(fexist(_cond)) %THEN %DO;
      %LET &mpColumnCountVar.=0;
      %LET &mpRowCountVar.=0;
   %end;
   %ELSE %DO;
      data _null_;
         length
            range_str range_from range_to $100
            y_rx x_rx 8
         ;
         retain x_max y_max y_rx x_rx 0;

         infile _cond encoding='UTF-8' missover  end=last_rec;

         input range_str $ ;

         if _N_=1 then do;
            y_rx = prxparse("/R(\d+)/");
            x_rx = prxparse("/C(\d+)/");
         end;

         range_from=kscan(range_str, 1, ':');
         range_to=kscan(range_str, 2, ':');

         if (prxmatch(y_rx, range_from)) then do;
            y_from = input(prxposn(y_rx, 1, range_from), best12.);
         end;
         if (prxmatch(x_rx, range_from)) then do;
            x_from = input(prxposn(x_rx, 1, range_from), best12.);
         end;

         if (prxmatch(y_rx, range_to)) then do;
            y_to = input(prxposn(y_rx, 1, range_to), best12.);
         end;
         if (prxmatch(x_rx, range_to)) then do;
            x_to = input(prxposn(x_rx, 1, range_to), best12.);
         end;

         x_max=max(x_max, x_from, x_to);
         y_max=max(y_max, y_from, y_to);

         if (last_rec) then do;
            call symput("&mpColumnCountVar.", ktrim(kleft(put(x_max, best12.))));
            call symput("&mpRowCountVar.", ktrim(kleft(put(y_max, best12.))));
         end;
      run;
   %end;
   %put %str(Количество строк и столбцов, используемых в тегах ConditionalFormatting было успешно рассчитано);
   %put Кол-во столбцов=&&&mpColumnCountVar. Кол-во строк=&&&mpRowCountVar.;

   %EXIT_MACRO:
   filename _cond clear;
   filename _xsltplt clear;
%mend;

/*Вытаскивает из таблицы xml-шаблона, расположенной на указанном листе, кол-во
колонок, перечисленных в тегах Column*/

/*
вырезает из xml содержимое тега Table (включая сам тег), расположенного на заданном листе.
вставляет на место содержимого тега Table (включая сам тег) метку вида
<InsertContentHere>&mpMarkName</InsertContentHere>

mpOutTableFile - файл с содержимым тега Table
mpOutReplacedFile - файл с содержимым шаблона, в котором тег Table заменен на метку
*/
%macro mExcelExtractTableContent(
    mpReportTemplateFile=
   ,mpWorkSheetName=
   ,mpMarkName=
   ,mpOutTemplateContent=
   ,mpOutTemplateAreaMarked=
);
data _null_;
   infile &mpReportTemplateFile. encoding="UTF-8" lrecl=32000 _INFILE_=cur_str end=last_rec;

   file &mpOutTemplateAreaMarked. encoding="UTF-8" lrecl=32000;
   input;
   do until (prxmatch("/Worksheet.*?&mpWorkSheetName./i", cur_str));
      put cur_str;
      input;
   end;
   do while (not kindex(cur_str, '<Table'));
      put cur_str;
      input;
   end;
   put "<InsertContentHere>TABLE</InsertContentHere>";

   file &mpOutTemplateContent. encoding="UTF-8" lrecl=32000;
   cur_str = prxchange(
      's/(Table)(\s*)(.*)/$1' ||
      ' xmlns="urn:schemas-microsoft-com:office:spreadsheet"' ||
      ' xmlns:o="urn:schemas-microsoft-com:office:office"' ||
      ' xmlns:x="urn:schemas-microsoft-com:office:excel"' ||
      ' xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet"' ||
      ' xmlns:html="http:\/\/www\.w3\.org\/TR\/REC-html40"' ||
      ' $3/', 1, cur_str);
   put cur_str;
   do until (kindex(cur_str, '</Table>'));
      input;
      /*маскируем теги B, Font, I, S, Span, Sub, Sup, U, иначе xml некорректно считается через xmlmap*/
      length tag $10;
      do tag='B', 'Font', 'I', 'S', 'Span', 'Sub', 'Sup', 'U';
         cur_str=tranwrd(cur_str, "<" || ktrim(kleft(tag)), '&lt;' || ktrim(kleft(tag)));
         cur_str=tranwrd(cur_str, "<" || ktrim(kleft(tag)) || ">", '&lt;' || ktrim(kleft(tag)) || '&gt;');
         cur_str=tranwrd(cur_str, "</"|| ktrim(kleft(tag)) || ">", '&lt;/'|| ktrim(kleft(tag)) || '&gt;');
      end;
      put cur_str;
   end;

   file &mpOutTemplateAreaMarked. encoding="UTF-8" lrecl=32000;
   do until (last_rec);
      input;
      put cur_str;
   end;

   run;

   %put %bquote(Содержимое тэга Table было успешно записано в файл &mpOutTemplateContent.);
   %put %bquote(Файл &mpOutTemplateAreaMarked. с меткой &mpMarkName. на месте тэга Table был успешно создан);
%mend;

/*заменяет метку вида
<InsertContentHere>&mpMarkName</InsertContentHere>
на содержимое файла mpContentPath
*/
%macro mExcelReplaceMarkWithContent(
    mpReportTemplateFile=
   ,mpMarkName=
   ,mpContentFile=
   ,mpOutFile=
);

   data _null_;
      file &mpOutFile. encoding='UTF-8' lrecl=32000;

      infile &mpReportTemplateFile. encoding='UTF-8' lrecl=32000;
      input;
      do while(not(prxmatch("/\<InsertContentHere.*&mpMarkName.*\<\/InsertContentHere\>/i", _INFILE_)));
         put _INFILE_;
         input;
      end;

      infile &mpContentFile. encoding='UTF-8' lrecl=32000 end=last_content_rec;
      do until(last_content_rec);
         input;
         put _INFILE_;
      end;

      infile &mpReportTemplateFile. encoding='UTF-8' lrecl=32000 end=last_tmplt_rec;
      do until(last_tmplt_rec);
         input;
         put _INFILE_;
      end;
   run;

   %put %bquote(Сгенерированное содержимое тэга Table было успешно записано на место метки в файл &mpOutFile.);

%mend;

/*генерация тега Table и его содержимого по данным
на вход подаются таблицы mpTableIntable, mpColumnIntable, mpRowIntable, mpCellIntable, mpNamedCellIntable
имеющие структуру, совпадающую с xmlmap xmlmapExcelTable
*/
%macro mExcelGenerateTable(
   mpTableIntable=
   ,mpColumnIntable=
   ,mpCellIntable=
   ,mpNamedCellIntable=
   ,mpOutFile=
);

   %macro mExcelPutAttr(mpAttrName=, mpAttrField=, mpIsOptional=Y, mpEncodeFlg=N);
      %IF (&mpIsOptional.=Y) %THEN %DO;
         if not missing(&mpAttrField.) then
      %END;
      do;
         %IF (&mpEncodeFlg.=Y) %THEN %DO;
            &mpAttrField.=htmlencode(&mpAttrField., 'amp gt lt apos quot');
         %END;
         put " &mpAttrName.=""" &mpAttrField. +(-1) '"' @;
      end;
   %mend;

   data _null_;
      set &mpTableIntable.;
      file &mpOutFile. encoding='UTF-8' lrecl=32000;
      put "<Table" @;
      %mExcelPutAttr(mpAttrName=%str(ss:DefaultColumnWidth), mpAttrField=Table_DefaultColumnWidth);
      %mExcelPutAttr(mpAttrName=%str(ss:DefaultRowHeight), mpAttrField=Table_DefaultRowHeight);
      %mExcelPutAttr(mpAttrName=%str(ss:ExpandedColumnCount), mpAttrField=Table_ExpandedColumnCount);
      %mExcelPutAttr(mpAttrName=%str(ss:ExpandedRowCount), mpAttrField=Table_ExpandedRowCount);
      %mExcelPutAttr(mpAttrName=%str(ss:LeftCell), mpAttrField=Table_LeftCell);
      %mExcelPutAttr(mpAttrName=%str(x:FullColumns), mpAttrField=Table_FullColumns);
      %mExcelPutAttr(mpAttrName=%str(x:FullRows), mpAttrField=Table_FullRows);
      put ">";
   run;

   proc sort data=&mpColumnIntable.; by Column_ORDINAL; run;
   data _null_;
      set &mpColumnIntable.;
      by Column_ORDINAL;

      file &mpOutFile. mod encoding='UTF-8' lrecl=32000;
      put "<ss:Column" @;
      %mExcelPutAttr(mpAttrName=%str(ss:AutoFitWidth), mpAttrField=Column_AutoFitWidth);
      %mExcelPutAttr(mpAttrName=%str(ss:Hidden), mpAttrField=Column_Hidden);
      %mExcelPutAttr(mpAttrName=%str(ss:Index), mpAttrField=Column_Index);
      %mExcelPutAttr(mpAttrName=%str(ss:Span), mpAttrField=Column_Span);
      %mExcelPutAttr(mpAttrName=%str(ss:StyleID), mpAttrField=Column_StyleID);
      %mExcelPutAttr(mpAttrName=%str(ss:Width), mpAttrField=Column_Width);
      put "/>";
   run;

   proc sql noprint;
      create table &mvWorkLib.._rows_table as
      select
          CELL.Row_ORDINAL
         ,CELL.Row_AutoFitHeight
         ,CELL.Row_Height
         ,CELL.Row_Index
         ,CELL.Row_StyleID
         ,CELL.Row_Span
         ,CELL.Row_Hidden
         ,CELL.Cell_ArrayRange
         ,CELL.Cell_Formula
         ,CELL.Cell_Href
         ,CELL.Cell_HRefScreenTip
         ,CELL.Cell_Index
         ,CELL.Cell_MergeAcross
         ,CELL.Cell_MergeDown
         ,CELL.Cell_ORDINAL
         ,CELL.Cell_StyleID
         ,CELL.Comment_Author
         ,CELL.Comment_Data
         ,CELL.Comment_Data_Xmlns
         ,CELL.Comment_ShowAlways
         ,CELL.Data
         ,CELL.Data_Ticked
         ,CELL.Data_Type
         ,CELL.Data_Xmlns
         ,CELL.empty_row_flg
         ,NamedCell_Name
         ,NamedCell_ORDINAL
      from &mpCellIntable. cell
         left join &mpNamedCellIntable. nc
            on cell.cell_ordinal=nc.cell_ordinal
      order by
          cell.row_ordinal
         ,cell.cell_ordinal
         ,nc.namedcell_ordinal
      ;
   quit;

   data _Null_;
      file &mpOutFile. mod encoding='UTF-8' lrecl=32000;
      set &mvWorkLib.._rows_table;
      by row_ordinal cell_ordinal namedcell_ordinal;

      retain rx_amp rx_nl rx_quote rx_apos;

      if _N_=1 then do;
         expr_amp    = 's/&(?!gt;|lt;|quote;|apos;|amp;|#10;)/&amp;/i';
         expr_nl  = 's/(\x0A)(?![^<]*>)/&#10;/';
         expr_quote  = 's/"(?![^<]*>)/&quot;/';
         expr_apos   = 's/''(?![^<]*>)/&apos;/';

         rx_amp      = prxparse(expr_amp);
         rx_nl       = prxparse(expr_nl);
         rx_quote    = prxparse(expr_quote);
         rx_apos  = prxparse(expr_apos);
      end;

      if first.row_ordinal then do;
         put "<ss:Row" @;
         %mExcelPutAttr(mpAttrName=%str(ss:AutoFitHeight), mpAttrField=Row_AutoFitHeight);
         %mExcelPutAttr(mpAttrName=%str(ss:Height), mpAttrField=Row_Height);
         %mExcelPutAttr(mpAttrName=%str(ss:Hidden), mpAttrField=Row_Hidden);
         %mExcelPutAttr(mpAttrName=%str(ss:Index), mpAttrField=Row_Index);
         %mExcelPutAttr(mpAttrName=%str(ss:Span), mpAttrField=Row_Span);
         %mExcelPutAttr(mpAttrName=%str(ss:StyleID), mpAttrField=Row_StyleID);
         put ">";
      end;

      if (cell_ordinal ne . and empty_row_flg ne 'Y') then do;
         if first.cell_ordinal then do;
         put "<ss:Cell" @;
            %mExcelPutAttr(mpAttrName=%str(ss:ArrayRange), mpAttrField=Cell_ArrayRange, mpEncodeFlg=Y);
            %mExcelPutAttr(mpAttrName=%str(ss:Formula), mpAttrField=Cell_Formula, mpEncodeFlg=Y);
            %mExcelPutAttr(mpAttrName=%str(ss:HRef), mpAttrField=Cell_Href, mpEncodeFlg=Y);
            %mExcelPutAttr(mpAttrName=%str(ss:Index), mpAttrField=Cell_Index);
            %mExcelPutAttr(mpAttrName=%str(ss:MergeAcross), mpAttrField=Cell_MergeAcross);
            %mExcelPutAttr(mpAttrName=%str(ss:MergeDown), mpAttrField=Cell_MergeDown);
            %mExcelPutAttr(mpAttrName=%str(ss:StyleID), mpAttrField=Cell_StyleID);
            %mExcelPutAttr(mpAttrName=%str(x:HRefScreenTip), mpAttrField=Cell_HRefScreenTip, mpEncodeFlg=Y);
            put ">";

            if (Comment_Author ne ' ' or Comment_ShowAlways ne . or Comment_Data ne ' ') then do;
               put "<ss:Comment" @;
               %mExcelPutAttr(mpAttrName=%str(ss:Author), mpAttrField=Comment_Author, mpEncodeFlg=Y);
               %mExcelPutAttr(mpAttrName=%str(ss:ShowAlways), mpAttrField=Comment_ShowAlways);
               put ">";
               if (comment_data ne ' ') then do;
                  comment_data = prxchange(rx_amp, -1, comment_data);
                  comment_data = prxchange(rx_nl, -1, comment_data);
                  comment_data = prxchange(rx_quote, -1, comment_data);
                  comment_data = prxchange(rx_apos, -1, comment_data);

                  if (Comment_Data_Xmlns eq '') then do;
                     comment_data = tranwrd(comment_data, '<', '&lt;');
                     comment_data = tranwrd(comment_data, '>', '&gt;');
                  end;

                  put "<ss:Data";
                  %mExcelPutAttr(mpAttrName=%str(xmlns), mpAttrField=Comment_Data_Xmlns);
                  put  ">" comment_data +(-1) "</Data>";
               end;
               put "</ss:Comment>";
            end;

            if (Data ne ' ' /*and Data_Type ne ' '*/) then do;
               Data = prxchange(rx_amp, -1, Data);
               Data = prxchange(rx_nl, -1, Data);
               Data = prxchange(rx_quote, -1, Data);
               Data = prxchange(rx_apos, -1, Data);
               Data_Type = coalescec(Data_Type, 'String');

               if (Data_Xmlns eq '') then do;
                  Data = tranwrd(Data, '<', '&lt;');
                  Data = tranwrd(Data, '>', '&gt;');
               end;

               put "<ss:Data" @;
               %mExcelPutAttr(mpAttrName=%str(ss:Type), mpAttrField=Data_Type, mpIsOptional=N);
               %mExcelPutAttr(mpAttrName=%str(x:Ticked), mpAttrField=Data_Ticked);
               %mExcelPutAttr(mpAttrName=%str(xmlns), mpAttrField=Data_Xmlns);
               put ">" Data +(-1) "</ss:Data>";
            end;
         end;

         if (NamedCell_Name ne ' ') then do;
            put "<ss:NamedCell" @;
               %mExcelPutAttr(mpAttrName=%str(ss:Name), mpAttrField=NamedCell_Name, mpIsOptional=N, mpEncodeFlg=Y);
            put "/>";
         end;

         if last.cell_ordinal then do;
            put "</ss:Cell>";
         end;
      end;

      if last.row_ordinal then do;
         put "</ss:Row>";
      end;
   run;

   data _null_;
      file &mpOutFile. mod encoding='UTF-8' lrecl=32000;
      put "</Table>";
   run;
%mend;

/********************* ОСНОВНОЙ МАКРОС **********************/

/*загружаем список стилей из xml-шаблона отчета в таблицу*/
%mExcelGetStyles (
   mpReportTemplateFile=&mpReportTemplateFile.
   ,mpOutTable=&mvWorkLib.._style_list
);
%IF (&&&mpResultVar. ne 0) %THEN %DO;
   %goto EXIT_MACRO;
%END;

/***************************** Считывание xml-шаблона отчета в таблицы*************************************/

/*вырезаем из шаблона все содержимое тега Table для последующей загрузки через xmlmap
и вставляем вместо вырезанного куска метку, которая
в дальнейшем будет заменена на сгенерированный кусок xml*/
%mExcelExtractTableContent(
    mpReportTemplateFile=&mpReportTemplateFile.
   ,mpWorkSheetName=&mpSheetName.
   ,mpMarkName=TABLE
   ,mpOutTemplateContent="&mpDebugFolder./template_content.xml"
   ,mpOutTemplateAreaMarked="&mpDebugFolder./template_area_marked.xml"
);

/*загружаем вырезанный кусок xml (все содержимое тега Table) в таблицы*/
filename  _reptmpl "&mpDebugFolder./template_content.xml" encoding="UTF-8";
filename  _SXLEMAP "&TEMPLATE_PATH./xmlmap/xmlmapExcelTable.map" encoding="UTF-8";

%IF not %sysfunc(fexist(_SXLEMAP)) %THEN %DO;
   %let &mpResultVar.=-4;
   %let mvErrMsg=%bquote(ОШИБКА: Файл &TEMPLATE_PATH/xmlmap/xmlmapExcelTable.map не существует);
   %goto EXIT_MACRO;
%END;

libname _reptmpl xmlv2 xmlmap=_SXLEMAP;

proc sql noprint;
   select count(*)=0 INTO :mvTemplateEmptyFlg from _reptmpl.cell;
quit;
%IF (&mvTemplateEmptyFlg.) %THEN %DO;
   %let &mpResultVar.=-6;
   %let mvErrMsg=%bquote(ОШИБКА: лист &mpSheetName. отсутствует в шаблоне отчета &mpReportTemplateFile.);
   %goto EXIT_MACRO;
%END;

/*объединяем row, cell, named_cell для удобной манипуляции в дальнейшем*/
proc sql noprint;
   create table &mvWorkLib.._full_tmpl as
   select
      ROW.Table_ORDINAL
      ,ROW.Row_ORDINAL
      ,ROW.Row_AutoFitHeight
      ,ROW.Row_Height
      ,ROW.Row_Index
      ,ROW.Row_StyleID
      ,ROW.Row_Span
      ,ROW.Row_Hidden
      ,CELL.Cell_ArrayRange
      ,CELL.Cell_Formula
      ,CELL.Cell_Href
      ,CELL.Cell_HRefScreenTip
      ,CELL.Cell_Index
      ,CELL.Cell_MergeAcross
      ,CELL.Cell_MergeDown
      ,CELL.Cell_ORDINAL
      ,CELL.Cell_StyleID
      ,CELL.Comment_Author
      ,CELL.Comment_Data
      ,CELL.Comment_Data_Xmlns
      ,CELL.Comment_ShowAlways
      ,CELL.Data
      ,CELL.Data_Ticked
      ,CELL.Data_Type
      ,CELL.Data_Xmlns
      ,NC.NamedCell_Name
      ,kupcase(NC.NamedCell_Name) as NamedCell_Name_upcase
      ,NC.NamedCell_ORDINAL
   from
      _reptmpl.row as row
      left join _reptmpl.cell as cell
         on row.row_ordinal=cell.row_ordinal
      left join _reptmpl.namedCell as nc
         on cell.cell_ordinal=nc.cell_ordinal
   order by
       table_ordinal
      ,row_ordinal
      ,cell_ordinal
      ,namedCell_ordinal
   ;
quit;

/*
Отбираем из всего содержимого Table только те строки, в которых
есть ячейки с именем &mpTemplateAreaName (вся динамическая область)
*/
%IF %klength(&mpTemplateAreaName.)>0 %then %do;
   proc sql noprint;
      select
         ktrim(kleft(put(coalesce(min(row_ordinal), 0), 4.)))
         ,ktrim(kleft(put(coalesce(max(row_ordinal), 0), 4.)))
      INTO
         :MIN_GENERATED_ROW_ORDINAL
         ,:MAX_GENERATED_ROW_ORDINAL
      from
         &mvWorkLib.._full_tmpl
      where NamedCell_Name_upcase = "&mpTemplateAreaName"
      ;
   quit;

   %IF (&max_generated_row_ordinal.=0) %THEN %DO;
      %let &mpResultVar.=-7;
      %let mvErrMsg=%bquote(ОШИБКА: в шаблоне &mpReportTemplateFile. на листе &mpSheetName. нет ни одной ячейки с меткой &mpTemplateAreaName.);
      %goto EXIT_MACRO;
   %END;
%END;
%ELSE %DO;
   %let min_generated_row_ordinal=0;
   %let max_generated_row_ordinal=0;
%END;

%put &=MIN_GENERATED_ROW_ORDINAL;
%put &=MAX_GENERATED_ROW_ORDINAL;

data &mvWorkLib.._generate_tmpl;
   set &mvWorkLib.._full_tmpl (where=(&MIN_GENERATED_ROW_ORDINAL. <= ROW_ORDINAL <= &MAX_GENERATED_ROW_ORDINAL.));
run;
proc sort data=&mvWorkLib.._generate_tmpl out=&mvWorkLib.._generate_tmpl_nodup nodupkey;
   by NamedCell_Name_upcase;
run;

/*отбираем статические ячейки*/
%IF (%klength(&mpStaticCellsName.) ne 0) %THEN %DO;
   proc sql noprint;
      create table &mvWorkLib.._static_cells as
         select *
         from &mvWorkLib.._full_tmpl
         where cell_Ordinal in (
            select cell_Ordinal
            from _reptmpl.namedCell
            where kupcase(namedCell_Name)="&mpStaticCellsName."
         )
      order by
          row_ORDINAL
         ,cell_ORDINAL
         ,namedCell_ORDINAL
      ;
   quit;

   /*перенумеровываем их*/
   data &mvWorkLib.._static_cells_enum;
      set &mvWorkLib.._static_cells;
      by row_ORDINAL cell_ORDINAL namedCell_ORDINAL;

      retain static_cell_ORDINAL 0;

      static_row_ORDINAL = row_ORDINAL-&MIN_GENERATED_ROW_ORDINAL.+1; /*перенумеровка строк с начала динамической зоны*/

      if first.row_ORDINAL then
         static_cell_ORDINAL=0;
      if first.cell_ORDINAL then
         static_cell_ORDINAL+1;

      output;
   run;
%END;
%ELSE %DO;
   /*инициализация пустого набора данных со статическими ячейками*/
   data &mvWorkLib.._static_cells_enum;
      set &mvWorkLib.._generate_tmpl;
      length static_row_ordinal static_cell_ORDINAL 8.;
      if (0>1) then do;
         call missing(static_row_ordinal, static_cell_ORDINAL);
      end;
      stop;
   run;
%END;

/********************************* end создание шаблона xml ****************************/


/*************************** Соединение шаблона с данными ********************************/
/*
если данные заданы в режиме rich text (с тегами I/bold/sub/sub), то необходимо
задать атрибут xmlns, чтобы rich text отобразился
*/

data &mvWorkLib.._report_data_corr;
   set &mpReportDataTable.;
   length
      data_xmlns
      comment_data_xmlns
      $1000
   ;

   Cell_styleName=kupcase(cell_styleName);
   NamedCell_Name = kupcase(NamedCell_Name);

   if kindex(data, '<B>')
      or kindex(data, '<Font')
      or kindex(data, '<I>')
      or kindex(data, '<S>')
      or kindex(data, '<Span>')
      or kindex(data, '<Sub>')
      or kindex(data, '<Sup>')
      or kindex(data, '<U>')
   then
      data_xmlns='http://www.w3.org/TR/REC-html40';
   else
      data_xmlns=' ';

   if kindex(comment_data, '<B>')
      or kindex(comment_data, '<Font')
      or kindex(comment_data, '<I>')
      or kindex(comment_data, '<S>')
      or kindex(comment_data, '<Span>')
      or kindex(comment_data, '<Sub>')
      or kindex(comment_data, '<Sup>')
      or kindex(comment_data, '<U>')
   then
      comment_data_xmlns='http://www.w3.org/TR/REC-html40';
   else
      comment_data_xmlns=' ';
run;


/*часть, сгенерированная на основе данных*/
proc sort data=&mvWorkLib.._report_data_corr;
   by row_ORDINAL cell_Ordinal;
run;

data &mvWorkLib.._full_generated_data;
   set &mvWorkLib.._report_data_corr;
   by row_ORDINAL cell_ORDINAL;

   length
      Table_ORDINAL     8
      NamedCell_Name_upcase $64
      NamedCell_Name       $64
      NamedCell_Ordinal    8
      row_AutoFitHeight    8
      row_Height        8
      row_Hidden        8
      Row_Span       8
      Row_StyleId       $40
      Cell_StyleID      $64
      Data_Type            $18
      Data_Ticked       8

      s_Cell_StyleName  $64
      s_Cell_StyleId    $64

      t_NamedCell_Name_upcase $64
      t_NamedCell_Name     $64
      t_row_AutoFitHeight  8
      t_row_Height         8
      t_row_Hidden         8
      t_Row_Span           8
      t_Row_StyleId        $40
      t_Cell_StyleID       $64
      t_Cell_Index         8
      t_Cell_ArrayRange    $1000
      t_Cell_Formula       $1000
      t_Cell_Href          $4000
      t_Cell_HRefScreenTip    $2000
      t_Comment_Author     $200
      t_Comment_ShowAlways 8
      t_Comment_Data       $32000
      t_Comment_Data_Xmlns $1000
      t_Data_Type          $18
      t_Data_Ticked        8
      t_Data_Xmlns         $1000
   ;

   retain
      Table_ORDINAL 1
   ;

   if (0>1) then do;
      call missing (Table_ORDINAL , NamedCell_Name , NamedCell_Name_upcase, NamedCell_Ordinal , row_AutoFitHeight , row_Height, row_Hidden, Row_Span, Row_StyleId, Cell_StyleID, Data_Type, Data_Ticked, s_Cell_StyleName, s_Cell_StyleId, t_NamedCell_Name, t_NamedCell_Name_upcase, t_row_AutoFitHeight , t_row_Height, t_row_Hidden, t_Row_Span, t_Row_StyleId, t_Cell_StyleID, t_Cell_Index, t_Cell_ArrayRange, t_Cell_Formula, t_Cell_Href, t_Cell_HRefScreenTip , t_Comment_Author, t_Comment_ShowAlways, t_Comment_Data, t_Comment_Data_Xmlns, t_Data_Type, t_Data_Ticked, t_Data_Xmlns);
   end;

   if (_N_=1) then do;
      declare hash hstyle(
         dataset:"&mvWorkLib.._style_list (
            rename=(
               style_Name=s_Cell_StyleName
               style_ID=s_Cell_StyleId
            )
            where=(s_Cell_StyleName ne ' ')
         )"
      );
      rc = hstyle.defineKey('s_Cell_StyleName');
      rc = hstyle.defineData('s_Cell_StyleId');
      rc = hstyle.defineDone();

      declare hash htmpl(
         dataset:"&mvWorkLib.._generate_tmpl_nodup (
            rename=(
               NamedCell_Name_upcase = t_NamedCell_Name_upcase
               NamedCell_Name = t_NamedCell_Name
               row_AutoFitHeight = t_row_AutoFitHeight
               row_Height = t_row_Height
               row_Hidden = t_row_Hidden
               Row_Span = t_Row_Span
               Row_StyleId = t_Row_StyleId
               Cell_StyleID = t_Cell_StyleID
               Cell_Index = t_Cell_Index
               Cell_ArrayRange = t_Cell_ArrayRange
               Cell_Formula = t_Cell_Formula
               Cell_Href = t_Cell_Href
               Cell_HRefScreenTip = t_Cell_HRefScreenTip
               Comment_Author = t_Comment_Author
               Comment_ShowAlways = t_Comment_ShowAlways
               Comment_Data = t_Comment_Data
               Comment_Data_Xmlns = t_Comment_Data_Xmlns
               Data_Type = t_Data_Type
               Data_Ticked = t_Data_Ticked
               Data_Xmlns = t_Data_Xmlns
            )
         )"
      );
      rc = htmpl.defineKey('t_NamedCell_Name_upcase');
      rc = htmpl.defineData(
           't_NamedCell_Name'
         , 't_row_AutoFitHeight'
         , 't_row_Height'
         , 't_row_Hidden'
         , 't_Row_Span'
         , 't_Row_StyleId'
         , 't_Cell_StyleID'
         , 't_Cell_Index'
         , 't_Cell_ArrayRange'
         , 't_Cell_Formula'
         , 't_Cell_Href'
         , 't_Cell_HRefScreenTip'
         , 't_Comment_Author'
         , 't_Comment_ShowAlways'
         , 't_Comment_Data'
         , 't_Comment_Data_Xmlns'
         , 't_Data_Type'
         , 't_Data_Ticked'
         , 't_Data_Xmlns'
      );
      rc = htmpl.defineDone();
   end;

   rc1 = htmpl.find(key:NamedCell_Name);
   rc2 = hstyle.find(key:Cell_StyleName);

   row_AutoFitHeight    = t_row_AutoFitHeight;
   row_Height        = t_row_Height;
   row_Hidden        = t_row_Hidden;
   row_Span          = t_row_Span;
   row_StyleId       = t_row_StyleId;

   Cell_StyleID      = coalescec(s_Cell_StyleID, t_Cell_StyleID);
   Cell_Index        = coalesce (Cell_Index, ifn(first.Row_Ordinal, t_Cell_Index, .));
   Cell_ArrayRange   = coalescec(Cell_ArrayRange, t_Cell_ArrayRange);
   Cell_Formula      = coalescec(Cell_Formula, t_Cell_Formula);
   Cell_Href         = coalescec(Cell_Href, t_Cell_Href);
   Cell_HrefScreenTip   = coalescec(Cell_HRefScreenTip, t_Cell_HRefScreenTip);
   Comment_Author    = coalescec(Comment_Author, t_Comment_Author);
   Comment_ShowAlways   = coalesce (Comment_ShowAlways, t_Comment_ShowAlways);
   Comment_Data      = coalescec(Comment_Data, t_Comment_Data);
   Comment_Data_Xmlns   = coalescec(Comment_Data_Xmlns, t_Comment_Data_Xmlns);

   Data_Type         = t_Data_Type;
   Data_Ticked       = t_Data_Ticked;
   Data_Xmlns        = coalescec(Data_Xmlns, t_Data_Xmlns);

   NamedCell_Name  = ' ';
   NamedCell_Ordinal = .;
run;

/*вставка пропущенных статических строк и ячеек*/
data &mvWorkLib.._full_gen_data_with_static_cells (
   drop=
      row_ordinal_prev
      static_cell_ordinal
      cell_ordinal_prev
      rc i
   );
   set &mvWorkLib.._full_generated_data end=last_rec;
   by Row_ORDINAL Cell_ORDINAL;

   length
      cell_type
      $20
      Row_ORDINAL_prev
      Cell_ORDINAL_prev
      initial_Row_ORDINAL
      initial_Cell_ORDINAL
      static_Cell_ORDINAL
      8
   ;

   retain
      Row_ORDINAL_prev
      Cell_ORDINAL_prev
      static_Cell_ORDINAL
      0
   ;

   if _N_=1 then do;
      *для поиска статических ячеек внутри строк;
      declare hash hcell(dataset:"&mvWorkLib.._static_cells_enum (rename=(cell_ordinal=initial_static_cell_ordinal row_ordinal=initial_static_row_ordinal static_row_ordinal=row_ordinal ))", multidata: 'y');
      rc = hcell.defineKey('row_ORDINAL', 'static_cell_ORDINAL');
      rc = hcell.defineData(ALL:'YES');
      rc = hcell.defineDone();

      *для поиска полных статических строк;
      declare hash hrow(dataset:"&mvWorkLib.._static_cells_enum (rename=(cell_ordinal=initial_static_cell_ordinal row_ordinal=initial_static_row_ordinal static_row_ordinal=row_ordinal ))", multidata: 'y');
      rc = hrow.defineKey('row_ORDINAL');
      rc = hrow.defineData(ALL:'YES');
      rc = hrow.defineDone();

      call missing (initial_static_cell_ordinal, initial_static_row_ordinal);
   end;

   cell_type='generated';
   initial_Row_ORDINAL=Row_ORDINAL;
   initial_Cell_ORDINAL=Cell_ORDINAL;
   output;

   if first.Row_ORDINAL then do;
      *если пропущены строки между текущей и предыдущей строкой, выводим их;
      if (Row_ORDINAL-Row_ORDINAL_prev > 1) then do;
         do i=1 to (Row_ORDINAL-Row_ORDINAL_prev-1);
            Row_ORDINAL = Row_ORDINAL_prev+i;
            initial_Row_ORDINAL=Row_ORDINAL;
            cell_type='static';

            if (hrow.find()=0) then do;
               Cell_ORDINAL=Static_cell_ordinal;
               initial_Cell_ORDINAL=initial_static_cell_ordinal;
               output;
               do while (hrow.find_next()=0);
                  Cell_ORDINAL=Static_cell_ordinal;
                  initial_Cell_ORDINAL=initial_static_cell_ordinal;
                  output;
               end;
            end;
         end;
         Row_ORDINAL=Row_ORDINAL+1;
      end;

      static_Cell_ORDINAL=0;
      cell_ORDINAL_prev=0;
   end;

   *если пропущены ячейки между текущей и предыдущей ячейкой, выводим их;
   if first.Cell_ORDINAL then do;
      do i=1 to (Cell_ORDINAL - Cell_ORDINAL_prev - 1);
         Cell_ORDINAL = Cell_ORDINAL_prev + i;
         static_Cell_ORDINAL + 1;

         if (hcell.find()=0) then do;
            cell_type='static';
            initial_row_ORDINAL=row_ordinal;
            initial_cell_ORDINAL=initial_static_cell_ORDINAL;
            *обнуляем индекс, чтобы не было конфликтов;
            if (cell_ordinal > 1) then cell_index=.;
            output;
            do while (hcell.find_next()=0);
               initial_cell_ORDINAL=initial_static_cell_ORDINAL;
               if (cell_ordinal > 1) then cell_index=.;
               output;
            end;
         end;
      end;
      Cell_ORDINAL = Cell_ORDINAL_prev + i;
   end;

   if last.Cell_ORDINAL then do;
      Cell_ORDINAL_prev=Cell_ORDINAL;
   end;

   if last.Row_ORDINAL then do;
      *вставка оставшихся статических ячеек в конец строки;
      static_cell_ordinal+1;
      do while (hcell.find()=0);
         cell_type='static';
         initial_row_ORDINAL=row_ordinal;
         initial_cell_ORDINAL=initial_static_cell_ORDINAL;
         Cell_ORDINAL=Cell_ORDINAL+1;
         if (cell_ordinal > 1) then cell_index=.;
         output;
         do while (hcell.find_next()=0);
            initial_cell_ORDINAL=initial_static_cell_ORDINAL;
            if (cell_ordinal > 1) then cell_index=.;
            output;
         end;
         static_cell_ordinal+1;
      end;

      *инкремент номера предыдущей строки;
      row_ordinal_prev=row_ordinal;
   end;
run;


/*вставляем полученные строки в полный шаблон и перенумеровываем все элементы*/
data &mvWorkLib.._full_report_data;
   set
      &mvWorkLib.._full_tmpl (where=(row_ordinal<&MIN_GENERATED_ROW_ORDINAL.) in=in_1)
      &mvWorkLib.._full_gen_data_with_static_cells (in=in_2)
      &mvWorkLib.._full_tmpl (where=(row_ordinal>&MAX_GENERATED_ROW_ORDINAL.) in=in_3)
   ;
   length
      table_num
      8
   ;

   if in_1 then table_num=1;
   else if in_2 then table_num=2;
   else if in_3 then table_num=3;

   if (in_1 or in_3) then do;
      cell_type='template';
      initial_cell_ordinal = cell_ordinal;
      initial_row_ORDINAL = row_ORDINAL;
   end;
run;
proc sort data=&mvWorkLib.._full_report_data;
   by table_num Row_ORDINAL Cell_ORDINAL NamedCell_ORDINAL;
run;

/*разделение на отдельные таблицы table, column, row, cell, namedCell
для подстановки в макрос генерации содержимого тега Table
*/
data
   &mvWorkLib.._cell_data (keep=
      initial_Row_ORDINAL
      initial_Cell_ORDINAL
      Row_ORDINAL
      Row_AutoFitHeight
      Row_Height
      Row_Index
      Row_StyleID
      Row_Span
      Row_Hidden
      Row_ORDINAL
      Cell_ORDINAL
      Cell_ORDINAL_IN_ROW
      Cell_StyleID
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
      Comment_Data_Xmlns
      Data_Type
      Data
      Data_Ticked
      Data_Xmlns
      Cell_Type
      empty_row_flg
   )
   &mvWorkLib.._NamedCell_data (keep=
      Cell_ORDINAL
      NamedCell_ORDINAL
      NamedCell_Name
   );

   set &mvWorkLib.._full_report_data (
      rename=(
         Row_ORDINAL=Row_ORDINAL_old
         Cell_ORDINAL=Cell_ORDINAL_old
         NamedCell_ORDINAL=NamedCell_ORDINAL_old
      )
   );
   by table_num Row_ORDINAL_old Cell_ORDINAL_old;

   length empty_row_flg $1;

   retain
      Row_ORDINAL
      Cell_ORDINAL
      Cell_ORDINAL_IN_ROW
      NamedCell_ORDINAL
      0
   ;

   if (first.Row_ORDINAL_old or first.Table_num) then do;
      Cell_ORDINAL_IN_ROW=0;
      Row_ORDINAL+1;
      Row_Index=Row_ORDINAL+(Row_Index-Row_ORDINAL_old);
   end;

   if (first.Cell_ORDINAL_old) then do;
      Cell_ORDINAL+1;
      Cell_ORDINAL_IN_ROW+1;
      if  (Cell_ORDINAL_old eq .) then
         empty_row_flg = 'Y';
      output &mvWorkLib.._cell_data;
   end;

   if (NamedCell_ORDINAL_old ne .) then do;
      NamedCell_ORDINAL+1;
      output &mvWorkLib.._NamedCell_data;
   end;
run;

/*если требуется, заменяем значения полей с заданными именами на основе param_table*/
%IF %klength(&mpParamDataTable.)>0 %THEN %DO;
   proc sql noprint;
      create table &mvWorkLib.._cell_data_to_update as
         select
             nc.cell_ORDINAL
            ,p.param_value as data_new
         from
            &mvWorkLib.._NamedCell_data nc
            inner join &mpParamDataTable. p
               on kupcase(nc.NamedCell_name)=kupcase(p.param_name)
         order by cell_ORDINAL
      ;
   quit;

   data &mvWorkLib.._cell_data;
      set &mvWorkLib.._cell_data;

      if _N_=1 then do;
         length cell_ORDINAL 8 data_new $32000;
         declare hash h(dataset:"&mvWorkLib.._cell_data_to_update");
         rc = h.defineKey('cell_ORDINAL');
         rc = h.defineData('data_new');
         rc = h.defineDone();
         call missing(data_new);
      end;

      if (h.find()=0) then do;
         data=data_new;

         if (kindex(data, '<B>')
            or kindex(data, '<Font')
            or kindex(data, '<I>')
            or kindex(data, '<S>')
            or kindex(data, '<Span>')
            or kindex(data, '<Sub>')
            or kindex(data, '<Sup>')
            or kindex(data, '<U>'))
            and data_xmlns=' '
         then
            data_xmlns='http://www.w3.org/TR/REC-html40';
      end;
   run;
%END;

/*проставление cell_index*/
data &mvWorkLib.._Cell_Data_with_index;
   set &mvWorkLib.._cell_data end=last;
   by row_ordinal cell_ordinal_in_row;

   length x y 8. fill_flg $1.;

   retain
      last_cell_index 0 /*индекс последней обработанной ячейки*/
      last_row_index 0 /*индекс последней обработанной строки*/
      fill_flg '1'
   ;

   if _N_=1 then do;
      declare hash h();
      rc = h.defineKey('x', 'y');
      rc = h.defineData('x', 'y', 'fill_flg');
      rc = h.defineDone();

      last_row_index=0;
   end;

   if first.row_ordinal then do;
      last_cell_index=0;
      last_row_index=coalesce(row_index, last_row_index + 1);
   end;

   /*если cell_index был задан в данных, надо проверить, корректно ли он задан*/
   if (cell_index ne .) then do;
      x = cell_index;
      y = last_row_index;
      if (h.find()=0 or cell_index <=last_cell_index) then do;
         call symput("&mpResultVar", "-5");
         call symput("mvErrMsg",
            "Ошибка: Некорректно задано значение поля Cell_index в " ||
            ifc(cell_type='template',
               'ячейке шаблона: ',
               ifc(cell_type='static',
                  'статической ячейке (ниже приводятся координаты ячейки в шаблоне): ',
                  'ячейке поданной на вход таблицы:  ')) ||
            "row_ORDINAL=" || ktrim(kleft(put(initial_ROW_ORDINAL, best12.))) ||
            " сell_ORDINAL=" || ktrim(kleft(put(initial_Cell_ORDINAL, best12.)))
         );
         stop;
      end;
   end;
   /*иначе находим первую свободную ячейку в строке, начиная с текущей*/
   else do;
      x = coalesce(cell_index, last_cell_index + 1);
      y = last_row_index;

      do while (h.find()=0);
         x = x + 1;
      end;
      cell_index = x;
   end;

   /*добавляем координаты текущей ячейки с учетом мержей, в hash*/
   last_cell_index = cell_index + coalesce(Cell_mergeAcross, 0);

   do x=cell_index to last_cell_index;
      do y=last_row_index to (last_row_index + coalesce(Cell_MergeDown, 0));
         if (h.find()=0) then do;
            call symput("&mpResultVar", "-5");
            call symput("mvErrMsg",
               "Ошибка: Некорректно задано значение полей Cell_MergeAcross/Cell_MergeDown в " ||
               ifc(cell_type='template',
                  'ячейке шаблона: ',
                  ifc(cell_type='static',
                     'статической ячейке (ниже приводятся координаты ячейки в шаблоне): ',
                     'ячейке поданной на вход таблицы:  ')) ||
               "row_ORDINAL=" || ktrim(kleft(put(initial_ROW_ORDINAL, best12.))) ||
               " сell_ORDINAL=" || ktrim(kleft(put(initial_Cell_ORDINAL, best12.)))
            );
            stop;
         end;
         rc = h.add();
      end;
   end;

   if (last) then do;
      h.output(dataset: "&mvWorkLib.._filled_cells");
   end;
run;
%IF (&&&mpResultVar. ne 0) %THEN %DO;
   %goto EXIT_MACRO;
%END;

/* подсчитываем кол-во строк и столбцов, и вставляем это значение в соответствующие атрибуты table*/
/*подсчет по заполненным ячейкам*/
proc sql noprint;
   select
       ktrim(kleft((put(max(y), 10.))))
      ,ktrim(kleft((put(max(x), 10.))))
   INTO
       :RowCount_by_FilledCell
      ,:ColumnCount_by_FilledCell
   from &mvWorkLib.._filled_cells;
quit;
%put &=RowCount_by_FilledCell &=ColumnCount_by_FilledCell;
/*подсчет по тегам Column*/
proc sql noprint;
   select sum(count(column_ordinal), sum(column_span))
   INTO :ColumnCount_by_ColumnTag
   from _reptmpl.column;
quit;
%put &=ColumnCount_by_ColumnTag;
/*подсчет по тегам Conditional Formatting*/
%mExcelGetCondFormatting(
    mpReportTemplateFile=&mpReportTemplateFile.
   ,mpWorkSheetName=&mpSheetName.
   ,mpColumnCountVar=ColumnCount_by_Cond
   ,mpRowCountVar=RowCount_by_Cond
);
%IF (&&&mpResultVar. ne 0) %THEN %DO;
   %goto EXIT_MACRO;
%END;

%let ExpandedRowCount=%sysfunc(max(&RowCount_by_FilledCell., &RowCount_by_Cond.));
%let ExpandedColumnCount=%sysfunc(max(&ColumnCount_by_FilledCell., &ColumnCount_by_Cond., &ColumnCount_by_ColumnTag.));
%put &=ExpandedRowCount &=ExpandedColumnCount;

data &mvWorkLib.._table_data;
   set _reptmpl.table;
   Table_ExpandedColumnCount = &ExpandedColumnCount.;
   Table_ExpandedRowCount = &ExpandedRowCount.;
run;

/*изменение ширины и применение атрибута hidden к ячейкам, если задана таблица mpColumnTable*/
%IF %klength(&mpColumnTable.) > 0 %THEN %DO;
   /*разворачиваем таблицу _reptmpl.column по тегу span (он объединяет подряд идущие столбцы с одинаковыми характиристиками)
   на каждый столбец выводим отдельную строку*/
   data &mvWorkLib.._column_expanded (keep=Column_Index Column_StyleID Column_AutoFitWidth Column_Width Column_Hidden);
      set _reptmpl.column (rename=(Column_Index=Column_Index_ini));

      retain
         Column_Index 0
      ;

      Column_Index = coalesce(Column_Index_ini, Column_Index+1);
      output;

      if (Column_Span ne .) then
         do i=1 to Column_Span;
            Column_index = Column_Index+1;
            output;
         end;
   run;

   /*объединяем характеристики колонок из шаблона с характеристиками колонок, поданными на вход макросу*/
   proc sort data=&mvWorkLib.._column_expanded; by Column_Index; run;
   proc sort data=&mpColumnTable. out=&mvWorkLib.._column_user; by Column_Index; run;

   data &mvWorkLib.._column_new (keep=Table_ORDINAL Column_ORDINAL Column_Index Column_StyleID Column_AutoFitWidth Column_Width Column_Hidden Column_Span);
      merge
         &mvWorkLib.._column_expanded
         &mvWorkLib.._column_user (rename=(column_width=column_width_new column_hidden=column_hidden_new))
      ;
      by Column_Index;

      retain
         Table_ORDINAL 1
         Column_ORDINAL 0
         Column_Span .
      ;

      Column_ORDINAL + 1;
      if (column_width_new ne .) then column_width = column_width_new;
      if (column_hidden_new ne .) then column_hidden = column_hidden_new;
   run;
%END;
%ELSE %DO;
   data &mvWorkLib.._column_new;
      set _reptmpl.column;
   run;
%END;

/*генерация содержимого тэга Table*/
%mExcelGenerateTable(
   mpTableIntable=&mvWorkLib.._table_data
   ,mpColumnIntable=&mvWorkLib.._column_new
   ,mpCellIntable=&mvWorkLib.._cell_data_with_index
   ,mpNamedCellIntable=&mvWorkLib.._namedCell_data
   ,mpOutFile="&mpDebugFolder./content_to_insert.xml"
);

/*Вставка сгенерированного содержимого на место проставленной метки в исходном xml-шаблоне*/
%mExcelReplaceMarkWithContent(
    mpReportTemplateFile="&mpDebugFolder./template_area_marked.xml"
   ,mpMarkName=TABLE
   ,mpContentFile="&mpDebugFolder./content_to_insert.xml"
   ,mpOutFile="&mpDebugFolder./content_inserted.xml"
);
%IF (&&&mpResultVar. ne 0) %THEN %DO;
   %goto EXIT_MACRO;
%END;
%let mvLastOutput="&mpDebugFolder./content_inserted.xml";

/*удаление имен ячеек и листа TEMPLATE_WORK при необходимости*/
%IF (&mpDeleteNamesFlg.=Y OR %kLENGTH(&mpDeleteWorksheets.)>0) %THEN %DO;

   data _null_;
      infile &mvLastOutput. encoding="UTF-8" lrecl=32000 _INFILE_=cur_str;
      file "&mpDebugFolder./elements_deleted.xml" encoding="UTF-8" lrecl=32000;

      input;

      find_flg=0;
      %IF (&mpDeleteNamesFlg.=Y) %THEN %DO;
         if kindex(cur_str, '<Names>') then do;
            find_flg=1;
            do until (kindex(cur_str, '</Names>'));
               input;
            end;
         end;
      %END;

      %IF (%kLENGTH(&mpDeleteWorksheets.)>0) %THEN %DO;
         if prxmatch("/<Worksheet.*?(&mpDeleteWorksheets.)/i", cur_str) then do;
            find_flg=1;
            do until (kindex("</Worksheet>", cur_str));
               input;
            end;
         end;
      %END;

      if not(find_flg) then put cur_str;
   run;

   %LET mvLastOutput="&mpDebugFolder./elements_deleted.xml";
%END;

/*Конвертация в XLS/XLSX*/
%IF (&mpOutFiletype. ne XML) %THEN %DO;
   %mXML2XlsDoc(mpType=&mpOutFileType., mpinFile=&mvLastOutput., mpoutFile="&mpDebugFolder./converted_to_xls.&mpOutFileType.", mpResultVar=convertFileResult);

   %mCheckFileExist(mpFile=%str(&mpDebugFolder./converted_to_xls.&mpOutFileType.), mpResultVar=extractTableResult);
   %IF &convertFileResult.>0 %THEN %DO;
      %let &mpResultVar.=-8;
      %let mvErrMsg=%str(Ошибка: ошибка преобразования из XML в &mpOutFileType.. Файл &mpOutFile. не был создан);
      %goto EXIT_MACRO;
   %END;

   %let mvLastOutput="&mpDebugFolder./converted_to_xls.&mpOutFileType.";

   %put %bquote(Отчет был успешно преобразован в формат &mpOutFileType.);
%END;


/*копирование в outfile*/
%mCopyFile(mpIn=&mvLastOutput., mpOut=&mpOutFile.);

%mCheckFileExist(mpFile=&mpOutFile., mpResultVar=copyToOutfile);
%IF &copyToOutfile.=0 %THEN %DO;
   %let &mpResultVar.=-9;
   %let mvErrMsg=%str(Ошибка: ошибка при копировании файла результата в указанный выходной файл &mpOutFile.. Отчет не был создан);
   %goto EXIT_MACRO;
%END;

%put Отчет &mpOutFile. был успешно создан;

%EXIT_MACRO: %put &mvErrMsg.;

%IF %klength(&mpDebugLib.)=0 %THEN %DO;
   proc datasets lib=&mvWorkLib. nowarn nolist;
      delete
         _CELL_DATA
         _CELL_DATA_TO_UPDATE
         _CELL_DATA_WITH_INDEX
         _COLUMN_EXPANDED
         _COLUMN_USER
         _COLUMN_NEW
         _FILLED_CELLS
         _FULL_GENERATED_DATA
         _FULL_GEN_DATA_WITH_STATIC_CELLS
         _FULL_REPORT_DATA
         _FULL_TMPL
         _GENERATE_TMPL
         _GENERATE_TMPL_NODUP
         _MIN_CELL_ORDINAL_BY_ROW
         _NAMEDCELL_DATA
         _REPORT_DATA_CORR
         _ROWS_TABLE
         _STATIC_CELLS
         _STATIC_CELLS_ENUM
         _STYLE_LIST
         _TABLE_DATA
      ;
   run;
   quit;
%END;
%mend;
