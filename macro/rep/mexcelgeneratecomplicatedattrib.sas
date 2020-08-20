/*********************************************************
Назначение:
Макрос генерирует attrib statement с полями, требуемыми для входной таблицы макроса
mExcelGenerateComplicatedReport через параметр mpReportDataTable

Параметры:
  mpKeepFlg          -  Y|N, если Y, то добавляется keep полей

Примеры:
  *** Создается таблица со структурой и
      пустой записью, поля x и y не выводятся;

  data WORK.REPORT_CELLS;
    %mExcelGenerateComplicatedAttrib (mpKeepFlg=Y);
    x = 1; y = 2;
  run;

**********************************************************
  11.03.2015, Светлана Урюпина - начальная версия
  03.06.2015, Суменков Илья - добавлен параметр mpKeepFlg,
     изменен порялок следования полей (для удобства отладки)
**********************************************************/

%macro mExcelGenerateComplicatedAttrib (mpKeepFlg=N);
  attrib
    NamedCell_Name    length=$64
    Data        length=$32000
    Row_Ordinal      length=8
    Row_Index      length=8
    Cell_ORDINAL    length=8
    Cell_Index      length=8
    Cell_StyleName     length=$64
    Cell_MergeDown     length=8
    Cell_MergeAcross  length=8
    Cell_ArrayRange    length=$1000
    Cell_Formula     length=$1000
    Cell_Href      length=$4000
    Cell_HRefScreenTip  length=$2000
    Comment_Author    length=$200
    Comment_ShowAlways   length=8
    Comment_Data    length=$32000
  ;

  /*06.03.2015: Добавлен параметр */
  %if &mpKeepFlg eq Y %then %do;
    keep  Row_Ordinal
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
    ;
  %end;

  /*18.06.2015: Начальная инициализация*/
  /*23.06.2015: условие if _N_=1 заменено на if (0>1), т.е. вызывало ошибку в retain*/
  if (0>1) then
    call missing(
          Row_Ordinal
          ,Row_Index
          ,Cell_ORDINAL
          ,Cell_StyleName
          ,Cell_MergeDown
          ,Cell_MergeAcross
          ,Cell_Index
          ,Cell_ArrayRange
          ,Cell_Formula
          ,Cell_Href
          ,Cell_HRefScreenTip
          ,Comment_Author
          ,Comment_ShowAlways
          ,Comment_Data
          ,Data
          ,NamedCell_Name);
%mend mExcelGenerateComplicatedAttrib;
