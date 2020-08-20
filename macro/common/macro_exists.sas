/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 52dfe1aa975f66d4cf0f5d00dc416e32ddf49f59 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для проверки существование макроса в sasautos и/или локальном каталоге.
*
*  ПАРАМЕТРЫ:
*     mpMacro                 -  имя макроса для поиска
*     mpExpandLibs            -  разорачивать либнеймы
*     mpSASMacr               -  искать в локальном каталоге
*
******************************************************************
*  Использует:
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %put %macro_exists(am_get_dtree_attrs,mpExpandLibs=Y);
*
****************************************************************************
*  09-11-2016  Сазонов     Начальное кодирование
****************************************************************************/

%macro macro_exists(mpMacro,mpExpandLibs=N,mpSASMacr=Y);
    %local lmvRes lmvSAutos lmvI lmvJ lmvSAutosPath lmvPath lmvCnt lmvLibCnt;
    %let lmvRes=0;

    /*Проверяем локальный каталог*/
    %if &mpSASMacr=Y %then %do;
        %let lmvRes=%sysmacexist(&mpMacro);
		%if &lmvRes=1 %then
			%goto exit;
    %end;
    
	/*Проверяем sasautos*/
    %let lmvSAutos=%sysfunc(getoption(sasautos));
    %let lmvCnt=%countw(&lmvSAutos,%str(%(%)%' ));
    %do lmvI=1 %to &lmvCnt;
        %let lmvSAutosPath=%scan(&lmvSAutos,&lmvI,%str(%(%)%' ));
        /*"Разворачиваем" пути системных SASAUTOS*/
        %if &mpExpandLibs=Y and %length(&lmvSAutosPath) <= 8 %then %do;
            %if %sysfunc(fileref(&lmvSAutosPath)) = 0 %then %do;
                %let lmvSAutosPath=%sysfunc(pathname(&lmvSAutosPath));
                %let lmvLibCnt=%countw(&lmvSAutosPath,%str(%(%)%' ));
                %do lmvJ=1 %to &lmvLibCnt;
                    %let lmvPath=%sysfunc(cats(%scan(&lmvSAutosPath,&lmvJ,%str(%(%)%' )),/,&mpMacro,.sas));
                    %if %sysfunc(fileexist(&lmvPath)) %then %do;
                        %let lmvRes=1;
                        %goto exit;
                    %end;
                %end;
            %end;
        %end;
        %else %do;
            %let lmvPath=%sysfunc(cats(&lmvSAutosPath,/,&mpMacro,.sas));
            %if %sysfunc(fileexist(&lmvPath)) %then %do;
                %let lmvRes=1;
                %goto exit;
            %end;
        %end;        
    %end;
%exit:
    &lmvRes
%mend macro_exists;
