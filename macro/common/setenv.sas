/*****************************************************************
* ВЕРСИЯ:
*   $Id: a46d9ff0f22a4eb813e2b3dec8747d89a7506078 $
*
******************************************************************
* НАЗНАЧЕНИЕ:
*   Извлекает список имён и значений переменных окружения из ОС и
*	заполняет одноимённые глобальные переменные сессии SAS
*
* ПАРАМЕТРЫ:
*
******************************************************************
* Пример использования:
*
*   %setenv;
*
******************************************************************
* 26-01-2017   Начальное кодирование
******************************************************************/

%macro setenv;
	
	filename os_env pipe 'set' lrecl=32000;
	filename glob_var temp;

	data _null_;
		length 	env $ 32 
				val $ 32000;
		infile os_env;
		file glob_var;
		input;

		env=scan(_infile_,1,'=');
		val=scan(_infile_,2,'=');
		if lengthn(val) <= 100 and lengthn(compress(val,';"''','k'))=0 and lengthn(compress(env,,'n'))=0 then do;
			put '%global ' env ';';
			put '%let ' env '=' val ';';
		end;
	run;

	%include glob_var;
	
	filename os_env clear;
	filename glob_var clear;

%mend setenv;


