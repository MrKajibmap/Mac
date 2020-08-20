/*****************************************************************
*  ВЕРСИЯ:
*     $Id: define_functions.sas 2975:cedfabb5d13c 2014-05-29 12:09:41Z rusane $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Определяет дополнительные функции (FCMP).
*
******************************************************************
*  27-05-2014  Нестерёнок     Начальное кодирование
*  27-05-2014  Нестерёнок     Добавил ascii_canonical, ascii_next
*  29-05-2014  Нестерёнок     Добавил hex_canonical, hex_next
*  29-05-2014  Нестерёнок     Добавил ascii_next_n, hex_next_n
******************************************************************/


/*****************************************************************
*  Функции для генерации суррогатных ключей методом HEX
*     В методе используется 16 знаков: 0-9, A-F, в указанном порядке
*
******************************************************************/

/*****************************************************************
*  String hex_canonical (String cd)
*  Возвращает каноническую форму ключа cd,
*  т.е. дополняет его справа символами hexFirstChar(=0) до полной длины,
*  а также заменяет символы не из hexCharSet на hexLastChar(=F)
******************************************************************/

/*****************************************************************
*  String hex_next (String ccd)
*  Возвращает следующий ключ после ключа ccd
*  Ключ ccd обязан быть в канонической форме
******************************************************************/

/*****************************************************************
*  String hex_next_n (String ccd, int n)
*  Возвращает n-й следующий ключ после ключа ccd
*  Ключ ccd обязан быть в канонической форме
******************************************************************/

%macro DWF_200_Define_FCMP;
	%let etls_jobName = 000_200_Define_FCMP;
	%etl_job_start;
	
	%let hexCharSet      =  0123456789ABCDEF;
	%let hexCharSetSize  =  %length(&hexCharSet);
	%let hexFirstChar    =  %substr(&hexCharSet, 1, 1);
	%let hexLastChar     =  %substr(&hexCharSet, &hexCharSetSize, 1);

	/* Удаляем прошлые определения */
	proc fcmp outlib=ETL_FMT.fcmp.dwf;
	   deletefunc hex_canonical;
	   deletefunc hex_next;
	   deletefunc hex_next_n;
	run;

	/* Задаем новые */
	proc fcmp outlib=ETL_FMT.fcmp.dwf;
	   function hex_canonical (cd $) $;
		  ccd = translate(right(cd), "&hexFirstChar", " ");
		  do until (i=0);
			 i = verify (ccd, "&hexCharSet");
			 if i<>0 then
				ccd = translate(ccd, "&hexLastChar", char(ccd, i));
		  end;
		  return (ccd);
	   endsub;

	   function hex_next (ccd $) $;
		  res = ccd;

		  do i = length(ccd) to 1 by -1;
			 c = findc("&hexCharSet", char(res, i));
			 if (c = 0) or (c ge &hexCharSetSize) then do;
				substr(res, i, 1) = char("&hexCharSet", 1);
			 end;
			 else do;
				substr(res, i, 1) = char("&hexCharSet", c+1);
				return(res);
			 end;
		  end;
		  return(res);
	   endsub;

	   function hex_next_n (ccd $, n) $;
		  res = ccd;
		  digit = length(ccd);

		  do while (n > 0);
			 add = mod (n, &hexCharSetSize);
			 do i = digit to 1 by -1;
				c = findc("&hexCharSet", char(res, i));
				if (c = 0) then do;
				   substr(res, i, 1) = char("&hexCharSet", 1+add);
				   add = 1;
				end;
				else if (c+add) gt &hexCharSetSize then do;
				   substr(res, i, 1) = char("&hexCharSet", c+add-&hexCharSetSize);
				   add = 1;
				end;
				else do;
				   substr(res, i, 1) = char("&hexCharSet", c+add);
				   i = 0;
				end;
			 end;

			 n = floor(n/&hexCharSetSize);
			 digit = digit-1;
		  end;
		  return(res);
	   endsub;
	run;


	/*****************************************************************
	*  Функции для генерации суррогатных ключей методом ASCII
	*     В методе используется 63 знака: 0-9, A-Z, _ и a-z
	*     Символы обязаны располагаться в порядке ASCII.
	*
	******************************************************************/

	/*****************************************************************
	*  String ascii_canonical (String cd)
	*  Возвращает каноническую форму ключа cd,
	*  т.е. дополняет его справа символами asciiFirstChar(=0) до полной длины,
	*  а также заменяет символы не из asciiCharSet на asciiLastChar(=z)
	******************************************************************/

	/*****************************************************************
	*  String ascii_next (String ccd)
	*  Возвращает следующий ключ после ключа ccd
	*  Ключ ccd обязан быть в канонической форме
	******************************************************************/

	/*****************************************************************
	*  String ascii_next_n (String ccd, int n)
	*  Возвращает n-й следующий ключ после ключа ccd
	*  Ключ ccd обязан быть в канонической форме
	******************************************************************/

	%let asciiCharSet      =  0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz;
	%let asciiCharSetSize  =  %length(&asciiCharSet);
	%let asciiFirstChar    =  %substr(&asciiCharSet, 1, 1);
	%let asciiLastChar     =  %substr(&asciiCharSet, &asciiCharSetSize, 1);

	/* Удаляем прошлые определения */
	proc fcmp outlib=ETL_FMT.fcmp.dwf;
	   deletefunc ascii_canonical;
	   deletefunc ascii_next;
	   deletefunc ascii_next_n;
	run;

	/* Задаем новые */
	proc fcmp outlib=ETL_FMT.fcmp.dwf;
	   function ascii_canonical (cd $) $;
		  ccd = translate(right(cd), "&asciiFirstChar", " ");
		  do until (i=0);
			 i = verify (ccd, "&asciiCharSet");
			 if i<>0 then
				ccd = translate(ccd, "&asciiLastChar", char(ccd, i));
		  end;
		  return (ccd);
	   endsub;

	   function ascii_next (ccd $) $;
		  res = ccd;

		  do i = length(ccd) to 1 by -1;
			 c = findc("&asciiCharSet", char(res, i));
			 if (c = 0) or (c ge &asciiCharSetSize) then do;
				substr(res, i, 1) = char("&asciiCharSet", 1);
			 end;
			 else do;
				substr(res, i, 1) = char("&asciiCharSet", c+1);
				return(res);
			 end;
		  end;
		  return(res);
	   endsub;

	   function ascii_next_n (ccd $, n) $;
		  res = ccd;
		  digit = length(ccd);

		  do while (n > 0);
			 add = mod (n, &asciiCharSetSize);
			 do i = digit to 1 by -1;
				c = findc("&asciiCharSet", char(res, i));
				if (c = 0) then do;
				   substr(res, i, 1) = char("&asciiCharSet", 1+add);
				   add = 1;
				end;
				else if (c+add) gt &asciiCharSetSize then do;
				   substr(res, i, 1) = char("&asciiCharSet", c+add-&asciiCharSetSize);
				   add = 1;
				end;
				else do;
				   substr(res, i, 1) = char("&asciiCharSet", c+add);
				   i = 0;
				end;
			 end;

			 n = floor(n/&asciiCharSetSize);
			 digit = digit-1;
		  end;
		  return(res);
	   endsub;
	run;


	/*****************************************************************
	*  ВЕРСИЯ:
	*     $Id$
	*
	******************************************************************
	*  НАЗНАЧЕНИЕ:
	*     Определяет дополнительные функции (FCMP).
	*
	******************************************************************
	*  16-04-2018  Нестерёнок     Добавлены log_<LEVEL>
	******************************************************************/


	/*****************************************************************
	*  Функции для логирования
	*  <LEVEL>: TRACE, DEBUG, INFO, WARN, ERROR, FATAL
	*
	******************************************************************/

	/*****************************************************************
	*  int log4sas_<LEVEL> (String logger, String message)
	*  Выводит сообщение message в лог logger с уровнем <LEVEL>.
	*  Возвращает 0, если успешно.
	******************************************************************/

	/* Удаляем прошлые определения */
	proc fcmp outlib=ETL_FMT.fcmp.cwf;
	   deletefunc log4sas_trace;
	   deletefunc log4sas_debug;
	   deletefunc log4sas_info;
	   deletefunc log4sas_warn;
	   deletefunc log4sas_error;
	   deletefunc log4sas_fatal;
	run;

	/* Задаем новые */
	proc fcmp outlib=ETL_FMT.fcmp.cwf;
	   function log4sas_trace (logger $, message $);
		  rc=log4sas_logevent(logger, "trace", message);
		  return (rc);
	   endsub;

	   function log4sas_debug (logger $, message $);
		  rc=log4sas_logevent(logger, "debug", message);
		  return (rc);
	   endsub;

	   function log4sas_info (logger $, message $);
		  rc=log4sas_logevent(logger, "info", message);
		  return (rc);
	   endsub;

	   function log4sas_warn (logger $, message $);
		  rc=log4sas_logevent(logger, "warn", message);
		  return (rc);
	   endsub;

	   function log4sas_error (logger $, message $);
		  rc=log4sas_logevent(logger, "error", message);
		  return (rc);
	   endsub;

	   function log4sas_fatal (logger $, message $);
		  rc=log4sas_logevent(logger, "fatal", message);
		  return (rc);
	   endsub;
	run;
%mend DWF_200_Define_FCMP;