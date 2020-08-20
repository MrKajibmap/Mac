/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 314af7abeb9d0145dfcc1e59e3e191606546613b $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*  Загружает в va таблицу из oracle
*
*  ПАРАМЕТРЫ:
*     mpTab=                	+  имя набора или view
*	  mpDomain 					+  authdomain
*     mpLasrLib           		+  VA библиотека
*     mpLasrTab                 -  Таблица в VA. Заполняется если имя отличается от имени таблицы в mpTab       
*	  mpFolder					+ Папка в метаданных для таблицы VA
*	  mpCompressFlg				+ Флаг сжатия
*
******************************************************************
*  Использует:
*     %member_names
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %load_ora_to_va(mpTab=REP_DM2.COMP_DETAIL_FIN_ALL, mpDomain=REPDMAUTH, mpLasrTab=test_two ); 
*
******************************************************************
*  28-10-2015  Zotikov     Начальное кодирование
******************************************************************/

%macro load_ora_to_va(
     mpTab=
     ,mpDomain=
	 ,mpLasrLib=
     ,mpLasrTab=
     ,mpFolder=
     ,mpCompressFlg=
);

     %if %klength(&mpLasrTab.)=0 %then %do;
          %let mpLasrTab=%scan(&mpTab., -1, %str(.)); 
     %end;

     %let mpLasrLib=%superq(mpLasrLib);
     %let mpFolder=%superq(mpFolder);

     /*удаление старой версии таблицы, если она существует*/
    %if %sysfunc(exist(VALIBLA.&mpLasrTab.)) %then %do;
        proc datasets library=VALIBLA nolist;
            delete &mpLasrTab.;
        quit;
    %end;

	%local lmvLibref lmvMemberName;
    %member_names (mpTable=&mpTab., mpLibrefNameKey=lmvLibref, mpMemberNameKey=lmvMemberName);
	%let lmvOraMemberName = %str(%')&lmvMemberName.%str(%');
	%let lmvOraLibr = %str(%')&lmvLibref.%str(%');
	%put &=lmvOraMemberName.;
	%put &=lmvOraLibr;
	
	proc sql;
		select strip(path), strip(sysvalue) into :lmvPath, :lmvOraSysLibref
		from sashelp.vlibnam
		where libname = "&lmvLibref."
		;
	quit;

	%let lmvOraLibref = %str(%')%sysfunc(compress(&lmvOraSysLibref.,,"d"))%str(%');
	%let lmvOraPath = %str(%")%sysfunc(strip(&lmvPath.))%str(%");
	%put &=lmvOraMemberName.;
	%put &=lmvOraLibref;
	%put &=lmvOraPath;

	proc sql;
		connect to oracle as oram (authdomain="&mpDomain." 
		path=&lmvOraPath.);
			create table for_load_0 as
			select *
			from connection to oram 
				(select COLUMN_ID as ID, COLUMN_NAME as COLMN, DATA_TYPE as TYPE, DATA_LENGTH as LENG
				from all_tab_columns 
				where owner=&lmvOraLibref.
				and table_name=&lmvOraMemberName.
				order by COLUMN_ID);
		disconnect from oram;
	quit;

	proc sql;
		select strip(put(count(*), best12.)) into :maxnum
		from for_load_0
	;
	quit;

	%let mvMaxIds = mvIds&maxnum.;
	%let mvMaxClm = mvClmns&maxnum.;
	%let mvMaxLen = mvLengs&maxnum.;
	%let mvMaxTyp = mvTypes&maxnum.;

	proc sql;
		select id, colmn, leng, type 
		into :mvIds1 - :&mvMaxIds., :mvClmns1 - :&mvMaxClm., :mvLengs1 - :&mvMaxLen., :mvTypes1 - :&mvMaxTyp.
		from for_load_0
	;
	quit;


		proc sql;
			create table for_load as
			select 
			%do i=1 %to &maxnum.;
				&&mvClmns&i
				%if &&mvTypes&i = VARCHAR2 %then %do;
					length = &&mvLengs&i.
					format = $&&mvLengs&i...
					informat = $&&mvLengs&i...
				%end; 

				%if &i ^= &maxnum. %then %do;
					,
				%end;
			%end;
			from &mpTab.
		;
		quit;

	
     /*загрузка таблицы в LASR*/
    data VALIBLA.&mpLasrTab. (
          %IF (&mpCompressFlg.=yes) %THEN %DO;
                squeeze=yes
          %END;
     );
        set for_load;
    run;

     /*регистрация таблицы в метаданных*/
     proc metalib;
          omr (library="&mpLasrLib.");
          folder="&mpFolder.";
          select ("&mpLasrTab");
     run;
     quit;

	 proc datasets library=work;
	   delete for_load for_load_0 libas;
	run;
%mend;

