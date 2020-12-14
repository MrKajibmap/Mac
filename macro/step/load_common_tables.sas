/*****************************************************************
*  ВЕРСИЯ:
*     $Id: $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Загружает служебные таблицы в CAS
*
*  ПАРАМЕТРЫ:
*     Нет
*									
*
******************************************************************
*  Использует: 
*	  нет
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*	%load_common_tables;
*
****************************************************************************
*  25-08-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro load_common_tables;
	%M_ETL_REDIRECT_LOG(START, load_common_tables, Main);
	%M_LOG_EVENT(START, load_common_tables);

	%if %sysfunc(SESSFOUND(casauto)) = 0 %then %do; 
		cas casauto;
		caslib _all_ assign;
	%end;

	/* ENCODING_CHANNEL_CD into DM_ABT */
	proc casutil;
		droptable casdata="ENCODING_CHANNEL_CD" incaslib="dm_abt" quiet;
	run;
	data dm_abt.ENCODING_CHANNEL_CD(drop=channel_lvl channel_nm member_rk parent_channel_cd promote=yes);
		set ETL_IA.CHANNEL_LOOKUP;
		channel_cd=channel_cd;
		channel_cd_id=member_rk;
	run;

	proc casutil;
		save incaslib="dm_abt" outcaslib="dm_abt" casdata="ENCODING_CHANNEL_CD" casout="ENCODING_CHANNEL_CD.sashdat" replace; 
	run;
	
	%M_LOG_EVENT(END, load_common_tables);
	%M_ETL_REDIRECT_LOG(END, load_common_tables, Main);
%mend load_common_tables;