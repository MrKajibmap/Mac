/*********************************************************************************
* ВЕРСИЯ:
*   $Id$
**********************************************************************************/

/*===================================== GLOBAL ===================================*/
/* Здесь определяются глобальные переменные, общие для всех фаз                   */
/*================================================================================*/

%global ETL_DBMS;                   /* Имя СУБД, используемой для ETL (oracle, db2) */
%global ETL_DBCS;                   /* Использовать или нет Double Byte Character Set (0|1) */
%global ETL_ROOT;                   /* Корневой каталог размещения всего, связанного с ETL */
%global ETL_DATA_ROOT;              /* Путь к данным среды (корневой каталог) */
%global ETL_CDATA_ROOT;             /* Путь к общим данным (корневой каталог) */

%global ETL_FILE_STATIC_ROOT;       /* Путь к статическим (не меняющимся) файлам */
%global ETL_FILE_INPUT_ROOT;        /* Путь к входным файлам (корневой каталог) */
%global ETL_FILE_OUTPUT_ROOT;       /* Путь к выходным файлам (корневой каталог) */

%global ETL_PRECISION;              /* Допустимое расхождение, при котором признается равенство величин */
%global ETL_TYPE;                   /* Тип идущей загрузки:  INIT - инициализация, FULL - полная, и INC - инкрементальная */
%global ETL_DEBUG;                  /* Признак отладочного запуска */

%global ETL_DWH_INIT_DT;            /* Дата начала истории хранилища */
%global ETL_CURRENT_JOB_ID;         /* ID текущего процесса ETL */
%global ETL_MODULE_RC;              /* Код ошибки в текущем процессе ETL */

%global ETL_TIME_FRAME_TYPE;        /* Тип окна загрузки:  FIXED (от фикс. даты) или FLOATING (скользящий) */
%global ETL_TIME_FRAME_VALUE;       /* Для FIXED - дата в формате числа, для FLOATING - глубина интервала в днях */
%global ETL_TIME_FRAME_DT;          /* Вычисляемая дата начала интервала загрузки */
%global ETL_TIME_FRAME_DTTM;        /* Вычисляемая дата-время начала интервала загрузки */

%global ETL_MIN_DT;                 /* Минимальная дата в системе, -бесконечность */
%global ETL_MAX_DT;                 /* Максимальная дата в системе, +бесконечность */
%global ETL_MIN_DTTM;               /* Минимальная дата-время в системе, -бесконечность */
%global ETL_MAX_DTTM;               /* Максимальная дата-время в системе, +бесконечность */
%global ETL_SCD_NEW_DTTM;           /* Дата, которой открываются первые версии записей SCD */
%global ETL_SCD_FUTURE_DTTM;        /* Дата, которой закрываются текущие версии записей SCD */
%global ETL_CURRENT_DT;             /* Текущая дата для ETL-процессов */
%global ETL_CURRENT_DTTM;           /* Текущее дата-время для ETL-процессов */

%global ETL_DEFAULT_COUNTRY;        /* Страна по умолчанию (Россия) */
%global ETL_DEFAULT_CURRENCY;       /* Валюта по умолчанию (рубли) */
%global ETL_DEFAULT_LANGUAGE;       /* Язык по умолчанию (русский) */

%global IORC_SOK;                   /* Константа - номер ошибки _SOK */
%global IORC_DSENOM;                /* Константа - номер ошибки _DSENOM */
%global IORC_DSENMR;                /* Константа - номер ошибки _DSENMR */

%global ETL_D0_ID;                  /* Бизнес-ключ для дамми-записи D0 */
%global ETL_D0_RK;                  /* Суррогатный ключ для дамми-записи D0 */
%global ETL_D0_CD;                  /* Внешний ключ для дамми-символьной ссылки D0 */
%global ETL_BK_INVALID;             /* Обозначение неверного значения в бизнес-ключе */

%global ETL_SYS_CONNECT_OPTIONS;    /* Параметры подключения к системной схеме ETL_SYS */
%global ETL_IA_CONNECT_OPTIONS;     /* Параметры подключения к системной схеме ETL_IA */
%global ETL_STG_CONNECT_OPTIONS;    /* Параметры подключения к системной схеме ETL_STG */
%global DM_REP_CONNECT_OPTIONS;		/* Параметры подключения к системной схеме DM_REP */
%global DM_ABT_CONNECT_OPTIONS;		/* Параметры подключения к системной схеме DM_ABT */
%global IA_CONNECT_OPTIONS;         /* Параметры подключения к системной схеме IA */
%global IA_CONNECT_SCHEMA;          /* Параметры подключения к системной схеме IA */

%global ETL_BULKLOAD_OPTIONS;       /* Опции BULKLOAD (только для работы со схемами ETL) */
%global ETL_FIX_ORPHANED_EVENTS;    /* Признак учета событий, не отнесенных ни к какому процессу */

%global SYS_OS_FAMILY;              /* Семейство текущей ОС (WIN, UNIX) */

%global CUR_API_URL;				/* Текущий API URL */

%global LSF_PENDING;                /* Статус процесса LSF - ожидание в очереди */
%global LSF_RUNNING;                /* Статус процесса LSF - исполнение */
%global LSF_SUSPENDED;              /* Статус процесса LSF - приостанов */
%global LSF_DONE;                   /* Статус процесса LSF - завершен успешно */
%global LSF_EXIT;                   /* Статус процесса LSF - завершен неуспешно */
%global LSF_KILLED;                 /* Статус процесса LSF - аварийно снят */
%global LSF_UNKNOWN;                /* Статус процесса LSF - неизвестен */
%global LSF_HIST_QUERY_DAYS;        /* Глубина, на которую обновляются статусы джобов, в днях */
%global LSF_ELIM_QUERY_S;           /* Интервал обновления ELIM, в секундах */

%global VF_FC_HORIZ;				/* Число интервалов для прогнозирования */
%global VF_FC_START_DT;				/* Дата начала прогноза (должна быть понедельником)*/
%global VF_FC_START_DT_SAS;			/* Дата начала прогноза (должна быть понедельником) в формате yymmdd10. */
%global VF_FC_START_MONTH_SAS;		/* Начало месяца, с которого начинается прогноз */
%global VF_HIST_END_DT;				/* Дата конца истории */
%global VF_FC_END_DT;				/* Понедельник последней недели, на которую должен быть прогноз */
%global VF_FC_AGG_END_DT;			/* Воскресенье последней недели, на которую должен быть прогноз */
%global VF_FC_AGG_END_DT_SAS;		/* воскресенье последней недели, на которую должен быть прогноз в формате yymmdd10.*/
%global VF_HIST_START_DT;			/* Дата начала истории */
%global VF_HIST_START_DT_SAS;		/* Дата начала истории (должна быть понедельником) в формате yymmdd10. */
%global VF_PMIX_ID;					/* ID VF-проекта, построенного на pbo_sal_abt*/
%global VF_PBO_ID;					/* ID VF-проекта, построенного на pmix_sal_abt*/


%global SAS_START_CMD;              /* Путь к start_sas */

/*===================================== GLOBAL ===================================*/
/* Здесь назначаются глобальные переменные, общие для всех фаз                    */
/*================================================================================*/

%let ETL_DBMS                       =  postgres;
%let ETL_ROOT                       =  /opt/sas/mcd_config;
%let ETL_DATA_ROOT                  =  /data;

%let ETL_FILE_STATIC_ROOT           =  &ETL_DATA_ROOT./files/static;
%let ETL_FILE_INPUT_ROOT            =  &ETL_DATA_ROOT./files/input;
%let ETL_FILE_OUTPUT_ROOT           =  &ETL_DATA_ROOT./files/output;

%let ETL_DBCS                       =  %eval(%sysfunc(getoption(DBCS)) eq DBCS);
%let ETL_PRECISION                  =  1e-10;
%let ETL_TYPE                       =  INC;
%let ETL_DEBUG                      =  1;

%let ETL_DWH_INIT_DT                =  %sysfunc(putn('01Jan2020'd, 8.));
%let ETL_CURRENT_JOB_ID             =  ;
%let ETL_MODULE_RC                  =  0;

%let ETL_TIME_FRAME_TYPE            =  FLOATING;
%let ETL_TIME_FRAME_VALUE           =  40;

%let ETL_MIN_DT                     =  %sysfunc(putn('01Jan2000'd, best.));
%let ETL_MAX_DT                     =  %sysfunc(putn('01Jan2100'd, best.));
%let ETL_MIN_DTTM                   =  %sysfunc(dhms(&ETL_MIN_DT, 0, 0, 0));
%let ETL_MAX_DTTM                   =  %sysfunc(dhms(&ETL_MAX_DT, 0, 0, 0));
%let ETL_SCD_NEW_DTTM               =  %sysfunc(dhms(&ETL_MIN_DT, 0, 0, 0));
%let ETL_SCD_FUTURE_DTTM            =  %sysfunc(putn('01Jan5999 00:00:00'dt, best.));
%let ETL_CURRENT_DT                 =  /*%sysfunc(date())*/ %sysfunc(inputn(01JUN2020,date9.));
%let ETL_CURRENT_DTTM               =  %sysfunc(dhms(&ETL_CURRENT_DT,0,0,0));

%let ETL_DEFAULT_COUNTRY            =  "RUS";
%let ETL_DEFAULT_CURRENCY           =  "RUR";
%let ETL_DEFAULT_LANGUAGE           =  "RUS";

%let IORC_SOK                       =  %sysrc(_SOK);
%let IORC_DSENOM                    =  %sysrc(_DSENOM);
%let IORC_DSENMR                    =  %sysrc(_DSENMR);

%let ETL_BK_INVALID                 =  F;
%let ETL_D0_ID                      =  "%sysfunc(repeat(&ETL_BK_INVALID, 2))";
%let ETL_D0_RK                      =  0;
%let ETL_D0_CD                      =  "DUM";

%let ETL_BULKLOAD_OPTIONS           =  BULKLOAD=NO BL_DEFAULT_DIR="&ETL_DATA_ROOT./sqlldr/" BL_DELETE_DATAFILE=YES;
%let ETL_FIX_ORPHANED_EVENTS        =  0;

%let CUR_API_URL					=  10.252.151.3;

%let LSF_PENDING                    =  Pending;
%let LSF_RUNNING                    =  Running;
%let LSF_SUSPENDED                  =  Suspended;
%let LSF_DONE                       =  Done;
%let LSF_EXIT                       =  Exit;
%let LSF_KILLED                     =  Killed;
%let LSF_UNKNOWN                    =  Unknown;
%let LSF_HIST_QUERY_DAYS            =  3;
%let LSF_ELIM_QUERY_S               =  20;

%let VF_FC_HORIZ					=  52;
%let VF_FC_START_DT 				= date'2019-11-04' /*date %tslit(&ETL_CURRENT_DT.)*/;
%let VF_FC_START_DT_SAS				= %sysfunc(inputn(%scan(%bquote(/*&ETL_CURRENT_DT.*/date'2019-11-04'),2,%str(%')),yymmdd10.));
%let VF_FC_START_MONTH_SAS 			= %sysfunc(intnx(month,&VF_FC_START_DT_SAS,0,b));
%let VF_HIST_END_DT 				= %sysfunc(intnx(day,&VF_FC_START_DT_SAS,-1),yymmddd10.);		
%let VF_FC_END_DT 					= %sysfunc(intnx(day,&VF_FC_START_DT_sas,7*(&VF_FC_HORIZ-1)),yymmddd10.);		
%let VF_FC_AGG_END_DT 				= %sysfunc(intnx(day,&VF_FC_START_DT_sas,7*&VF_FC_HORIZ-1),yymmddd10.);
%let VF_FC_AGG_END_DT_SAS 			= %sysfunc(intnx(day,&VF_FC_START_DT_sas,7*&VF_FC_HORIZ-1));
%let VF_HIST_START_DT 				= date'2017-01-02';
%let VF_HIST_START_DT_SAS			= %sysfunc(inputn(%scan(%bquote(&VF_HIST_START_DT),2,%str(%')),yymmdd10.));
%let VF_PMIX_ID						= 1ef9c222-17c4-477b-9667-a3ac07320c4e;
%let VF_PBO_ID 						= c27c04d6-8789-4b2a-af8d-b2f751dc8cd0;

%let SAS_START_CMD                  =  &ETL_ROOT/config/start_sas.cmd;

/*===================================== GLOBAL ===================================*/
/* Здесь исполняются глобальные назначения                                        */
/*================================================================================*/
libname ETL_FMT "&ETL_ROOT/format" filelockwait=10;

options
   lrecl          =  30000
   append         =  (sasautos="&ETL_ROOT/macro/common")
   append         =  (sasautos="&ETL_ROOT/macro/dbms/oracle")
   append         =  (sasautos="&ETL_ROOT/macro/dbms/postgres")
   append         =  (sasautos="&ETL_ROOT/macro/etl")
   append         =  (sasautos="&ETL_ROOT/macro/job")
   append         =  (sasautos="&ETL_ROOT/macro/step")
   fmtsearch      =  (FORMATS WORK ETL_FMT)
   cmplib         =  ETL_FMT.fcmp
   compress       =  binary
   mprint
   mprintnest
   mlogic
   mlogicnest
   symbolgen
   mrecall
   source
   source2
   fullstimer
   msglevel    = 'I'
   missing     = '.'
   nosortequals
   varinitchk     =  NOTE
   dsoptions      =  "note2err"
   /*sastrace=',,,d'
   sastraceloc=saslog*/
;

/* Настройка логирования */
%log4sas;

/* Назначение переменных, которые не должны выводиться в лог */
%log_disable;
%let ETL_SYS_CONNECT_OPTIONS        =  server="10.252.151.3" port=5452 user=etl_sys password="{SAS002}DCB5DA3808FAC9EE26380F5007B9E276" database=etl defer=yes;
%let ETL_IA_CONNECT_OPTIONS         =  server="10.252.151.3" port=5452 user=etl_ia password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=etl defer=yes readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=32768";
%let ETL_STG_CONNECT_OPTIONS        =  server="10.252.151.3" port=5452 user=etl_stg password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=etl defer=yes readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=32768";
%let DM_REP_CONNECT_OPTIONS         =  server="10.252.151.3" port=5452 user=dm_rep password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=etl defer=yes readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=32768";
%let DM_ABT_CONNECT_OPTIONS         =  server="10.252.151.3" port=5452 user=dm_abt password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=etl defer=yes readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=32768";
%let IA_CONNECT_OPTIONS             =  DEFER=YES  PATH=WARE  USER=SAS_USER  PASSWORD="{SAS002}C4A120480F3F302F49249CD238FA3D0F" UPDATE_LOCK_TYPE=row;
%let IA_CONNECT_SCHEMA              =  sas_interf;
%log_enable;

/*==================================== LIBNAMES ==================================*/
/* Здесь исполняются назначения библиотек                                         */
/*================================================================================*/

libname etl_sys postgres &ETL_SYS_CONNECT_OPTIONS schema=etl_sys;

libname etl_ia postgres &ETL_IA_CONNECT_OPTIONS schema=etl_ia;

libname etl_stg postgres &ETL_STG_CONNECT_OPTIONS schema=etl_stg;

/*libname dm_rep postgres &DM_REP_CONNECT_OPTIONS schema=dm_rep;*/

/*libname dm_abt postgres &DM_ABT_CONNECT_OPTIONS schema=dm_abt;*/

libname pt postgres server="10.252.151.3" port=5452 user=pt password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" database=pt defer=yes schema=public readbuff=32767 conopts="UseServerSidePrepare=1;UseDeclareFetch=1;Fetch=8192";

LIBNAME ia ORACLE &IA_CONNECT_OPTIONS SCHEMA=sas_interf;

libname ETL_STG2 "/data/ETL_STG";

/*==================================== COMPUTED  =================================*/
/* Вычисляемые настройки                                                          */
/*================================================================================*/
%macro init_computed;

   /* ETL_TIME_FRAME_DT - Вычисляемая дата начала интервала загрузки */
   %let ETL_TIME_FRAME_DT = &ETL_MIN_DT;
   %if &ETL_TYPE eq INC %then %do;
      %if &ETL_TIME_FRAME_TYPE eq FIXED %then %do;
         %let ETL_TIME_FRAME_DT = &ETL_TIME_FRAME_VALUE;
      %end;
      %else %if &ETL_TIME_FRAME_TYPE eq FLOATING %then %do;
         %let ETL_TIME_FRAME_DT = %eval(%sysfunc(today()) - &ETL_TIME_FRAME_VALUE);
      %end;
   %end;

   %let ETL_TIME_FRAME_DTTM = %sysfunc(dhms(&ETL_TIME_FRAME_DT, 0, 0, 0));

   /* Внешнее указание режима загрузки */
   %if %sysfunc(envlen(ETL_TYPE)) ne -1 %then
      %let ETL_TYPE = %sysget(ETL_TYPE);

   /* SYS_OS_FAMILY - Семейство текущей ОС */
   %if &SYSSCP = WIN %then %do;
      %let SYS_OS_FAMILY = WIN;
   %end;
   %else %if "&SYSSCPL" = "HP-UX" or %upcase(&SYSSCPL) = LINUX or &SYSSCPL = AIX or %upcase(&SYSSCPL) = SUNOS %then %do;
      %let SYS_OS_FAMILY = UNIX;
   %end;
   %else %do;
      %let SYS_OS_FAMILY = ;
      %log4sas_fatal (cwf.config.initialize_global, OS &SYSSCP is not supported);
   %end;

   /* Настройка логирования */
   %if &ETL_DEBUG %then %do;
      %log4sas_logger(cwf, "additivity=true level=DEBUG");
      %log4sas_logger(dwf, "additivity=true level=DEBUG");
   %end;

   /* Переопределение переменных из окружения */
   %macro sm_override_var;
      %if %sysfunc(sysexist(&name)) %then %do;
         %let &name = %sysget(&name);
         %log4sas_info (cwf.config.initialize_global, Value of &name overridden to %nrbquote(&&&name));
   %end;
   %mend sm_override_var;
   %util_loop_data (mpData=sashelp.vmacro, mpLoopMacro=sm_override_var, mpWhere= scope="GLOBAL");
%mend init_computed;
%init_computed;
