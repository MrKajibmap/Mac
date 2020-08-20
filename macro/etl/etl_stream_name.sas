/*****************************************************************
*  ВЕРСИЯ:
*     $Id: fafd2729993743f6edcb1e5f364a0002a588865a $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Возвращает имя, уникальный ид и контекст текущего потока ETL.
*
*  ПАРАМЕТРЫ:
*     mpOutNameKey         -  имя выходной макропеременной, в которую будет помещено имя текущего потока
*     mpOutIdKey           -  имя выходной макропеременной, в которую будет помещен уникальный ид текущего потока
*     mpOutContextKey      -  имя выходной макропеременной, в которую будет помещен контекст текущего потока (LSF, DIS, WSS (WorkSpaceServer), OSS (OperatingSystemServices))
*
******************************************************************
*  Использует:
*     %is_blank
*
*  Устанавливает макропеременные:
*     &mpOutNameKey
*     &mpOutIdKey
*     &mpOutContextKey
*
******************************************************************
*  Пример использования:
*     %local lmvStreamName lmvContext;
*     %etl_stream_name (mpOutNameKey=lmvStreamName, mpOutContextKey=lmvContext);
*     %if &lmvContext = LSF %then ...
*
******************************************************************
*  22-12-2014  Нестерёнок     Начальное кодирование
*  18-03-2015  Сазонов 		  Добавил тип WSS (для режима без LSF)
*  16-02-2017  Нестерёнок     Добавил mpOutIdKey
******************************************************************/

%macro etl_stream_name (
   mpOutNameKey            =  ,
   mpOutIdKey              =  ,
   mpOutContextKey         =
);
   %if %is_blank(mpOutNameKey) %then %do;
      %local lmvStreamName;
      %let mpOutNameKey = lmvStreamName;
   %end;
   %if %is_blank(mpOutIdKey) %then %do;
      %local lmvStreamId;
      %let mpOutIdKey = lmvStreamId;
   %end;
   %if %is_blank(mpOutContextKey) %then %do;
      %local lmvContext;
      %let mpOutContextKey = lmvContext;
   %end;

   %if %sysfunc(sysexist(LSB_JOBNAME)) %then %do;
      %let &mpOutContextKey   = LSF;

      %local lmvLsfName;
      /* LSB_JOBNAME = 344:.\sassrv:Flow_Service_User_Request_VTB:704_000_SERVICE_USER_REQUEST */
      %let lmvLsfName      = %sysfunc(sysget(LSB_JOBNAME));
      %let &mpOutIdKey     = %scan(&lmvLsfName, 1, :);
      %let &mpOutNameKey   = %scan(&lmvLsfName, 3, :);
   %end;
   %else %if %sysfunc(sysexist(ETL_BATCH)) %then %do;
      %let &mpOutContextKey   = WSS;
      %let &mpOutIdKey        = &SYSJOBID;
      %let &mpOutNameKey      = &ETLS_JobName;
   %end;
   %else %if "&SYSPROCESSMODE" = "SAS Stored Process Server" and %symexist(_SESSIONID) %then %do;
      %let &mpOutContextKey   = STP;
      %let &mpOutIdKey        = &SYSJOBID;
      %let &mpOutNameKey      = &_SESSIONID;
   %end;
/* Параметры CMD_FLOWNAME=${BASH_REMATCH[1]} и BASH_PROCESS_ID=$PPID задаются в sh скрипте*/
   %else %if %sysfunc(sysexist(CMD_FLOWNAME)) %then %do;
      %let &mpOutContextKey   = OSS;
      %let &mpOutIdKey        = %sysfunc(sysget(BASH_PROCESS_ID));	  
      %let &mpOutNameKey      = %sysfunc(sysget(CMD_FLOWNAME));
   %end;   
   %else %do;
      %let &mpOutContextKey  = DIS;
      %let &mpOutIdKey       = &SYSJOBID;
      %let &mpOutNameKey     = DIS_&SYSJOBID;
   %end;
%mend etl_stream_name;
