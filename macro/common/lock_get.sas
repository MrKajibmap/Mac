/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 684f631359b04d4e9d24537fe368ada50ccf8ace $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для получения блокировки
*
*  ПАРАМЕТРЫ:
*     mpLib                +  библиотека содержащая ресурс для блокировки 
*                             должна быть доступна всем процессам работающим с критической секцией
*     mpLockNm             +  название блокировки
*                             По умолчанию lck
*     mpTimeout            +  таймаут
*                             По умолчанию 60
*
******************************************************************
*  Использует:
*     
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %lock_get(work_dq);
*       ...
*     %lock_release(work_dq);  
*
****************************************************************************
*  28-04-2017  Сазонов     Начальное кодирование
****************************************************************************/
%macro lock_get(mpLib,mpLockNm=lck,mpTimeout=60);
    %local lmvLockTab lmvStopDttm lmvRc;
    %let lmvLockTab=&mpLib..&mpLockNm._lck;
    %let lmvStopDttm=%sysevalf(%sysfunc(datetime())+&mpTimeout);
    %if ^%member_exists(&lmvLockTab) %then %do;
        data &lmvLockTab;
            stop;
        run;
    %end;
    lock &lmvLockTab nomsg;
    %do %while(&SYSLCKRC ne 0 and %sysfunc(datetime()) < &lmvStopDttm);
        %let lmvRc=%sysfunc(sleep(1));
        %if ^%member_exists(&lmvLockTab) %then %do;
            data &lmvLockTab;
                stop;
            run;
        %end;
        lock &lmvLockTab nomsg;
    %end;
    %if &SYSLCKRC ne 0 %then %do;
        %job_event_reg (mpEventTypeCode=DATA_NOT_AVAILABLE,
            mpEventDesc=%bquote(Невозможно получить lock на &lmvLockTab)
            mpEventValues=%bquote(SYSLCKRC=&SYSLCKRC));
        %return;
    %end;
%mend lock_get;