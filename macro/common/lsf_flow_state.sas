/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 40f62882d8ee7d86789f11233380a8e0ba564016 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для получения flow state по flow id в lsf
*
*  ПАРАМЕТРЫ:
*     mpFlowId                +  id потока lsf
*     mpOutFlowState          +  выходня переменная с flow state
*
******************************************************************
*  Использует:
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*   %local lmvFlowState;
*   %lsf_flow_state(mpFlowId=36411,mpOutFlowState=lmvFlowState);
*
****************************************************************************
*  28-02-2017  Сазонов     Начальное кодирование
****************************************************************************/
%macro lsf_flow_state(mpFlowId=,mpOutFlowState=);
    filename jflows pipe "jflows &mpFlowId" lrecl=200;
    data _null_;
        infile jflows;
        input;
        if _infile_=:'STATE' then do;
            input;
            call symput("&mpOutFlowState", scan(_infile_,1));
            stop;
        end;
    run;
%mend lsf_flow_state;