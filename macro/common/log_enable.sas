/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 00b7fef9c8d3e6dfa252cc10bbfdd1a1df9c0f52 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос восстановления вывода в лог.
*
*  ПАРАМЕТРЫ:
*     нет
*
******************************************************************
*  Использует:
*     %util_loop
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     нет
*
******************************************************************
*  Пример использования:
*     %log_disable;
*     %let MySecretPassword = Orion123;
*     libname test oracle path=myserver user=myuser password="&MySecretPassword";
*     %log_enable;
*
******************************************************************
* 19-12-2011   Нестерёнок  Начальное кодирование
******************************************************************/

%macro log_enable;
   %macro _switch_option_on (option);
       options &&LOG_&OPTION.;
   %mend _switch_option_on;

   /* Восстановление вывода информации в лог по опциям MPRINT, MLOGIC, SYMBOLGEN и SOURCE(2) */
   %util_loop (mpMacroName=_switch_option_on, mpWith=mprint mlogic symbolgen source source2);
%mend log_enable;