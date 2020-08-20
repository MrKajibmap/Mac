/*****************************************************************
*  ВЕРСИЯ:
*     $Id: fb7651994d65a9a499b3acb240cebf4a02473609 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос отключения вывода в лог.
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

%macro log_disable;
   %macro _switch_option_off (option);
       %global LOG_&OPTION;
       %let LOG_&OPTION = %sysfunc(getoption(&OPTION));
       options NO&OPTION;
   %mend _switch_option_off;

   /* Отключение вывода информации в лог по опциям MPRINT, MLOGIC, SYMBOLGEN и SOURCE(2) */
   %util_loop (mpMacroName=_switch_option_off, mpWith=mprint mlogic symbolgen source source2);
%mend log_disable;