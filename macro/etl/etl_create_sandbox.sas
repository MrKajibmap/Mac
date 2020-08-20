/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 76ca704180dc28cb7f17bf5083f5380502d6b181 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Создает песочницу для ETL потока.
*
*  ПАРАМЕТРЫ:
*	  mpSnapId         +   Номер среза
* 	  mpSnapDataIDs    +   Номера блоков данных (через запятую)
*
******************************************************************
*  Использует:
*     %unique_id
*     %etl_get_stream_id
*     %sys_command
*
*  Устанавливает макропеременные:
*
******************************************************************
*  Пример использования:
*     %etl_create_sandbox;
*
******************************************************************
*  01-01-2019  Колосов     Начальное кодирование
*  15-01-2019  Задояный    Связь песочницы с потоком
*  01-02-2019  Колосов     Добавил входные параметры
******************************************************************/

%macro etl_create_sandbox(mpSnapId=,mpSnapDataIDs=);

	/* Инициализация */
	%local lmvCMDRes lmvSandboxFolder lmvStreamID lmvSandboxID;

	%let lmvCMDRes = ;
	

	/* Получаем ID потока */
	%etl_get_stream_id(mpJobID=&JOB_ID., mpOutStreamID=lmvStreamID);
	%put &=lmvStreamID;

	/* Создаем ID песочницы	*/
	%unique_id (mpOutKey=lmvSandboxID, mpSequenceName=ETL_FLOW_SANDBOX_SEQ, mpLoginSet=ETL_SYS);
	%put &=lmvSandboxID;

	%let lmvSandboxFolder = %bquote(&lmvSandboxID._&mpSnapId._(&mpSnapDataIDs));

	/* Инициализация песочницы */
	%_init_sandbox;

	/*	Создаем запись о песочнице с привязкой к JOB_ID*/
	proc sql noprint;
      %&ETL_DBMS._connect(mpLoginSet=ETL_SYS);

      execute (
			insert into etl_sys.ETL_FLOW_SANDBOX (JOB_ID, STREAM_ID, HOST_NM, HOST_PROCESS_ID, SANDBOX_PATH, SANDBOX_ID)
			values (	%&ETL_DBMS._number(&JOB_ID.), 
						%&ETL_DBMS._number(&lmvStreamID.), 
						%&ETL_DBMS._string(&SYSHOSTNAME), 
						%&ETL_DBMS._number(&SYSJOBID), 
						%&ETL_DBMS._string(&IFRS_SANDBOX_ROOT./&lmvSandboxFolder.), 
						%&ETL_DBMS._string(&lmvSandboxID.)
					)
      ) by &ETL_DBMS;

      %error_check (mpStepType=SQL_PASS_THROUGH);

      disconnect from &ETL_DBMS;
	quit;

%mend etl_create_sandbox;
