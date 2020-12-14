/*****************************************************************
*  ВЕРСИЯ:
*     $Id: $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Загрузка ресурсов, со статусом N, из ETL_STG в ETL_IA 
*
******************************************************************
*  14-04-2020  Зотиков     Начальное кодирование
******************************************************************/
%macro m_002_load_etl_ia;

		
	proc sql;
		create table open_res as
		select  put(resource_id,res_id_cd.) as mpResource
		from etl_sys.etl_resource_registry
		where version_id in(select MAX(version_id) 
							from etl_sys.etl_resource_registry 
							where status_cd = 'N' 
							group by resource_id)
		/*and put(resource_id,res_id_cd.) in ('PRICE', 'MEDIA')*/
		;
	quit;
	
	%util_loop_data (mpData=work.open_res, mpLoopMacro=load_etl_ia);
		

%mend m_002_load_etl_ia;