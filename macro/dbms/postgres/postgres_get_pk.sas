/*****************************************************************
*  ВЕРСИЯ:
*     $Id:  $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Выводит PK в ворковую таблицу
*     
*
*  ПАРАМЕТРЫ:
*     mpSchema              	 +  имя набора параметров подключения к БД (ETL_SYS, ETL_STG и т.д.)
*     mpTable                    +  имя таблицы
*
******************************************************************
*  Использует:
*     %postgres_connect
*	  %postgres_string
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     нет
*
******************************************************************
*  ПРИМЕР ИСПОЛЬЗОВАНИЯ:
*     %postgres_get_pk(mpSchema=ETL_IA, mpTable=media_SNUP);
*
******************************************************************
*  13-04-2020  Зотиков     Начальное кодирование
******************************************************************/
%macro postgres_get_pk (
   mpSchema                       =  ,
   mpTable                   	  =  
);
  
  %let lmvTable = %lowcase(&mpTable.);
  
	proc sql;
		%postgres_connect (mpLoginSet=&mpSchema.);

			create table PK_&lmvTable. as
			select * 
			from connection to postgres       
				(SELECT c.column_name, c.data_type
				FROM information_schema.table_constraints tc 
				JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
				JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
				AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
				WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = %postgres_string(&lmvTable.));  

		disconnect from postgres;
	quit;

%mend postgres_get_pk;
