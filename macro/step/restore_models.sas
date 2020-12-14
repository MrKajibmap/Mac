%macro restore_models;
	%if %sysfunc(SESSFOUND(casauto)) = 0 %then %do; 
		cas casauto;
		caslib _all_ assign;
	%end;
	/*restore models*/
	%let external_modeltable=/data/files/input/PMIX_MODEL_TABLE.csv;
	%let modeltable=PMIX_MODEL_TABLE;
	%macro load_model_table(mpFile=&external_modeltable., mpModTable=&modeltable.);
		proc casutil incaslib="Models" outcaslib="Models";
			droptable casdata="&mpModTable." quiet;
		run;

		%let max_length = $500;

		data models.&mpModTable.;
			length filter model params interval nominal &max_length.;
			infile "&mpFile." dsd firstobs=2;                 
			input filter $ model $ params $ interval $ nominal $ train score n;                            
		run;
		
		proc casutil;                           
			save casdata="&mpModTable." incaslib="models" outcaslib="models" replace; 
			promote casdata="&mpModTable." incaslib="Models" outcaslib="Models";
		run;
	%mend load_model_table;
	%load_model_table;
	%macro restore_mod;
		data models;
			set models.&modeltable.;
		run;
		data _null_;
			set models end=_end;
			if _end then call symputx('nmod',_n_);
		run;
		%do i=1 %to &nmod;
			data _null_;
				set models (obs=&i. firstobs=&i);
				call symputx('model',model);
			run;
		    %put &i &model;
					  
			%if %sysfunc(fileexist(/data/dm_abt/&model..ast)) %then %do;
				proc casutil;
					droptable casdata="&model." incaslib="models" quiet;
				run;
				proc astore;
					upload RSTORE=MODELS.&model store="/data/dm_abt/&model..ast"; 
				run;
				proc casutil;
					promote casdata="&model." incaslib="models" outcaslib="models";
				run;
			%end;
			%else %do;
					%put WARNING: Current file "/data/dm_abt/&model..ast" does not exist;
			%end;
		%end;
	
	%mend;
	%restore_mod;

	%let external_modeltable=/data/files/input/MASTER_MODEL_TABLE.csv;
	%let modeltable=MASTER_MODEL_TABLE;
	%macro load_model_table(mpFile=&external_modeltable., mpModTable=&modeltable.);
		proc casutil incaslib="Models" outcaslib="Models";
			droptable casdata="&mpModTable." quiet;
		run;

		%let max_length = $500;

		data models.&mpModTable.;
			length filter model params interval nominal &max_length.;
			infile "&mpFile." dsd firstobs=2;                 
			input filter $ model $ params $ interval $ nominal $ train score n;                            
		run;
		
		proc casutil;                           
			save casdata="&mpModTable." incaslib="models" outcaslib="models" replace; 
			promote casdata="&mpModTable." incaslib="Models" outcaslib="Models";
		run;
	%mend load_model_table;
	%load_model_table;
	%macro restore_mod_mast;
	data models;
		set models.&modeltable.;
	run;
	data _null_;
		set models end=_end;
		if _end then call symputx('nmod',_n_);
	run;
	%do i=1 %to &nmod;
		%symdel model;
		
		data _null_;
		set models (obs=&i. firstobs=&i.);
		call symputx('model',model);
		run;
		%put &i &model;
		
	 	%if %sysfunc(fileexist(/data/dm_abt/master_FOREST_&I..ast)) %then %do;
			  proc casutil;
				droptable casdata="&model." incaslib="models" quiet;
			  run;
			  proc astore;
				/*upload RSTORE=MODELS.MASTER_&model store="/data/dm_abt/&model..ast";*/
				upload RSTORE=MODELS.&model store="/data/dm_abt/master_FOREST_&I..ast";
			  run;
			  proc casutil;
				promote casdata="&model." incaslib="models" outcaslib="models";
			  run;
		%end;
		%else %do;
				%put WARNING: Current file "/data/dm_abt/master_FOREST_&I..ast" does not exist;
		%end;
		
		%symdel model;
	%end;
	%mend;
	%restore_mod_mast;

%mend restore_models;