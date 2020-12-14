cas casauto;

libname public cas caslib=public;
libname mycas cas;

/* data mycas.MCD_SCENARIO_COMPARISON; */
/* 	set dm_rep.MCD_SCENARIO_COMPARISON; */
/* run; */
/*  */
/* proc casutil outcaslib="public"; */
/* 	DROPTABLE CASDATA="MCD_SCENARIO_COMPARISON" INCASLIB="public" QUIET; */
/* 	promote casdata="MCD_SCENARIO_COMPARISON"; */
/* quit; */
/*  */
data mycas.VA_DATAMART;
	set dm_rep.VA_DATAMART;
run;

/*  */
/* proc casutil; */
/*   load data=dm_rep.VA_DATAMART casout='VA_DATAMART' outcaslib='public' replace; */
/* run; */

proc casutil outcaslib="public";
	DROPTABLE CASDATA="VA_DATAMART" INCASLIB="public" QUIET;
	promote casdata="VA_DATAMART";
quit;

cas casauto terminate;