%etl_arch_to_delta (
	   mpIn                       =  &tpIn,
	   mpFieldPK                  =  &tpFieldPK,
	   mpOut                      =  &tpOut,
	   mpDeleteFlg                =  &tpDeleteFlg,
	   mpNodupKey				      =  &tpNodupKey 
	);

%etl_update_dds (
   mpIn                    =  ,
   mpFieldsPK              =  ,
   mpFieldStartDttm        =  ,
   mpFieldEndDttm          =  ,
   mpFieldProcessedDttm    =  ,
   mpOut                   =  ,
   mpJrnl                  =  ,
   mpJrnlStartDttm         =  jrnl_from_dttm,
   mpJrnlEndDttm           =  jrnl_to_dttm,
   mpGenericUpdate         =  No,
   mpEngine				   =  base
);