/* Макрос разворачивает переменную var в список элементов массива, разделенных запятой */
/* var[t-0],var[t-1],var[t-2],... */
%macro rtp_argt(var,index,start,end);
	%do ii=&start. %to &end.;
	 &var.[&index.-&ii.]
	 %if &ii. ne &end. %then %do;
	  ,
	 %end;
	%end;
%mend rtp_argt;