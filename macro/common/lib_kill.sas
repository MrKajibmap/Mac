/*****************************************************************
*  ������:
*     
*
******************************************************************
*  ����������:
*     ������� ��� ������� �� �������� ����������.
*
*  ���������:
*     mpLib       	+ LIBNAME ����������
*
******************************************************************
*  ����������:
*	  
*  ������������� ���������������:
*
*  �����������:
*
******************************************************************
*  ������ �������������:
*
*  %lib_kill(mpLib=WORK);
*
******************************************************************
*  25-01-2016   �������        ��������� �����������
******************************************************************/

%macro lib_kill(mpLib=WORK);
	proc datasets lib=&mpLib kill nolist;
	quit;
%mend lib_kill;