/*****************************************************************
* ВЕРСИЯ:
*   $Id: c510eb2d4cec4c4b9587157ba7329d7232fb3c16 $
*
******************************************************************
* НАЗНАЧЕНИЕ:
*   Мержит два дельта набора (+snup)
*
* ПАРАМЕТРЫ:
******************************************************************
* Пример использования:
*%etl_merge_deltas(
*   mpFieldPK=&tpFieldPK
*  ,mpFieldDigest1SNUP1=&tpFieldDigest1SNUP1
*  ,mpFieldDigest1SNUP2=&tpFieldDigest1SNUP2
*  ,mpFieldDigest2SNUP1=&tpFieldDigest2SNUP1
*  ,mpFieldDigest2SNUP2=&tpFieldDigest2SNUP2
*  ,mpDelta1=&tpDelta1
*  ,mpSnup1=&tpSnup1
*  ,mpDelta2=&tpDelta2
*  ,mpSnup2=&tpSnup2
*  ,mpOutDelta=&tpOutDelta
*  ,mpOutSnup=&tpOutSnup
*);
*
******************************************************************
* 30-10-2012   kuzenkov    Начальное кодирование
******************************************************************/

%MACRO etl_merge_deltas(
   mpFieldPK=
  ,mpFieldDigest1SNUP1= ,mpFieldDigest1SNUP2= ,mpFieldDigest2SNUP1= ,mpFieldDigest2SNUP2=
  ,mpDelta1= ,mpSnup1=
  ,mpDelta2= ,mpSnup2=
  ,mpOutDelta= ,mpOutSnup=
  ,mpFieldDelta=etl_delta_cd
  );

  DATA &mpOutDelta;
    MERGE &mpDelta1 &mpDelta2;
    BY &mpFieldPK &mpFieldDelta;
  RUN;  
  %error_check (mpStepType=DATA);

  DATA &mpOutSnup;
    MERGE
      &mpSnup1(RENAME=(
  %IF not %is_blank(mpFieldDigest1SNUP1) %THEN %DO;
        &mpFieldDigest1SNUP1=__etl_dg11
  %END;
  %IF not %is_blank(mpFieldDigest2SNUP1) %THEN %DO;
        &mpFieldDigest2SNUP1=__etl_dg21
  %END;
        ))
      &mpSnup2(RENAME=(
  %IF not %is_blank(mpFieldDigest1SNUP2) %THEN %DO;
        &mpFieldDigest1SNUP2=__etl_dg12
  %END;
  %IF not %is_blank(mpFieldDigest2SNUP2) %THEN %DO;
        &mpFieldDigest2SNUP2=__etl_dg22
  %END;
        ))
    ;
    BY &mpFieldPK &mpFieldDelta;

  %IF not %is_blank(mpFieldDigest1SNUP1) %THEN %DO;
    &mpFieldDigest1SNUP1 = CoalesceC(__etl_dg11, &mpFieldDigest1SNUP1);
  %END;
  %IF not %is_blank(mpFieldDigest1SNUP2) %THEN %DO;
    &mpFieldDigest1SNUP2 = CoalesceC(__etl_dg12, &mpFieldDigest1SNUP2);
  %END;
  %IF not %is_blank(mpFieldDigest2SNUP1) %THEN %DO;
    &mpFieldDigest2SNUP1 = CoalesceC(__etl_dg21, &mpFieldDigest2SNUP1);
  %END;
  %IF not %is_blank(mpFieldDigest2SNUP2) %THEN %DO;
    &mpFieldDigest2SNUP2 = CoalesceC(__etl_dg22, &mpFieldDigest2SNUP2);
  %END;

    DROP
  %IF not %is_blank(mpFieldDigest1SNUP1) %THEN %DO;
      __etl_dg11
  %END;
  %IF not %is_blank(mpFieldDigest1SNUP2) %THEN %DO;
      __etl_dg12
  %END;
  %IF not %is_blank(mpFieldDigest2SNUP1) %THEN %DO;
      __etl_dg21
  %END;
  %IF not %is_blank(mpFieldDigest2SNUP2) %THEN %DO;
      __etl_dg22
  %END;
    ;
  RUN;

  %error_check (mpStepType=DATA);

%MEND;
