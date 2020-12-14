%macro load_PT;

	libname sas_stg '/data/ETL_STG';

	%hier_pt(mpLvl=5, mpIn=SAS_STG.IA_PRODUCT_HIERARCHY, mpOut=PT_PRODUCT_HIERARCHY);

	proc sql;
		create table PT_PRODUCT_HIERARCHY_DTTM as
		select *, datetime() as valid_from_dttm format=DATETIME25.6, &ETL_SCD_FUTURE_DTTM. as valid_to_dttm format=DATETIME25.6
		from PT_PRODUCT_HIERARCHY 
		;
	quit;

	proc sql;
		insert into PT.PRODUCT_HIERARCHY
		select * from PT_PRODUCT_HIERARCHY_DTTM
		;
	quit;

	%hier_pt(mpLvl=4, mpIn=SAS_STG.IA_PBO_LOC_HIERARCHY, mpOut=PT_PBO_LOC_HIERARCHY);

	proc sql;
		create table PT_PBO_LOC_HIERARCHY_DTTM as
		select *, datetime() as valid_from_dttm format=DATETIME25.6, &ETL_SCD_FUTURE_DTTM. as valid_to_dttm format=DATETIME25.6
		from PT_PBO_LOC_HIERARCHY 
		;
	quit;

	proc sql;
		insert into PT.INTERNAL_ORG_HIERARCHY
		select * from PT_PBO_LOC_HIERARCHY_DTTM
		;
	quit;

	proc sql;
		create table PRODUCT as
		select p.PRODUCT_ID as member_rk, ifc(h.product_lvl=5,'['||strip(put(p.PRODUCT_ID,best.))||'] '||p.PRODUCT_NM,p.PRODUCT_NM) as member_nm, 
			datetime() as valid_from_dttm format=DATETIME25.6, 
			&ETL_SCD_FUTURE_DTTM. as valid_to_dttm format=DATETIME25.6,
			. as order_no
		from SAS_STG.IA_PRODUCT p
		left join SAS_STG.IA_PRODUCT_HIERARCHY h
			on h.PRODUCT_ID=p.PRODUCT_ID
		;
	quit;

	proc sql;
		insert into PT.PRODUCT
		select * from PRODUCT
		;
	quit;

	proc sql;
		create table PBO_LOC as
		select  ph4.PBO_LOCATION_ID as member_rk, 
				case when ph.pbo_location_lvl=4 then
				'['||catx('] [',strip(put(ph4.PBO_LOCATION_ID,best.)),
					pa1.pbo_loc_attr_value,
					ifc(strip(pa5.pbo_loc_attr_value)='Yes','BREAKFAST',''),
					ifc(strip(pa9.pbo_loc_attr_value)='Yes','McCafe',ifc(strip(pa9.pbo_loc_attr_value)='Yes EOTF','McCafe EOTF','')))
				||'] '||PBO_LOCATION_NM
				else PBO_LOCATION_NM end as member_nm,
			datetime() as valid_from_dttm format=DATETIME25.6, 
			&ETL_SCD_FUTURE_DTTM. as valid_to_dttm format=DATETIME25.6,
			. as order_no
		from SAS_STG.IA_PBO_LOCATION ph4
		left join SAS_STG.IA_PBO_LOC_HIERARCHY ph
			on ph.pbo_location_id=ph4.pbo_location_id
		left join SAS_STG.IA_PBO_LOC_ATTRIBUTES pa1
			on pa1.pbo_location_id=ph4.pbo_location_id
			and pa1.pbo_loc_attr_nm='BUILDING_TYPE'
		left join SAS_STG.IA_PBO_LOC_ATTRIBUTES pa5
			on pa5.pbo_location_id=ph4.pbo_location_id
			and pa5.pbo_loc_attr_nm='BREAKFAST'
		left join SAS_STG.IA_PBO_LOC_ATTRIBUTES pa9
			on pa9.pbo_location_id=ph4.pbo_location_id
			and pa9.pbo_loc_attr_nm='MCCAFE_TYPE'
		;
	quit;

	proc sql;
		insert into PT.INTERNAL_ORG
		select * from PBO_LOC
		;
	quit;
	
	data sas_stg.channel_lookup;
		set SAS_STG.IA_CHANNEL;
		pt_member_rk=_n_;
	run;

	/*proc sql;
		create table CHANNEL as
		select pt_member_rk as member_rk, CHANNEL_NM as member_nm, 
				datetime() as valid_from_dttm format=DATETIME25.6, 
				&ETL_SCD_FUTURE_DTTM. as valid_to_dttm format=DATETIME25.6,
				. as order_no
		from SAS_STG.channel_lookup
		;
	quit;

	proc sql;
		create table CHANNEL_HIERARCHY as
		select a.pt_member_rk as CHANNEL_ID, a.CHANNEL_LVL, b.pt_member_rk as PARENT_CHANNEL_ID
		from SAS_STG.channel_lookup a
		left join SAS_STG.channel_lookup b
			on b.channel_cd=a.parent_channel_cd
		;
	quit;

	%hier_pt(mpLvl=2, mpIn=WORK.CHANNEL_HIERARCHY, mpOut=PT_CHANNEL_HIERARCHY);

	proc sql;
		create table PT_CHANNEL_HIERARCHY_DTTM as
		select *, datetime() as valid_from_dttm format=DATETIME25.6, &ETL_SCD_FUTURE_DTTM. as valid_to_dttm format=DATETIME25.6
		from PT_CHANNEL_HIERARCHY 
		;
	quit;*/
	
	proc sql;
		create table PT_CHANNEL_HIERARCHY_DTTM as
		select 
				pt_member_rk as prnt_member_rk,
				pt_member_rk as member_rk, 
				0 as btwn_lvl_cnt, 
				'Y' as is_bottom_flg,
				'Y' as is_top_flg,
				datetime() as valid_from_dttm format=DATETIME25.6, 
				&ETL_SCD_FUTURE_DTTM. as valid_to_dttm format=DATETIME25.6
		from sas_stg.channel_lookup
		where channel_cd='ALL'
		;
	quit;
	
	proc sql;
		insert into PT.CHANNEL
		select * from CHANNEL
		;
	quit;

	proc sql;
		insert into PT.CHANNEL_HIERARCHY
		select * from PT_CHANNEL_HIERARCHY_DTTM
		;
	quit;
	
	proc sql;
		create table SEGMENT as
		select SEGMENT_ID as member_rk, SEGMENT_NM as member_nm, 
					datetime() as valid_from_dttm format=DATETIME25.6, 
					&ETL_SCD_FUTURE_DTTM. as valid_to_dttm format=DATETIME25.6,
					. as order_no
		from SAS_STG.IA_SEGMENT
		;
	quit;
	
	proc sql;
		create table PT_SEGMENT_HIERARCHY_DTTM as
		select 
			SEGMENT_ID as prnt_member_rk,
			SEGMENT_ID as member_rk, 
			0 as btwn_lvl_cnt, 
			'Y' as is_bottom_flg,
			'Y' as is_top_flg,
			datetime() as valid_from_dttm format=DATETIME25.6,
			&ETL_SCD_FUTURE_DTTM. as valid_to_dttm format=DATETIME25.6
		from SAS_STG.IA_SEGMENT
		;
	quit;
	
	proc sql;
		insert into PT.SEGMENT
		select * from SEGMENT
		;
	quit;
	
	proc sql;
		insert into PT.SEGMENT_HIERARCHY
		select * from PT_SEGMENT_HIERARCHY_DTTM
		;
	quit;

%mend load_PT;