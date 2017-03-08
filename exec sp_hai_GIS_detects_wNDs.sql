use equis
go


exec
sp_hai_GIS_detects_wNDs 

	 47  --@facility_id int =   2016423 
	,null --@loc_grp varchar (1000) = null
	,'BS_ROW_GW_2016_4Q' --@task_code varchar (1000)
	,'pge gw tph' --@mth_grp as varchar (200)
	,null --@param varchar (1000)
	,'N83SPCA III Ft' --@coord_type varchar (50) 
	,'ug/l' --@units varchar (20) 
	,'PGE-SL-WG-201602' --@screening_level varchar (200) 
	,'n' --@show_unvalidated_yn varchar (10)
	,'y' --@show_detects_only varchar (10)