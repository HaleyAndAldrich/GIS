use equis
go


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
set nocount on
go

alter procedure sp_hai_GIS_detects_wNDs (
	 @facility_id int =   2016423 
	,@loc_grp varchar (1000) = null
	,@task_code varchar (1000)
	,@mth_grp as varchar (200)
	,@param varchar (1000)
	,@coord_type varchar (50) 
	,@units varchar (20) 
	,@screening_level varchar (200) 
	,@show_unvalidated_yn varchar (10)
	,@show_detects_only varchar (10) 
	)
	as begin

	declare @start_time datetime
	declare @end_time datetime
	declare @str_len int
	declare @chem_count int
	declare @text_string varchar (255)

	--create list of task codes so they can be ordered with ID field
	declare @task table (task_code varchar (100), task_id int identity(1,1))
	insert into @task
	select distinct task_code from dt_sample s where task_code in(select cast(value as varchar (100)) from fn_split( @task_code)) order by task_code

	--determine whether all or just detects
	if @show_detects_only = 'n' 
		begin
			set @show_detects_only = 'y|n'
		end

	exec [rpt].[sp_HAI_GetParams] @facility_id,@mth_grp, @param --creates ##mthgrps

	/*get a list of detected locations*/
	declare @detect_locs table (sys_sample_code varchar (50),  sys_loc_code varchar (50), matrix_code varchar (10), sample_date datetime, start_depth varchar (10), x_coord float, y_coord float )
	insert into @detect_locs
	select distinct sys_sample_code, s.sys_loc_code, s.matrix_code, s.sample_date, s.start_depth, c.x_coord, c.y_coord
		from dt_sample s
		inner join dt_test t on s.facility_id = t.facility_id and s.sample_id = t.sample_id
		inner join dt_result r on t.facility_id = r.facility_id and t.test_id = r.test_id
		inner join rt_analyte ra on r.cas_rn  = ra.cas_rn
		inner join (select facility_id, sys_loc_code, x_coord, y_coord from dt_coordinate c where coord_type_code = @coord_Type)c on s.facility_id = c.facility_id and s.sys_loc_code = c.sys_loc_code
		inner join ##mthgrps mg on t.analytic_method = mg.analytic_method and replace(t.fraction, 'N','T') = mg.fraction and r.cas_rn = mg.cas_rn
		WHERE sample_type_code in ('n','fd')
				AND reportable_result like 'y%'
				AND result_type_code = 'trg' 
				and task_code in(select value from fn_split(@task_code))
				--and detect_flag in (select cast(value as varchar (10)) from fn_split(@show_detects_only ))
				--and validated_yn in (select case when @show_detects_only = 'y'
				and s.facility_id = @facility_id

select * from @detect_locs
		raiserror('Locs table selected...' ,0,1) with nowait	

		set @end_time = getdate() - @start_time
		set @text_string = convert(varchar,@end_time,114)
		raiserror(@text_string,0,1) with nowait	

	/* Get a temporary table for main data set ready*/
	if object_id('tempdb..#R1') is not null drop table #r1

		create table #r1 (facility_id int, mth_grp_name varchar(100), task_code varchar (30), sys_sample_code varchar (40), sys_loc_code varchar (20), sample_type_code varchar (10), start_Depth varchar(10), sample_date varchar (20), x_coord  float , y_coord  float
		,task_id int, row_id  int,fraction varchar (10),  chemical_name varchar (100), cas_rn varchar(20),result_label  varchar(30), result_value  float, detect_flag varchar(10), exceed_flag varchar(10), reporting_qualifier varchar(10)
		,interpreted_qualifiers varchar (10), converted_result_unit varchar (10), action_level varchar (20), action_level_unit varchar (10))
	
		set @start_Time = getdate()

		insert into #R1
			SELECT 
			r.facility_id
			,mg.grp_name
			,r.task_code
			,r.sys_sample_code
			,sys_loc_code
			,sample_type_code
			,start_depth
			,CONVERT(varchar, sample_date, 101) AS sample_date
			,CAST(x_coord AS float) AS x_coord
			,CAST(y_coord AS float) AS y_coord
			,task.task_ID
			,row_number() over(partition by sys_sample_code, sys_loc_code,sample_type_code, sample_Date, task_id, r.fraction order by sys_sample_code,sys_loc_Code,sample_type_code, sample_Date, chemical_name,r.fraction) as Row_ID
			,r.fraction
			,r.chemical_name
			,r.cas_rn
			,rpt.fn_hai_result_qualifier(rpt.fn_thousands_separator(converted_result) , case when r.detect_flag = 'N' then '<' else null end,replace(replace(reporting_qualifier,'+',''),'-',''),interpreted_qualifiers, '< # Q') AS Result_Label
			,converted_Result as Result_Value
			,case when r.detect_flag = 'Y' then '1' else '0' end as detect_flag
			,case when r.detect_flag = 'Y' and cast(converted_result as float) >= cast(action_level as float) then '1' else '0' end as exceed_flag
			,reporting_qualifier
			,interpreted_qualifiers
			,converted_result_unit
			,al.action_level
			,al.action_level_unit


			FROM     rpt.fn_HAI_EQuIS_Results(@facility_id, @units, NULL, @coord_Type) AS r
			inner join (select cast(value as varchar (10)) as detect_flag from fn_split(@show_detects_only ))detect on r.detect_flag = detect.detect_flag

			left join (select param_code as cas_Rn, equis.unit_conversion(action_level,unit,@units,default) as action_level, unit 
				as action_level_unit from  dt_action_level_parameter where action_level_code = @screening_level) al
			on r.cas_rn = al.cas_rn
			inner join @task task
				on r.task_code = task.task_code
			inner join ##mthgrps mg on r.analytic_method = mg.analytic_method and r.cas_rn = mg.cas_rn and replace(r.fraction, 'N','T') = mg.fraction
			WHERE sample_type_code in ('n','fd')
			AND reportable_result like 'y%'
			AND result_type_code = 'trg' 
			

		raiserror(' Results selected...' ,0,1) with nowait	

		insert into #R1
			SELECT 
			 r.facility_id
			,null --mg.grp_name
			,r.task_code
			,r.sys_sample_code
			,dl.sys_loc_code
			,sample_type_code
			,dl.start_depth
			,CONVERT(varchar, dl.sample_date, 101) AS sample_date
			,CAST(dl.x_coord AS float) AS x_coord
			,CAST(dl.y_coord AS float) AS y_coord
			,null --task.task_ID
			,99 --row_number() over(partition by sys_sample_code, sys_loc_code,sample_type_code, sample_Date, task_id, r.fraction order by sys_sample_code,sys_loc_Code,sample_type_code, sample_Date, chemical_name,r.fraction) as Row_ID
			,r.fraction
			,'no detections' --r.chemical_name
			,'no detections'-- r.cas_rn
			,'--' --rpt.fn_hai_result_qualifier(rpt.fn_thousands_separator(converted_result) , case when r.detect_flag = 'N' then '<' else null end,replace(replace(reporting_qualifier,'+',''),'-',''),interpreted_qualifiers, '< # Q') AS Result_Label
			,null --converted_Result as Result_Value
			,null --case when r.detect_flag = 'Y' then '1' else '0' end as detect_flag
			,'0' --case when r.detect_flag = 'Y' and cast(converted_result as float) >= cast(action_level as float) then '1' else '0' end as exceed_flag
			,null --reporting_qualifier
			,null --interpreted_qualifiers
			,null --converted_result_unit
			,null --al.action_level
			,null --al.action_level_unit

		from  @detect_locs dl 

		left join #r1 r on  dl.sys_sample_code = r.sys_sample_code 
		where r.chemical_name is null

		select * from #r1

		set @end_time = getdate() - @start_time
		set @text_string = convert(varchar,@end_time,114)
		raiserror(@text_string,0,1) with nowait	

	/*Figure out the number of chemicals for loop in dynamic query*/
	set @chem_count = 
		(select max(chem_count) from (
		select sys_sample_code, sys_loc_code, sample_type_code, count(chemical_name) chem_count,fraction
		from #r1 
		group by sys_sample_code,sys_loc_code,  sample_type_code, fraction)z)

	--*********************************************************************
	/*Pad all chemical_names with spaces. Adds 3 spaces to longest chemical_name for each location. Adds additional
	spaces to each shorter name so the total number spaces creates a string the same length as the longest.*/
		update #r1
		set chemical_name = [rpt].[GIS_Pad_Columns] (chemical_name, max_length, 3)  -- last value is the number of spaces added to longest chemical name
		from #r1 r1
		inner join
		(select sys_loc_code,max(len(chemical_name)) as max_length from #r1  --'max_length' is the longest chemical_name string length for each location
		group by sys_loc_code)m on r1.sys_loc_Code = m.sys_loc_code


	/*Pad all result_labels with spaces. Adds 3 spaces to longest result_label for each location. Adds additional
	spaces to each shorter name so the total number spaces creates a string the same length as the longest.*/
		update #r1
		set result_label = [rpt].[GIS_Pad_Columns] (result_label, max_length, 0)  -- last value is the number of spaces added to longest result_label
		from #r1 r1
		inner join
		(select sys_loc_code,max(len(result_label)) as max_length from #r1  --'max_length' is the longest result_label string length for each location
		group by sys_loc_code)m on r1.sys_loc_Code = m.sys_loc_code
	--***************************************************************************

		raiserror('Begin Dynamic Query...' ,0,1) with nowait	

		set @end_time = getdate() - @start_time
		set @text_string = convert(varchar,@end_time,114)
		raiserror(@text_string,0,1) with nowait	


		print  'chem count ' + cast( @chem_count as varchar)

		declare @sql1 varchar (max)
		declare @sql2 varchar (max) = ''
		declare @sql3 varchar (max)
		declare @count int =1

		set @sql1 = 


		 'select ' + char(10) +
		'task_code,' + char(10) +
		'mth_grp_name,' + char(10) + 
		'cast(sys_sample_code as varchar) as sys_sample_code, ' + char(10) +
		'cast(sys_loc_code as varchar) as sys_loc_code, ' + char(10) +
		'case when sample_type_code = ' + '''' + 'fd' +'''' + ' then ' + '''' + '(dup)' + '''' + ' else ' + '''' + 'primary' + '''' + ' end  as sample_type_code ,' + char(10) +
		'sample_date,' + char(10) +
		'fraction as Total_or_Dissolved,' + char(10) +
		'cast(x_coord as float) as x_coord,' + char(10) +
		'cast(y_coord as float) as y_coord,' + char(10) +
		'max(cast(converted_result_unit as varchar)) as Units,' + char(10) 

		While @count < @chem_count + 1
		begin

			set @sql2 = @sql2 + 
			'max(case when row_ID = ' + cast(@count as varchar) + ' then  chemical_name   when row_id in(98,99) then ' + '''' + 'No Detections' + '''' + ' else ' + '''' + '' + '''' + '   end )as [Chem_' + cast(@count as varchar)  +'] ,' + char(10) +
			'max(case ' + char(10) +
					'when row_ID = ' + cast(@count as varchar) + ' and exceed_flag = 1 then ' + '''' + '<bol>' + '''' +  ' + cast(result_label  as varchar) + ' + '''' + '</bol>' + '''' + char(10) +
					'when row_ID = ' + cast(@count as varchar) + '   and exceed_flag = 0 then    cast(result_label  as varchar) ' + char(10) +
					'when row_id in(98,99) then ' + '''' + '--'  + '''' + char(10) +
					' else ' + '''' + '' + ''''  + char(10) +
				 'end )as [Result_Label_' + cast(@count as varchar) +'] ,' + char(10) +
			'max(case when row_ID = ' + cast(@count as varchar) + '  then  cast(result_value as varchar)  else ' + '''' + '' + '''' + '  end )as [Result_Value_ '+ cast(@count as varchar) +'] ,' + char(10) +	
			'max(case when row_ID = ' + cast(@count as varchar) + ' then  coalesce(cast(action_level as varchar),' + '''' + 'NL' + '''' + ') else  ' + '''' + '' + '''' + ' end )as [Screening_Level_ '+ cast(@count as varchar) +'] ,' + char(10) +
			'max(case when row_ID = ' + cast(@count as varchar) + ' then  Detect_Flag else ' + '''' + '' + '''' + ' end )as ' +   '[Detect_Flag_' + cast(@count as varchar) +'] ,' + char(10) +
			'max(case when row_ID = ' + cast(@count as varchar) + ' then  exceed_Flag else ' + '''' + '' + '''' + ' end )as ' +  '[Exceed_Flag_' + cast(@count as varchar) +'] ,' + char(10) +
			char(10)+
			char(10)

			set @count = @count + 1

			

			set @end_time = getdate() - @start_time
			set @text_string = convert(varchar,@count) + ' ' + convert(varchar,@end_time,114)
			raiserror(@text_string,0,1) with nowait	


		end

		if len(@sql2) > 5
		begin
			set @sql2 = left(@sql2,len(@sql2) -4) + char(10) + char(10)
		end

		set @sql3 =
		'from #r1' + char(10) +
		
	
			'group by ' + char(10) +
				'task_code,' + char(10) +
				'sample_date,' + char(10) +
				'mth_grp_name,' + char(10) +
				'sys_loc_code,' + char(10) +
				'sys_sample_code,' + char(10) +
				'sample_type_code,' + char(10) +
				'fraction,' + char(10) +
				'x_coord,' + char(10) +
				'y_coord,' + char(10) +
				'converted_result_unit' + char(10) +
			'order by #r1.sys_loc_code, sample_date, total_or_dissolved, sample_type_code' + char(10)
		  
		print 'End dynamic query...'
		set @end_time = getdate() - @start_time
		print  convert(varchar,@end_time,114)

		--if len(@sql2) > 0 
		--begin
			exec( @sql1 + @sql2 + @sql3)
		--end
		----select * from ##gis_table

		print @sql1 + @sql2 + @sql3
		print 'Done..'
		set @end_time = getdate() - @start_time
		print  convert(varchar,@end_time,114)

End