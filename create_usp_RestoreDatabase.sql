USE [master]
GO

/****** Object:  StoredProcedure [dbo].[usp_RestoreDatabase]    Script Date: 7/10/2017 6:55:14 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[usp_RestoreDatabase] 
--Parameters
	@FromDate nvarchar(10) = '',
	@FromTime nvarchar(15) = '',
	@DBinput nvarchar(max) = '',
	@testFlag nvarchar(1) = 'Y',
	@StopAtDate nvarchar(10) = '',
	@StopAtTime nvarchar(15) = '',
	--@Primary bit = 1;
	--@RestoreType bit = 0;  --0=all/1=Full Only/2=Full + Diff/3=Diff Only/4=Log Only
	@msg nvarchar(max) = '' OUTPUT
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
--------------------------------------------------------------------------------------------------
--Local Variables--
--------------------------------------------------------------------------------------------------
DECLARE @Restore TABLE(
	dbname nvarchar(128),
	server_name nvarchar(128),
	butype nvarchar(1),
	damaged bit,
	last_date datetime,
	database_id int,
	backup_path nvarchar(260),
	group_name nvarchar(128),
	replica_type bit,
	sqlcmd nvarchar(max)
)

DECLARE @DiffRestore TABLE(
	dbname nvarchar(128),
	server_name nvarchar(128),
	butype nvarchar(1),
	damaged bit,
	diff_chkpt numeric(25,0),
	last_date datetime,
	database_id int,
	backup_path nvarchar(260),
	group_name nvarchar(128),
	replica_type bit,
	sqlcmd nvarchar(max)
)

DECLARE @LogRestore TABLE(
	dbname nvarchar(128),
	server_name nvarchar(128),
	position int,
	butype nvarchar(1),
	damaged bit,
	first_lsn numeric(25,0),
	last_lsn numeric(25,0),
	checkpoint_lsn numeric(25,0),
	last_date datetime,
	database_id int,
	backup_path nvarchar(260),
	group_name nvarchar(128),
	replica_type bit,
	last_diff datetime,
	diff_chkpt numeric(25,0),
	sqlcmd nvarchar (max)
)

DECLARE @AlterCmds TABLE(

	sqlcmd nvarchar (max)
)

DECLARE @DBS TABLE(
  database_name nvarchar(128)
)

DECLARE @strDate nvarchar(25);
DECLARE @dbname nvarchar(128);
DECLARE @backup_path nvarchar(260);
DECLARE @server_name nvarchar(128);
DECLARE @group_name nvarchar(128);
DECLARE @replica_type bit;
DECLARE @RestFromDate datetime;
DECLARE @RestFromTime datetime;
DECLARE @sqlcmd nvarchar(MAX);
DECLARE @error int;
DECLARE @DBList nvarchar(max) = '';
DECLARE @pos int = 0;
DECLARE @len int = 0;
DECLARE @start int = 1;
DECLARE @filetype nvarchar(3);
DECLARE @lstDiff datetime;
DECLARE @check_pt numeric(25,0);
DECLARE @stopatstr nvarchar(25);
DECLARE @stopatflg bit;

/*---------------------------------------------------------------------------------------------------
--Code
----------------------------------------------------------------------------------------------------*/
IF @FromDate = ''
  SET @FromDate = CONVERT(varchar(10), GETDATE(),101)
IF @FromTime =''
  SET @FromTime = CONVERT(varchar(8),GETDATE(),108)

SET @strDate = @FromDate + ' ' + @FromTime
IF (ISDATE(@strDate) = 0)
	Begin
		SET @msg = @msg + 'Invalid date/time for file selection Option.'
		GOTO Error
	End


IF (LEN(@StopAtDate) > 0 AND LEN(@stopAtTime) > 0)
  Begin
	SET @StopAtDate = CONVERT(varchar(10), @StopAtDate,101)
	SET @StopAtTime = CONVERT(varchar(10), @StopAtTime,108)
    SET @stopatstr = @StopAtDate + ' ' + @stopatTime
	--Print @stopatstr
	IF (ISDATE(@stopatstr)) = 1
		SET @stopatflg = 1					--Valid date/time provided for stop at option
	ELSE
	   Begin
		SET @stopatflg = 0
		SET @msg = @msg + 'Invalid date/time for StopAt Option 1.'
		GOTO Error
	   End
   End
ELSE 
  Begin
	IF ((LEN(@StopAtDate) > 0 AND LEN(@stopAtTime) < 0) OR
	   (LEN(@StopAtDate) < 0 AND LEN(@stopAtTime) > 0))
	   --PRINT LEN(@StopATDate)
	   Begin
		SET @stopatflg = 0
		SET @msg = @msg + 'Invalid date/time for StopAt Option 2.'
		GOTO Error
	   End
   End
--Print @strDate

--Build the DB Selection List 

IF @DBInput IS NOT NULL OR
	LEN(LTRIM(@DBInput)) > 0

BEGIN

	IF charindex(',',@DBInput) = 0	--There's only a single value in the list
	 begin
		INSERT INTO @DBS
			VALUES  ( LTRIM(SUBSTRING(@DBInput,1,LEN(@DBInput))) )
	 end
	ELSE
	  Begin

		SET @len = LEN(@DBInput)

		WHILE CHARINDEX(',',@DBInput,@start)<>0
			BEGIN
				SET @pos = CHARINDEX(',',@DBInput,@start)
				IF @pos <> 0
				  begin
					INSERT INTO @DBS
						VALUES  ( LTRIM(SUBSTRING(@DBInput,@start,@pos-@start)) )					
					SET @start =CHARINDEX(',',@DBInput,@start) + 1
				  end
				ELSE
				  break
			END
		INSERT INTO @DBS
			VALUES  ( LTRIM(SUBSTRING(@DBInput,@start,@len-(@start-1))) )

	  END
END

/*---------------------------------------------------------------------------------------------------
/*Retrieve the most current FULL back up information for the databases
on the server  */
----------------------------------------------------------------------------------------------------*/
 INSERT INTO @Restore
 select a.database_name, a.server_name,  a.type, a.is_damaged, a.backup_finish_date
	, a2.database_id
	, b.physical_device_name
	, c.name as group_name
	, d.is_primary_replica as replica_type
	,''
	
	from msdb..backupset a
		inner join msdb..backupmediafamily b
		on a.media_set_id = b.media_set_id
		inner join sys.databases a2
		on a.database_name = a2.name

		LEFT OUTER JOIN
		(SELECT x.database_name, y.name
			from sys.Availability_databases_cluster x
			  inner join sys.availability_groups y
			  ON x.group_id = y.group_id) as c
			  ON a.database_name = c.database_name
		LEFT OUTER JOIN
		(SELECT database_id, is_primary_replica
			FROM sys.dm_hadr_database_replica_states
			where is_local = 1) as d
			ON a2.database_id = d.database_id

			
	WHERE EXISTS (SELECT database_name
					FROM @DBS as e
					WHERE a.database_name = e.database_name)
	  and a.type = 'D'
	  and a.is_damaged = 0
	  and a.backup_finish_date = (
		  SELECT MAX(backup_finish_date)
					from msdb..backupset a2
					where type = 'D'
					  and backup_finish_date <= @FromDate
					  and a2.database_name = a.database_name)

/*--------------------------------------------------------------------------------------------------------------------
---------------------	FULL RESTORE
*/---------------------------------------------------------------------------------------------------------------------
IF @testFlag = 'Y'
   SELECT * FROM @Restore
   DECLARE @AlterOne nvarchar(50) = 'ALTER AVAILABILITY GROUP [';
   DECLARE @AlterTwo nvarchar(50) = '] REMOVE DATABASE  [';
   DECLARE @command nvarchar(max);
   DECLARE @Rest1 nvarchar(50) = 'RESTORE DATABASE ';
   DECLARE @REST2 nvarchar(50) = ' FROM DISK = ';
   DECLARE @REST3 NVARCHAR(50) = ' WITH REPLACE, NORECOVER, STATS=5'

   DECLARE full_cursor CURSOR FOR  
   SELECT dbname, server_name, backup_path, group_name, replica_type
			FROM @Restore
   
	OPEN full_cursor  
	FETCH NEXT FROM full_cursor INTO @dbname, @server_name, @backup_path, @group_name, @replica_type 

	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF (@group_name IS NOT NULL
		    AND @replica_type = 1)

			BEGIN
				SET @sqlcmd = 'ALTER AVAILABILITY GROUP ['+ @group_name + '] REMOVE DATABASE  [' + @dbname + ']'
				INSERT INTO @AlterCmds
				  SELECT @AlterOne + @group_name + @AlterTwo + @dbname + ']'
				IF @testFlag = 'N'
				  Begin
					EXEC (@sqlcmd);
					SELECT @Error = @@Error
					if @error <> 0
					  Begin
						SET @msg = 'Error ' +  RTRIM(CAST(@error as nvarchar(10))) + ' during full backup.'
						GOTO Error
					  End
					Else
						set @msg += 'Removed DB ' + @group_name + ' ' + @dbname + CHAR(13) + CHAR(10)
						SELECT * from @AlterCmds
				  End
				ELSE
					set @msg += 'Test Remove DB ' + @group_name + ' ' + @dbname + CHAR(13) + CHAR(10)

			END
/*  After removing db from availability group if necessary, restore file */

			   SET @sqlcmd = 'RESTORE database ' + @dbname + ' from disk =  '''+ @backup_path + ''' WITH REPLACE, NORECOVERY, NOUNLOAD, STATS=5'
			   INSERT INTO @AlterCmds
			     SELECT @REST1 + @DBNAME + @REST2 + '''' + @BACKUP_PATH+ '''' + @REST3

			   IF @testFlag = 'N'
			     Begin
					EXEC (@sqlcmd);
					SELECT @error = @@ERROR
					IF @error <> 0
					  Begin
						SET @msg = 'Error ' +  RTRIM(CAST(@error as nvarchar(10))) + ' during full backup.'
						GOTO Error
					  End
				 End
				ELSE
					set @msg += 'Test Full Restore on File '  + @dbname + ' from ' + @backup_path +  CHAR(13) + CHAR(10)
			 --END
			 FETCH NEXT FROM full_cursor INTO @dbname, @server_name, @backup_path, @group_name, @replica_type 

	END
	CLOSE full_cursor
	DEALLOCATE full_cursor

	SET @backup_path = '';
	SET @dbname = '';
	SET @group_name = '';
	SET @replica_type = '';


---------------------------------------------------------------------------------------------------------------------------
/*Retrieve the most current Differential back up information for the databases
on the server  */
---------------------------------------------------------------------------------------------------------------------------
 
 --Select list of differential backups 
 INSERT INTO @Diffrestore
 select a.database_name, a.server_name,  a.type, a.is_damaged, a.checkpoint_lsn, a.backup_finish_date
	, a2.database_id
	, b.physical_device_name
	, c.name as group_name
	, d.is_primary_replica as replica_type
	, ''
	
	from msdb..backupset a
		inner join msdb..backupmediafamily b
		on a.media_set_id = b.media_set_id
		inner join sys.databases a2
		on a.database_name = a2.name

		LEFT OUTER JOIN
		(SELECT x.database_name, y.name
			from sys.Availability_databases_cluster x
			  inner join sys.availability_groups y
			  ON x.group_id = y.group_id) as c
			  ON a.database_name = c.database_name
		LEFT OUTER JOIN
		(SELECT database_id, is_primary_replica
			FROM sys.dm_hadr_database_replica_states
			where is_local = 1) as d
			ON a2.database_id = d.database_id
			
	WHERE EXISTS (SELECT database_name
					FROM @DBS as e
					WHERE a.database_name = e.database_name)
	  and a.type = 'I'
	  and a.is_damaged = 0
	  and a.backup_finish_date = (
		  SELECT MAX(backup_finish_date)
					from msdb..backupset a2
					where type = 'I'
					  and backup_finish_date <= @FromDate
					  and a2.database_name = a.database_name)

 UPDATE @Diffrestore
   SET group_name = b.group_name,
       replica_type = b.replica_type
	   FROM @DiffRestore a
	   INNER JOIN @Restore b
	   on a.dbname = b.dbname

 UPDATE @DiffRestore
 SET replica_type = 0
   WHERE group_name is NOT null
     AND replica_type is null

/*--------------------------------------------------------------------------------------------------------------------
---------------------	DIFFERENTIAL RESTORE
*/---------------------------------------------------------------------------------------------------------------------

IF @testFlag = 'Y'
  SELECT * from @DiffRestore

  	DECLARE diff_cursor CURSOR FOR  
		SELECT dbname, server_name, backup_path, group_name, replica_type
			FROM @DiffRestore
   
	OPEN diff_cursor  
	FETCH NEXT FROM diff_cursor INTO @dbname, @server_name, @backup_path, @group_name, @replica_type 

	WHILE @@FETCH_STATUS = 0
	BEGIN
		     BEGIN
				SET @sqlcmd = 
				'RESTORE database ' + @dbname + ' from disk =  '''+ @backup_path + ''' WITH NORECOVERY, NOUNLOAD, STATS=5'
			    INSERT INTO @AlterCmds
			     SELECT @REST1 + @DBNAME + @REST2 + '''' + @BACKUP_PATH+ '''' + @REST3
				IF @testFlag = 'N'
				  Begin
					EXEC (@sqlcmd);
					SET @error = @@ERROR
					IF @error <> 0
					  Begin
						SET @msg = 'Error during differential backup. Error: ' + RTRIM(CAST(@error as nvarchar(10)))
						GOTO Error
					  End
				  End
				ELSE
				  set @msg += 'Test Differential Restore on File '+ @dbname + ' from ' + @backup_path  +  CHAR(13) + CHAR(10)
			 END
			 FETCH NEXT FROM diff_cursor INTO @dbname, @server_name, @backup_path, @group_name, @replica_type 

	END

	CLOSE diff_cursor
	DEALLOCATE diff_cursor

	SET @backup_path = '';
	SET @dbname = '';
	SET @group_name = '';
	SET @replica_type = '';
--------------------------------------------------------------------------------------------------------------------
/*Retrieve the list of log backups information for the databases
on the server  */
---------------------------------------------------------------------------------------------------------------------


--Select list of log files created between the time of the last differential and some date
  INSERT INTO @LogRestore
 select a.database_name, a.server_name, a.position, a.type, a.is_damaged, a.first_lsn, a.last_lsn
	, a.checkpoint_lsn, a.backup_finish_date
	, a2.database_id
	, b.physical_device_name
	, c.name as group_name
	, d.is_primary_replica as replica_type
	, f.last_date
	, f.diff_chkpt
	, ''
	from msdb..backupset a
		inner join msdb..backupmediafamily b
		on a.media_set_id = b.media_set_id
		inner join sys.databases a2
		on a.database_name = a2.name

		LEFT OUTER JOIN
		(SELECT x.database_name, y.name
			from sys.Availability_databases_cluster x
			  inner join sys.availability_groups y
			  ON x.group_id = y.group_id) as c
			  ON a.database_name = c.database_name
		LEFT OUTER JOIN
		(SELECT database_id, is_primary_replica
			FROM sys.dm_hadr_database_replica_states
			where is_local = 1) as d
			ON a2.database_id = d.database_id
		LEFT OUTER JOIN
		(SELECT dbname, last_date, diff_chkpt
		   from @DiffRestore) as f
		   on a.database_name = f.dbname
			
	WHERE EXISTS (SELECT database_name
					FROM @DBS as e
					WHERE a.database_name = e.database_name)
	  and a.type = 'L'
	  and a.is_damaged = 0
	  and a.backup_finish_date BETWEEN 
		(SELECT last_date 
			FROM @DiffRestore
			WHERE dbname = a.database_name
			  and diff_chkpt = a.checkpoint_lsn)
			AND
		(SELECT @strDate)


 UPDATE @Logrestore
   SET group_name = b.group_name,
       replica_type = b.replica_type
	   FROM @DiffRestore a
	   INNER JOIN @Restore b
	   on a.dbname = b.dbname

 UPDATE @LogRestore
 SET replica_type = 0
   WHERE group_name is NOT null
     AND replica_type is null

IF @stopatflg = 1
  DELETE @LogRestore
    WHERE last_date > @strDate

/*---------------------------------------------------------------------------------------------------
--											LOG RESTORE			
*/--------------------------------------------------------------------------------------------------
IF @testFlag = 'Y'
  SELECT * from @LogRestore

  DECLARE @last_date datetime;
  DECLARE @cntldbname nvarchar(128);
  DECLARE @cntlreptype bit;
  DECLARE @cntlpath nvarchar(260);
  DECLARE @reccnt int;
  DECLARE @REST4 nvarchar(50) = 'RESTORE LOG ';
  DECLARE @STOP nvarchar(25) = ', STOPAT = ';
  DECLARE @cnt int = 1;
  
    SET @reccnt = ( SELECT Count(*)
	   FROM @LogRestore )

  	DECLARE log_cursor CURSOR FOR  
		SELECT dbname, server_name, backup_path, group_name, replica_type, last_date
			FROM @LogRestore
			ORDER by database_id, last_date asc
   
	OPEN log_cursor  
	FETCH NEXT FROM log_cursor INTO @dbname, @server_name, @backup_path, @group_name, @replica_type, @last_date

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @sqlcmd = 'RESTORE LOG ' + @dbname + ' from disk =  '''+ @backup_path + ''' WITH NORECOVERY, NOUNLOAD,STATS=5'
		INSERT INTO @AlterCmds
			     SELECT @REST4 + @DBNAME + @REST2 + '''' + @BACKUP_PATH+ '''' + @REST3
		--SET @msg += 'Count = ' + CAST(@Cnt as nvarchar(5)) + ' and Reccount = ' + Cast(@Reccnt as nvarchar(5)) + CHAR(13) + CHAR(10)
		IF (@cnt > @reccnt)
			BREAK

		IF (@cnt < @reccnt)
			BEGIN
				IF @testFlag = 'N'
					Begin
						EXEC (@sqlcmd);
						SET @error = @@ERROR
						IF @error <> 0
							Begin
			   					SET @msg = 'Error during log backup. Error: ' + RTRIM(CAST(@error as nvarchar(10)))
								GOTO Error
							End   -- Error check
					End   
				 ELSE	-- Test Mode
					set @msg += 'Test Log Restore on File '  + @dbname+ ' from ' + @backup_path  +  CHAR(13) + CHAR(10)	
			END

			ELSE	-- Last Record
			BEGIN
				--SET @msg += 'Count last loop = ' + Cast(@Cnt as nvarchar(5)) + ' and Reccount = ' + Cast(@Reccnt as nvarchar(5)) + CHAR(13) + CHAR(10)
				IF (@stopatflg = 1)
					SET @sqlcmd += ', STOPAT= '''+@stopatstr+''''
					INSERT INTO @AlterCmds
						SELECT @REST4 + @DBNAME + @REST2 + '''' + @BACKUP_PATH+ '''' + @REST3 + @Stop + '''' + @stopatstr + ''''
						IF @testFlag = 'N'
							Begin
							   EXEC (@sqlcmd);
							   SET @error = @@ERROR
								IF @error <> 0
									  Begin
										SET @msg += 'Error during final log restore. Error: ' + RTRIM(CAST(@error as nvarchar(10)))
										GOTO Error
									  End  -- Error Check
							  End	-- Test flag
						ELSE
						  set @msg += 'Test Final Log Restore on File ' + @dbname + ' from ' + @backup_path +  CHAR(13) + CHAR(10)
				--End

				IF (( @group_name IS NULL ) OR (@replica_type = 1)) --Primary or standalone, set db out of recovery
					BEGIN
						SET @sqlcmd = 'RESTORE DATABASE ' + @dbname + ' WITH RECOVERY'
						INSERT INTO @AlterCmds
							SELECT @REST1 + @DBNAME + ' WITH RECOVERY'
							IF @testFlag = 'N'
								Begin
									EXEC (@sqlcmd);
									SET @error = @@ERROR
									IF @error <> 0
										Begin
											SET @msg += 'Error during final database restore. Error: ' + RTRIM(CAST(@error as nvarchar(10)))
											GOTO Error
										End  -- Error Check
									End	-- Test flag
								ELSE
									set @msg += 'Test Recovery on ' + @dbname +  CHAR(13) + CHAR(10) 

					END
				IF (@replica_type = 1)
		
					BEGIN
													
						SET @sqlcmd = 'ALTER AVAILABILITY GROUP ['+ @group_name + '] ADD DATABASE  [' + @dbname + ']'
						INSERT INTO @AlterCmds
							SELECT @AlterOne + @group_name + '] ADD DATABASE [' + @dbname + ']'
						IF @testFlag = 'N'
							Begin
								EXEC (@sqlcmd);
								SET @error = @@ERROR
								IF @error <> 0
									Begin
										SET @msg += 'Error during AG database ADD. Error: ' + RTRIM(CAST(@error as nvarchar(10)))
										GOTO Error
									End
									  End
								ELSE	--  Test Mode
									set @msg += 'Test Database  ' + @dbname + ' added to AG Group ' + @group_name + CHAR(13) + CHAR(10)
						END   --Replica Type = 1	

				IF (@replica_type = 0)

					BEGIN
						SET @sqlcmd = 'ALTER DATABASE ['+ @dbname + '] SET HADR AVAILABILITY GROUP = ['+ @group_name + ']'

						IF @testFlag = 'N'
							Begin
								EXEC (@sqlcmd);
								SET @error = @@ERROR
								IF @error <> 0
									Begin
										SET @msg += 'Error adding database to HADR group ' + @group_name
										GOTO Error
									End
								End
						ELSE	--  Test Mode
							set @msg += 'Test Database  ' + @dbname + ' added to AG Group ' + @group_name + CHAR(13) + CHAR(10)
					END   --Replica Type = 1
			END -- Last Record

		FETCH NEXT FROM log_cursor INTO @dbname, @server_name, @backup_path, @group_name, @replica_type, @last_date
		SET @cnt +=  1 
	END	--ENDWHILE	

			
	CLOSE log_cursor
	DEALLOCATE log_cursor



	IF @testFlag = 'Y'
		GOTO MessageSec


	ERROR:
	   BEGIN
		Print @msg
		RETURN 8
	   END

	MESSAGESEC:
		Print @msg
		select * from @AlterCmds
END
GO

