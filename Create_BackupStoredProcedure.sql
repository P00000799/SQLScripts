USE [master]
GO

/****** Object:  StoredProcedure [dbo].[usp_BackupDatabases]    Script Date: 7/10/2017 6:54:49 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		jwhyde
-- Create date: 6/13/2017
-- Description:	Stored procedure for backing up db
-- =============================================
CREATE PROCEDURE [dbo].[usp_BackupDatabases] 

	@ExcludeDB nvarchar(max) = NULL,		--comma delimited string
	@BackupType nvarchar(10) = NULL,		--will default to 'FULL'
	@filepath nvarchar(max) = NULL,		--must be specified
	@fileExtension nvarchar(3) = NULL,	--will default to 'bak'
	@ErrorMsg nvarchar(500) = '' OUTPUT
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
--Variables
DECLARE @sqlstring nvarchar(MAX);
DECLARE @dbname nvarchar(128);
DECLARE @dbstate bit;
DECLARE @defExclude nvarchar(500) = '''model'''+','+'''tempdb''';
DECLARE @fullFileString nvarchar(max);
DECLARE @fileName nvarchar(max);
DECLARE @fileDate nvarchar(8);
DECLARE @fileTime nvarchar(8);
DECLARE @pos int = 0;
DECLARE @len int = 0;
DECLARE @start int = 1;


DECLARE @Name TABLE (
  name nvarchar(128),
  dbstate bit
)

--For testing
--SET @filepath = 'C:\SQL\Backups\'
--SET @ExcludeDB = 'master'

--Validations and parameter value checks
IF @fileExtension is NULL			--Set default file exension
	SET @fileExtension = 'bak'

IF @BackupType IS NULL OR @BackupType = ''				--Set default backup type
	SET @BackupType = 'FULL'

--Build the DB Exclusion list
IF @ExcludeDB IS NOT NULL OR
	LEN(LTRIM(@ExcludeDB)) > 0

BEGIN

	IF charindex(',',@ExcludeDB) = 0	--There's only a single value in the list
	 begin
		SET @defExclude +=  ',' + '''' + LTRIM(SUBSTRING(@ExcludeDB,1,LEN(@ExcludeDB))) + ''''
	 end
	ELSE
	  Begin
		SET @len = LEN(@excludeDB)

		WHILE CHARINDEX(',',@ExcludeDB,@start)<>0
			BEGIN
				SET @pos = CHARINDEX(',',@ExcludeDB,@start)
				IF @pos <> 0
				  begin
					SET @defExclude += ',' + '''' + LTRIM(SUBSTRING(@ExcludeDB,@start,@pos-@start)) + ''''
					SET @start =CHARINDEX(',',@ExcludeDB,@start) + 1
				  end
				ELSE
				  break
			END
	    SET @defExclude += ',' + '''' + LTRIM(SUBSTRING(@ExcludeDB,@start,@len-(@start-1))) + ''''
	  END
END

IF @BackupType NOT IN ( 'FULL', 'DIFF', 'LOG')		--Check for errors
	SET @ErrorMsg = 'Invalid backup type specified'+ CHAR(13) + CHAR(10)

IF @filePath is NULL
	SET @ErrorMsg += 'Backup Path must be specified' + CHAR(13) + CHAR(10)

IF @ErrorMsg <> ''
 BEGIN
	  PRINT @errorMsg
	  RETURN
 END
ELSE
	BEGIN

	IF @BackupType <> 'FULL'
		SET @fileDate = CONVERT(nvarchar(8),GetDate(),112)
	IF @BackupType = 'LOG'
		SET @fileTime = REPLACE(Convert(nvarchar(8),GetDate(),108),':','')

	--Set filename based on backup type
	IF @BackupType = 'FULL'
		SET @fileName = '_FULL.' + @fileExtension
	IF @BackupType = 'DIFF'
		SET @fileName = '_DIFF_' + @fileDate + '.' + @fileExtension
	IF @BackupType = 'LOG'
	   SET @fileName = '_LOG_' + @filedate + '_' + @filetime + '.' + @fileExtension

	--PRINT @filename
	--Select DB and execute backup

SET @sqlstring = 
'SELECT a.name, b.is_primary_replica as dbstate
   FROM sys.databases a
     left join sys.dm_hadr_database_replica_states b
	 ON a.database_id = b.database_id 
	 WHERE name NOT IN (' + @defExclude +')' +
	   'AND (b.Is_primary_replica  = 1 OR
	        b.Is_primary_replica IS NULL)'

	INSERT INTO @NAME
		EXEC sp_executesql @sqlstring


	DECLARE db_cursor CURSOR FOR  
		SELECT name, COALESCE(dbstate,1)
			FROM @Name
   
	OPEN db_cursor  
	FETCH NEXT FROM db_cursor INTO @dbname, @dbstate  

	WHILE @@FETCH_STATUS = 0  
	BEGIN  
		IF @dbstate = 1

		 BEGIN
		   SET @fullFileString = @filepath + @dbname + @filename 

			   IF (@BackupType ='FULL') 
			     Begin
    	   			BACKUP DATABASE @dbname TO DISK = @fullfilestring with Format, Compression
				 End
			   ELSE 
				BEGIN 
				  IF (@BackupType = 'LOG') 
					 BACKUP LOG @dbname TO DISK = @fullfilestring with Format, Compression
			     ELSE 
					 BACKUP database @dbname TO DISK = @fullfilestring with Differential, Compression, format
				 END
		END --DBSTATE check
		ELSE
			Begin
				SET @ErrorMsg += @dbname +  ' is a secondary DB and cannot be backed up' + CHAR(13) + CHAR(10)
			END  --ELSE

		   FETCH NEXT FROM db_cursor INTO @dbname, @dbstate
	END  --WHILE
  END	--No error messages
	IF @ErrorMsg <> ''
	  PRINT @errorMsg

END		--Procedure
GO

