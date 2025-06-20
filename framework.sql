
SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE dbo.usp_MeasureOutcomesLoader
	(
		@ProcessDate INT
	   ,@measureIDToLoad INT
	)
AS
	BEGIN
		/***************************************************************************************************
		Procedure:          dbo.usp_MeasureOutcomesLoader
		Created Date:       
		Author:             
		Description:        Loads a single Measure and all of its' rules into dbo.MemberOutcomes. Log of outputs go into Sigma.dbo.MeasureOutcomesControlLog
		Called by:            
		Affected table(s):  
		Used By:            Data Warehouse (ECUDW), Model/Cube (TBD), PBI Service (TBD), Automation (TBD)  
		Parameter(s):		@Processdate = Int Date to apply measures to
							@measureIDToLoad =  MeasureID to apply
		Usage:              EXEC 
		****************************************************************************************************
		SUMMARY OF CHANGES
		Date(yyyy-mm-dd)    Author              Comments
		------------------- ------------------- ------------------------------------------------------------

		***************************************************************************************************/

		SET NOCOUNT ON;
		SET TRAN ISOLATION LEVEL READ UNCOMMITTED;

		DECLARE @insertCount INT = 0
			   ,@updateCount INT = 0
			   ,@deleteCount INT = 0;
		DECLARE @mergeResultsTable TABLE
			(
				MergeAction VARCHAR(20) NULL
			);

		/*################################
			Create table if it does not exist.
		################################*/
		IF OBJECT_ID('dbo.MeasureOutcomes') IS NULL
			BEGIN
				RAISERROR('Create Table: dbo.MeasureOutcomes', 0, 1) WITH NOWAIT;
				/*TRUNCATE TABLE dbo.MeasureOutcomes*/
				/*DROP TABLE dbo.MeasureOutcomes*/
				CREATE TABLE dbo.MeasureOutcomes -- 375963
					(
						MeasureOutcomeSK VARCHAR(64) NOT NULL PRIMARY KEY
					   ,UniqueID CHAR(17) NOT NULL
					   ,MeasureID INT NOT NULL
					   ,ProcessDate INT NOT NULL
					   ,OutcomeValue FLOAT NOT NULL
					   ,RunDate DATETIME NOT NULL
					   ,MeasureApplys BIT NOT NULL
					   --   ,IsActive BIT NOT NULL DEFAULT 1
					   ,audInsertDateTime DATETIME NOT NULL DEFAULT GETDATE()
					   ,audUpdateDateTime DATETIME NOT NULL DEFAULT GETDATE()
					--,CONSTRAINT PK_MemberAttributeValues PRIMARY KEY (UniqueID, AttributeName, ProcessDate)
					);
			END;

		/*################################
			Main Script
		################################*/
		BEGIN TRY


			/*=============================================
				Member Population
			=============================================*/
			DECLARE @totalLoadStartTime VARCHAR(50)
				   ,@totalLoadEndTime VARCHAR(50)
				   ,@totalLoadTime VARCHAR(50);
			SET @totalLoadStartTime = GETDATE();
			RAISERROR('Starting MeasureOutcomes', 0, 1) WITH NOWAIT;


			--DECLARE @ProcessDate INT = 20250430
			--DECLARE @measureIDToLoad INT = 2

			/* SQL VARIABLES */
			DECLARE @DynamicSQLRules NVARCHAR(MAX) = N'';
			DECLARE @DynamicSQLBase NVARCHAR(MAX) = N'';
			DECLARE @ComparisonInsertSQL NVARCHAR(1024);
			DECLARE @SourceConversionSQL NVARCHAR(MAX);
			DECLARE @TargetConversionSQL NVARCHAR(MAX);

			/* AUDIT VARIABLES*/
			DECLARE @errorMessage NVARCHAR(MAX) = N'';
			DECLARE @runDate DATETIME = GETUTCDATE();

			/* RULE CURSOR VARIABLES */
			DECLARE @MeasureID INT;
			DECLARE @MeasureWeight FLOAT;
			DECLARE @MeasureResult FLOAT;

			DECLARE @rulegroupid INT
				   ,@ruleID INT
				   ,@RuleWeight FLOAT
				   ,@RuleResult FLOAT
				   ,@RuleVersion FLOAT;

			DECLARE @SourceTableDatabase VARCHAR(255)
				   ,@SourceTableSchema VARCHAR(255)
				   ,@SourceTableObject VARCHAR(255)
				   ,@SourceTableType VARCHAR(255)
				   ,@SourceAttributeName VARCHAR(255)
				   ,@SourceEAVColumnAttributeName VARCHAR(255)
				   ,@SourceEAVColumnValueName VARCHAR(255)
				   ,@SourceTableUniqueKeyColumnName VARCHAR(255)
				   ,@SourceTableSnapshotDateColumnName VARCHAR(255)
				   ,@SourceTableReference VARCHAR(255);

			DECLARE @TargetTableDatabase VARCHAR(255)
				   ,@TargetTableSchema VARCHAR(255)
				   ,@TargetTableObject VARCHAR(255)
				   ,@TargetTableType VARCHAR(255)
				   ,@TargetAttributeName VARCHAR(MAX)
				   ,@TargetEAVColumnAttributeName VARCHAR(255)
				   ,@TargetEAVColumnValueName VARCHAR(255)
				   ,@TargetTableUniqueKeyColumnName VARCHAR(255)
				   ,@TargetTableSnapshotDateColumnName VARCHAR(255)
				   ,@TargetTableReference VARCHAR(255);


			DECLARE @BaseTableReference VARCHAR(255)
				   ,@BaseUniqueKeyColumnName VARCHAR(255)
				   ,@BaseTableSnapshotDateColumnName VARCHAR(255);

			DECLARE @Operator VARCHAR(10);
			DECLARE @StaticValueIfNullTarget VARCHAR(255);
			DECLARE @ComparisonOrCalculation VARCHAR(255);


			IF OBJECT_ID('tempdb.dbo.#groupdata') IS NOT NULL
				DROP TABLE #groupdata;
			IF OBJECT_ID('tempdb.dbo.#base') IS NOT NULL
				DROP TABLE #base;
			IF OBJECT_ID('tempdb.dbo.#tempMeasureOutcomes') IS NOT NULL
				DROP TABLE #tempMeasureOutcomes;
			IF OBJECT_ID('tempdb.dbo.#tempRuleResults') IS NOT NULL
				DROP TABLE #tempRuleResults;
			IF OBJECT_ID('tempdb.dbo.#tempRuleGroup') IS NOT NULL
				DROP TABLE #tempRuleGroup;
			IF OBJECT_ID('tempdb.dbo.#tempRuleResultsOutput') IS NOT NULL
				DROP TABLE #tempRuleResultsOutput;




			INSERT INTO dbo.MeasureOutcomesControlLog
			(
				MeasureControlID
			   ,StartDateTime
			   ,Status
			   ,Message
			)
			VALUES
			(@measureIDToLoad, @runDate, 'In Progress', 'Starting processing');


			CREATE TABLE #base
				(
					MeasureOutcomeSK VARCHAR(64) NOT NULL
				   ,UniqueID CHAR(17) NOT NULL
				   ,MeasureID INT NOT NULL
				   ,ProcessDate INT NOT NULL
				   ,OutcomeValue FLOAT NOT NULL
				   ,RunDate DATETIME NOT NULL
				   ,MeasureApplys BIT NOT NULL
				);

			CREATE TABLE #tempRuleResults
				(
					UniqueID CHAR(17) NOT NULL
				   ,MeasureID INT NOT NULL
				   ,RuleID INT NOT NULL
				   ,RuleGroupID INT NOT NULL
				   ,RuleWeight FLOAT NOT NULL
				   ,RuleResult FLOAT NOT NULL
				   ,ComparisonOrCalculation VARCHAR(255) NULL DEFAULT NULL
				);

			SELECT		mrm.MeasureID
					   ,mrm.RuleID
					   ,mrm.ExecutionOrder
					   ,m.MeasureName
					   ,m.MeasureVersion
					   ,m.measureResult
					   ,m.MeasureWeight
					   ,r.RuleName
					   ,r.ComparisonOrCalculation
					   ,st.SourceTableDatabase
					   ,st.SourceTableSchema
					   ,st.SourceTableObject
					   ,st.SourceTableUniqueKeyColumnName
					   ,st.SourceTableSnapshotDateColumnName
					   ,st.SourceTableType
					   ,st.SourceEAVColumnAttributeName
					   ,st.SourceEAVColumnValueName
					   ,r.SourceAttributeName
					   ,tt.SourceTableDatabase AS TargetTableDatabase
					   ,tt.SourceTableSchema AS Targettableschema
					   ,tt.SourceTableObject AS Targettableobject
					   ,tt.SourceTableUniqueKeyColumnName AS Targettableuniquekeycolumnname
					   ,tt.SourceTableSnapshotDateColumnName AS Targettablesnapshotdatecolumnname
					   ,tt.SourceTableType AS Targettabletype
					   ,tt.SourceEAVColumnAttributeName AS TargetEAVColumnattributeName
					   ,tt.SourceEAVColumnValueName AS TargetEAVColumnValueName
					   ,r.TargetAttributeName
					   ,r.Operator
					   ,r.StaticValueIfNullTarget
					   ,r.RuleVersion
					   ,r.RuleWeight
					   ,r.RuleResult
					   ,grm.ExecutionOrder AS ruleGroupExecutionOrder
					   ,rg.RuleGroupID
					   ,rg.RuleGroupName
					   ,sst.SourceTableDatabase AS BaseTableDatabase
					   ,sst.SourceTableSchema AS BaseTableSchema
					   ,sst.SourceTableObject AS BaseTableObject
					   ,sst.SourceTableUniqueKeyColumnName AS BaseTableUniqueKeyColumnName
					   ,sst.SourceTableSnapshotDateColumnName AS BaseTableSnapshotDateColumnName
					   ,sst.SourceTableType AS BaseTableType
			INTO		#groupdata
			FROM		config.MeasureRuleMap AS mrm
						INNER JOIN config.Measures AS m
							ON m.MeasureID = mrm.MeasureID
							   AND m.IsActive = 1
						INNER JOIN config.Rules AS r
							ON r.RuleID = mrm.RuleID
							   AND r.IsActive = 1
						INNER JOIN config.GroupRuleMap AS grm
							ON grm.RuleID = r.RuleID
						INNER JOIN config.RuleGroups AS rg
							ON rg.RuleGroupID = grm.RuleGroupID
							   AND rg.IsActive = 1
						INNER JOIN config.sourcetables AS st
							ON st.SourceTableID = r.SourceTableID
							   AND st.IsActive = 1
						INNER JOIN config.sourcetables AS sst
							ON sst.sourcetableID = m.BaseTableID
							   AND sst.IsActive = 1
						LEFT JOIN config.sourcetables AS tt
							ON tt.SourceTableID = r.TargetTableID
							   AND tt.IsActive = 1
			WHERE		m.MeasureID = @measureIDToLoad
			ORDER BY	mrm.ExecutionOrder ASC
					   ,grm.ExecutionOrder ASC;

			SET @measureResult = (SELECT MAX(measureResult) FROM #groupdata)			
			SET @measureWeight = (SELECT MAX(measureWeight) FROM #groupdata)

			IF NOT EXISTS (
							  SELECT	1
							  FROM		#groupdata
						  )
				BEGIN
					SET @errorMessage = @errorMessage + N'#GroupData is Empty, check all config tables are updated; ';
				END;

			IF EXISTS (
						  SELECT	COUNT(DISTINCT RuleID)
						  FROM		#groupdata AS m
						  WHERE		m.ComparisonOrCalculation = 'Calculation'
						  GROUP BY	m.MeasureID
						  HAVING	COUNT(DISTINCT RuleID) <> (
																  SELECT	COUNT(DISTINCT RuleID)
																  FROM		#groupdata
																  WHERE		ComparisonOrCalculation = 'calculation'
															  )
					  )
				BEGIN
					SET @errorMessage = @errorMessage + N'Distinct Rule Count not matching for a measure for your Calculation, check all rules have a correctly associated measure; ';
				END;



			DECLARE RuleGroupCursor_cursor CURSOR FOR
				SELECT	RuleGroupID
					   ,MeasureID
					   ,RuleID
					   ,ComparisonOrCalculation
					   ,SourceTableDatabase
					   ,SourceTableSchema
					   ,SourceTableObject
					   ,SourceTableUniqueKeyColumnName
					   ,SourceTableSnapshotDateColumnName
					   ,SourceTableType
					   ,SourceEAVColumnAttributeName
					   ,SourceAttributeName
					   ,SourceEAVColumnValueName
					   ,TargetTableDatabase
					   ,Targettableschema
					   ,Targettableobject
					   ,Targettableuniquekeycolumnname
					   ,Targettablesnapshotdatecolumnname
					   ,Targettabletype
					   ,TargetEAVColumnattributeName
					   ,TargetAttributeName
					   ,TargetEAVColumnValueName
					   ,Operator
					   ,StaticValueIfNullTarget
					   ,RuleVersion
					   ,RuleWeight
					   ,RuleResult
				FROM	#groupdata;

			OPEN RuleGroupCursor_cursor;
			FETCH NEXT FROM RuleGroupCursor_cursor
			INTO @rulegroupid
				,@MeasureID
				,@ruleID
				,@ComparisonOrCalculation
				,@SourceTableDatabase
				,@SourceTableSchema
				,@SourceTableObject
				,@SourceTableUniqueKeyColumnName
				,@SourceTableSnapshotDateColumnName
				,@SourceTableType
				,@SourceEAVColumnAttributeName
				,@SourceAttributeName
				,@SourceEAVColumnValueName
				,@TargetTableDatabase
				,@TargetTableSchema
				,@TargetTableObject
				,@TargetTableUniqueKeyColumnName
				,@TargetTableSnapshotDateColumnName
				,@TargetTableType
				,@TargetEAVColumnAttributeName
				,@TargetAttributeName
				,@TargetEAVColumnValueName
				,@Operator
				,@StaticValueIfNullTarget
				,@RuleVersion
				,@RuleWeight
				,@RuleResult
			WHILE @@FETCH_STATUS = 0
				BEGIN
					BEGIN TRY
						SET @SourceTableReference = @SourceTableDatabase + '.' + @SourceTableSchema + '.' + @SourceTableObject;
						SET @TargetTableReference = @TargetTableDatabase + '.' + @TargetTableSchema + '.' + @TargetTableObject;

						SET @SourceConversionSQL = MachineLearningDataMart.dbo.fn_BuildEAVConversionBlock(@SourceTableType, @SourceTableReference, @SourceTableUniqueKeyColumnName, @SourceTableSnapshotDateColumnName, @SourceAttributeName, @SourceEAVColumnAttributeName, @SourceEAVColumnValueName, @ProcessDate, '#EAVSourceConversion');

						SET @TargetConversionSQL = MachineLearningDataMart.dbo.fn_BuildEAVConversionBlock(@TargetTableType, @TargetTableReference, @TargetTableUniqueKeyColumnName, @TargetTableSnapshotDateColumnName, @TargetAttributeName, @TargetEAVColumnAttributeName, @TargetEAVColumnValueName, @ProcessDate, '#EAVtargetConversion');

						SET @DynamicSQLRules = @SourceConversionSQL + N'   ' + @TargetConversionSQL;

						SET @ComparisonInsertSQL = N'INSERT INTO #tempRuleResults (UniqueID, MeasureID, RuleID, RuleGroupID, RuleWeight, RuleResult, ComparisonOrCalculation)
							select  ' + CONVERT(VARCHAR(255), ISNULL(@SourceTableUniqueKeyColumnName, 'MeasureIDNull')) + N', ' + CONVERT(VARCHAR(255), ISNULL(@MeasureID, 'MeasureIDNull')) + N' as measureID, ' + CONVERT(VARCHAR(255), ISNULL(@ruleID, 'RuleIDNull')) + N' as ruleID, ' + CONVERT(VARCHAR(255), ISNULL(@rulegroupid, 'RuleGroupIdNull')) + N' as groupID, ' + CONVERT(VARCHAR(255), ISNULL(@RuleWeight, 'RuleWeightNull')) + N' as RuleWeight, ' + CONVERT(VARCHAR(255), ISNULL(@RuleResult, 'RuleResultNull')) + N' as RuleResult, ''' + @ComparisonOrCalculation + N''' AS ComparisonOrCalculation';



						IF @DynamicSQLRules IS NULL
							BEGIN
								SET @errorMessage = @errorMessage + N' A cursor Variable is NULL, Possibly an issue with EAV Conversion SQL; ';
							END;

						ELSE
							BEGIN

								IF @ComparisonOrCalculation = 'Comparison'
									BEGIN
										IF @TargetTableReference IS NULL
										   OR	@TargetTableType IS NULL
											BEGIN
												PRINT ('Running Rule ' + CONVERT(VARCHAR(255), @ruleID) + ' in Comparison NULL Target Block');
												SET @DynamicSQLRules = @DynamicSQLRules + N' ' + @ComparisonInsertSQL + N'
							from #EAVsourceConversion 
							where  convert(float,'					   + @SourceEAVColumnValueName + N')  ' + @Operator + N' ' + @StaticValueIfNullTarget + N'  
							'					;
											END;

										ELSE
											BEGIN

												PRINT ('Running Rule ' + CONVERT(VARCHAR(255), @ruleID) + ' in Comparison NON NULL Target Block');
												SET @DynamicSQLRules = @DynamicSQLRules + N'					
							'										   + @ComparisonInsertSQL + N'
							from #EAVSourceConversion as es
							left join #EAVTargetConversion as et on et.' + @TargetTableUniqueKeyColumnName + N' = es.' + @SourceTableUniqueKeyColumnName + N' and et.' + @TargetTableSnapshotDateColumnName + N' = es.' + @SourceTableSnapshotDateColumnName + N'
							where  convert(float,es.'				   + @SourceEAVColumnValueName + N')  ' + @Operator + N' ' + @TargetEAVColumnValueName + N'  
							'					;
											END;
									END;
								ELSE IF @ComparisonOrCalculation = 'Calculation'
									BEGIN
										PRINT ('Running Rule ' + CONVERT(VARCHAR(255), @ruleID) + ' in Calculation NULL Target Block');
										IF @TargetTableReference IS NULL
										   OR	@TargetTableType = 'None'
											BEGIN
												SET @DynamicSQLRules = @DynamicSQLRules + N' INSERT INTO #tempRuleResults (UniqueID, MeasureID, RuleID, RuleGroupID, RuleWeight, RuleResult,ComparisonOrCalculation)
									select '						   + CONVERT(VARCHAR(255), @SourceTableUniqueKeyColumnName) + N', 
									'								   + CONVERT(VARCHAR(255), @MeasureID) + N' as measureID,
									'								   + CONVERT(VARCHAR(255), @ruleID) + N' as ruleID, 
									'								   + CONVERT(VARCHAR(255), @rulegroupid) + N' as groupID, 
									'								   + CONVERT(VARCHAR(255), @RuleWeight) + N' as RuleWeight, 
									'								   + N'convert(float,' + @SourceEAVColumnValueName + N') ' + N'  ' + @Operator + N' ' + @StaticValueIfNullTarget + N'  as RuleResult , 
									'''								   + @ComparisonOrCalculation + N''' AS ComparisonOrCalculation
										from #EAVsourceConversion';
											END;
									END;
								ELSE
									BEGIN
										SET @errorMessage = @errorMessage + N' Comparison Or Calculation Column most likely Incorrectly spelled; ';
									END;

								EXEC sp_executesql @DynamicSQLRules;

								IF @DynamicSQLRules IS NULL
									BEGIN
										SET @errorMessage = @errorMessage + N'Error with ruleID ' + CONVERT(VARCHAR(255), @ruleID) + N'. A cursor Variable is most likely NULL resulting in a NULL DynamicRuleSQL, Check Config Files look correct; ';
									END;

							END;


					END TRY
					BEGIN CATCH
						SET @errorMessage = N'Rule Error- RuleID - ' + CONVERT(VARCHAR(255), NULLIF(@ruleID, -1)) + N'; ' + ERROR_MESSAGE() + N'Dynamic Rule Query -' + @DynamicSQLRules + N'; ';

					END CATCH;
					FETCH NEXT FROM RuleGroupCursor_cursor
					INTO @rulegroupid
						,@MeasureID
						,@ruleID
						,@ComparisonOrCalculation
						,@SourceTableDatabase
						,@SourceTableSchema
						,@SourceTableObject
						,@SourceTableUniqueKeyColumnName
						,@SourceTableSnapshotDateColumnName
						,@SourceTableType
						,@SourceEAVColumnAttributeName
						,@SourceAttributeName
						,@SourceEAVColumnValueName
						,@TargetTableDatabase
						,@TargetTableSchema
						,@TargetTableObject
						,@TargetTableUniqueKeyColumnName
						,@TargetTableSnapshotDateColumnName
						,@TargetTableType
						,@TargetEAVColumnAttributeName
						,@TargetAttributeName
						,@TargetEAVColumnValueName
						,@Operator
						,@StaticValueIfNullTarget
						,@RuleVersion
						,@RuleWeight
						,@RuleResult
				END;

			CLOSE RuleGroupCursor_cursor;
			DEALLOCATE RuleGroupCursor_cursor;

			IF NOT EXISTS (
							  SELECT	1
							  FROM		#tempRuleResults
						  )
				BEGIN
					SET @errorMessage = @errorMessage + N'No Rules applied, check processdate exists in base/source/target tables. Check other Configs to ensure col names match; ';
				END;


			IF EXISTS (
						  SELECT	COUNT(DISTINCT RuleID)
						  FROM		#tempRuleResults AS m
						  WHERE		m.ComparisonOrCalculation = 'Calculation'
						  GROUP BY	m.MeasureID
						  HAVING	COUNT(DISTINCT RuleID) <> (
																  SELECT	COUNT(DISTINCT RuleID)
																  FROM		#groupdata
																  WHERE		ComparisonOrCalculation = 'calculation'
															  )
					  )
				BEGIN
					SET @errorMessage = @errorMessage + N'Missing Rule in Cursor Error;';
				END;


			SELECT		m.UniqueID
					   ,m.RuleGroupID
					   ,MAX(m.MeasureID) AS measureID
					   ,SUM((m.RuleWeight * m.RuleResult)) AS Result
			INTO		#tempRuleGroup
			FROM		#tempRuleResults AS m
			GROUP BY	m.UniqueID
					   ,m.RuleGroupID
			HAVING		COUNT(DISTINCT RuleID) = (
													 SELECT COUNT(DISTINCT RuleID)
													 FROM	#tempRuleResults
													 WHERE	RuleGroupID = m.RuleGroupID
												 );

			SELECT		UniqueID
					   ,measureID
					   ,SUM(Result) + (@MeasureWeight * @MeasureResult) AS Result
			INTO		#tempRuleResultsOutput
			FROM		#tempRuleGroup AS trg
			GROUP BY	UniqueID
					   ,measureID;


			BEGIN TRY --Try to Build Base

				SET @BaseTableReference = (
											  SELECT	DISTINCT
														QUOTENAME(BaseTableDatabase) + '.' + QUOTENAME(BaseTableSchema) + '.' + QUOTENAME(BaseTableObject)
											  FROM		#groupdata
										  );

				SET @BaseUniqueKeyColumnName = (
												   SELECT	DISTINCT
															QUOTENAME(BaseTableUniqueKeyColumnName)
												   FROM		#groupdata
											   );
				SET @BaseTableSnapshotDateColumnName = (
														   SELECT	DISTINCT
																	QUOTENAME(BaseTableSnapshotDateColumnName)
														   FROM		#groupdata
													   );

				SET @DynamicSQLBase = N'
				select distinct
						' + @BaseUniqueKeyColumnName + N' as UniqueID
					into #distinctPop
				from ' + @BaseTableReference + N'
				where ' + @BaseTableSnapshotDateColumnName + N' = ' + CONVERT(CHAR(8), @ProcessDate) + N'
				
				INSERT INTO #base
				(
					MeasureOutcomeSK
					,UniqueID
					,MeasureID
				
					,ProcessDate
					,OutcomeValue
					,RunDate
					,MeasureApplys
				)
				SELECT	CONVERT(BIGINT, HASHBYTES(''SHA1'', CONCAT(ISNULL(dmp.UniqueID, ''''), '';'', ISNULL(COALESCE(tmr.measureID, ' + CONVERT(VARCHAR(255), @measureIDToLoad) + N'), 0),'';'', ISNULL(' + CONVERT(CHAR(8), @ProcessDate) + N', 19000101), '';''))) AS MeasureOutcomeSK	
						,dmp.UniqueID
						,COALESCE(tmr.measureID, ' + CONVERT(VARCHAR(255), @measureIDToLoad) + N') AS MeasureID
						
						,' + CONVERT(CHAR(8), @ProcessDate) + N' as ProcessDate
						,COALESCE(tmr.Result, 0) AS OutcomeValue
						,''' + CONVERT(VARCHAR(255), @runDate) + N''' AS RunDate
						,CASE WHEN tmr.UniqueID IS NULL THEN 0 ELSE 1 END AS MeasureApplys
				FROM	#distinctPop AS dmp
						LEFT JOIN #tempRuleResultsOutput AS tmr
							ON tmr.UniqueID = dmp.UniqueID
					';

				EXEC sp_executesql @DynamicSQLBase;

			END TRY
			BEGIN CATCH
				SET @errorMessage = @errorMessage + N'Base load error; ' + ERROR_MESSAGE() + N'; DynamicSQLBase = ' + @DynamicSQLBase + N'; ';
			END CATCH;

			IF NOT EXISTS (
							  SELECT	1
							  FROM		#base
						  )
				BEGIN
					SET @errorMessage = @errorMessage + N'Base is Empty;';
				--RAISERROR(@errorMessage, 16, 1);
				END;



			IF @errorMessage <> ''
				BEGIN
					RAISERROR('', 16, 1);
				END;

			SET @totalLoadEndTime = GETDATE();
			SET @totalLoadTime = 'Total Load Time = ' + CONVERT(VARCHAR(6), DATEDIFF(SECOND, @totalLoadStartTime, @totalLoadEndTime) / 3600) + ':' + RIGHT('0' + CONVERT(VARCHAR(2), (DATEDIFF(SECOND, @totalLoadStartTime, @totalLoadEndTime) % 3600) / 60), 2) + ':' + RIGHT('0' + CONVERT(VARCHAR(2), DATEDIFF(SECOND, @totalLoadStartTime, @totalLoadEndTime) % 60), 2);
			RAISERROR(@totalLoadTime, 0, 1) WITH NOWAIT;
			/*--------------------------------
			Merge Results
			--------------------------------*/
			RAISERROR('MERGE Results', 0, 1) WITH NOWAIT;

			/*
			EXEC ecudw.dbo.usp_BuildMergeCommand
				@databaseName = 'MachineLearningDataMart'
				,@schemaName = 'dbo'
				,@tableName = 'MeasureOutcomes'
				,@sourceTableName = '#base'
			*/
			MERGE INTO dbo.MeasureOutcomes AS t
			USING #base AS s
			ON s.MeasureOutcomeSK = t.MeasureOutcomeSK
			--WHEN NOT MATCHED BY SOURCE THEN DELETE
			WHEN MATCHED AND EXISTS (
										SELECT	s.MeasureOutcomeSK
											   ,s.UniqueID
											   ,s.MeasureID
											   ,s.ProcessDate
											   ,s.OutcomeValue
											   ,s.RunDate
											   ,s.MeasureApplys
										EXCEPT
										SELECT	t.MeasureOutcomeSK
											   ,t.UniqueID
											   ,t.MeasureID
											   ,t.ProcessDate
											   ,t.OutcomeValue
											   ,t.RunDate
											   ,t.MeasureApplys
									) THEN UPDATE SET MeasureOutcomeSK = s.MeasureOutcomeSK
													 ,UniqueID = s.UniqueID
													 ,MeasureID = s.MeasureID
													 ,ProcessDate = s.ProcessDate
													 ,OutcomeValue = s.OutcomeValue
													 ,RunDate = s.RunDate
													 ,MeasureApplys = s.MeasureApplys
													 --  ,[audInsertDateTime] = s.[audInsertDateTime]
													 ,audUpdateDateTime = GETUTCDATE()
			WHEN NOT MATCHED THEN INSERT (
											 MeasureOutcomeSK
											,UniqueID
											,MeasureID
											,ProcessDate
											,OutcomeValue
											,RunDate
											,MeasureApplys
											,audInsertDateTime
											,audUpdateDateTime
										 )
								  VALUES
								  (s.MeasureOutcomeSK, s.UniqueID, s.MeasureID, s.ProcessDate, s.OutcomeValue, s.RunDate, s.MeasureApplys, GETUTCDATE(), GETUTCDATE())
			OUTPUT $action
			INTO @mergeResultsTable;

			/*--------------------------------
				Capture Merge Results
			--------------------------------*/
			SELECT	@insertCount = [INSERT]
				   ,@updateCount = [UPDATE]
				   ,@deleteCount = [DELETE]
			FROM	(
						SELECT	'NOOP' AS MergeAction /*Row for NULL merge into NULL.*/
						UNION ALL
						SELECT	MergeAction
						FROM	@mergeResultsTable
					) AS mergeResultsPlusEmptyRow
			PIVOT (
					  COUNT(MergeAction)
					  FOR MergeAction IN ([INSERT], [UPDATE], [DELETE])
				  ) AS mergeResultsPivot;

			/*################################
				End Logging - Succeed
			################################*/
			PRINT ('Sucess Running MeasureID ' + CONVERT(VARCHAR(255), @MeasureID) + ' on ' + CONVERT(VARCHAR(255), @runDate));
			UPDATE	dbo.MeasureOutcomesControlLog
			SET		EndDateTime = GETUTCDATE()
				   ,Status = 'Completed'
				   ,Message = 'Processed successfully'
				   ,MergeInsertCount = @insertCount
				   ,MergeUpdateCount = @updateCount
				   ,MergeDeleteCount = @deleteCount
				   ,DynamicRuleSQL = @DynamicSQLRules
				   ,DynamicBaseSQL = @DynamicSQLBase
			WHERE	MeasureControlID = @measureIDToLoad
					AND status = 'In Progress';
		END TRY
		BEGIN CATCH
			--/*################################
			--	End Logging - Fail
			--################################*/
			PRINT ('Failure Running MeasureID ' + CONVERT(VARCHAR(255), @MeasureID) + ' on ' + CONVERT(VARCHAR(255), @runDate));
			SET @errorMessage = @errorMessage + ERROR_MESSAGE();
			UPDATE	dbo.MeasureOutcomesControlLog
			SET		EndDateTime = GETUTCDATE()
				   ,Status = 'Failed'
				   ,Message = @errorMessage
				   ,MergeInsertCount = @insertCount
				   ,MergeUpdateCount = @updateCount
				   ,MergeDeleteCount = @deleteCount
				   ,DynamicRuleSQL = @DynamicSQLRules
				   ,DynamicBaseSQL = @DynamicSQLBase
			WHERE	MeasureControlID = @measureIDToLoad
					AND Status = 'In Progress';

		END CATCH;

	END;

GO
