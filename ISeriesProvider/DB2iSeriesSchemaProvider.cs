using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using LinqToDB.Extensions;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using Common;
	using Data;
	using SchemaProvider;
	using System.Data.Common;

	public class DB2iSeriesSchemaProvider : SchemaProviderBase
	{
		protected override List<ColumnInfo> GetColumns(DataConnection dataConnection)
		{
			var sql = $@"
				Select 
				  Column_text 
				, case 
                    when CCSID = 65535 and Data_Type in ('CHAR', 'VARCHAR')
                    then TRIM(Data_Type) || ' FOR BIT DATA'
                    when Data_Type = 'TIMESTMP' then 'TIMESTAMP'
                    else Data_Type end as Data_Type
				, Is_Identity
				, Is_Nullable
				, Length
				, COALESCE(Numeric_Scale,0) Numeric_Scale
				, Ordinal_Position
				, Column_Name
				, Table_Name
				, Table_Schema
				, Column_Name
				From {dataConnection.GetTableName("QSYS2","SYSCOLUMNS")}
				where System_Table_Schema in('{GetLibList(dataConnection)}')
				 ";

			ColumnInfo drf(IDataReader dr)
			{
				var ci = new ColumnInfo
				{
					DataType = dr["Data_Type"].ToString().TrimEnd(),
					Description = dr["Column_Text"].ToString().TrimEnd(),
					IsIdentity = dr["Is_Identity"].ToString().TrimEnd() == "YES",
					IsNullable = dr["Is_Nullable"].ToString().TrimEnd() == "Y",
					Name = dr["Column_Name"].ToString().TrimEnd(),
					Ordinal = Converter.ChangeTypeTo<int>(dr["Ordinal_Position"]),
					TableID = dataConnection.Connection.Database + "." + Convert.ToString(dr["Table_Schema"]).TrimEnd() + "." + Convert.ToString(dr["Table_Name"]).TrimEnd(),
				};
				SetColumnParameters(ci, Convert.ToInt64(dr["Length"]), Convert.ToInt32(dr["Numeric_Scale"]));
				return ci;
			}

			var list = dataConnection.Query(drf, sql).ToList();
			return list;
		}

        protected override DataType GetDataType(string dataType, string columnType, long? length, int? prec, int? scale)
		{
			switch (dataType)
			{
				case "BIGINT": return DataType.Int64;
				case "BINARY": return DataType.Binary;
				case "BLOB": return DataType.Blob;
				case "CHAR": return DataType.Char;
				case "CHAR FOR BIT DATA": return DataType.Binary;
				case "CLOB": return DataType.Text;
				case "DATALINK": return DataType.Undefined;
				case "DATE": return DataType.Date;
				case "DBCLOB": return DataType.Undefined;
				case "DECIMAL": return DataType.Decimal;
				case "DOUBLE": return DataType.Double;
				case "GRAPHIC": return DataType.Text;
				case "INTEGER": return DataType.Int32;
				case "NUMERIC": return DataType.Decimal;
				case "REAL": return DataType.Single;
				case "ROWID": return DataType.Undefined;
				case "SMALLINT": return DataType.Int16;
				case "TIME": return DataType.Time;
				case "TIMESTAMP": return DataType.Timestamp;
                case "VARBINARY": return DataType.VarBinary;
				case "VARCHAR": return DataType.VarChar;
				case "VARCHAR FOR BIT DATA": return DataType.VarBinary;
				case "VARGRAPHIC": return DataType.Text;
				default: return DataType.Undefined;
			}
		}

		protected override List<ForeignKeyInfo> GetForeignKeys(DataConnection dataConnection)
		{
			var sql = $@"
		  Select ref.Constraint_Name 
		  , fk.Ordinal_Position
		  , fk.Column_Name  As ThisColumn
		  , fk.Table_Name   As ThisTable
		  , fk.Table_Schema As ThisSchema
		  , uk.Column_Name  As OtherColumn
		  , uk.Table_Schema As OtherSchema
		  , uk.Table_Name   As OtherTable
		  From {dataConnection.GetTableName("QSYS2", "SYSREFCST")} ref
		  Join {dataConnection.GetTableName("QSYS2", "SYSKEYCST")} fk on(fk.Constraint_Schema, fk.Constraint_Name) = (ref.Constraint_Schema, ref.Constraint_Name)
		  Join {dataConnection.GetTableName("QSYS2", "SYSKEYCST")} uk on(uk.Constraint_Schema, uk.Constraint_Name) = (ref.Unique_Constraint_Schema, ref.Unique_Constraint_Name)
		  Where uk.Ordinal_Position = fk.Ordinal_Position
		  And fk.System_Table_Schema in('{GetLibList(dataConnection)}')
		  Order By ThisSchema, ThisTable, Constraint_Name, Ordinal_Position
		  ";

			//And {GetSchemaFilter("col.TBCREATOR")}
			Func<IDataReader, ForeignKeyInfo> drf = (IDataReader dr) => new ForeignKeyInfo
			{
				Name = dr["Constraint_Name"].ToString().TrimEnd(),
				Ordinal = Converter.ChangeTypeTo<int>(dr["Ordinal_Position"]),
				OtherColumn = dr["OtherColumn"].ToString().TrimEnd(),
				OtherTableID = dataConnection.Connection.Database + "." + Convert.ToString(dr["OtherSchema"]).TrimEnd() + "." + Convert.ToString(dr["OtherTable"]).TrimEnd(),
				ThisColumn = dr["ThisColumn"].ToString().TrimEnd(),
				ThisTableID = dataConnection.Connection.Database + "." + Convert.ToString(dr["ThisSchema"]).TrimEnd() + "." + Convert.ToString(dr["ThisTable"]).TrimEnd()
			};

			var list = dataConnection.Query(drf, sql).ToList();
			return list;
		}

		protected override List<PrimaryKeyInfo> GetPrimaryKeys(DataConnection dataConnection)
		{
			var sql = $@"
		  Select cst.constraint_Name  
			   , cst.table_SCHEMA
			   , cst.table_NAME 
			   , col.Ordinal_position 
			   , col.Column_Name   
		  From {dataConnection.GetTableName("QSYS2", "SYSKEYCST")} col
		  Join {dataConnection.GetTableName("QSYS2", "SYSCST")}    cst On(cst.constraint_SCHEMA, cst.constraint_NAME, cst.constraint_type) = (col.constraint_SCHEMA, col.constraint_NAME, 'PRIMARY KEY')
		  And cst.System_Table_Schema in('{GetLibList(dataConnection)}')
		  Order By cst.table_SCHEMA, cst.table_NAME, col.Ordinal_position
		  ";

			PrimaryKeyInfo drf(IDataReader dr) => new PrimaryKeyInfo
			{
				ColumnName = Convert.ToString(dr["Column_Name"]).TrimEnd(),
				Ordinal = Converter.ChangeTypeTo<int>(dr["Ordinal_position"]),
				PrimaryKeyName = Convert.ToString(dr["constraint_Name"]).TrimEnd(),
				TableID = dataConnection.Connection.Database + "." + Convert.ToString(dr["table_SCHEMA"]).TrimEnd() + "." + Convert.ToString(dr["table_NAME"]).TrimEnd()
			};

			var list = dataConnection.Query(drf, sql).ToList();
			return list;
		}

		protected override List<ProcedureInfo> GetProcedures(DataConnection dataConnection)
		{
			var sql = $@"
		  Select
			CAST(CURRENT_SERVER AS VARCHAR(128)) AS Catalog_Name
		  , Function_Type
		  , Routine_Definition
		  , Routine_Name
		  , Routine_Schema
		  , Routine_Type
		  , Specific_Name
		  , Specific_Schema
		  From {dataConnection.GetTableName("QSYS2", "SYSROUTINES")}
		  Where Specific_Schema in('{GetLibList(dataConnection)}')
		  Order By Specific_Schema, Specific_Name
		  ";

			//And {GetSchemaFilter("col.TBCREATOR")}
			var defaultSchema = dataConnection.Execute<string>("select current_schema from " + DB2iSeriesTools.iSeriesDummyTableName(dataConnection));

			ProcedureInfo drf(IDataReader dr)
			{
				return new ProcedureInfo
				{
					CatalogName = Convert.ToString(dr["Catalog_Name"]).TrimEnd(),
					IsDefaultSchema = Convert.ToString(dr["Routine_Schema"]).TrimEnd() == defaultSchema,
					IsFunction = Convert.ToString(dr["Routine_Type"]) == "FUNCTION",
					IsTableFunction = Convert.ToString(dr["Function_Type"]) == "T",
					ProcedureDefinition = Convert.ToString(dr["Routine_Definition"]).TrimEnd(),
					ProcedureID = dataConnection.Connection.Database + "." + Convert.ToString(dr["Specific_Schema"]).TrimEnd() + "." + Convert.ToString(dr["Specific_Name"]).TrimEnd(),
					ProcedureName = Convert.ToString(dr["Routine_Name"]).TrimEnd(),
					SchemaName = Convert.ToString(dr["Routine_Schema"]).TrimEnd()
				};
			}

			var list = dataConnection.Query(drf, sql).ToList();
			return list;
		}

		protected override List<ProcedureParameterInfo> GetProcedureParameters(DataConnection dataConnection)
		{
			var sql = $@"
		  Select 
			CHARACTER_MAXIMUM_LENGTH
		  , Data_Type
		  , Numeric_Precision
		  , Numeric_Scale
		  , Ordinal_position
		  , Parameter_Mode
		  , Parameter_Name
		  , Specific_Name
		  , Specific_Schema
		  From {dataConnection.GetTableName("QSYS2", "SYSPARMS")} 
		  where Specific_Schema in('{GetLibList(dataConnection)}')
		  Order By Specific_Schema, Specific_Name, Parameter_Name
		  ";

			//And {GetSchemaFilter("col.TBCREATOR")}
			Func<IDataReader, ProcedureParameterInfo> drf = (IDataReader dr) =>
			{
				return new ProcedureParameterInfo
				{
					DataType = Convert.ToString(dr["Parameter_Name"]),
					IsIn = dr["Parameter_Mode"].ToString().Contains("IN"),
					IsOut = dr["Parameter_Mode"].ToString().Contains("OUT"),
					Length = Converter.ChangeTypeTo<long?>(dr["CHARACTER_MAXIMUM_LENGTH"]),
					Ordinal = Converter.ChangeTypeTo<int>(dr["Ordinal_position"]),
					ParameterName = Convert.ToString(dr["Parameter_Name"]).TrimEnd(),
					Precision = Converter.ChangeTypeTo<int?>(dr["Numeric_Precision"]),
					ProcedureID = dataConnection.Connection.Database + "." + Convert.ToString(dr["Specific_Schema"]).TrimEnd() + "." + Convert.ToString(dr["Specific_Name"]).TrimEnd(),
					Scale = Converter.ChangeTypeTo<int?>(dr["Numeric_Scale"]),
				};
			};
			List<ProcedureParameterInfo> _list = dataConnection.Query(drf, sql).ToList();
			return _list;
		}

		protected override string GetProviderSpecificTypeNamespace()
		{
			return DB2iSeriesTools.AssemblyName;
		}

		protected override List<TableInfo> GetTables(DataConnection dataConnection)
		{
			var sql = $@"
				  Select 
					CAST(CURRENT_SERVER AS VARCHAR(128)) AS Catalog_Name
				  , Table_Schema
				  , Table_Name
				  , Table_Text
				  , Table_Type
				  , System_Table_Schema
				  From {dataConnection.GetTableName("QSYS2", "SYSTABLES")}
				  Where Table_Type In('L', 'P', 'T', 'V')
				  And System_Table_Schema in ('{GetLibList(dataConnection)}')	
				  Order By System_Table_Schema, System_Table_Name
				 ";

			var defaultSchema = dataConnection.Execute<string>("select current_schema from " + DB2iSeriesTools.iSeriesDummyTableName(dataConnection));
            Func<IDataReader, TableInfo> drf = (IDataReader dr) => new TableInfo
			{
			    CatalogName = dr["Catalog_Name"].ToString().TrimEnd(),
			    Description = dr["Table_Text"].ToString().TrimEnd(),
			    IsDefaultSchema = dr["System_Table_Schema"].ToString().TrimEnd() == defaultSchema,
			    IsView = new[] { "L", "V" }.Contains<string>(dr["Table_Type"].ToString()),
			    SchemaName = dr["Table_Schema"].ToString().TrimEnd(),
			    TableID = dataConnection.Connection.Database + "." + dr["Table_Schema"].ToString().TrimEnd() + "." + dr["Table_Name"].ToString().TrimEnd(),
			    TableName = dr["Table_Name"].ToString().TrimEnd()
			};
			var _list = dataConnection.Query(drf, sql).ToList();
			return _list;
		}

		#region Helpers

		public static void SetColumnParameters(ColumnInfo ci, long? size, int? scale)
		{
			switch (ci.DataType)
			{
				case "DECIMAL":
				case "NUMERIC":
					if (((size ?? 0)) > 0)
					{
						ci.Precision = (int?)size.Value;
					}
					if (((scale ?? 0)) > 0)
					{
						ci.Scale = scale;
					}
					break;
				case "BINARY":
				case "BLOB":
				case "CHAR":
				case "CHAR FOR BIT DATA":
				case "CLOB":
				case "DATALINK":
				case "DBCLOB":
				case "GRAPHIC":
				case "VARBINARY":
				case "VARCHAR":
				case "VARCHAR FOR BIT DATA":
				case "VARGRAPHIC":
					ci.Length = size;
					break;
				case "INTEGER":
				case "SMALLINT":
				case "BIGINT":
				case "TIMESTMP":
				case "TIMESTAMP":
                case "DATE":
				case "TIME":
				case "VARG":
				case "DECFLOAT":
				case "FLOAT":
				case "ROWID":
				case "VARBIN":
				case "XML":
					break;
				default:
					throw new NotImplementedException($"unknown data type: {ci.DataType}");
			}
		}

        #endregion

	    private string GetLibList(DataConnection dataConnection)
	    {
	        if (dataConnection.Connection == null || dataConnection.Connection.GetType().Name != "iDB2Connection")
	            throw new LinqToDBException("dataconnection is not iDB2Connection.");

	        var libListProp = dataConnection.Connection.GetType()
	            .GetPropertiesEx(BindingFlags.Public | BindingFlags.Instance)
	            .FirstOrDefault(p => p.Name == "LibraryList"); 

	        if (libListProp == null)
	            throw new LinqToDBException("iDB2Connection is missing LibraryList property, perhaps the IBM library has moved to non supported version");

	        var liblist = Expression.Lambda<Func<object>>(
	                Expression.Convert(
	                    Expression.MakeMemberAccess(Expression.Constant(dataConnection.Connection), libListProp),
	                    typeof(object)))
	            .Compile()();

            return string.Join("','", liblist.ToString().Split(','));
	    }
        public override DatabaseSchema GetSchema(DataConnection dataConnection, GetSchemaOptions options = null)
        {
            if(options is GetTablesSchemaOptions)
            {
                var opt = options as GetTablesSchemaOptions;
                if (options == null)
                    opt = new GetTablesSchemaOptions();

                IncludedSchemas = GetHashSet(options.IncludedSchemas, options.StringComparer);
                ExcludedSchemas = GetHashSet(options.ExcludedSchemas, options.StringComparer);
                IncludedCatalogs = GetHashSet(options.IncludedCatalogs, options.StringComparer);
                ExcludedCatalogs = GetHashSet(options.ExcludedCatalogs, options.StringComparer);
                var includedTables = GetHashSet(opt.IncludedTables, options.StringComparer);
                var excludedTables = GetHashSet(opt.ExcludedTables, options.StringComparer);
                GenerateChar1AsString = options.GenerateChar1AsString;

                var dbConnection = (DbConnection)dataConnection.Connection;

                InitProvider(dataConnection);

                DataTypes = GetDataTypes(dataConnection);
                DataTypesDic = new Dictionary<string, DataTypeInfo>(DataTypes.Count, StringComparer.OrdinalIgnoreCase);

                foreach (var dt in DataTypes)
                    if (!DataTypesDic.ContainsKey(dt.TypeName))
                        DataTypesDic.Add(dt.TypeName, dt);

                List<TableSchema> tables;
                List<ProcedureSchema> procedures;

                if (options.GetTables)
                {
                    tables =
                    (
                        from t in GetTables(dataConnection)
                        where
                            (IncludedSchemas.Count == 0 || IncludedSchemas.Contains(t.SchemaName)) &&
                            (ExcludedSchemas.Count == 0 || !ExcludedSchemas.Contains(t.SchemaName)) &&
                            (IncludedCatalogs.Count == 0 || IncludedCatalogs.Contains(t.CatalogName)) &&
                            (ExcludedCatalogs.Count == 0 || !ExcludedCatalogs.Contains(t.CatalogName)) &&
                            (includedTables.Count == 0 || includedTables.Contains(t.TableName)) &&
                            (excludedTables.Count == 0 || !excludedTables.Contains(t.TableName))
                        select new TableSchema
                        {
                            ID = t.TableID,
                            CatalogName = t.CatalogName,
                            SchemaName = t.SchemaName,
                            TableName = t.TableName,
                            Description = t.Description,
                            IsDefaultSchema = t.IsDefaultSchema,
                            IsView = t.IsView,
                            TypeName = ToValidName(t.TableName),
                            Columns = new List<ColumnSchema>(),
                            ForeignKeys = new List<ForeignKeySchema>(),
                            IsProviderSpecific = t.IsProviderSpecific
                        }
                    ).ToList();

                    var pks = GetPrimaryKeys(dataConnection);

                    #region Columns

                    var columns =
                        from c in GetColumns(dataConnection)

                        join pk in pks
                            on c.TableID + "." + c.Name equals pk.TableID + "." + pk.ColumnName into g2
                        from pk in g2.DefaultIfEmpty()

                        join t in tables on c.TableID equals t.ID

                        orderby c.Ordinal
                        select new { t, c, dt = GetDataType(c.DataType), pk };

                    foreach (var column in columns)
                    {
                        var dataType = column.c.DataType;
                        var systemType = GetSystemType(dataType, column.c.ColumnType, column.dt, column.c.Length, column.c.Precision, column.c.Scale);
                        var isNullable = column.c.IsNullable;
                        var columnType = column.c.ColumnType ?? GetDbType(dataType, column.dt, column.c.Length, column.c.Precision, column.c.Scale);

                        column.t.Columns.Add(new ColumnSchema
                        {
                            Table = column.t,
                            ColumnName = column.c.Name,
                            ColumnType = columnType,
                            IsNullable = isNullable,
                            MemberName = ToValidName(column.c.Name),
                            MemberType = ToTypeName(systemType, isNullable),
                            SystemType = systemType ?? typeof(object),
                            DataType = GetDataType(dataType, column.c.ColumnType, column.c.Length, column.c.Precision, column.c.Scale),
                            ProviderSpecificType = GetProviderSpecificType(dataType),
                            SkipOnInsert = column.c.SkipOnInsert || column.c.IsIdentity,
                            SkipOnUpdate = column.c.SkipOnUpdate || column.c.IsIdentity,
                            IsPrimaryKey = column.pk != null,
                            PrimaryKeyOrder = column.pk?.Ordinal ?? -1,
                            IsIdentity = column.c.IsIdentity,
                            Description = column.c.Description,
                            Length = column.c.Length,
                            Precision = column.c.Precision,
                            Scale = column.c.Scale,
                        });
                    }

                    #endregion

                    #region FK

                    var fks = GetForeignKeys(dataConnection);

                    foreach (var fk in fks.OrderBy(f => f.Ordinal))
                    {
                        var thisTable = (from t in tables where t.ID == fk.ThisTableID select t).FirstOrDefault();
                        var otherTable = (from t in tables where t.ID == fk.OtherTableID select t).FirstOrDefault();

                        if (thisTable == null || otherTable == null)
                            continue;

                        var thisColumn = (from c in thisTable.Columns where c.ColumnName == fk.ThisColumn select c).SingleOrDefault();
                        var otherColumn = (from c in otherTable.Columns where c.ColumnName == fk.OtherColumn select c).SingleOrDefault();

                        if (thisColumn == null || otherColumn == null)
                            continue;

                        var key = thisTable.ForeignKeys.FirstOrDefault(f => f.KeyName == fk.Name);

                        if (key == null)
                        {
                            key = new ForeignKeySchema
                            {
                                KeyName = fk.Name,
                                MemberName = ToValidName(fk.Name),
                                ThisTable = thisTable,
                                OtherTable = otherTable,
                                ThisColumns = new List<ColumnSchema>(),
                                OtherColumns = new List<ColumnSchema>(),
                                CanBeNull = true,
                            };

                            thisTable.ForeignKeys.Add(key);
                        }

                        key.ThisColumns.Add(thisColumn);
                        key.OtherColumns.Add(otherColumn);
                    }

                    #endregion

                    var pst = GetProviderSpecificTables(dataConnection);

                    if (pst != null)
                        tables.AddRange(pst);
                }
                else
                    tables = new List<TableSchema>();

                if (options.GetProcedures)
                {
                    #region Procedures

                    var sqlProvider = dataConnection.DataProvider.CreateSqlBuilder();
                    var procs = GetProcedures(dataConnection);
                    var procPparams = GetProcedureParameters(dataConnection);
                    var n = 0;

                    if (procs != null)
                    {
                        procedures =
                        (
                            from sp in procs
                            where
                                (IncludedSchemas.Count == 0 || IncludedSchemas.Contains(sp.SchemaName)) &&
                                (ExcludedSchemas.Count == 0 || !ExcludedSchemas.Contains(sp.SchemaName)) &&
                                (IncludedCatalogs.Count == 0 || IncludedCatalogs.Contains(sp.CatalogName)) &&
                                (ExcludedCatalogs.Count == 0 || !ExcludedCatalogs.Contains(sp.CatalogName))
                            join p in procPparams on sp.ProcedureID equals p.ProcedureID
                            into gr
                            select new ProcedureSchema
                            {
                                CatalogName = sp.CatalogName,
                                SchemaName = sp.SchemaName,
                                ProcedureName = sp.ProcedureName,
                                MemberName = ToValidName(sp.ProcedureName),
                                IsFunction = sp.IsFunction,
                                IsTableFunction = sp.IsTableFunction,
                                IsResultDynamic = sp.IsResultDynamic,
                                IsAggregateFunction = sp.IsAggregateFunction,
                                IsDefaultSchema = sp.IsDefaultSchema,
                                Parameters =
                                (
                                    from pr in gr

                                    join dt in DataTypes
                                        on pr.DataType equals dt.TypeName into g1
                                    from dt in g1.DefaultIfEmpty()

                                    let systemType = GetSystemType(pr.DataType, null, dt, pr.Length, pr.Precision, pr.Scale)

                                    orderby pr.Ordinal
                                    select new ParameterSchema
                                    {
                                        SchemaName = pr.ParameterName,
                                        SchemaType = GetDbType(pr.DataType, dt, pr.Length, pr.Precision, pr.Scale),
                                        IsIn = pr.IsIn,
                                        IsOut = pr.IsOut,
                                        IsResult = pr.IsResult,
                                        Size = pr.Length,
                                        ParameterName = ToValidName(pr.ParameterName ?? "par" + ++n),
                                        ParameterType = ToTypeName(systemType, true),
                                        SystemType = systemType ?? typeof(object),
                                        DataType = GetDataType(pr.DataType, null, pr.Length, pr.Precision, pr.Scale),
                                        ProviderSpecificType = GetProviderSpecificType(pr.DataType),
                                    }
                                ).ToList()
                            } into ps
                            where ps.Parameters.All(p => p.SchemaType != "table type")
                            select ps
                        ).ToList();

                        var current = 1;

                        var isActiveTransaction = dataConnection.Transaction != null;

                        if (GetProcedureSchemaExecutesProcedure && isActiveTransaction)
                            throw new LinqToDBException("Cannot read schema with GetSchemaOptions.GetProcedures = true from transaction. Remove transaction or set GetSchemaOptions.GetProcedures to false");

                        if (!isActiveTransaction)
                            dataConnection.BeginTransaction();

                        try
                        {
                            foreach (var procedure in procedures)
                            {
                                if (!procedure.IsResultDynamic && (!procedure.IsFunction || procedure.IsTableFunction) && options.LoadProcedure(procedure))
                                {
                                    var commandText = sqlProvider.ConvertTableName(new System.Text.StringBuilder(),
                                         procedure.CatalogName,
                                         procedure.SchemaName,
                                         procedure.ProcedureName).ToString();

                                    LoadProcedureTableSchema(dataConnection, procedure, commandText, tables);
                                }

                                options.ProcedureLoadingProgress(procedures.Count, current++);
                            }
                        }
                        finally
                        {
                            if (!isActiveTransaction)
                                dataConnection.RollbackTransaction();
                        }
                    }
                    else
                        procedures = new List<ProcedureSchema>();

                    var psp = GetProviderSpecificProcedures(dataConnection);

                    if (psp != null)
                        procedures.AddRange(psp);

                    #endregion
                }
                else
                    procedures = new List<ProcedureSchema>();

                return ProcessSchema(new DatabaseSchema
                {
                    DataSource = GetDataSourceName(dbConnection),
                    Database = GetDatabaseName(dbConnection),
                    ServerVersion = dbConnection.ServerVersion,
                    Tables = tables,
                    Procedures = procedures,
                    ProviderSpecificTypeNamespace = GetProviderSpecificTypeNamespace(),
                    DataTypesSchema = DataTypesSchema,

                }, options);

            }
            return base.GetSchema(dataConnection, options);
        }
    }
    public class GetTablesSchemaOptions : GetSchemaOptions
    {
        public string[] ExcludedTables;
        public string[] IncludedTables;

        public GetTablesSchemaOptions()
        {
        }
    }
}