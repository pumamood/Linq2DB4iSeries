﻿using System;

namespace LinqToDB.DataProvider.DB2iSeries
{
	using Data;
	using Mapping;
	using SqlQuery;

	class DB2iSeriesMultipleRowsHelper<T> : MultipleRowsHelper<T>
	{
		public DB2iSeriesMultipleRowsHelper(DataConnection dataConnection, BulkCopyOptions options) 
		    : base(dataConnection, options)
		{
        }

		public override void BuildColumns(object item, Func<ColumnDescriptor, bool> skipConvert = null)
		{
			skipConvert = skipConvert ?? (sc => false);

			for (var i = 0; i < Columns.Length; i++)
			{
				var column = Columns[i];
				var value = column.GetValue(DataConnection.MappingSchema, item);
				var columnType = ColumnTypes[i];

				if (column.DbType != null)
				{
					if (column.DbType.Equals("time", StringComparison.CurrentCultureIgnoreCase))
						columnType = new SqlDataType(DataType.Time);
					else if (column.DbType.Equals("date", StringComparison.CurrentCultureIgnoreCase))
						columnType = new SqlDataType(DataType.Date);
				}

				if (skipConvert(column) || value == null || !ValueConverter.TryConvert(StringBuilder, columnType, value))
				{
					var name = ParameterName == "?" ? ParameterName : ParameterName + ++ParameterIndex;

					if (value is DataParameter)
					{
						value = ((DataParameter)value).Value;
					}

					var dataParameter = new DataParameter(ParameterName == "?" ? ParameterName : "p" + ParameterIndex, value, column.DataType);

					Parameters.Add(dataParameter);

					// wrap the parameter with a cast
					var dbType = value == null ? columnType : DataConnection.MappingSchema.GetDataType(value.GetType());
					var casttype = ((DB2iSeriesSqlBuilder) SqlBuilder).GetTypeForCast(dbType.Type);
					var nameWithCast = $"CAST(@{dataParameter.Name} AS {casttype})";  

					StringBuilder.Append(nameWithCast);
				}

				StringBuilder.Append(",");
			}

			StringBuilder.Length--;
		}
	}
}
