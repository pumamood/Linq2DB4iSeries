﻿namespace LinqToDB.DataProvider.DB2iSeries
{
	using SqlQuery;
    using SqlProvider;

    public class DB2iSeriesSqlBuilder7_2 : DB2iSeriesSqlBuilder
    {
		public DB2iSeriesSqlBuilder7_2(ISqlOptimizer sqlOptimizer, SqlProviderFlags sqlProviderFlags,
            ValueToSqlConverter valueToSqlConverter, DB2iSeriesNamingConvention namingConvention = default)
            : base(sqlOptimizer, sqlProviderFlags, valueToSqlConverter, namingConvention)
	    {
	    }

		protected override string OffsetFormat(SelectQuery selectQuery) => "OFFSET {0} ROWS";

		protected override bool OffsetFirst => true;

		protected override string LimitFormat(SelectQuery selectQuery) => "FETCH FIRST {0} ROWS ONLY";

		protected override void BuildSql() => DefaultBuildSqlMethod();

        protected override void BuildTruncateTableStatement(SqlTruncateTableStatement truncateTable)
        {
            var table = truncateTable.Table;

            AppendIndent();
            StringBuilder.Append("TRUNCATE TABLE ");
            BuildPhysicalTable(table, null);

            if (truncateTable.ResetIdentity)
                StringBuilder.Append(" RESTART IDENTITY");
        }
    }
}