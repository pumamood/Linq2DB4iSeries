﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LinqToDB.DataProvider.DB2iSeries
{
    using SqlProvider;
    using SqlQuery;
    using System.Data;

    public class DB2iSeriesSqlBuilder : BasicSqlBuilder
    {
        public static DB2iSeriesIdentifierQuoteMode IdentifierQuoteMode = DB2iSeriesIdentifierQuoteMode.None;
        public DB2iSeriesNamingConvention NamingConvention { get; private set; }
        protected readonly bool mapGuidAsString;
        protected readonly bool preventNVarChar;

        public DB2iSeriesSqlBuilder(ISqlOptimizer sqlOptimizer, SqlProviderFlags sqlProviderFlags,
            ValueToSqlConverter valueToSqlConverter, DB2iSeriesNamingConvention namingConvention = default)
            : base(sqlOptimizer, sqlProviderFlags, valueToSqlConverter)
        {
            NamingConvention = namingConvention;
            this.mapGuidAsString = sqlProviderFlags.CustomFlags.Contains(DB2iSeriesTools.MapGuidAsString);
            this.preventNVarChar = sqlProviderFlags.CustomFlags.Contains(DB2iSeriesTools.PreventNVarChar);
        }

        protected override string LimitFormat(SelectQuery selectQuery) => selectQuery.Select.SkipValue == null ? " FETCH FIRST {0} ROWS ONLY" : null;

        protected override void BuildColumnExpression(SelectQuery selectQuery, ISqlExpression expr, string alias, ref bool addAlias) =>
            BuildColumnExpression(selectQuery, expr, alias, ref addAlias, true);

        protected void BuildColumnExpression(SelectQuery selectQuery, ISqlExpression expr, string alias, ref bool addAlias, bool wrapParameter)
        {
            var wrap = false;
            if (expr.SystemType == typeof(bool))
            {
                if (expr is SqlSearchCondition)
                {
                    wrap = true;
                }
                else
                {
                    wrap = expr is SqlExpression ex && ex.Expr == "{0}" && ex.Parameters.Length == 1 && ex.Parameters[0] is SqlSearchCondition;
                }
            }

            if (wrapParameter)
            {
                if (expr is SqlParameter)
                {
                    if (((SqlParameter)expr).Name != null)
                    {
                        var dataType = SqlDataType.GetDataType(expr.SystemType);

                        expr = new SqlFunction(expr.SystemType, dataType.DataType.ToString(), expr);
                    }
                }
                else if (expr is SqlValue && ((SqlValue)expr).Value == null)
                {
                    var colType = GetTypeForCast(expr.SystemType);

                    expr = new SqlExpression(expr.SystemType, "Cast({0} as {1})", Precedence.Primary, expr, new SqlExpression(colType, Precedence.Primary));
                }
            }

            if (wrap)
            {
                StringBuilder.Append("CASE WHEN ");
            }
            base.BuildColumnExpression(selectQuery, expr, alias, ref addAlias);
            if (wrap)
            {
                StringBuilder.Append(" THEN 1 ELSE 0 END");
            }
        }

        public virtual string GetiSeriesType(SqlDataType dataType)
        {
            switch (dataType.DataType)
            {
                case DataType.Variant:
                case DataType.Binary:
                    return $"BINARY({(dataType.Length == 0 ? 1 : dataType.Length)})";
                case DataType.Int64:
                case DataType.UInt32:
                    return "BIGINT";
                case DataType.Blob:
                    return $"BLOB({(dataType.Length == 0 ? 1 : dataType.Length)})";
                case DataType.VarBinary:
                    return $"VARBINARY({(dataType.Length == 0 ? 1 : dataType.Length)})";
                case DataType.Char:
                    return "CHAR";
                case DataType.Date:
                    return "DATE";
                case DataType.UInt64:
                    return "DECIMAL(28,0)";
                case DataType.Decimal:
                    return "DECIMAL";
                case DataType.Double:
                    return "DOUBLE";
                case DataType.UInt16:
                case DataType.Int32:
                    return "INTEGER";
                case DataType.Single:
                    return "REAL";
                case DataType.Int16:
                case DataType.Boolean:
                case DataType.Byte:
                    return "SMALLINT";
                case DataType.Time:
                case DataType.DateTimeOffset:
                    return "TIME";
                case DataType.Timestamp:
                case DataType.DateTime:
                case DataType.DateTime2:
                    return "TIMESTAMP";
                case DataType.VarChar:
                    return $"VARCHAR({(dataType.Length == 0 ? 1 : dataType.Length)})";
                case DataType.NVarChar:
                    return preventNVarChar ? $"VARCHAR({(dataType.Length == 0 ? 1 : dataType.Length)})" :
                        $"NVARCHAR({(dataType.Length == 0 ? 1 : dataType.Length)})";
                case DataType.Guid:
                    return mapGuidAsString ? "CHAR(32)" : "char(16) for bit data";
                default:
                    return dataType.DataType.ToString();
            }
        }


        public string GetTypeForCast(Type dataType)
        {
            string colType = "CHAR";

            if (dataType != null)
            {
                var actualType = SqlDataType.GetDataType(dataType);

                colType = GetiSeriesType(actualType);
            }

            return colType;
        }

        protected override void BuildCommand(SqlStatement selectQuery, int commandNumber) =>
            StringBuilder.AppendLine($"SELECT {DB2iSeriesTools.IdentityColumnSql} FROM {DB2iSeriesTools.iSeriesDummyTableName(NamingConvention)}");

        protected override void BuildCreateTableIdentityAttribute1(SqlField field) => StringBuilder.Append("GENERATED ALWAYS AS IDENTITY");

        protected override void BuildDataType(SqlDataType type, bool createDbType = false)
        {
            switch (type.DataType)
            {
                case DataType.DateTime: StringBuilder.Append("timestamp"); break;
                case DataType.DateTime2: StringBuilder.Append("timestamp"); break;
                default: base.BuildDataType(type, false); break;
            }
        }

        protected override void BuildEmptyInsert(SqlInsertClause insertClause)
        {
            StringBuilder.Append("VALUES");
            foreach (var col in insertClause.Into.Fields)
            {
                StringBuilder.Append("(DEFAULT)");
            }
            StringBuilder.AppendLine();
        }

        protected override void BuildFunction(SqlFunction func)
        {
            func = ConvertFunctionParameters(func);

            base.BuildFunction(func);
        }

        protected override void BuildFromClause(SqlStatement statement, SelectQuery selectQuery)
        {
            if (!statement.IsUpdate())
                base.BuildFromClause(statement, selectQuery);
        }

        protected override void BuildInsertOrUpdateQuery(SqlInsertOrUpdateStatement insertOrUpdate) =>
            BuildInsertOrUpdateQueryAsMerge(insertOrUpdate, $"FROM {DB2iSeriesTools.iSeriesDummyTableName(NamingConvention)} FETCH FIRST 1 ROW ONLY");

        protected override void BuildInsertOrUpdateQueryAsMerge(SqlInsertOrUpdateStatement insertOrUpdate, string fromDummyTable)
        {
            var table = insertOrUpdate.Insert.Into;
            var targetAlias = Convert(insertOrUpdate.SelectQuery.From.Tables[0].Alias, ConvertType.NameToQueryTableAlias).ToString();
            var sourceAlias = Convert(GetTempAliases(1, "s")[0], ConvertType.NameToQueryTableAlias).ToString();
            var keys = insertOrUpdate.Update.Keys;

            AppendIndent().Append("MERGE INTO ");
            BuildPhysicalTable(table, null);
            StringBuilder.Append(' ').AppendLine(targetAlias);

            AppendIndent().Append("USING (SELECT ");

            ExtractMergeParametersIfCannotCombine(insertOrUpdate, keys);

            for (var i = 0; i < keys.Count; i++)
            {
                var key = keys[i];
                var expr = key.Expression;

                if (expr is SqlParameter || expr is SqlValue)
                {
                    var exprType = SqlDataType.GetDataType(expr.SystemType);
                    var asType = GetiSeriesType(exprType);

                    StringBuilder.Append("CAST(");
                    BuildExpression(expr, false, false);
                    StringBuilder.AppendFormat(" AS {0})", asType);
                }
                else
                    BuildExpression(expr, false, false);


                StringBuilder.Append(" AS ");
                BuildExpression(key.Column, false, false);

                if (i + 1 < keys.Count)
                    StringBuilder.Append(", ");
            }

            if (!string.IsNullOrEmpty(fromDummyTable))
                StringBuilder.Append(' ').Append(fromDummyTable);

            StringBuilder.Append(") ").Append(sourceAlias).AppendLine(" ON");

            AppendIndent().AppendLine("(");

            Indent++;

            for (var i = 0; i < keys.Count; i++)
            {
                var key = keys[i];

                AppendIndent();

                StringBuilder.Append(targetAlias).Append('.');
                BuildExpression(key.Column, false, false);

                StringBuilder.Append(" = ").Append(sourceAlias).Append('.');
                BuildExpression(key.Column, false, false);

                if (i + 1 < keys.Count)
                    StringBuilder.Append(" AND");

                StringBuilder.AppendLine();
            }

            Indent--;

            AppendIndent().AppendLine(")");

            if (insertOrUpdate.Update.Items.Any())
            {
                AppendIndent().AppendLine("WHEN MATCHED THEN");

                Indent++;
                AppendIndent().AppendLine("UPDATE ");
                BuildUpdateSet(insertOrUpdate.SelectQuery, insertOrUpdate.Update);
                Indent--;
            }

            AppendIndent().AppendLine("WHEN NOT MATCHED THEN");

            Indent++;
            BuildInsertClause(insertOrUpdate, insertOrUpdate.Insert, "INSERT", false, false);
            Indent--;

            while (EndLine.Contains(StringBuilder[StringBuilder.Length - 1]))
                StringBuilder.Length--;
        }

        protected override void BuildUpdateSet(SelectQuery selectQuery, SqlUpdateClause updateClause)
        {
            AppendIndent()
                .AppendLine("SET");

            Indent++;

            var first = true;

            foreach (var expr in updateClause.Items)
            {
                if (!first)
                    StringBuilder.Append(',').AppendLine();
                first = false;

                AppendIndent();

                BuildExpression(expr.Column, SqlProviderFlags.IsUpdateSetTableAliasSupported, true, false);
                StringBuilder.Append(" = ");

                var addAlias = false;

                BuildColumnExpression(selectQuery, expr.Expression, null, ref addAlias, false);
            }

            Indent--;

            StringBuilder.AppendLine();
        }

        protected override void BuildSelectClause(SelectQuery selectQuery)
        {
            if (selectQuery.HasUnion)
            {
                // need to set any column aliases as the same as the top level one
                var topquery = selectQuery;

                while (topquery.ParentSelect != null && topquery.ParentSelect.HasUnion)
                {
                    topquery = topquery.ParentSelect;
                }
                var alia = selectQuery.Select.Columns.Select(c => c.Alias).ToArray();

                selectQuery.Unions.ForEach((u) =>
                {
                    int colNo = 0;
                    u.SelectQuery.Select.Columns
                    .ForEach(c =>
                    {
                        c.Alias = alia[colNo];
                        colNo++;
                    });
                });
            }

            if (selectQuery.From.Tables.Count == 0)
            {
                AppendIndent().AppendLine("SELECT");
                BuildColumns(selectQuery);
                AppendIndent().AppendLine($"FROM {DB2iSeriesTools.iSeriesDummyTableName(NamingConvention)} FETCH FIRST 1 ROW ONLY");
            }
            else
            {
                base.BuildSelectClause(selectQuery);
            }
        }

        protected void DefaultBuildSqlMethod()
        {
            base.BuildSql();
        }

        protected override void BuildSql()
        {
            AlternativeBuildSql(true, base.BuildSql, "\t0");
        }

        protected override IEnumerable<SqlColumn> GetSelectedColumns(SelectQuery selectQuery)
        {
            if (NeedSkip(selectQuery) && !selectQuery.OrderBy.IsEmpty)
                return AlternativeGetSelectedColumns(selectQuery, () => base.GetSelectedColumns(selectQuery));

            return base.GetSelectedColumns(selectQuery);
        }

        public override int CommandCount(SqlStatement statement)
        {
            return statement is SqlInsertStatement insertStatement && insertStatement.Insert.WithIdentity ? 2 : 1;
        }

        public override object Convert(object value, ConvertType _convertType)
        {
            switch (_convertType)
            {
                case ConvertType.NameToQueryParameter:
                    return "@" + value.ToString();
                case ConvertType.NameToCommandParameter:
                case ConvertType.NameToSprocParameter:
                    return ":" + value;
                case ConvertType.SprocParameterToName:
                    if (value != null)
                    {
                        var str = value.ToString();
                        return ((str.Length > 0 && str[0] == ':') ? str.Substring(1) : str);
                    }
                    break;
                case ConvertType.NameToQueryField:
                case ConvertType.NameToQueryFieldAlias:
                case ConvertType.NameToQueryTable:
                case ConvertType.NameToQueryTableAlias:
                    if (value != null && IdentifierQuoteMode != DB2iSeriesIdentifierQuoteMode.None)
                    {
                        var name = value.ToString();
                        if (name.Length > 0 && name[0] == '"')
                        {
                            return name;
                        }
                        if (IdentifierQuoteMode == DB2iSeriesIdentifierQuoteMode.Quote ||
                            name.StartsWith("_") ||
                            name

#if NETFX_CORE
								.ToCharArray()
#endif
                            .Any((c) => char.IsWhiteSpace(c)))
                        {
                            return '"' + name + '"';
                        }
                    }
                    break;
            }
            return value;
        }

        protected override ISqlBuilder CreateSqlBuilder()
        {
            return new DB2iSeriesSqlBuilder(SqlOptimizer, SqlProviderFlags, ValueToSqlConverter);
        }

        protected override string GetProviderTypeName(IDbDataParameter parameter)
        {
            dynamic p = parameter;
            return p.iDB2DbType.ToString();
        }

        protected override void BuildCreateTableNullAttribute(SqlField field, DefaultNullable defaulNullable)
        {
            if (defaulNullable == DefaultNullable.Null && field.CanBeNull)
                return;

            if (defaulNullable == DefaultNullable.NotNull && !field.CanBeNull)
                return;

            StringBuilder.Append(field.CanBeNull ? " " : "NOT NULL");
        }

        protected override void BuildPredicate(ISqlPredicate predicate)
        {
            var newpredicate = predicate;

            switch (predicate.ElementType)
            {
                case QueryElementType.LikePredicate:
                    var p = (SqlPredicate.Like)predicate;

                    var param2 = GetParm(p.Expr2 as IValueContainer, p.Expr1.SystemType);

                    if (param2 != null)
                    {
                        if (param2 is SqlValue value && value.Value == null)
                        {
                            if (p.IsNot)
                                newpredicate = new SqlPredicate.ExprExpr(p.Expr1, SqlPredicate.Operator.NotEqual, p.Expr2);
                            else
                                newpredicate = new SqlPredicate.ExprExpr(p.Expr1, SqlPredicate.Operator.Equal, p.Expr2);
                        }
                        else
                            newpredicate = new SqlPredicate.Like(p.Expr1, p.IsNot, param2, p.Escape);
                    }

                    break;

                case QueryElementType.ExprExprPredicate:

                    var ep = (SqlPredicate.ExprExpr)predicate;

                    if (ep.Expr1 is SqlFunction function && function.Name == "Date")
                    {
                        if (ep.Expr2 is SqlParameter parameter)
                        {
                            parameter.DataType = DataType.Date;
                        }
                    }

                    break;
            }

            base.BuildPredicate(newpredicate);
        }

        protected override void BuildInsertQuery(SqlStatement statement, SqlInsertClause insertClause, bool addAlias)
        {
            BuildStep = Step.InsertClause; BuildInsertClause(statement, insertClause, addAlias);
            BuildStep = Step.WithClause; BuildWithClause(statement.GetWithClause());

            if (statement.QueryType == QueryType.Insert && statement.SelectQuery.From.Tables.Count != 0)
            {
                BuildStep = Step.SelectClause; BuildSelectClause(statement.SelectQuery);
                BuildStep = Step.FromClause; BuildFromClause(statement, statement.SelectQuery);
                BuildStep = Step.WhereClause; BuildWhereClause(statement.SelectQuery);
                BuildStep = Step.GroupByClause; BuildGroupByClause(statement.SelectQuery);
                BuildStep = Step.HavingClause; BuildHavingClause(statement.SelectQuery);
                BuildStep = Step.OrderByClause; BuildOrderByClause(statement.SelectQuery);
                BuildStep = Step.OffsetLimit; BuildOffsetLimit(statement.SelectQuery);
            }

            if (insertClause.WithIdentity)
                BuildGetIdentity(insertClause);
        }

        protected override void BuildDeleteQuery(SqlDeleteStatement deleteStatement)
        {
            if (deleteStatement.With != null)
                throw new NotSupportedException("iSeries doesn't support Cte in Delete statement");

            base.BuildDeleteQuery(deleteStatement);
        }

        protected override void BuildUpdateQuery(SqlStatement statement, SelectQuery selectQuery, SqlUpdateClause updateClause)
        {
            if (statement.GetWithClause() != null)
                throw new NotSupportedException("iSeries doesn't support Cte in Update statement");


            base.BuildUpdateQuery(statement, selectQuery, updateClause);
        }

        private ISqlExpression GetDateParm(IValueContainer parameter)
        {
            if (parameter != null && parameter is SqlParameter)
            {
                var p = ((SqlParameter)parameter);
                p.DataType = DataType.Date;
                return p;
            }

            return null;

        }

        private ISqlExpression GetParm(IValueContainer parameter, Type type)
        {
            if (type != null && parameter != null)
            {
                if (parameter is SqlValue)
                {
                    if (((SqlValue)parameter).SystemType == null)
                        return new SqlValue(type, parameter.Value);
                }
                else if (parameter is SqlParameter)
                {
                    var p = ((SqlParameter)parameter);
                    p.SystemType = p.SystemType ?? type;
                    return p;
                }
            }
            return null;
        }
    }
}