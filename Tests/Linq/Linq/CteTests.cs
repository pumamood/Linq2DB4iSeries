﻿using System;
using System.Linq;
using System.Linq.Expressions;

using LinqToDB;
using LinqToDB.DataProvider.DB2iSeries;
using LinqToDB.Expressions;

using NUnit.Framework;

namespace Tests.Linq
{
    using Model;

    public class CteTests : TestBase
    {
        public static string[] CteSupportedProviders = new[]
        {
            ProviderName.SqlServer2008, ProviderName.SqlServer2012, ProviderName.SqlServer2014,
            ProviderName.Firebird,
            ProviderName.PostgreSQL,
            ProviderName.DB2,
            ProviderName.SQLite, ProviderName.SQLiteClassic, ProviderName.SQLiteMS,
            ProviderName.Oracle, ProviderName.OracleManaged, ProviderName.OracleNative,
            DB2iSeriesProviderName.DB2_73, DB2iSeriesProviderName.DB2, DB2iSeriesProviderName.DB2_GAS, DB2iSeriesProviderName.DB2_73_GAS
			//ProviderName.Informix,
			// Will be supported in SQL 8.0 - ProviderName.MySql
		};

        class CteContextSourceAttribute : IncludeDataSourcesAttribute
        {
            public CteContextSourceAttribute() : this(true)
            {
            }

            public CteContextSourceAttribute(bool includeLinqService)
                : base(includeLinqService, CteSupportedProviders)
            {
            }

            public CteContextSourceAttribute(params string[] excludedProviders)
                : base(CteSupportedProviders.Except(excludedProviders).ToArray())
            {
            }

            public CteContextSourceAttribute(bool includeLinqService, params string[] excludedProviders)
                : base(includeLinqService, CteSupportedProviders.Except(excludedProviders).ToArray())
            {
            }
        }

        [Test, Combinatorial]
        public void Test1([CteContextSource] string context)
        {
            using (var db = GetDataContext(context))
            {
                var cte1 = db.GetTable<Child>().Where(c => c.ParentID > 1).AsCte();
                var query = from p in db.Parent
                            join c in cte1 on p.ParentID equals c.ParentID
                            join c2 in cte1 on p.ParentID equals c2.ParentID
                            select p;

                var cte1_ = db.GetTable<Child>().Where(c => c.ParentID > 1);

                var expected =
                    from p in db.Parent
                    join c in cte1_ on p.ParentID equals c.ParentID
                    join c2 in cte1_ on p.ParentID equals c2.ParentID
                    select p;

                AreEqual(expected, query);
            }
        }

        [Test, Combinatorial]
        public void Test2([CteContextSource] string context)
        {
            using (var db = GetDataContext(context))
            {
                var cte1 = db.GetTable<Child>().Where(c => c.ParentID > 1).AsCte("CTE1_");
                var cte2 = db.Parent.Where(p => cte1.Any(c => c.ParentID == p.ParentID)).AsCte("CTE2_");
                var cte3 = db.Parent.Where(p => cte2.Any(c => c.ParentID == p.ParentID)).AsCte("CTE3_");
                var result = from p in cte2
                             join c in cte1 on p.ParentID equals c.ParentID
                             join c2 in cte2 on p.ParentID equals c2.ParentID
                             join c3 in cte3 on p.ParentID equals c3.ParentID
                             from c4 in db.Child.Where(c4 => c4.ParentID % 2 == 0).AsCte("LAST").InnerJoin(c4 => c4.ParentID == c3.ParentID)
                             select c3;

                var ncte1 = db.GetTable<Child>().Where(c => c.ParentID > 1);
                var ncte2 = db.Parent.Where(p => ncte1.Any(c => c.ParentID == p.ParentID));
                var ncte3 = db.Parent.Where(p => ncte2.Any(c => c.ParentID == p.ParentID));
                var expected = from p in ncte2
                               join c in ncte1 on p.ParentID equals c.ParentID
                               join c2 in ncte2 on p.ParentID equals c2.ParentID
                               join c3 in ncte3 on p.ParentID equals c3.ParentID
                               from c4 in db.Child.Where(c4 => c4.ParentID % 2 == 0).InnerJoin(c4 => c4.ParentID == c3.ParentID)
                               select c3;

                var expectedStr = expected.ToString();
                var resultdStr = result.ToString();

                // Looks like we do not populate needed field for CTE. It is aproblem thta needs to be solved
                AreEqual(expected, result);
            }
        }

        [Test, Combinatorial]
        public void TestAsTable([CteContextSource] string context)
        {
            using (var db = GetDataContext(context))
            {
                var cte1 = db.GetTable<Child>().AsCte("CTE1_");
                var expected = db.GetTable<Child>();

                AreEqual(expected, cte1);
            }
        }

        static IQueryable<TSource> RemoveCte<TSource>(IQueryable<TSource> source)
        {
            var newExpr = source.Expression.Transform(e =>
            {
                if (e is MethodCallExpression methodCall && methodCall.Method.Name == "AsCte")
                    return methodCall.Arguments[0];
                return e;
            });

            return source.Provider.CreateQuery<TSource>(newExpr);
        }
     
        [Test, Combinatorial]
        public void Test4([CteContextSource] string context)
        {
            using (var db = GetDataContext(context))
            {
                var cte1 = db.GetTable<Child>().Where(c => c.ParentID > 1).AsCte("CTE1_");
                var result = from p in cte1
                             from c4 in db.Child.Where(c4 => c4.ParentID % 2 == 0).AsCte("LAST").InnerJoin(c4 => c4.ParentID == p.ParentID)
                             select c4;

                var _cte1 = db.GetTable<Child>().Where(c => c.ParentID > 1);
                var expected = from p in _cte1
                               from c4 in db.Child.Where(c4 => c4.ParentID % 2 == 0).InnerJoin(c4 => c4.ParentID == p.ParentID)
                               select c4;

                var expectedStr = expected.ToString();
                var resultdStr = result.ToString();

                AreEqual(expected, result);
            }
        }

        private class CteDMLTests
        {
            protected bool Equals(CteDMLTests other)
            {
                return ChildID == other.ChildID && ParentID == other.ParentID;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != this.GetType()) return false;
                return Equals((CteDMLTests)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (ChildID * 397) ^ ParentID;
                }
            }

            public int ChildID { get; set; }
            public int ParentID { get; set; }
        }

        [Test, Combinatorial]
        public void CteTestInsert([CteContextSource(true, ProviderName.DB2)] string context)
        {
            using (var db = GetDataContext(context))
            using (var testTable = db.CreateLocalTable<CteDMLTests>("CteChild"))
            {
                var cte1 = db.GetTable<Child>().Where(c => c.ParentID > 1).AsCte("CTE1_");
                var toInsert = from p in cte1
                               from c4 in db.Child.Where(c4 => c4.ParentID % 2 == 0).AsCte("LAST").InnerJoin(c4 => c4.ParentID == p.ParentID)
                               select new CteDMLTests
                               {
                                   ChildID = c4.ChildID,
                                   ParentID = c4.ParentID
                               };

                var affected = toInsert.Insert(testTable, c => c);

                var _cte1 = db.GetTable<Child>().Where(c => c.ParentID > 1);
                var expected = from p in _cte1
                               from c4 in db.Child.Where(c4 => c4.ParentID % 2 == 0).InnerJoin(c4 => c4.ParentID == p.ParentID)
                               select new CteDMLTests
                               {
                                   ChildID = c4.ChildID,
                                   ParentID = c4.ParentID
                               };

                var result = testTable.OrderBy(c => c.ChildID).ThenBy(c => c.ParentID);
                expected = expected.OrderBy(c => c.ChildID).ThenBy(c => c.ParentID);

                AreEqual(expected, result);
            }
        }

        [Test, Combinatorial]
        // Delete with Cte not supported on iSeries
        public void CteTestDelete(
            //[CteContextSource(true, ProviderName.Oracle, ProviderName.OracleManaged, ProviderName.OracleNative, DB2iSeriesProviderName.DB2_73, DB2iSeriesProviderName.DB2, DB2iSeriesProviderName.DB2_GAS, DB2iSeriesProviderName.DB2_73_GAS)]
            [CteContextSource(false)]
            string context)
        {
            using (var db = GetDataContext(context))
            using (var tmp = db.CreateLocalTable("CteChild",
                Enumerable.Range(0, 10).Select(i => new CteDMLTests { ParentID = i, ChildID = 1000 + i })))
            {
                var cte = tmp.Where(c => c.ParentID % 2 == 0).AsCte();

                var toDelete =
                    from c in tmp
                    from ct in cte.InnerJoin(ct => ct.ParentID == c.ParentID)
                    select c;

                Assert.Throws<NotSupportedException>(() => toDelete.Delete());
            }
        }

        [Test, Combinatorial]
        // Delete with Cte not supported on iSeries
        public void CteTestUpdate(
            [CteContextSource(false)]
            string context)
        {
            using (var db = GetDataContext(context))
            using (var testTable = db.CreateLocalTable("CteChild",
                Enumerable.Range(0, 10).Select(i => new CteDMLTests { ParentID = i, ChildID = 1000 + i })))
            {
                var cte = testTable.Where(c => c.ParentID % 2 == 0).AsCte();
                var toUpdate =
                    from c in testTable
                    from ct in cte.InnerJoin(ct => ct.ParentID == c.ParentID)
                    select c;

                Assert.Throws<NotSupportedException>(() => toUpdate.Update(prev => new CteDMLTests { ParentID = prev.ChildID }));
            }
        }

        class RecursiveCTE
        {
            public int? ParentID;
            public int? ChildID;
            public int? GrandChildID;
        }

        [Test, Combinatorial]
        public void CteRecursiveTest(
            [CteContextSource(true, ProviderName.DB2)]
            string context)
        {
            using (var db = GetDataContext(context))
            {
                var cteRecursive = db.GetCte<RecursiveCTE>(cte =>
                        (
                            from gc1 in db.GrandChild
                            select new RecursiveCTE
                            {
                                ChildID = gc1.ChildID,
                                ParentID = gc1.GrandChildID,
                                GrandChildID = gc1.GrandChildID,
                            }
                        )
                        .Concat
                        (
                            from gc in db.GrandChild
                            from p in db.Parent.InnerJoin(p => p.ParentID == gc.ParentID)
                            from ct in cte.InnerJoin(ct => Sql.AsNotNull(ct.ChildID) == gc.ChildID)
                            where ct.GrandChildID <= 10
                            select new RecursiveCTE
                            {
                                ChildID = ct.ChildID,
                                ParentID = ct.ParentID,
                                GrandChildID = ct.ChildID + 1
                            }
                        )
                    , "MY_CTE");

                var str = cteRecursive.ToString();
                var result = cteRecursive.ToArray();
            }
        }

    }
}
