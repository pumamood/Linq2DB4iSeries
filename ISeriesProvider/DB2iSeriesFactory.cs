using System.Collections.Generic;
using LinqToDB.Configuration;
using System.Linq;

namespace LinqToDB.DataProvider.DB2iSeries
{

    public class DB2iSeriesFactory : IDataProviderFactory
    {
        public IDataProvider GetDataProvider(IEnumerable<NamedValue> attributes)
        {
            if (attributes == null)
            {
                return new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2, DB2iSeriesLevels.Any, false);
            }

            var attribs = attributes.ToDictionary(x => x.Name, x => x.Value);

            var mapGuidAsString = false;
            var preventNVarChar = false;

            //var attrib = attribs.FirstOrDefault(_ => _.Name == DB2iSeriesTools.MapGuidAsString);

            if (attribs.ContainsKey(DB2iSeriesTools.MapGuidAsString))
            {
                bool.TryParse(attribs[DB2iSeriesTools.MapGuidAsString], out mapGuidAsString);
            }
            if (attribs.ContainsKey(DB2iSeriesTools.PreventNVarChar))
            {
                bool.TryParse(attribs[DB2iSeriesTools.PreventNVarChar], out preventNVarChar);
            }

            //var version = attribs.FirstOrDefault(_ => _.Name == "MinVer");
            var level = DB2iSeriesLevels.Any;
            if (attribs.ContainsKey(DB2iSeriesTools.MinVer))
            {
                if (attribs[DB2iSeriesTools.MinVer] == "7.1.38") level = DB2iSeriesLevels.V7_1_38;
            }

            if (mapGuidAsString)
            {
                return new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2_GAS, level, true, preventNVarChar);
            }

            return new DB2iSeriesDataProvider(DB2iSeriesProviderName.DB2, level, false, preventNVarChar);
        }
    }
}