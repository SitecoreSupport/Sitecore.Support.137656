using Sitecore.Analytics.Aggregation.Data.Model;
using Sitecore.PathAnalyzer.Data.Models;

namespace Sitecore.Support.PathAnalyzer.Processing.Aggregation.Filters
{
  internal class HistoricVisitAggregationContextFilter : CommonVisitAggregationContextFilter
  {
    protected override bool DeployDateIsValid(TreeDefinition definition, IVisitAggregationContext context) => true;
  }
}