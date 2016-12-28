using Sitecore.Analytics.Aggregation.Data.Model;
using Sitecore.PathAnalyzer.Data.Models;

namespace Sitecore.Support.PathAnalyzer.Processing.Aggregation.Filters
{
  internal interface IVisitAggregationContextFilter
  {
    bool Applicable(IVisitAggregationContext context, TreeDefinition definition);
  }
}