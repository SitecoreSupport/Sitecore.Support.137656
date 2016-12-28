using Sitecore.Analytics.Aggregation.Data.Model;
using Sitecore.PathAnalyzer.Data.Models;

namespace Sitecore.Support.PathAnalyzer.Processing.Aggregation.Filters
{
  internal class NewMapVisitAggregationContextFilter : LiveVisitAggregationContextFilter
  {
    protected override bool DeployDateIsValid(TreeDefinition definition, IVisitAggregationContext context)
    {
      var saveDateTime = context.Visit.SaveDateTime;
      var deployDate = definition.DeployDate;
      if (!deployDate.HasValue)
        return false;
      return saveDateTime <= deployDate.GetValueOrDefault();
    }
  }
}