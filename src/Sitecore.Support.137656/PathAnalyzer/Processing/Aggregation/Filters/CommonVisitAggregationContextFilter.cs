using Sitecore.Analytics.Aggregation.Data.Model;
using Sitecore.Diagnostics;
using Sitecore.PathAnalyzer.Data.Models;

namespace Sitecore.Support.PathAnalyzer.Processing.Aggregation.Filters
{
  internal abstract class CommonVisitAggregationContextFilter : IVisitAggregationContextFilter
  {
    public bool Applicable(IVisitAggregationContext context, TreeDefinition definition)
    {
      Assert.Required(definition, "definition");
      Assert.Required(context, "context");
      return !definition.DeployDate.HasValue || DeployDateIsValid(definition, context);
    }

    protected abstract bool DeployDateIsValid(TreeDefinition definition, IVisitAggregationContext context);
  }
}