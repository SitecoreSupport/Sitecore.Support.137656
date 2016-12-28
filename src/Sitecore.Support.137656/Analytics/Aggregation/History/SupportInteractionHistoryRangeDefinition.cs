using System;
using Sitecore.Analytics.DataAccess;

namespace Sitecore.Support.Analytics.Aggregation.History
{
  internal class SupportInteractionHistoryRangeDefinition : ObjectRangeDefinition
  {
    public SupportInteractionHistoryRangeDefinition(DateTime cutoff)
    {
      CutoffDate = cutoff;
    }

    public DateTime CutoffDate { get; private set; }
  }
}