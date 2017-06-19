using System;
using Sitecore.Analytics.DataAccess;

namespace Sitecore.Support.Analytics.Aggregation.History
{
    public class SupportInteractionHistoryRangeDefinition : ObjectRangeDefinition
    {
        public SupportInteractionHistoryRangeDefinition(DateTime cutoff)
        {
            CutoffDate = cutoff;
        }

        public DateTime CutoffDate { get; private set; }
    }
}