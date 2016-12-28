using System;
using System.Globalization;
using Sitecore.Analytics.Aggregation.Data;
using Sitecore.Analytics.Core;
using Sitecore.Analytics.Model;
using Sitecore.Analytics.Processing.ProcessingPool;
using Sitecore.Diagnostics;
using Sitecore.Support.PathAnalyzer.Processing.Aggregation;
using Sitecore.Xdb.Configuration;

namespace Sitecore.Support.PathAnalyzer.Processing.Agents
{
  internal class TreeAggregatorAgent : IAgent
  {
    private const int DEFAULT_BATCH_SIZE = 0x40;
    private int maximumBatchSize = 0x40;

    public TreeAggregator Aggregator { get; set; }

    public TreeAggregationContext Context { get; set; }

    public IDateTimePrecisionStrategy DateTimeStrategy { get; set; }

    public int MaximumBatchSize
    {
      get
      {
        return
          maximumBatchSize;
      }
      set
      {
        Assert.ArgumentCondition(value >= 0, "value", "The specified batch size is not valid.");
        maximumBatchSize = value != 0 ? value : 0x40;
      }
    }

    public void Execute()
    {
      Assert.Required(Context, "The InteractionBatchAggregationAgent.Context property has to be set.");
      Assert.Required(Context.Pool, "The InteractionBatchAggregationAgent.Context.Pool property has to be set.");
      Assert.Required(Context.Source, "The InteractionBatchAggregationAgent.Context.Source property has to be set.");
      Assert.Required(Aggregator, "The InteractionBatchAggregationAgent.Aggregator property has to be set.");
      if (!ShouldExecute())
        return;
      Aggregator.Context = Context;
      Aggregator.DateTimeStrategy = DateTimeStrategy;
      var processingPoolScheduler = new ProcessingPoolScheduler<InteractionKey>(Context.Pool,
        new ProcessingPoolItemMapper(), MaximumBatchSize);
      var num = 0;
      bool next;
      do
      {
        ItemBatch<InteractionKey> workItem;
        next = processingPoolScheduler.TryGetNext(out workItem);
        if (next)
        {
          try
          {
            Logger.Instance.Debug("Aggregating batch", "", "");
            Aggregator.Aggregate(workItem);
            processingPoolScheduler.MarkProcessed(workItem);
          }
          catch (Exception ex)
          {
            Logger.Instance.Error("Error during aggregation.", ex);
          }
          num += workItem.Items.Count - workItem.PostponedItems.Count;
        }
      } while (next);
      if (num <= 0)
        return;
      Logger.Instance.Debug("Flushing aggregator", "", "");
      Aggregator.Store();
    }

    private bool ShouldExecute()
    {
      if (XdbSettings.Enabled)
        return true;
      var message = string.Format(CultureInfo.InvariantCulture, "Agent '{0}' not executed as Analytics is disabled.",
        GetType().Name);
      Logger.Instance.Info(message, "", "");
      return false;
    }
  }
}