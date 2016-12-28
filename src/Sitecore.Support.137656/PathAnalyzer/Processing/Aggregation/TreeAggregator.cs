using System;
using System.Collections.Generic;
using System.Linq;
using Sitecore.Analytics.Aggregation;
using Sitecore.Analytics.Aggregation.Data;
using Sitecore.Analytics.Aggregation.Data.Model;
using Sitecore.Analytics.Core;
using Sitecore.Analytics.Model;
using Sitecore.Diagnostics;
using Sitecore.PathAnalyzer.Data.Models;
using Sitecore.PathAnalyzer.Processing;
using Sitecore.StringExtensions;
using Sitecore.Support.PathAnalyzer.Processing.Aggregation.Filters;

namespace Sitecore.Support.PathAnalyzer.Processing.Aggregation
{
  internal class TreeAggregator : IRecordAggregator<ItemBatch<InteractionKey>>
  {
    private List<AggregationContextCollector> _collectors;
    private Guid[] _newTreeDefinitionIds;
    private List<TreeDefinition> _treeDefinitions;

    protected List<AggregationContextCollector> Collectors
    {
      get
      {
        if (_collectors == null)
          Reset();
        return _collectors;
      }
    }

    public TreeAggregationContext Context { get; set; }

    public IDateTimePrecisionStrategy DateTimeStrategy { get; set; }

    protected IVisitAggregationContextFilter Filter
    {
      get
      {
        if (Mode.Equals("live", StringComparison.InvariantCultureIgnoreCase))
          return new LiveVisitAggregationContextFilter();
        if (Mode.Equals("historic", StringComparison.InvariantCultureIgnoreCase))
          return new HistoricVisitAggregationContextFilter();
        return new NewMapVisitAggregationContextFilter();
      }
    }

    public string Mode { get; set; }

    public Guid[] NewTreeDefinitionIds
    {
      get
      {
        return
          _newTreeDefinitionIds;
      }
      set
      {
        var flag = (_newTreeDefinitionIds != null) && !_newTreeDefinitionIds.SequenceEqual(value);
        _newTreeDefinitionIds = value;
        if (flag)
          Reset();
      }
    }

    protected List<TreeDefinition> TreeDefinitions
    {
      get
      {
        if (_treeDefinitions == null)
        {
          var storage = Context.TreeDefinitionStores.FirstOrDefault();
          Assert.Required(storage, "primaryDefinitionStore");
          _treeDefinitions = (NewTreeDefinitionIds == null) || !NewTreeDefinitionIds.Any()
            ? storage.GetDefinitions().ToList()
            : storage.GetDefinitions(NewTreeDefinitionIds).ToList();
        }
        return _treeDefinitions;
      }
      set { _treeDefinitions = value; }
    }

    public void Aggregate(ItemBatch<InteractionKey> batch)
    {
      Func<InteractionKey, Guid> selector = null;
      Func<IVisitAggregationContext, Guid> func2 = null;
      Assert.ArgumentNotNull(batch, "batch");
      Assert.Required(Context, "The BatchAggregator.Context property has to be set.");
      Assert.Required(Context.Source, "The BatchAggregator.Context.Source property has to be set.");
      var dictionary = new Dictionary<IAggregationContextCollector, List<IVisitAggregationContext>>();
      foreach (var collector in Collectors)
      {
        if (selector == null)
          selector = i => GetTrailKey(i.InteractionId.Guid);
        var trail = collector.Storage.GetTrail(batch.Items.Select(selector));
        var source = new List<IVisitAggregationContext>();
        foreach (var key in from i in batch.Items
          where !trail.Contains(GetTrailKey(i.InteractionId.Guid))
          select i)
          try
          {
            var item = Context.Source.CreateContextForInteraction(key);
            if (item != null)
            {
              if ((item.Visit == null) || (item.Contact == null))
                Logger.Instance.SingleWarn(
                  "Some aggregation contexts skipped processing due to either missing Contact or VisitData.", "", "");
              else
                source.Add(item);
            }
            else
            {
              Logger.Instance.Debug(
                "Interaction {0} could not be collected (not found) and is skipped".FormatWith(key), "", "");
              batch.PostponeItem(key);
            }
          }
          catch (Exception exception)
          {
            Logger.Instance.Error("Interaction {0} could not be collected due to error".FormatWith(key), exception);
            batch.PostponeItem(key);
          }
        if (source.Any())
          dictionary.Add(collector, source);
      }
      foreach (var collector2 in dictionary.Keys)
      {
        var list2 = dictionary[collector2];
        foreach (var context2 in list2)
          collector2.Collect(context2);
        if (func2 == null)
          func2 = c => GetTrailKey(c.Visit.InteractionId);
        collector2.Storage.SaveTrail(list2.Select(func2).ToArray());
      }
    }

    private Guid GetTrailKey(Guid interactionId) =>
      Hash128.Compute(interactionId, Mode).ToGuid();

    private void Reset()
    {
      _treeDefinitions = null;
      _collectors = new List<AggregationContextCollector>();
      foreach (var storage in from s in Context.TreeStores
        where s.Enabled
        select s)
        _collectors.Add(new AggregationContextCollector(TreeDefinitions, storage, Filter));
    }

    public void Store()
    {
      foreach (var collector in Collectors)
        collector.Flush();
    }
  }
}