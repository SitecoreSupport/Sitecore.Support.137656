using System;
using System.Linq;
using Sitecore.Analytics.Core;
using Sitecore.Analytics.Model;
using Sitecore.Analytics.Processing;
using Sitecore.Configuration;
using Sitecore.Diagnostics;
using Sitecore.PathAnalyzer;
using Sitecore.PathAnalyzer.Data.Maps;
using Sitecore.PathAnalyzer.Data.Models;
using Sitecore.StringExtensions;

namespace Sitecore.Support.PathAnalyzer.Processing.Aggregation
{
  [Serializable]
  internal class MapWorker : DistributedWorker<InteractionKey>
  {
    private int _batchCounter;
    private int _itemCounter;

    protected TreeAggregator Aggregator
    {
      get
      {
        var aggregator = Factory.CreateObject("pathAnalyzer/newMapAggregator", true) as TreeAggregator;
        Assert.IsNotNull(aggregator, "_aggregator");
        aggregator.NewTreeDefinitionIds = TreeDefinitionIds;
        aggregator.Mode = Mode;
        return aggregator;
      }
    }

    public string Mode { get; set; }

    public Guid[] TreeDefinitionIds { get; set; }

    public override ProcessingResult ProcessItems(ItemBatch<InteractionKey> batch)
    {
      Assert.ArgumentNotNull(batch, "batch");
      if (_batchCounter == 0)
        UpdateMapDefinitionsStatus();
      Aggregator.Aggregate(batch);
      Aggregator.Store();
      _batchCounter++;
      _itemCounter += batch.Items.Count;
      Logger.Instance.Debug("Processed batch {0}. {1} items so far".FormatWith(_batchCounter, _itemCounter), "", "");
      return ProcessingResult.Processed;
    }

    public virtual void UpdateMapDefinitionsStatus()
    {
      var definitionService = ApplicationContainer.GetDefinitionService();
      var definitions = definitionService.GetDefinitions(TreeDefinitionIds);
      definitionService.UpdateDefinitions(definitions.Select(delegate(TreeDefinition def)
      {
        def.Status = MapRebuildStatus.Building;
        return def;
      }));
    }
  }
}