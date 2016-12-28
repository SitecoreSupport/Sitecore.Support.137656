using System.Collections.Generic;
using Sitecore.Analytics.Aggregation.Data;
using Sitecore.Analytics.Aggregation.Data.DataAccess;
using Sitecore.Analytics.Processing.ProcessingPool;
using Sitecore.Diagnostics;
using Sitecore.PathAnalyzer.Data.Storage;
using Sitecore.SequenceAnalyzer.Data;

namespace Sitecore.Support.PathAnalyzer.Processing
{
  internal class TreeAggregationContext : AggregationContext
  {
    private List<ITreeDefinitionStorage> _treeDefinitionStores;
    private List<ITreeStorage> _treeStores;
    private ProcessingPool pool;
    private ICollectionDataProvider2 source;

    public ProcessingPool Pool
    {
      get
      {
        return
          Assert.ResultNotNull(pool);
      }
      set
      {
        Assert.ArgumentNotNull(value, "value");
        pool = value;
      }
    }

    public ICollectionDataProvider2 Source
    {
      get
      {
        return
          Assert.ResultNotNull(source);
      }
      set
      {
        Assert.ArgumentNotNull(value, "value");
        source = value;
      }
    }

    public List<ITreeDefinitionStorage> TreeDefinitionStores =>
      _treeDefinitionStores ?? (_treeDefinitionStores = new List<ITreeDefinitionStorage>());

    public List<ITreeStorage> TreeStores =>
      _treeStores ?? (_treeStores = new List<ITreeStorage>());

    public void AddDefinitionStore(ITreeDefinitionStorage store)
    {
      Assert.ArgumentNotNull(store, "store");
      TreeDefinitionStores.Add(store);
    }

    public void AddTreeStore(ITreeStorage store)
    {
      Assert.ArgumentNotNull(store, "store");
      TreeStores.Add(store);
    }
  }
}