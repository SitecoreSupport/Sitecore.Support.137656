using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Sitecore.Analytics.Aggregation.Data.Model;
using Sitecore.Diagnostics;
using Sitecore.PathAnalyzer.Construction;
using Sitecore.PathAnalyzer.Contracts;
using Sitecore.PathAnalyzer.Data.Models;
using Sitecore.PathAnalyzer.Processing;
using Sitecore.SequenceAnalyzer;
using Sitecore.SequenceAnalyzer.Data;
using Sitecore.Support.PathAnalyzer.Processing.Aggregation.Filters;
using Sitecore.Support.PathAnalyzer.Processing.Aggregation.Sitecore.PathAnalyzer.Processing;

namespace Sitecore.Support.PathAnalyzer.Processing.Aggregation
{
  internal class AggregationContextCollector : IAggregationContextCollector
  {
    private readonly ConcurrentDictionary<TreeKey, ITreeBuilder> _cache;
    private readonly List<TreeDefinition> _definitions;
    private readonly IVisitAggregationContextFilter _filter;
    private readonly ITreeDbWriter _treeDbWriter;

    public AggregationContextCollector(List<TreeDefinition> definitions, ITreeStorage storage,
      IVisitAggregationContextFilter filter)
      : this(definitions, storage, filter, new TreeDbWriter(definitions, storage))
    {
    }

    public AggregationContextCollector(List<TreeDefinition> definitions, ITreeStorage storage,
      IVisitAggregationContextFilter filter, ITreeDbWriter treeDbWriter)
    {
      _cache = new ConcurrentDictionary<TreeKey, ITreeBuilder>();
      Assert.IsNotNull(definitions, "definitions");
      Assert.IsNotNull(storage, "storage");
      Assert.IsTrue(definitions.Any(), "definitions cannot be empty");
      _filter = filter;
      _definitions = definitions;
      _treeDbWriter = treeDbWriter;
      Storage = storage;
    }

    public void Collect(IVisitAggregationContext context)
    {
      Assert.IsNotNull(context, "context");
      Initialize(context);
      foreach (var builder in GetTreeBuilders(context))
        builder.Build(context);
    }

    public void Flush()
    {
      Assert.IsNotNull(_treeDbWriter, "_treeDbWriter != null");
      if (_cache.Any())
      {
        _treeDbWriter.WriteTrees(_cache.ToDictionary(pair => pair.Key, pair => pair.Value.Tree));
        _cache.Clear();
      }
    }

    public ITreeStorage Storage { get; }

    private IEnumerable<ITreeBuilder> GetTreeBuilders(IVisitAggregationContext context)
    {
      Assert.Required(context, "context cannot be null");
      var endDate = context.Visit.StartDateTime.Date.AddDays(1.0);
      return from pair in _cache
        where pair.Key.EndDate == endDate
        select pair.Value;
    }

    private void Initialize(IVisitAggregationContext context)
    {
      var time = context.Visit.StartDateTime.Date.AddDays(1.0);
      foreach (var definition in from d in _definitions
        where _filter.Applicable(context, d)
        select d)
      {
        var key = new TreeKey
        {
          DefinitionId = definition.Id,
          StartDate = time.AddDays(-1.0),
          EndDate = time
        };
        _cache.AddOrUpdate(key, TreeBuilder.Create(definition), (k, treeBuilder) => treeBuilder);
      }
    }
  }

  namespace Sitecore.PathAnalyzer.Processing
  {
    internal interface ITreeDbWriter
    {
      void WriteTrees(Dictionary<TreeKey, ISerializableTree> trees);
    }
  }
}