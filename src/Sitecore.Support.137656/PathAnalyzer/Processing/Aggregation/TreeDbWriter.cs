using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using Sitecore.Diagnostics;
using Sitecore.PathAnalyzer.Construction;
using Sitecore.PathAnalyzer.Data.Models;
using Sitecore.SequenceAnalyzer;
using Sitecore.SequenceAnalyzer.Data;
using Sitecore.Support.PathAnalyzer.Contracts;
using Sitecore.Support.PathAnalyzer.Processing.Aggregation.Sitecore.PathAnalyzer.Processing;

namespace Sitecore.Support.PathAnalyzer.Processing.Aggregation
{
  internal class TreeDbWriter : ITreeDbWriter
  {
    private readonly List<TreeDefinition> _definitions;
    private readonly ILogger _logger;
    private readonly ITreeStorage _storage;
    private Dictionary<TreeKey, ISerializableTree> _trees;

    public TreeDbWriter(List<TreeDefinition> definitions, ITreeStorage storage)
      : this(definitions, storage, Logger.Instance)
    {
    }

    public TreeDbWriter(List<TreeDefinition> definitions, ITreeStorage storage, ILogger logger)
    {
      Assert.Required(definitions, "definitions");
      Assert.Required(definitions, "definitions");
      Assert.Required(logger, "logger");
      _definitions = definitions;
      _storage = storage;
      _logger = logger;
    }

    public void WriteTrees(Dictionary<TreeKey, ISerializableTree> trees)
    {
      LogDebugInfo("WriteTrees starts");
      var dictionary = (from pair in trees
        where pair.Value.Root.SubtreeCount == 0
        select pair).ToDictionary(pair => pair.Key, pair => pair.Value);
      ProcessEmptyTrees(dictionary);
      var dictionary2 = (from pair in trees
        where pair.Value.Root.SubtreeCount != 0
        select pair).ToDictionary(pair => pair.Key, pair => pair.Value);
      ProcessNonEmptyTrees(dictionary2);
      LogDebugInfo("WriteTrees ends");
    }

    private void CreateNewTrees()
    {
      List<TreeKey> list;
      LogDebugInfo("CreateNewTrees is processing {0} source trees", _trees.Count());
      var count = _trees.Count;
      var num2 = 0;
      do
      {
        num2++;
        try
        {
          list = _storage.CreateNewTrees(_trees).ToList();
        }
        catch (SqlException exception)
        {
          if ((exception.Errors[0].Number != 0xa43) || (num2 > count + 1))
          {
            _logger.Error("Can't create new trees.", exception);
            list = new List<TreeKey>();
            break;
          }
          list = count == 1 ? new List<TreeKey>() : null;
        }
      } while (list == null);
      if (num2 > 1)
        LogDebugInfo(
          "CreateNewTrees() retried saving trees more than once, Original tree count = {0}, transactionCount = {1}",
          count, num2);
      if (list.Any())
        RemoveTrees(list);
      LogDebugInfo("CreateNewTrees saved {0} new trees, {1} trees still need to be processed", count - _trees.Count,
        _trees.Count);
    }

    private ISerializableTree CreateTree(byte[] blob) =>
      TreeFactory.ParseTree(blob);

    private void LogDebugInfo(string format, params object[] parameters)
    {
      _logger?.Debug(string.Format("TreeDbWriter::" + format, parameters), "", "");
    }

    private void MergeTrees()
    {
      LogDebugInfo("MergeTrees is processing {0} source trees", _trees.Count());
      var source = _storage.GetTrees(_trees.Keys).ToList();
      Assert.AreEqual(source.Count(), _trees.Keys.Count, "tree count");
      var trees = new List<Tuple<TreeKey, ISerializableTree, long>>();
      foreach (var data in source)
      {
        ISerializableTree tree;
        TreeKey key = data;
        if (_trees.TryGetValue(key, out tree))
        {
          var tree2 = UpdateTree(tree, data.TreeBlob);
          trees.Add(Tuple.Create(key, tree2, data.RowVersion));
        }
        else
        {
          _logger?.Error("Can't find tree by key.",
            new KeyNotFoundException("TreeKey entry is missing in _trees dictionary"));
        }
      }
      var list3 = _storage.ReplaceExistingTrees(trees).ToList();
      if (list3.Any())
        RemoveTrees(list3);
      LogDebugInfo("MergeTrees saved {0} trees, {1} trees still need to be processed", list3.Count, _trees.Count);
    }

    private void OverwriteExistingEmptyTrees()
    {
      if (!_trees.Any()) return;
      LogDebugInfo("OverwriteExistingEmptyTrees is processing {0} source trees", _trees.Count());
      var source = _storage.ReplaceEmptyTrees(_trees).ToList();
      if (source.Any())
        RemoveTrees(source);
      LogDebugInfo("OverwriteExistingEmptyTrees saved {0} trees, {1} trees still need to be processed", source.Count,
        _trees.Count);
    }

    private void ProcessEmptyTrees(Dictionary<TreeKey, ISerializableTree> trees)
    {
      if (!trees.Any()) return;
      LogDebugInfo("ProcessEmptyTrees processing {0} empty trees", trees.Count());
      var num = _storage.CreateNewTrees(trees).Count();
      LogDebugInfo("ProcessEmptyTrees saved {0} empty trees", num);
    }

    private void ProcessNonEmptyTrees(Dictionary<TreeKey, ISerializableTree> trees)
    {
      if (!trees.Any()) return;
      _trees = trees;
      CreateNewTrees();
      OverwriteExistingEmptyTrees();
      while (_trees.Count > 0)
        MergeTrees();
    }

    private void RemoveTrees(IEnumerable<TreeKey> keysToRemove)
    {
      foreach (var key in keysToRemove)
        Assert.IsTrue(_trees.Remove(key), "Tree Does not exist");
    }

    private ISerializableTree UpdateTree(ISerializableTree treeFromInMemoryCache, byte[] blob)
    {
      var tree = CreateTree(blob);
      tree.Merge(treeFromInMemoryCache.Root);
      return tree;
    }
  }
}