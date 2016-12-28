using System;
using MongoDB.Driver;
using Sitecore.Analytics.DataAccess;
using Sitecore.Analytics.DataAccess.RangeScheduler;
using Sitecore.Analytics.Model;
using Sitecore.Analytics.Processing;
using Sitecore.Analytics.Processing.Tasks;
using Sitecore.Diagnostics;
using Sitecore.Support.Analytics.Aggregation.History;
using Sitecore.Support.Analytics.RangeScheduler;

namespace Sitecore.Support.Analytics.Processing.Tasks
{
  public class MongoDbSequenceFactory2 : MongoDbSequenceFactoryBase, ISequenceFactory<Guid, ObjectRangeDefinition>,
    ISequenceFactory
  {
    public MongoDbSequenceFactory2(string connectionStringName) : base(connectionStringName)
    {
      Assert.ArgumentNotNull(connectionStringName, "connectionStringName");
    }

    public Sequence<TKey> GetSequence<TKey>(ObjectRangeDefinition range)
    {
      Assert.ArgumentNotNull(range, "range");
      Sequence<TKey> sequence = null;
      if (range is SupportInteractionHistoryRangeDefinition)
      {
        sequence = CreateInteractionSequence(range as SupportInteractionHistoryRangeDefinition) as Sequence<TKey>;
      }
      else if (range is MongoDbObjectRangeDefinition)
      {
        var range1 = range as MongoDbObjectRangeDefinition;
        Assert.IsNotNull(range1, "Unexpected XDB query type: " + range.GetType().FullName);
        sequence = GetGuidSequence(range1) as Sequence<TKey>;
      }
      if (sequence == null)
        throw new InvalidOperationException("Work item type or range definition is not supported. (WI type: " +
                                            typeof(TKey).FullName + ",  range definition type: " + range.GetType() + ")");
      return sequence;
    }

    Sequence<Guid> ISequenceFactory<Guid, ObjectRangeDefinition>.GetSequence(ObjectRangeDefinition query)
    {
      Assert.ArgumentNotNull(query, "query");
      return GetSequence<Guid>(query);
    }

    private Sequence<InteractionKey> CreateInteractionSequence(SupportInteractionHistoryRangeDefinition range)
    {
      var interactions = Driver.Interactions;

      int numberOfChunks;

      try
      {
        numberOfChunks = interactions.ShardingInformation.GetNumberOfChunks();
      }
      catch (MongoCommandException exception)
      {
        numberOfChunks = 1;
        Log.Warn(exception.Message, this);
      }

      var innerSequence = new MongoDbDescendingInteractionSequence(interactions, range.CutoffDate, numberOfChunks);
      return new SequenceItemTypeAdaptor<InteractionHistoryKey, InteractionKey>(innerSequence,
        new InteractionHistoryKeyMapper());
    }

    private Sequence<Guid> GetGuidSequence(MongoDbObjectRangeDefinition range)
    {
      var collection = Driver[range.CollectionName];
      return range.GetSequence(collection);
    }
  }
}