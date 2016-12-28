using System;
using Sitecore.Analytics.Core.RangeScheduler;
using Sitecore.Analytics.Data.DataAccess.MongoDb;
using Sitecore.Analytics.DataAccess.RangeScheduler;
using Sitecore.Diagnostics;

namespace Sitecore.Support.Analytics.Aggregation.History
{
  internal abstract class MongoDbInteractionSequence : Sequence<InteractionHistoryKey>
  {
    private readonly MongoDbInteractionHistoryKeyOrderProvider keyOrderProvider;

    protected MongoDbInteractionSequence(MongoDbCollection interactions, DateTime cutoff, int chunks)
    {
      Assert.ArgumentNotNull(interactions, "interactions");
      Assert.ArgumentCondition(cutoff.Kind == DateTimeKind.Utc, "cutoff",
        "The cut-off date has to be specified as UTC time.");
      Interactions = interactions;
      CutOff = cutoff;
      Chunks = chunks;
      keyOrderProvider = new MongoDbInteractionHistoryKeyOrderProvider(GetRightBoundary().SaveDateTime);
    }

    protected int Chunks { get; }

    protected DateTime CutOff { get; }

    protected MongoDbCollection Interactions { get; }

    public override int CompareKeys(InteractionHistoryKey left, InteractionHistoryKey right)
    {
      Assert.ArgumentNotNull(left, "left");
      Assert.ArgumentNotNull(right, "right");
      return keyOrderProvider.GetArithmetic()
        .CompareKeys(keyOrderProvider.GetOrder(left), keyOrderProvider.GetOrder(right));
    }

    public override KeyOrderProvider<InteractionHistoryKey> GetOrderProvider() =>
      keyOrderProvider;

    public override long GetRecordCount() =>
      Interactions.Count();

    public override InteractionHistoryKey KeySplit(InteractionHistoryKey left, InteractionHistoryKey right)
    {
      Assert.ArgumentNotNull(left, "left");
      Assert.ArgumentNotNull(right, "right");
      var arithmetic = keyOrderProvider.GetArithmetic();
      return
        keyOrderProvider.GetKey(arithmetic.GetMedian(keyOrderProvider.GetOrder(left), keyOrderProvider.GetOrder(right)));
    }

    public override bool RangeIsEmpty(InteractionHistoryKey left, InteractionHistoryKey right)
    {
      Assert.ArgumentNotNull(left, "left");
      Assert.ArgumentNotNull(right, "right");
      return keyOrderProvider.GetArithmetic()
        .IsIntervalEmpty(keyOrderProvider.GetOrder(left), keyOrderProvider.GetOrder(right));
    }
  }
}