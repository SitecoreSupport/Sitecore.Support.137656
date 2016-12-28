using System;
using System.Linq;
using MongoDB.Driver.Builders;
using Sitecore.Analytics.Data.DataAccess.MongoDb;
using Sitecore.Analytics.Model;
using Sitecore.Diagnostics;

namespace Sitecore.Support.Analytics.Aggregation.History
{
  internal class MongoDbDescendingInteractionSequence : MongoDbAscendingInteractionSequence
  {
    internal MongoDbDescendingInteractionSequence(MongoDbCollection interactions, DateTime cutoff, int chunks)
      : base(interactions, cutoff, chunks)
    {
    }

    protected Guid BuildLowerChunkBoundary(InteractionHistoryKey left, InteractionHistoryKey right)
    {
      var destinationArray = new byte[0x10];
      Array.Copy(right.ShardId, destinationArray, Math.Min(destinationArray.Length, right.ShardId.Length));
      var second = new byte[right.ShardId.Length];
      if ((right.ShardId.Zip(second, (l, r) => l != r).Any(x => x) || (right.ContactId != Guid.Empty)) &&
          right.ShardId.Zip(left.ShardId, (l, r) => l != r).Any(x => x))
      {
        destinationArray = BitOperations.RaiseBitsExcept(destinationArray, right.ShardIdBits);
        for (var i = right.ShardId.Length - 1; i >= 0; i--)
          if (destinationArray[i] == 0xff)
          {
            destinationArray[i] = 0;
          }
          else
          {
            destinationArray[i] = (byte) (destinationArray[i] + 1);
            break;
          }
      }
      return new Guid(BitOperations.MaskBitsExcept(destinationArray, right.ShardIdBits));
    }

    protected Guid BuildUpperChunkBoundary(InteractionHistoryKey left, InteractionHistoryKey right) =>
      new Guid(BitOperations.RaiseBitsExcept(BuildLeftChunkBoundary(left).ToByteArray(), left.ShardIdBits));

    public override int CompareKeys(InteractionHistoryKey left, InteractionHistoryKey right)
    {
      Assert.ArgumentNotNull(left, "left");
      Assert.ArgumentNotNull(right, "right");
      return base.CompareKeys(right, left);
    }

    public override InteractionHistoryKey GetLeftBoundary() =>
      base.GetRightBoundary();

    public override bool GetNextKey(InteractionHistoryKey left, InteractionHistoryKey right, bool exclusive,
      out InteractionHistoryKey result)
    {
      Assert.ArgumentNotNull(left, "left");
      Assert.ArgumentNotNull(right, "right");
      if ((lastReturnedResult != null) && (CompareKeys(left, lastReturnedResult) == 0))
      {
        if (!exclusive)
        {
          result = lastReturnedResult;
          return true;
        }
        if (prefetchedKeys.Count > 0)
        {
          InteractionHistoryKey key3;
          result = prefetchedKeys.Dequeue();
          if (CompareKeys(result, right) < 0)
          {
            result.ShardId = left.ShardId;
            lastReturnedResult = result;
            return true;
          }
          result = key3 = null;
          lastReturnedResult = key3;
          return false;
        }
      }
      lastReturnedResult = null;
      prefetchedKeys.Clear();
      var query = Query.And(Query.LTE(x => x.SaveDateTime, left.SaveDateTime), Query.LT(x => x.SaveDateTime, CutOff));
      if (left.ShardIdBits > 0)
      {
        var guid = BuildLowerChunkBoundary(left, right);
        var guid2 = BuildUpperChunkBoundary(left, right);
        query = Query.And(query, Query.GTE(x => x.ContactId, guid), Query.LTE(x => x.ContactId, guid2));
      }
      var fields =
        Fields<InteractionData>.Include(x => x.SaveDateTime).Include(x => x.ContactId).Include(x => x.InteractionId);
      var sortBy =
        SortBy<InteractionData>.Descending(x => x.SaveDateTime)
          .Descending(x => x.ContactId)
          .Descending(x => x.InteractionId);
      var cursor = Interactions.FindAs<VisitData>(query);
      if (cursor == null)
      {
        result = null;
        return false;
      }
      foreach (var data in cursor.SetFields(fields).SetSortOrder(sortBy).SetLimit(100))
      {
        var key = new InteractionHistoryKey
        {
          ShardIdBits = left.ShardIdBits,
          ShardId = left.ShardId,
          SaveDateTime = data.SaveDateTime,
          ContactId = data.ContactId,
          InteractionId = data.InteractionId
        };
        var num = CompareKeys(left, key);
        if ((num <= 0) && (!exclusive || (num != 0)))
        {
          var array = BitOperations.MaskBitsExcept(data.ContactId.ToByteArray(), left.ShardIdBits);
          Array.Resize(ref array, left.ShardId.Length);
          key.ShardId = array;
          if (CompareKeys(key, right) >= 0)
            break;
          prefetchedKeys.Enqueue(key);
        }
      }
      if (prefetchedKeys.Count == 0)
      {
        result = null;
        return false;
      }
      result = prefetchedKeys.Dequeue();
      if (CompareKeys(result, right) >= 0)
      {
        result = null;
        return false;
      }
      result.ShardId = left.ShardId;
      lastReturnedResult = result;
      return true;
    }

    public override InteractionHistoryKey GetRightBoundary() =>
      base.GetLeftBoundary();

    public override bool RangeIsEmpty(InteractionHistoryKey left, InteractionHistoryKey right)
    {
      Assert.ArgumentNotNull(left, "left");
      Assert.ArgumentNotNull(right, "right");
      return base.RangeIsEmpty(right, left);
    }
  }
}