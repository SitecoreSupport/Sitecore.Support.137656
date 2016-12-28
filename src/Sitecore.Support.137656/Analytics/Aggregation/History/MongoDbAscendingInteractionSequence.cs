using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver.Builders;
using Sitecore.Analytics.Data.DataAccess.MongoDb;
using Sitecore.Analytics.Model;
using Sitecore.Diagnostics;

namespace Sitecore.Support.Analytics.Aggregation.History
{
  internal class MongoDbAscendingInteractionSequence : MongoDbInteractionSequence
  {
    private static readonly Guid MaxGuid = new Guid("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
    protected static readonly QueryBuilder<InteractionData> Query = new QueryBuilder<InteractionData>();
    protected readonly Queue<InteractionHistoryKey> prefetchedKeys;
    protected InteractionHistoryKey lastReturnedResult;

    internal MongoDbAscendingInteractionSequence(MongoDbCollection interactions, DateTime cutoff, int chunks)
      : base(interactions, cutoff, chunks)
    {
      prefetchedKeys = new Queue<InteractionHistoryKey>();
      Assert.ArgumentNotNull(interactions, "interactions");
    }

    protected Guid BuildLeftChunkBoundary(InteractionHistoryKey key)
    {
      Assert.ArgumentNotNull(key, "key");
      var destinationArray = new byte[0x10];
      Array.Copy(key.ShardId, destinationArray, Math.Min(destinationArray.Length, key.ShardId.Length));
      return new Guid(BitOperations.MaskBitsExcept(destinationArray, key.ShardIdBits));
    }

    protected Guid BuildRightChunkBoundary(InteractionHistoryKey left, InteractionHistoryKey right)
    {
      Assert.ArgumentNotNull(left, "left");
      Assert.ArgumentNotNull(right, "right");
      var destinationArray = new byte[0x10];
      Array.Copy(right.ShardId, destinationArray, Math.Min(destinationArray.Length, right.ShardId.Length));
      var second = BitOperations.MaskBitsExcept(Enumerable.Repeat<byte>(0xff, right.ShardId.Length).ToArray(),
        right.ShardIdBits);
      if (right.ShardId.Zip(second, (l, r) => l != r).Any(x => x) &&
          right.ShardId.Zip(left.ShardId, (l, r) => l != r).Any(x => x))
        for (var i = right.ShardId.Length - 1; i >= 0; i--)
          if (destinationArray[i] == 0)
          {
            destinationArray[i] = 0xff;
          }
          else
          {
            destinationArray[i] = (byte) (destinationArray[i] - 1);
            break;
          }
      return new Guid(BitOperations.RaiseBitsExcept(destinationArray, right.ShardIdBits));
    }

    public override InteractionHistoryKey GetLeftBoundary()
    {
      var fields = Fields<InteractionData>.Include(x => x.SaveDateTime);
      var sortBy = SortBy<InteractionData>.Ascending(x => x.SaveDateTime);
      var cursor = Interactions.FindAllAs<VisitData>();
      if (cursor == null)
        return new InteractionHistoryKey {SaveDateTime = new DateTime(0L)};
      InteractionData data = cursor.SetFields(fields).SetSortOrder(sortBy).SetLimit(1).FirstOrDefault();
      if (data == null)
        return new InteractionHistoryKey {SaveDateTime = new DateTime(0L)};
      var bitsForN = (byte) BitOperations.GetBitsForN(Math.Max(0, Chunks - 1));
      var bytesForBits = BitOperations.GetBytesForBits(bitsForN);
      return new InteractionHistoryKey
      {
        ShardIdBits = bitsForN,
        ShardId = new byte[bytesForBits],
        SaveDateTime = data.SaveDateTime,
        ContactId = Guid.Empty,
        InteractionId = Guid.Empty
      };
    }

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
      var query = Query.And(Query.GTE(x => x.SaveDateTime, left.SaveDateTime), Query.LT(x => x.SaveDateTime, CutOff));
      if (left.ShardIdBits > 0)
      {
        var guid = BuildLeftChunkBoundary(left);
        var guid2 = BuildRightChunkBoundary(left, right);
        query = Query.And(query, Query.GTE(x => x.ContactId, guid), Query.LTE(x => x.ContactId, guid2));
      }
      var fields =
        Fields<InteractionData>.Include(x => x.SaveDateTime).Include(x => x.ContactId).Include(x => x.InteractionId);
      var sortBy =
        SortBy<InteractionData>.Ascending(x => x.SaveDateTime)
          .Ascending(x => x.ContactId)
          .Ascending(x => x.InteractionId);
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

    public override InteractionHistoryKey GetRightBoundary()
    {
      var bitsForN = (byte) BitOperations.GetBitsForN(Math.Max(0, Chunks - 1));
      var bytesForBits = BitOperations.GetBytesForBits(bitsForN);
      var buffer = BitOperations.MaskBitsExcept(Enumerable.Repeat<byte>(0xff, bytesForBits).ToArray(), bitsForN);
      return new InteractionHistoryKey
      {
        ShardIdBits = bitsForN,
        ShardId = buffer,
        SaveDateTime = CutOff,
        ContactId = MaxGuid,
        InteractionId = MaxGuid
      };
    }
  }
}