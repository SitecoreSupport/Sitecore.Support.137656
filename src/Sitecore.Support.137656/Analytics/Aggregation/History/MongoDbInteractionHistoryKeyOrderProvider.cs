using System;
using Sitecore.Analytics.Aggregation.History;
using Sitecore.Analytics.Core.RangeScheduler;
using Sitecore.Diagnostics;

namespace Sitecore.Support.Analytics.Aggregation.History
{
  internal class MongoDbInteractionHistoryKeyOrderProvider : KeyOrderProvider<InteractionHistoryKey>
  {
    private readonly MongoDbInteractionHistoryKeyArithmetic arithmetic;

    public MongoDbInteractionHistoryKeyOrderProvider(DateTime cutoff)
    {
      arithmetic = new MongoDbInteractionHistoryKeyArithmetic(cutoff);
    }

    public override Arithmetic<byte[]> GetArithmetic() =>
      arithmetic;

    public override InteractionHistoryKey GetKey(byte[] order)
    {
      Assert.ArgumentNotNull(order, "order");
      var index = 0;
      var bits = order[index];
      index++;
      var bytesForBits = BitOperations.GetBytesForBits(bits);
      var destinationArray = new byte[bytesForBits];
      Array.Copy(order, index, destinationArray, 0, bytesForBits);
      index += bytesForBits;
      var ticks = BitOperations.ReadInt64BigEndian(order, index);
      index += 8;
      var buffer2 = new byte[0x10];
      Array.Copy(order, index, buffer2, 0, 0x10);
      index += 0x10;
      var buffer3 = new byte[0x10];
      Array.Copy(order, index, buffer3, 0, 0x10);
      return new InteractionHistoryKey
      {
        ShardIdBits = bits,
        ShardId = destinationArray,
        SaveDateTime = new DateTime(ticks, DateTimeKind.Utc),
        ContactId = new Guid(buffer2),
        InteractionId = new Guid(buffer3)
      };
    }

    public override byte[] GetOrder(InteractionHistoryKey key)
    {
      Assert.ArgumentNotNull(key, "key");
      var bytesForBits = BitOperations.GetBytesForBits(key.ShardIdBits);
      Assert.AreEqual(key.ShardId.Length, bytesForBits, "ShardId length is not aligned with ShardIdBits");
      var destinationArray = new byte[1 + bytesForBits + 8 + 0x10 + 0x10];
      var index = 0;
      destinationArray[index] = key.ShardIdBits;
      index++;
      Array.Copy(key.ShardId, 0, destinationArray, index, bytesForBits);
      index += bytesForBits;
      BitOperations.WriteInt64BigEndian(key.SaveDateTime.Ticks, destinationArray, index);
      index += 8;
      Array.Copy(key.ContactId.ToByteArray(), 0, destinationArray, index, 0x10);
      index += 0x10;
      Array.Copy(key.InteractionId.ToByteArray(), 0, destinationArray, index, 0x10);
      return destinationArray;
    }
  }
}