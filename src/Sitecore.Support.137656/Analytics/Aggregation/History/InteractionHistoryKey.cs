using System;

namespace Sitecore.Support.Analytics.Aggregation.History
{
  internal class InteractionHistoryKey
  {
    private static readonly byte[] EmptyByteArray = new byte[0];

    public InteractionHistoryKey()
    {
      ShardId = EmptyByteArray;
    }

    public Guid ContactId { get; set; }

    public Guid InteractionId { get; set; }

    public DateTime SaveDateTime { get; set; }

    public byte[] ShardId { get; set; }

    public byte ShardIdBits { get; set; }
  }
}