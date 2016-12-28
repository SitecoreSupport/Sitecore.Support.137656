using Sitecore.Analytics.Model;
using Sitecore.Analytics.Processing;
using Sitecore.Diagnostics;

namespace Sitecore.Support.Analytics.Aggregation.History
{
  internal class InteractionHistoryKeyMapper : IWorkItemMapper<InteractionHistoryKey, InteractionKey>
  {
    public InteractionKey Map(InteractionHistoryKey workItem)
    {
      Assert.ArgumentNotNull(workItem, "workItem");
      return new CustomInteractionKey(workItem);
    }

    public InteractionHistoryKey Unmap(InteractionKey mapped)
    {
      Assert.ArgumentNotNull(mapped, "mapped");
      var key = mapped as CustomInteractionKey;
      Assert.IsNotNull(key, "provided item is not mapped by this mapper");
      return key.InnerKey;
    }

    private class CustomInteractionKey : InteractionKey
    {
      public CustomInteractionKey(InteractionHistoryKey innerKey) : base(innerKey.ContactId, innerKey.InteractionId)
      {
        Assert.ArgumentNotNull(innerKey, "innerKey");
        var key = new InteractionHistoryKey
        {
          ContactId = innerKey.ContactId,
          InteractionId = innerKey.InteractionId,
          SaveDateTime = innerKey.SaveDateTime,
          ShardId = (byte[]) innerKey.ShardId.Clone(),
          ShardIdBits = innerKey.ShardIdBits
        };
        InnerKey = key;
      }

      public InteractionHistoryKey InnerKey { get; }
    }
  }
}