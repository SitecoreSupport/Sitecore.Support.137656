// Decompiled with JetBrains decompiler
// Type: Sitecore.Analytics.RangeScheduler.SequenceItemTypeAdaptor`2
// Assembly: Sitecore.Analytics.MongoDB, Version=8.1.0.0, Culture=neutral, PublicKeyToken=null
// MVID: 0309912F-933C-4876-AAA4-B0AFC4C36816
// Assembly location: S:\Instances\sc81u3smf\Website\bin\Sitecore.Analytics.MongoDB.dll

using Sitecore.Analytics.Core.RangeScheduler;
using Sitecore.Analytics.DataAccess.RangeScheduler;
using Sitecore.Analytics.Processing;
using Sitecore.Diagnostics;

namespace Sitecore.Support.Analytics.RangeScheduler
{
  internal class SequenceItemTypeAdaptor<TOriginalItem, TTargetItem> : Sequence<TTargetItem>
  {
    private readonly Sequence<TOriginalItem> innerSequence;
    private readonly IWorkItemMapper<TOriginalItem, TTargetItem> mapper;

    public SequenceItemTypeAdaptor(Sequence<TOriginalItem> innerSequence,
      IWorkItemMapper<TOriginalItem, TTargetItem> mapper)
    {
      Assert.ArgumentNotNull(innerSequence, "innerSequence");
      Assert.ArgumentNotNull(mapper, "mapper");
      this.innerSequence = innerSequence;
      this.mapper = mapper;
    }

    public override int CompareKeys(TTargetItem left, TTargetItem right)
    {
      Assert.ArgumentNotNull(left, "left");
      Assert.ArgumentNotNull(right, "right");
      return innerSequence.CompareKeys(mapper.Unmap(left), mapper.Unmap(right));
    }

    public override TTargetItem GetLeftBoundary()
    {
      return mapper.Map(innerSequence.GetLeftBoundary());
    }

    public override bool GetNextKey(TTargetItem left, TTargetItem right, bool exclusive, out TTargetItem result)
    {
      TOriginalItem local3;
      Assert.ArgumentNotNull(left, "left");
      Assert.ArgumentNotNull(right, "right");
      var local = mapper.Unmap(left);
      var local2 = mapper.Unmap(right);
      var flag = innerSequence.GetNextKey(local, local2, exclusive, out local3);
      if (Equals(local3, default(TOriginalItem)))
      {
        result = default(TTargetItem);
        return flag;
      }
      result = mapper.Map(local3);
      return flag;
    }


    public override long GetRecordCount()
    {
      return innerSequence.GetRecordCount();
    }

    public override TTargetItem GetRightBoundary()
    {
      return mapper.Map(innerSequence.GetRightBoundary());
    }

    public override TTargetItem KeySplit(TTargetItem left, TTargetItem right)
    {
      Assert.ArgumentNotNull(left, "left");
      Assert.ArgumentNotNull(right, "right");
      return mapper.Map(innerSequence.KeySplit(mapper.Unmap(left), mapper.Unmap(right)));
    }

    public override bool RangeIsEmpty(TTargetItem left, TTargetItem right)
    {
      Assert.ArgumentNotNull(left, "left");
      Assert.ArgumentNotNull(right, "right");
      return innerSequence.RangeIsEmpty(mapper.Unmap(left), mapper.Unmap(right));
    }

    public override KeyOrderProvider<TTargetItem> GetOrderProvider()
    {
      return new OrderProviderTypeAdaptor(innerSequence.GetOrderProvider(), mapper);
    }

    private class OrderProviderTypeAdaptor : KeyOrderProvider<TTargetItem>
    {
      private readonly KeyOrderProvider<TOriginalItem> baseProvider;
      private readonly IWorkItemMapper<TOriginalItem, TTargetItem> mapper;

      public OrderProviderTypeAdaptor(KeyOrderProvider<TOriginalItem> baseProvider,
        IWorkItemMapper<TOriginalItem, TTargetItem> mapper)
      {
        Assert.ArgumentNotNull(baseProvider, "baseProvider");
        Assert.ArgumentNotNull(mapper, "mapper");
        this.baseProvider = baseProvider;
        this.mapper = mapper;
      }

      public override TTargetItem GetKey(byte[] order)
      {
        Assert.ArgumentNotNull(order, "order");
        return mapper.Map(baseProvider.GetKey(order));
      }

      public override byte[] GetOrder(TTargetItem key)
      {
        Assert.ArgumentNotNull(key, "key");
        return baseProvider.GetOrder(mapper.Unmap(key));
      }

      public override Arithmetic<byte[]> GetArithmetic()
      {
        return baseProvider.GetArithmetic();
      }
    }
  }
}