using Sitecore.Analytics.Model;
using Sitecore.Analytics.Processing;
using Sitecore.Analytics.Processing.ProcessingPool;
using Sitecore.Diagnostics;

namespace Sitecore.Support.PathAnalyzer.Processing
{
  internal class ProcessingPoolItemMapper : IWorkItemMapper<InteractionKey, ProcessingPoolItem>
  {
    public ProcessingPoolItem Map(InteractionKey mapped)
    {
      Assert.ArgumentNotNull(mapped, "mapped");
      return new ProcessingPoolItem(mapped.ToByteArray());
    }

    public InteractionKey Unmap(ProcessingPoolItem workItem)
    {
      Assert.ArgumentNotNull(workItem, "workItem");
      return new InteractionKey(workItem.Key);
    }
  }
}