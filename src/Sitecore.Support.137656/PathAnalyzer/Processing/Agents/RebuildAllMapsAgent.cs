using System;
using System.Collections.Generic;
using System.Linq;
using Sitecore.Diagnostics;
using Sitecore.PathAnalyzer;
using Sitecore.PathAnalyzer.Data.Models;

namespace Sitecore.Support.PathAnalyzer.Processing.Agents
{
  [Serializable]
  public class RebuildAllMapsAgent : BuildMapAgent
  {
    protected override string Mode =>
      "historic";

    protected override List<TreeDefinition> GetTreeDefinitions()
    {
      var definitionService = ApplicationContainer.GetDefinitionService();
      Assert.Required(definitionService, "tree definition service");
      return definitionService.GetAllDefinitions().ToList();
    }

    protected override DateTime ResolveCutoffDate(IEnumerable<TreeDefinition> definitions)
    {
      var utcNow = DateTime.UtcNow;
      Logger.Instance.Info($"Resolved cutoff date = ({utcNow})");
      return utcNow;
    }

    protected override void ScheduleRebuild(List<TreeDefinition> definitions)
    {
      Assert.ArgumentNotNull(definitions, "definitions");
      if (!definitions.Any())
      {
        Logger.Instance.Warn("RebuildAllMapsAgent did not execute. No definitons were found.");
      }
      else
      {
        ApplicationContainer.GetTreeStorageManager().ClearAll();
        base.ScheduleRebuild(definitions);
      }
    }
  }
}