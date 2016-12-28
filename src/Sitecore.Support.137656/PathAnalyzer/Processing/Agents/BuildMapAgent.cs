using System;
using System.Collections.Generic;
using System.Linq;
using Sitecore.Analytics.DataAccess;
using Sitecore.Analytics.Processing;
using Sitecore.Configuration;
using Sitecore.Diagnostics;
using Sitecore.PathAnalyzer;
using Sitecore.PathAnalyzer.Data.Maps;
using Sitecore.PathAnalyzer.Data.Models;
using Sitecore.Support.Analytics.Aggregation.History;
using Sitecore.Support.PathAnalyzer.Processing.Aggregation;

namespace Sitecore.Support.PathAnalyzer.Processing.Agents
{
  [Serializable]
  public abstract class BuildMapAgent : Sitecore.PathAnalyzer.Processing.Agents.BuildMapAgent
  {
    protected override void ScheduleRebuild(List<TreeDefinition> definitions)
    {
      Assert.ArgumentNotNull(definitions, "definitions");
      var definitionService = ApplicationContainer.GetDefinitionService();
      Assert.Required(definitionService, "tree definition service");
      if (!definitions.Any())
      {
        Logger.Instance.Debug("BuildMapAgent skipped. No definitons were found.", "", "");
      }
      else
      {
        Logger.Instance.Info(string.Format("Starting map rebuild. Total: ({0})", definitions.Count), "", "");
        var taskManager = Factory.CreateObject("processing/taskManager", true) as TaskManager;
        var mapWorker = Factory.CreateObject("pathAnalyzer/mapWorker", true) as MapWorker;
        var cutoff = ResolveCutoffDate(definitions);
        if (cutoff > DateTime.UtcNow)
        {
          Logger.Instance.Info("Cutoff date isn't reached. Will reschedule for later.", "", "");
        }
        else
        {
          ObjectRangeDefinition interactionsRange = new SupportInteractionHistoryRangeDefinition(cutoff);
          var list = definitions.Select(def => def.Id).ToList();
          Logger.Instance.Info(
            $"Resolved tree definition ids = ({(object) string.Join(",", list.Select(i => i.ToString()))})", "", "");
          mapWorker.TreeDefinitionIds = list.ToArray();
          mapWorker.Mode = Mode;
          try
          {
            var taskHandle = taskManager.StartDistributedProcessing(interactionsRange, mapWorker, null);
            definitionService.UpdateDefinitions(definitions.Select(d =>
            {
              d.Status = MapRebuildStatus.PickedUp;
              d.TaskId = taskHandle.TaskId;
              return d;
            }));
            Logger.Instance.Info("Registered history processing");
          }
          catch (Exception ex)
          {
            Logger.Instance.Error("BuildMapAgent failed", ex);
            definitionService.UpdateDefinitions(definitions.Select(d =>
            {
              d.Status = MapRebuildStatus.Failed;
              return d;
            }));
          }
        }
      }
    }
  }
}