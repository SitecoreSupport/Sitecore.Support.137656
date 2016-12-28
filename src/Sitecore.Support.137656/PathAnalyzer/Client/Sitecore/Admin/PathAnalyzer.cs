using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Web;
using System.Web.UI.HtmlControls;
using System.Web.UI.WebControls;
using Sitecore.Analytics.Processing;
using Sitecore.Configuration;
using Sitecore.Data.Items;
using Sitecore.Data.Validators;
using Sitecore.Diagnostics;
using Sitecore.Globalization;
using Sitecore.PathAnalyzer;
using Sitecore.PathAnalyzer.Contracts;
using Sitecore.PathAnalyzer.Data;
using Sitecore.PathAnalyzer.Data.Maps;
using Sitecore.PathAnalyzer.Processing.Agents;
using Sitecore.PathAnalyzer.Services;
using Sitecore.sitecore.admin;
using Sitecore.SequenceAnalyzer.Data;
using Sitecore.Xdb.Configuration;
using BaseValidator = Sitecore.Data.Validators.BaseValidator;
using DataID = Sitecore.Data.ID;
using RebuildAllMapsAgent = Sitecore.Support.PathAnalyzer.Processing.Agents.RebuildAllMapsAgent;

namespace Sitecore.Support.PathAnalyzer.Client.Sitecore.Admin
{
  public class PathAnalyzer : AdminPage
  {
    private readonly IConfiguration _configuration;
    private readonly IMapItemDeploymentManager _mapDeploymentManager;
    private readonly IMapItemRepository _mapRepository;
    private readonly ITreeStorage _storage;
    private readonly ITreeDefinitionService _treeDefinitionService;
    private string _servicePath;
    protected HtmlGenericControl agentsContainer;
    protected Button btnDeployAllMaps;
    protected Button btnNewMapAgent;
    protected Button btnRebuildAllDeployedMaps;
    protected Button btnUpgrade;
    protected HtmlForm form1;
    protected Literal ltlBuilderError;
    protected Literal ltlBuilderMessage;
    protected Literal ltlManagerError;
    protected Literal ltlManagerMessage;
    protected Literal ltlMapCount;
    protected Literal ltlMapsDeployedCount;
    protected Literal ltlSystemError;
    protected Literal ltlUpgradeError;
    protected Literal ltlUpgradeMessage;
    protected HtmlGenericControl mapManagerContainer;
    protected HtmlGenericControl rebuildContainer;
    protected Literal rebuildStatusErrorLiteral;
    protected Literal rebuildStatusLiteral;
    protected Repeater rptMaps;
    protected HtmlGenericControl upgradeContainer;

    public PathAnalyzer()
    {
      if (XdbSettings.Enabled)
      {
        _storage = ApplicationContainer.GetStorage();
        _mapRepository = ApplicationContainer.GetMapItemRepository();
        _mapDeploymentManager = ApplicationContainer.GetMapItemDeploymentManager();
        _configuration = ApplicationContainer.GetConfiguration();
        _treeDefinitionService = ApplicationContainer.GetDefinitionService();
      }
    }

    protected bool RemoteClientEnabled =>
      !string.IsNullOrWhiteSpace(ServicePath);

    protected string ServicePath
    {
      get
      {
        if (string.IsNullOrWhiteSpace(_servicePath) &&
            _configuration.ConfigNodeExists("pathAnalyzer/remoteServices/endpoints/adminService"))
          _servicePath = _configuration.GetConfigNodeValue("pathAnalyzer/remoteServices/endpoints/adminService");
        return _servicePath;
      }
    }

    protected IWebRequestFactory WebRequestFactory
    {
      get
      {
        if (RemoteClientEnabled)
          return ApplicationContainer.GetWebRequestFactory();
        return null;
      }
    }

    protected void btnDeployAllMaps_OnClick(object sender, EventArgs e)
    {
      if ((_mapRepository != null) && (_mapDeploymentManager != null))
      {
        foreach (var item in _mapRepository.GetAll())
        {
          var mapDeploymentStatus = _mapDeploymentManager.GetMapDeploymentStatus(item);
          if (mapDeploymentStatus != MapDeploymentStatus.Deployed)
          {
            var list = ValidateMap(item.InnerItem);
            if (list.Count > 0)
            {
              ltlManagerError.Text = ltlManagerError.Text +
                                     $"<p>Map '{item.DisplayName}' not deployed due to validation errors:</p>";
              ltlManagerError.Text = ltlManagerError.Text + "<ul>";
              foreach (var error in list)
                ltlManagerError.Text = ltlManagerError.Text +
                                       $"<li>{item.DisplayName} : {error.Status} - {error.Text}</li>";
              ltlManagerError.Text = ltlManagerError.Text + "</ul>";
            }
            else
            {
              try
              {
                switch (mapDeploymentStatus)
                {
                  case MapDeploymentStatus.NotDeployed:
                  {
                    _mapDeploymentManager.DeployMapItem(item, true);
                    continue;
                  }
                  case MapDeploymentStatus.PartialWorkflow:
                  case MapDeploymentStatus.PartialStorage:
                  {
                    _mapDeploymentManager.RedeployMapItem(item, true);
                    continue;
                  }
                }
              }
              catch (TargetInvocationException exception)
              {
                ltlManagerError.Text = ltlManagerError.Text +
                                       $"<p>Map '{item.DisplayName}' not deployed due to the following deployment error:<br /> '{exception.InnerException.Message}' <br />Check log for more details.</p>";
              }
              catch (Exception exception2)
              {
                ltlManagerError.Text = ltlManagerError.Text +
                                       $"<p>Map '{item.DisplayName}' not deployed due to the following deployment error:<br /> '{exception2.Message}' <br />Check log for more details.</p>";
              }
            }
          }
        }
        RenderMapsManagerOptions();
      }
    }

    protected void btnRebuildAllDeployedMaps_OnClick(object sender, EventArgs e)
    {
      var rebuildAllMapsAgent = new RebuildAllMapsAgent();

      (Factory.CreateObject("processing/taskManager", true) as TaskManager).ScheduleAction(rebuildAllMapsAgent.Execute);
      rebuildStatusLiteral.Text =
        "Queued map rebuild. Check the log files on the processing server for more information. You will need to configure the log file appender to DEBUG level.";
    }

    protected void btnRunNewMapAgent_OnClick(object sender, EventArgs e)
    {
      (Factory.CreateObject("processing/taskManager", true) as TaskManager).ScheduleAction(new NewMapAgent().Execute);
      ltlBuilderMessage.Text =
        "Queued new map agent. Check the log files on the processing server for more information. You will need to configure the log file appender to DEBUG level.";
    }

    protected void btnUpgrade_OnClick(object sender, EventArgs e)
    {
      try
      {
        using (new InitialMapDeploymentContext(MapRebuildStatus.Built))
        {
          _mapDeploymentManager.RedeployAll(false);
        }
        ltlUpgradeMessage.Text = "Map definitions were upgraded successfully.";
      }
      catch (Exception exception)
      {
        ltlUpgradeError.Text = string.Format("There was an error while upgrading maps: <br />" + exception);
      }
    }

    protected virtual HttpWebRequest BuildRemoteRequest(string requestPath, HttpMethod requestMethod, byte[] requestData)
    {
      var request = WebRequestFactory.CreateRequest(ServicePath + requestPath, requestMethod, requestData);
      request.Method = requestMethod.Method;
      return request;
    }

    protected void CheckConfiguration()
    {
      var source = ApplicationContainer.FailedRegisteredTypes.ToList();
      source.AddRange(ApiContainer.FailedRegisteredTypes);
      if (source.Any())
      {
        ltlSystemError.Text = ltlSystemError.Text + "<ul>";
        foreach (var str in source)
          ltlSystemError.Text = ltlSystemError.Text +
                                $"<li>Component '{str}' cannot be found. Please check configuration.</li>";
        ltlSystemError.Text = ltlSystemError.Text + "</ul>";
        ltlSystemError.Visible = true;
        mapManagerContainer.Visible = false;
        agentsContainer.Visible = false;
        rebuildContainer.Visible = false;
        upgradeContainer.Visible = false;
      }
    }

    protected void CheckXdbEnabled()
    {
      if (!XdbSettings.Enabled)
      {
        HttpContext.Current.Response.Redirect(XdbSettings.XdbDisabledUrl);
        HttpContext.Current.Response.End();
      }
    }

    protected void LoadBuilderSection()
    {
      if (IsPostBack)
        ltlBuilderMessage.Text = string.Empty;
    }

    protected void LoadMapsManagerSection()
    {
      if (!IsPostBack)
        RenderMapsManagerOptions();
    }

    protected override void OnInit(EventArgs arguments)
    {
      Assert.ArgumentNotNull(arguments, "arguments");
      CheckSecurity(true);
      CheckConfiguration();
      CheckXdbEnabled();
      base.OnInit(arguments);
    }

    protected void Page_Load(object sender, EventArgs e)
    {
      LoadBuilderSection();
      LoadMapsManagerSection();
    }

    protected void RenderMapsManagerOptions()
    {
      if ((_mapRepository == null) || (_mapDeploymentManager == null))
        return;
      var list = _mapRepository.GetAll().ToList();
      rptMaps.DataSource = list;
      rptMaps.DataBind();
      var str1 = list.Count.ToString(CultureInfo.InvariantCulture);
      var str2 =
        list.Count(m => _mapDeploymentManager.GetMapDeploymentStatus(m) == MapDeploymentStatus.Deployed)
          .ToString(CultureInfo.InvariantCulture);
      ltlMapCount.Text = str1;
      ltlMapsDeployedCount.Text = str2;
      if (str1 != str2)
        return;
      btnDeployAllMaps.Enabled = false;
    }

    protected void rptMaps_OnItemCommand(object sender, RepeaterCommandEventArgs e)
    {
      string str;
      if ((_mapRepository != null) && (_mapDeploymentManager != null) && ((str = e.CommandName) != null) &&
          ((str == "Deploy") || (str == "Redeploy")))
      {
        var itemId = DataID.Parse(e.CommandArgument);
        var item = _mapRepository.CreateMapItem(_configuration.ContentDatabase.GetItem(itemId));
        Assert.IsNotNull(item, "mapItem");
        var list = ValidateMap(item.InnerItem);
        if (list.Count == 0)
        {
          try
          {
            if (e.CommandName == "Deploy")
              _mapDeploymentManager.DeployMapItem(item, true);
            else if (e.CommandName == "Redeploy")
              _mapDeploymentManager.RedeployMapItem(item, true);
          }
          catch (TargetInvocationException exception)
          {
            ltlManagerError.Text = ltlManagerError.Text +
                                   $"<p>Map '{item.DisplayName}' not deployed due to the following deployment error:<br /> '{exception.InnerException.Message}' <br />Check log for more details.</p>";
          }
          catch (Exception exception2)
          {
            ltlManagerError.Text = ltlManagerError.Text +
                                   $"<p>Map '{item.DisplayName}' not deployed due to the following deployment error:<br /> '{exception2.Message}' <br />Check log for more details.</p>";
          }
          RenderMapsManagerOptions();
        }
        else
        {
          ltlManagerError.Text = ltlManagerError.Text +
                                 $"<p>Map '{item.DisplayName}' not deployed due to validation errors:</p>";
          ltlManagerError.Text = ltlManagerError.Text + "<ul>";
          foreach (var error in list)
            ltlManagerError.Text = ltlManagerError.Text + $"<li>{error.Status} - {error.Text}</li>";
          ltlManagerError.Text = ltlManagerError.Text + "</ul>";
        }
      }
    }

    protected void rptMaps_OnItemDataBound(object sender, RepeaterItemEventArgs e)
    {
      if ((_mapRepository != null) && (_mapDeploymentManager != null) &&
          ((e.Item.ItemType == ListItemType.Item) || (e.Item.ItemType == ListItemType.AlternatingItem)))
      {
        var literal = e.Item.FindControl("ltlDeploymentStatus") as Literal;
        if (literal != null)
        {
          var button = e.Item.FindControl("btnDeploy") as Button;
          if (button != null)
            switch (_mapDeploymentManager.GetMapDeploymentStatus((MapItem) e.Item.DataItem))
            {
              case MapDeploymentStatus.Deployed:
                literal.Text = "Already Deployed";
                button.Visible = false;
                return;

              case MapDeploymentStatus.NotDeployed:
                button.Visible = true;
                return;

              case MapDeploymentStatus.PartialWorkflow:
              case MapDeploymentStatus.PartialStorage:
                literal.Text = "Partially Deployed<br />";
                button.Text = "Redeploy";
                button.Visible = true;
                button.CommandName = "Redeploy";
                return;
            }
        }
      }
    }

    protected IList<ValidationError> ValidateMap(Item item)
    {
      Assert.ArgumentNotNull(item, "item");
      var list = new List<ValidationError>();
      var validators = ValidatorManager.BuildValidators(ValidatorsMode.ValidatorBar, item);
      if (validators.Count != 0)
      {
        ValidatorManager.Validate(validators, new ValidatorOptions(true));
        foreach (BaseValidator validator in validators)
          if (!validator.IsEvaluating && (validator.Result != ValidatorResult.Valid) &&
              (validator.Result != ValidatorResult.Unknown))
            list.Add(new ValidationError(validator));
      }
      return list;
    }

    public class ValidationError
    {
      public ValidationError(BaseValidator validator)
      {
        Assert.IsNotNull(validator, "validator");
        Status = Translate.Text(validator.Result.ToString().Replace("CriticalError", "Critical Error"));
        Text = validator.Text;
        Level = validator.Result;
      }

      public ValidatorResult Level { get; set; }

      public string Status { get; set; }

      public string Text { get; set; }
    }
  }
}