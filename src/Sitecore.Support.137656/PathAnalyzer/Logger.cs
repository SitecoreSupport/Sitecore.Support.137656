// Decompiled with JetBrains decompiler
// Type: Sitecore.PathAnalyzer.Logger
// Assembly: Sitecore.PathAnalyzer, Version=1.1.0.0, Culture=neutral, PublicKeyToken=null
// MVID: 9CE4732E-3A06-40BB-A28C-BD9A0F38511D
// Assembly location: S:\Instances\sc81u3wffmu3\Website\bin\Sitecore.PathAnalyzer.dll

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Web;
using Sitecore.Diagnostics;
using Sitecore.Support.PathAnalyzer.Contracts;

namespace Sitecore.Support.PathAnalyzer
{
  [Serializable]
  internal class Logger : ILogger
  {
    private const string prefix = "[Path Analyzer]";
    private static readonly Lazy<Logger> LazyConfiguration = new Lazy<Logger>(() => new Logger());
    private static readonly bool _enabled = HttpContext.Current != null;

    private Logger()
    {
    }

    public static Logger Instance => LazyConfiguration.Value;

    public void Debug(string message, [CallerFilePath] string callerFilePath = "",
      [CallerMemberName] string memberName = "")
    {
      if (!_enabled)
        return;
      Log.Debug(GetMessage(message, callerFilePath, memberName), this);
    }

    public void Error(string message, [CallerFilePath] string callerFilePath = "",
      [CallerMemberName] string memberName = "")
    {
      if (!_enabled)
        return;
      Log.Error(GetMessage(message, callerFilePath, memberName), this);
    }

    public void Error(string message, Exception exception)
    {
      if (!_enabled)
        return;
      Log.Error(string.Format("{0} {1}", "[Path Analyzer]", message), exception, this);
    }

    public void Fatal(string message, [CallerFilePath] string callerFilePath = "",
      [CallerMemberName] string memberName = "")
    {
      if (!_enabled)
        return;
      Log.Fatal(GetMessage(message, callerFilePath, memberName), this);
    }

    public void Info(string message, [CallerFilePath] string callerFilePath = "",
      [CallerMemberName] string memberName = "")
    {
      if (!_enabled)
        return;
      Log.Info(GetMessage(message, callerFilePath, memberName), this);
    }

    public void SingleError(string message, [CallerFilePath] string callerFilePath = "",
      [CallerMemberName] string memberName = "")
    {
      if (!_enabled)
        return;
      Log.SingleError(GetMessage(message, callerFilePath, memberName), this);
    }

    public void Warn(string message, [CallerFilePath] string callerFilePath = "",
      [CallerMemberName] string memberName = "")
    {
      if (!_enabled)
        return;
      Log.Warn(GetMessage(message, callerFilePath, memberName), this);
    }

    public void SingleWarn(string message, [CallerFilePath] string callerFilePath = "",
      [CallerMemberName] string memberName = "")
    {
      if (!_enabled)
        return;
      Log.SingleWarn(GetMessage(message, callerFilePath, memberName), this);
    }

    private static string GetMessage(string message, string callerFilePath, string memberName)
    {
      return string.Format("{0}({1}) {2}", "[Path Analyzer]",
        string.Format("{0}.{1}", Path.GetFileNameWithoutExtension(callerFilePath), memberName), message);
    }
  }
}