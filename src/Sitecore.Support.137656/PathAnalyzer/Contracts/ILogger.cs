using System;

namespace Sitecore.Support.PathAnalyzer.Contracts
{
  internal interface ILogger
  {
    void Debug(string message, string callerFilePath = "", string caller = "");
    void Error(string message, Exception exception);
    void Error(string message, string callerFilePath = "", string caller = "");
    void Fatal(string message, string callerFilePath = "", string caller = "");
    void Info(string message, string callerFilePath = "", string caller = "");
    void SingleError(string message, string callerFilePath = "", string caller = "");
    void SingleWarn(string message, string callerFilePath = "", string caller = "");
    void Warn(string message, string callerFilePath = "", string caller = "");
  }
}