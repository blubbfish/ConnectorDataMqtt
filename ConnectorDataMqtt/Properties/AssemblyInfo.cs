using System.Reflection;
using System.Resources;
using System.Runtime.InteropServices;

// Allgemeine Informationen über eine Assembly werden über die folgenden
// Attribute gesteuert. Ändern Sie diese Attributwerte, um die Informationen zu ändern,
// die einer Assembly zugeordnet sind.
[assembly: AssemblyTitle("ConnectorDataMqtt")]
[assembly: AssemblyDescription("ADataBackend Connector that connects to mqtt using M2Mqtt")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("BlubbFish")]
[assembly: AssemblyProduct("ConnectorDataMqtt")]
[assembly: AssemblyCopyright("Copyright ©  2017 - 27.05.2019")]
[assembly: AssemblyTrademark("© BlubbFish")]
[assembly: AssemblyCulture("")]
[assembly: NeutralResourcesLanguage("de-DE")]

// Durch Festlegen von ComVisible auf FALSE werden die Typen in dieser Assembly
// für COM-Komponenten unsichtbar.  Wenn Sie auf einen Typ in dieser Assembly von
// COM aus zugreifen müssen, sollten Sie das ComVisible-Attribut für diesen Typ auf "True" festlegen.
[assembly: ComVisible(false)]

// Die folgende GUID bestimmt die ID der Typbibliothek, wenn dieses Projekt für COM verfügbar gemacht wird
[assembly: Guid("ee6c8f68-ed46-4c1c-abdd-cfcdf75104f2")]

// Versionsinformationen für eine Assembly bestehen aus den folgenden vier Werten:
//
//      Hauptversion
//      Nebenversion
//      Buildnummer
//      Revision
//
// Sie können alle Werte angeben oder Standardwerte für die Build- und Revisionsnummern verwenden,
// indem Sie "*" wie unten gezeigt eingeben:
// [assembly: AssemblyVersion("1.0.*")]
[assembly: AssemblyVersion("1.1.0")]
[assembly: AssemblyFileVersion("1.1.0")]

/*
 * 1.1.0 Rewrite Module to reconnect itselfs, so you dont need to watch over the the state of the connection
 */