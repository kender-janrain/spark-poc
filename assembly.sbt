import AssemblyKeys._

assemblySettings

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case "log4j.properties" => MergeStrategy.first
  case x => old(x)
}
}