resolvers += Resolver.url("commbank-releases-ivy", new URL("http://commbank.artifactoryonline.com/commbank/ext-releases-local-ivy"))(Patterns("[organization]/[module]_[scalaVersion]_[sbtVersion]/[revision]/[artifact](-[classifier])-[revision].[ext]"))

val uniformVersion = "1.10.0-20160509042812-b923980"

addSbtPlugin("au.com.cba.omnia" % "uniform-core"        % uniformVersion)

addSbtPlugin("au.com.cba.omnia" % "uniform-dependency"  % uniformVersion)

addSbtPlugin("au.com.cba.omnia" % "uniform-assembly"    % uniformVersion)
