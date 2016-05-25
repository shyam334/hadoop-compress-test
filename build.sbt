uniform.project("hadoop-compress", "")

libraryDependencies ++= depend.hadoopClasspath ++
                        Seq("com.github.scopt" %% "scopt" % "3.4.0")
