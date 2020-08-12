
resolvers += Resolver.jcenterRepo

addSbtPlugin("com.cavorite" % "sbt-avro" % "3.0.0")

libraryDependencies +=  "org.apache.avro" % "avro-compiler" % "1.10.0",
