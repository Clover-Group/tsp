# Debugging guidelines

It is advised to use IntelliJ IDEA debugging features, the `sbt Task` configuration to run should use `http/"run flink-local"` as main task to run.

Using Visual Studio Code with Scalameta LSP implementation may not work as expected, since the extension supports Scala 2.13+ (including 3.0), and Flink does not yet support Scala versions beyond 2.12.