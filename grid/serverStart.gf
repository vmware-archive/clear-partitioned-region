start locator --name=locator1 --port=10334 --properties-file=config/locator.properties --initial-heap=250m --max-heap=250m

start server --name=server1 --server-port=0 --properties-file=config/gemfire-server.properties --classpath=$CLASSPATH:../../target/classes/:../../target/gfsh-function.jar --initial-heap=250m --max-heap=250m