./run_rmi.sh > /dev/null

java -Djava.security.policy=java.policy -Djava.rmi.server.codebase=file:$(pwd)/Server Server.Middleware.RMIMiddleware $1 $2 $3
