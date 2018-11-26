all: git.push java.policy RMIInterface.jar RMItem.jar compile-server-rmi compile-middleware

git.push: FORCE
	git add .
	git commit -m "Hey"
	git push -f origin master

java.policy: FORCE
	@echo "Creating server java policy"
	@echo "grant codebase \"file:`pwd`/\" {" > java.policy
	@echo "permission java.security.AllPermission;" >> java.policy
	@echo "};" >> java.policy

FORCE: ;

RMIInterface.jar: Server/Interface/*.java 
	@echo "Compiling RMI server interface"
	javac Server/Interface/*.java
	jar cvf RMIInterface.jar Server/Interface/IResourceManager.class Server/Interface/TransactionAbortedException.class Server/Interface/InvalidTransactionException.class

RMItem.jar: Server/Common/RMItem.java
	@echo "Compiling RMItem jar"
	javac Server/Common/RMItem.java
	jar cvf RMItem.jar Server/Common/RMItem.class

compile-server-rmi: Server/RMI/*.java Server/Interface/IResourceManager.java Server/Common/*.java Server/LockManager/*.java
	javac Server/RMI/*.java Server/Interface/IResourceManager.java Server/Common/*.java Server/LockManager/*.java

compile-middleware: Server/RMI/*.java Server/Interface/IResourceManager.java Server/Middleware/*.java Server/Common/*.java
	@echo "Compiling Middleware"
	javac Server/RMI/*.java	Server/Interface/IResourceManager.java Server/Middleware/*.java Server/Common/*.java

clean:
	rm -f Server/Interface/*.class Server/Common/*.class Server/RMI/*.class
	rm -f RMIInterface.jar
	rm -f java.policy 
