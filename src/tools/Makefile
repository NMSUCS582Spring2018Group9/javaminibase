# modify the paths appropriately
JDKPATH = $(JAVA_HOME)
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -g -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# batch insert program
all: BatchInsert Query QNLJ

BatchInsert: batchinsert.java
	$(JAVAC) batchinsert.java

batchinsert: BatchInsert
	$(JAVA) tools.batchinsert

Query: query.java
	$(JAVAC) query.java

query: Query
	$(JAVA) tools.query

QNLJ: qNLJ.java
	$(JAVAC) qNLJ.java

qNLJ: QNLJ
	$(JAVA) tools.qNLJ
