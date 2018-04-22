# modify the paths appropriately
JDKPATH = $(JAVA_HOME)
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -g -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# batch insert program
<<<<<<< HEAD
all: BatchInsert Query QNLJ Qsort
=======
all: BatchInsert Query Qsort
>>>>>>> 0ab336d3812d83c91dca424c4cd9be9e14068591

BatchInsert: batchinsert.java
	$(JAVAC) batchinsert.java

batchinsert: BatchInsert
	$(JAVA) tools.batchinsert

Query: query.java
	$(JAVAC) query.java

query: Query
	$(JAVA) tools.query

<<<<<<< HEAD
QNLJ: qNLJ.java
	$(JAVAC) qNLJ.java

qNLJ: QNLJ
	$(JAVA) tools.qNLJ

=======
>>>>>>> 0ab336d3812d83c91dca424c4cd9be9e14068591
Qsort: qsort.java
	$(JAVAC) qsort.java

qsort: Qsort
	$(JAVA) tools.qsort
