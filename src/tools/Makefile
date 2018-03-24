# modify the paths appropriately
JDKPATH = $(JAVA_HOME)
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -g -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# batch insert program

BatchInsert: batchinsert.java
	$(JAVAC) batchinsert.java

batchinsert: BatchInsert
	$(JAVA) tools.batchinsert
