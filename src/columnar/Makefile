JDKPATH = $(JAVA_HOME)
LIBPATH = ../../construction
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -g -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

cf:Columnarfile.java
	$(JAVAC) Columnarfile.java

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
