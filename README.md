# Tailor

A tool for analyzing Slack exports, except not really.  Really this is a demo
for people learning about [Apache Spark](https://spark.apache.org).

## Setup

This repo contains both python and scala code.   You can set up and run the
python code without setting up scala, but not vice versa.  You will also need to
have Spark set up.

### Spark

This code was built and tested with Spark 2.4.0.

See
[https://spark.apache.org/docs/latest/](https://spark.apache.org/docs/latest/)
for official instructions.  If you have a Mac, it should be fairly
straightforward:

1. Download Spark 2.4.0 (see link above)
2. Untar the download to some known location (I use `/opt/apache/spark-2.4.0`)
3. Make sure you have Java 1.8.0.  If you have homebrew, `brew tap caskroom/versions && brew cask install java8`

To check it out, try running the following:
```
# adjust as necessary for your java 8 location
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home
# adjust as necessary for where you installed spark
/opt/apache/spark-2.4.0/bin/spark-shell
```

You should get a bit of logging and eventually see this:

```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.0
      /_/
         
Using Scala version 2.11.12 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_161)
Type in expressions to have them evaluated.
Type :help for more information.

scala> 
```
You can exit with `:quit` or `Ctrl+d`.
