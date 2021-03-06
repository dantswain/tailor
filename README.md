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
2. Untar the download to some known location (I use `/opt/apache/spark-2.4.0`).
   This location will be `SPARK_HOME`.
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

### Python

Assuming you have python 3.6 (I recommend
[pyenv](https://github.com/pyenv/pyenv) to manage python versions) and
[pipenv](https://pipenv.readthedocs.io/en/latest/), this should be as simple as

```
pipenv install -d
```

I have included a `requirements.txt` file, though it may become out of date.

### Scala

This repo uses [sbt](http://www.scala-sbt.org/download.html) (`brew install
sbt`) to build.  I recommend installing [IntelliJ IDEA Community
Edition](https://www.jetbrains.com/idea/download/) to work with the scala code,
though the build can still be done through the command line.  Spark requires
scala 2.11.x and the included `build.sbt` file will configure that.  If you have
sbt installed, you do not need to separately install scala.

### Jupyter

This repo also contains a few [Jupyter](https://jupyter.org/) notebooks.  Some
are python and some are scala.  To get the scala notebooks to work, you will
need to install the [toree](https://toree.apache.org/) kernel.  This project's
Pipfile already includes toree and jupyter, so after running `pipenv install
-d`, you just need to run

```
# adjust per your SPARK_HOME
jupyter toree install --spark_home=/opt/apache/spark-2.4.0
```

To launch [Jupyter Lab](https://jupyterlab.readthedocs.io/en/stable/), use the
script provided:

```
# note you may need to adjust some vars in the script
./start-spark-notebook.sh
```

I find Jupyter Lab to work more smoothly in Chrome.  YMMV.

## ETL

Our ETL ([Extract, Transform,
Load](https://en.wikipedia.org/wiki/Extract,_transform,_load)) step is performed
with python to make json processing a bit easier.

You can run the ETL either from the command line or through the `etl.ipynb`
notebook.  To run it from the command line:

```
# assuming the raw json data is in raw_data/rocdev and we want to write
#  our parquet to parquet_data/rocdev
python tailor/etl.py raw_data/rocdev parquet_data/rocdev
```

## Analysis

We perform analysis with Scala.  To build and run the analysis code:

```
# alternatively you can specify the full path to spark-submit
export SPARK_HOME=/opt/apache/spark-2.4.0
export PATH=${PATH}:${SPARK_HOME}/bin

sbt package
spark-submit --class "org.tailor.TailorApp" --master "local[4]" target/scala-2.11/tailor_2.11-0.1.jar
```

## Bayesian Code Classifier

The `CodeClassifier.scala` file contains an implementation of a programming
language classifier.  The classifier needs to be trained by feeding it examples
of various programming languages.  It looks for subdirectories of
`training_data` with the name of the subdirectory being the name of the
language.  E.g., `training_data/python/` could contain example python files.
You should make sure to have a "text" training directory containing plain text
files.  This acts as a "null" hypothesis - i.e., "this is not actually source
code".

The script `find_training.sh` could be helpful for finding training data, but it
will need some tweaking to run for a general case.  See comments in
[find_training.sh](find_training.sh) for guidance.

Once you have some training languages, you can train the model by running

```
spark-submit --class "org.tailor.CodeClassifierTrainer" --master "local[4]" target/scala-2.11/tailor_2.11-0.1.jar
```

You can then re-run the analysis job (
`spark-submit --class "org.tailor.TailorApp" ... ` from above) to perform
analysis on the data.  Some aggregate analysis will be printed to the console
and a `language_predictions.json` file is written to this directory.

There is a simple [flask](http://flask.pocoo.org/) app provided to view the
output json file:

```
python tailor/view_predictions.py
```

and then visit [http://localhost:5000](http://localhost:5000)