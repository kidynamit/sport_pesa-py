# sport_pesa-py

An Apache's Spark application for training sports data.

## Installation Instructions

This application is built and tested on an Ubuntu Debian distribution.

### Dependencies

1. Download and install [Python 2.7](https://www.python.org/download/releases/2.7/)
    and the following python packages

    * [NumPy](https://sourceforge.net/projects/numpy/)
    * [SciPy](https://sourceforge.net/project/scipy/)

    `sudo apt-get install python python-numpy python-scipy python-matplotlib ipython
    ipython-notebook python-pandas python-sympy python-nose`

2. Download and install [OpenJDK Java 7+](http://openjdk.java.net/install/)

    `sudo apt-get install openjdk-7-jdk openjdk-7-jre`

3. Download and install [Scala 2.11+](http://www.scala-lang.org/download/)

    `sudo apt-get install scala-library scala`

4. Download and install [Apache's Spark 1.6.1](http://spark.apache.org/downloads.html)


### Environment Variables

Set the following environment variables:

  * **SPARK_HOME**
  * **JAVA_HOME**

Example for [fish](https://fishshell.com/):

  * `set -x JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/`

  * `set -x SPARK_HOME /opt/spark-1.6.1/`

### PySpark Libraries

Decompress the PySpark and Py4j libraries to the local Python 2.7 library folder.

`unzip $(SPARK_HOME)/python/lib/pyspark.zip -d /usr/lib/python2.7/`

`unzip $(SPARK_HOME)/python/lib/py4j-0.9-src.zip -d /usr/lib/python2.7/`

## Testing

1. Create a data directory and an out directory

    `mkdir data`

    `mkdir out`

2. Change into the data directory

    `cd data`

3. Mirror the data from [Football Data](https://www.football-data.co.uk/) into the dat

    `wget --mirror --no-parent --convert-links --adjust-extension --page-requisites --accept html,php,zip,csv --tries=inf --continue www.football-data.co.uk`

4. Run sports_pyspark.py

  `cd src & python sports_pyspark.py`

The results would be shown on the terminal.

## Usage

You can change the parameters in the `.conf` files in the src folder.
