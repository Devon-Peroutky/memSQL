#!/bin/bash          
javac SQLBenchmark.java
java -cp .:"mysql-connector-java-5.1.26/mysql-connector-java-5.1.26-bin.jar" SQLBenchmark
