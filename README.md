Spark..Unique.Seq.Generator
===========================

This is an example of how to make Unique Sequences in a distributed way with Spark (No dups, No Skips)


This project contains three main classes

1. GenerateSampleData: a program to create test data on HDFS
2. UniqueSeqGenerator: a spark program to create unique sequence in a distributed way
3. ValidateSeqGeneration: a spark program to validate is the sequence are correct.