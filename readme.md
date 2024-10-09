# Assignment 3: Spark assignment

This is the Spark Assignment for CSE2520 Big Data Processing course.

## IMPORTANT

Make sure that the language level is set to 8 in `| File | Project Structure | Modules`, as Spark/Scala is not compatible
with Java versions higher than 8.

## Setup of the assignment

The assignment consists of 2 parts that are worth 95 points:

1. [Spark and RDD's](<src/main/scala/RDDAssignment/readme.md>)
    In this part you will focus on the basics of Spark RDDs.

2. [Spark and DataFrames](<src/main/scala/DataFrameAssignment/readme.md>)
    In this part you will focus on the basics of Spark SQL, and Spark Catalyst.


## About Spark.

In the lectures you learned about Spark and its core components. It is intended to process large
data volumes in a distributed fashion. 
Although, you will use a single laptop for the purpose of this assignment, 
doing so is not entirely representative of distributed processing in the real world since you do not have multiple nodes. 
However, you can still understand the underlying concept of Big Data Processing.

## Logging
The default logging is set to Info. However, this can be adjusted to *Warn* by
uncommenting lines 24 and 25 of tests.
```
Logger.getLogger("akka").setLevel(Level.WARN)
```


## Common Errors

1) ``scalac: Error: Error compiling the sbt component 'compiler-interface-2.11.8-59.0'``

- Solution:
    - Change Jdk to 1.8
    - Run Maven compile


2) ``Not enough spark.driver.memory``

- Solution:
    - Import project using maven
    - Run maven compile
    - Increase Idea heap memory

3) ``An exception or error caused a run to abort: Path does not exist:``

- Solution:
    - This project needs spark module as root and therefore does not recognize the path to data folder, this can be fixed by changing the relative path of data/data-raw.json to an absolute path.

4) ``Invalid Spark Url``
- Solution:
    - Set the hostname to 'localhost', this is included in the updated template.

5) ``Method not found error``
- Solution:
    - Run maven clean 
    - Invalidate caches
    - Run maven install
    - Run maven compile
  
## Grading


### Handing in your solutions

You must hand in your solutions on [Weblab](https://weblab.tudelft.nl).
Create a zip archive of the `src` folder and upload it to Weblab. The structure of the zip should be as follows:

* src
    * main
        * scala
            * utils
            * RDDAssignment
                * RDDAssignment.scala
            * DataFrameAssignment
                * DFAssignment.scala

### Automatic grading

After handing in your solutions, you can see the results of the automatic grading.
This only shows you the overall grade and which question(s) is/are incorrect,
but will not give any details about the failures.\
You are encouraged to write more unit tests yourself to test your solutions.
You can find some help on writing tests in [test/scala/DataFrameAssignment/StudentTest.scala](<src/test/scala/DataFrameAssignment/StudentTest.scala>).
IntelliJ's [evaluate expression](https://www.jetbrains.com/help/idea/evaluating-expressions.html) might be useful
for debugging to inspect during runtime.

**Warning**: The automatic grader may fail if you do any of the following actions:
- change the names of template files
- change function signatures
- use external dependencies that were not in the original `pom.xml` file
- hand in your solutions in another format than the one specified earlier
- remove any imports that are given in the templates

If you are in doubt regarding why the grader fails, ask the TAs for more details during the lab sessions.

### Programming style

The autograder only tests your solutions for correctness. No manual grading will be done, but
very slow solutions, or those with side effects might not pass on the autograders. Some assignments
state that certain operations may not be used, which will be taken into account by the graders,
as Spark allows RDD inspection on execution level by inspection of the `debugString`.

