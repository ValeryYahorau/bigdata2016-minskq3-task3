# bigdata2016-minskq3-task3
Task3: MapReduce jobs


###STEP 1 
Build project
```
mvn clean install
```

###STEP 2 
Run tags count with next command
```
yarn jar ~/bigdata2016-minskq3-task3/target/bigdata2016-minskq3-task3-1.0.0-jar-with-dependencies.jar com.epam.bigdata2016.minskq3.task3.TagsCount <in> <out> <inoptional>
```
Where

`in` - path to input file,

`out` - path for output,

`inoptional` - optional parameter, path to file with stop/bad words (1 word in 1 line).

e.g. 

yarn jar /root/Documents/bigdata2016-minskq3-task3/target/bigdata2016-minskq3-task3-1.0.0-jar-with-dependencies.jar com.epam.bigdata2016.minskq3.task3.TagsCount /tmp/admin/in1.txt /tmp/admin/ou1.txt 

###STEP 3 
Run visit counts and sum of bidding price with next command
```
yarn jar ~/bigdata2016-minskq3-task3/target/bigdata2016-minskq3-task3-1.0.0-jar-with-dependencies.jar com.epam.bigdata2016.minskq3.task3.VisitsSpendsCount <in> <out>
```
Where 

`in` - path to input file/directory,

`out` - path for output.

e.g. 

yarn jar /root/Documents/bigdata2016-minskq3-task3/target/bigdata2016-minskq3-task3-1.0.0-jar-with-dependencies.jar com.epam.bigdata2016.minskq3.task3.VisitsSpendsCount /tmp/admin/in2.txt /tmp/admin/ou2.txt


###Notes
- Was used distributed cache for optional input for stop/bad words in tags count job.
- Was used output as Sequence file with Snappy compression for output in visits spends count job.
- Was implememnted custom implementation of WritableComparable `VisitSpendComparable` for visits spends count job.
- For parsing User Agent was used [https://github.com/HaraldWalker/user-agent-utils](https://github.com/HaraldWalker/user-agent-utils).
- Unit tests for both jobs were implemeted.
- Also its possible to run jobs and unit tests like simple java applications.

