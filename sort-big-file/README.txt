SORT BIG FILE PROJECT
==================================================================
- Run using Intellij IDEA and Java 8

Required Dependencies:
- Tests were written in JUnit 4.12
- OpenCSV 5.1 

- Both projects (Serial and Parallel) has some basic tests
- Currently the algorithm does not supports FileSize > MemorySize^2, 
  there is failing test for that

I chose to parallelize the sort algorithm of each chunk, 
and loading records from each sorted file.

In two projects has 4 data Records for tests under 'data' folder:
- 24 Records.csv
- 100 Records.csv
- 1000 Records.csv
- 10000 Records.csv



Linor Dolev.