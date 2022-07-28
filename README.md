# Filtered Jstat
Filtered Jstat graphs Old Storage, full garbage collection occurances, and full garbage collection time for all applicable Processes in all instances in all running EMR clusters. 

## Description
*Main.py* runs jstat_starter, sleeps for one hour and extracts the jstat output from each instance in an endless loop. In order to graph O, FGCT, and FGC, *grapher.py* should be run.

If desired, all jstat processes can be killed by running *jstat_killer.py*. 
___
### Getting Started 
In order to use Filtered Jstat, there must be at least one running EMR Cluster with instance(s) and the security group must have ssh/port 22 available.
___
### Executing Program
within *jstat_capture.py* `SEARCH_TERM` should be changed to reflect the desired processes. 

within *grapher.py* and *extractor.py* the `CHANGE_PATH` variable should be changed to reflect the parent directory to which *extractor.py* writes to and *grapher.py* reads from.

within *grapher.py* the `SAVE_PATH` variable should be changed to reflect the desired location to save the graph.png file.
___
### Graph Output
![expected output](/graph/refined_jstat.png)
