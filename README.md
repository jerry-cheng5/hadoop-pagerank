# PageRank Hadoop Program
This is a MapReduce program that simulates Google's PageRank algorithm that is used to compute a 'popularity' score for webpages on the internet. Read more about the algorithm [here](https://en.wikipedia.org/wiki/PageRank#:~:text=PageRank%20(PR)%20is%20an%20algorithm,illustration%20of%20the%20Pagerank%20algorithm.).

## How to run
```
javac *.java
jar cfm PageRank.jar PageRank *.class
```
In Hadoop:
```
hadoop jar PageRank.jar PageRank [alpha] [iteration] [threshold] [infile] [outdir]
```

\[alpha\]: random jump factor

\[iteration\]: number of PageRank iterations

\[threshold\]: output will include nodes with PR value > threshold

\[infile\]: file describing graph structure with the format `nodeID, nodeID, weight` for each line

\[outdir\]: where the output file will be generated

## How it works

The PR value for each node is initialized as 1/|out neighbors|. 

Each iteration of the MapReduce program the PR value for each node is distributed equally to each of its out neighbors. 

The missing mass for dangling nodes is redistributed as well, along with the random jump factor.

This is repeated until the number of desired iterations is reached.