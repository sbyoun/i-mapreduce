For PageRank example, if you would like to use some other datasets, please first transform the input data format to the correct one. iMapReduce requires the input format should be like this:
src1       dest1 dest2 dest3
src2       dest1 dest4
src3       dest2 dest4
src4       dest1

There are two requirement:
  * The src ids should be continuous
  * The src ids should cover all the appeared ids, that is if there are 1000 nodes (including src and dest) there should be 1000 src ids covering all the node ids.

