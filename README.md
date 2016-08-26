hadoop-invertedIndex-proto
========================
项目介绍
------------------------
本项目包含两个hadoop作业，一个用来解析文本，生成第一级数据，然后留给第二级hadoop作业来完成合并。
*NOTICE*
=================
你应该在使用hadoop的Combiner之前认识它的运行机制，combiner可能根本不会运行，或者在map之后和reduce之后多次运行。

