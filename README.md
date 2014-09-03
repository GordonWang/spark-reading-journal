# Spark源码读书笔记

标签（空格分隔）： Spark Reading-Journal

---

最近终于下定决心开始阅读Spark的源代码，网上对Spark的原理分析的文章很多，但是我看到的最好的原理分析的文章是[Spark Internal][1]。

我希望自己能用下面一系列的读书笔记记录整个代码阅读的时候的收获和思路，也方便日后自己回顾的时候能够快速的回忆。每个读书笔记偏重于记录源代码实现层面的分析，原理方面讲得少些，笔记主要覆盖一些关键路径的代码。建议要阅读这些笔记的同学能够先阅读Spark相关的论文和文档，熟悉基本的原理和概念。当然，由于spark是采用scala实现的，因此在阅读源码之前，推荐能学习一些scala的基本语法。

- [Spark Scheduler相关代码阅读笔记][2]
- [Spark RDD相关代码阅读笔记][3]
- [Spark Stand Alone模式][4]


  [1]: https://github.com/JerryLead/SparkInternals/tree/master/markdown
  [2]: https://github.com/GordonWang/spark-reading-journal/blob/master/journals/scheduler.md
  [3]: https://github.com/GordonWang/spark-reading-journal/blob/master/journals/RDD.md
  [4]: https://github.com/GordonWang/spark-reading-journal/blob/master/journals/stand-alone-deploy.md
