# Spark RDD代码阅读笔记

标签（空格分隔）： Spark Reading-Journal

---

RDD是Spark当中的核心数据结构，任何要在Spark当中计算的数据均可以抽象为RDD。RDD类似一张table，但是，通常的RDD是没有索引的，因此，其中的每一个元素只能够通过遍历的RDD的方式来访问。
RDD相关的代码Spark设计的很好，由于做了很好的抽象，代码结构非常清晰，很容易看懂。

# RDD类

在RDD中，不同的RDD之间只是在下面的方法上不同而已。在每一个RDD的子类当中，都分别重载了下面几个方法。
```scala
  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[T]
  // compute 是RDD元素计算发生的地方，从参数上可以看出来，每一个compute方法的调用分别对应着要计算这个rdd的一个partition


  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getPartitions: Array[Partition]
  // getPartition是在DAGScheduler生成LogicPlan的时候调用，如果一个RDD是resultRDD，那么partition的数量就对应着task的数量

  /**
   * Implemented by subclasses to return how this RDD depends on parent RDDs. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getDependencies: Seq[Dependency[_]] = deps
  // getDependencies在生成LogicPlan的时候调用，分析RDD之间的依赖关系

  /**
   * Optionally overridden by subclasses to specify placement preferences.
   */
  protected def getPreferredLocations(split: Partition): Seq[String] = Nil
  // 决定每个parition所在的位置

  /** Optionally overridden by subclasses to specify how they are partitioned. */
  @transient val partitioner: Option[Partitioner] = None
  // 在ShuffleMapTask中，partitioner用来将数据分片，默认采用hashPartition
```

RDD类当中包含了所有的Spark Action的接口和Spark Transformation的接口。由于接口数量很多，就挑选其中的几个代表做分析。
对于Spark Transformation来说，由于转换操作不会触发Job的提交，因此，转换操作的核心思想都是创建RDD之间的依赖关系。例如
```Scala
  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   */
  def map[U: ClassTag](f: T => U): RDD[U] = new MappedRDD(this, sc.clean(f))

  /**
   *  Return a new RDD by first applying a function to all elements of this
   *  RDD, and then flattening the results.
   */
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] =
    new FlatMappedRDD(this, sc.clean(f))
```
从上面的代码可以看到，核心的地方在于创建一个RDD的时候，即传入了他得parent RDD，又传入了一个运算函数。因此，child RDD可以利用parent RDD和传入的计算function来构造出compute，getPartition，getDependencies三个函数。

对于Action接口，由于会触发Job的生成和提交，因此，在调用Action函数的时候，会通过调用SparkContext的runJob接口向DAGScheduler提交job。
```scala
  /**
   * Applies a function f to all elements of this RDD.
   */
  def foreach(f: T => Unit) {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
  }

  /**
   * Applies a function f to each partition of this RDD.
   */
  def foreachPartition(f: Iterator[T] => Unit) {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => cleanF(iter))
  }
```

RDD类有很多的子类，每一个子类分别实现了一种transformation得到的RDD。子类如下图：
![RDD子类][1]



# RDD之间的dependency，即Lineage

由于在RDD创建的时候，都有parentRDD和相应的transformation function传入，因此，对于任何一个由Transformation生成的RDD，都可以根据在parentRDD的基础上计算出lineage关系。

# RDD是如何计算得到的

RDD的计算非常直观，RDD在计算的过程当中通过调用iterator方法生成一个迭代器，通过将transformation function应用到迭代器中的数据，逐渐将childRDD计算出来。
```scala
// 创建RDD的迭代器
  final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    if (storageLevel != StorageLevel.NONE) {
      SparkEnv.get.cacheManager.getOrCompute(this, split, context, storageLevel)
    } else {
      computeOrReadCheckpoint(split, context)
    }
  }
```
可以看到，在创建迭代器的时候，如果一个RDD已经被持久化到底层的存储系统，那么就根据inputformat将数据读取出来。如果一个RDD已经被计算过，存储到Memory当中，则直接从memory中读取出。如果RDD还未被计算过，或者以前计算的结果已经丢失了，那么则调用compute函数计算出来。

compute方法是计算RDD的核心，每个RDD类都会重载相应的compute函数，该函数要么将transformation function应用到每一个RDD的元素，计算出新的RDD。要么从存储系统中读取相应的数据构成RDD。
MapRDD是通过计算得到RDD的典型。
```scala
  override def compute(split: Partition, context: TaskContext) =
    firstParent[T].iterator(split, context).map(f)
```

BlockRDD是读取得到RDD的典型
```scala
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    assertValid()
    val blockManager = SparkEnv.get.blockManager
    val blockId = split.asInstanceOf[BlockRDDPartition].blockId
    blockManager.get(blockId) match {
      case Some(block) => block.data.asInstanceOf[Iterator[T]]
      case None =>
        throw new Exception("Could not compute split, block " + blockId + " not found")
    }
  }
```

  [1]: https://github.com/GordonWang/spark-reading-journal/blob/master/figures/RDDClasses.png
