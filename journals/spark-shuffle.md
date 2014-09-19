# Spark Shuffle部分的读书笔记

标签（空格分隔）： Spark Reading-Journal

---

Shuffle是Spark和Mapreduce这类数据处理程序当中非常重要也是非常核心的逻辑。shuffle效率的高低直接影响着程序的性能。

# Spark的shuffle的种类

## Hash Shuffle
HashShuffle的逻辑在HashShuffleManager中实现。HashShuffleManager实现了ShuffleManager的接口。HashShuffleManager创建出HashShuffleReader和HashShuffleWriter。在HashShuffle当中，同一个shuffle输出文件当中的key-value对不是有序的，在reducer输入的时候，同样的key的数据也并非相邻，因为数据在写入shuffle file的时候的是直接哈希完写入的，因此，并没有保证相同key的数据要相邻写入。

## Sort Based Shuffle
Sort Based Shuffle的主要逻辑在SortShuffleManager中实现。SortShuffleManager实现了ShuffleManager得接口。SortShuffleManager创建出HashShuffleReader和SortShuffleWriter。由于Sorted Based Shuffle是在shuffle map task当中，写入Shuffle数据的时候，先根据key进行排序。但是在reduer读取数据的时候，是可以按照hash based shuffle的逻辑对partition进行划分的。


# shuffle相关的类
在shuffleTask当中，计算shuffleTask的核心逻辑如下
```scala
      val manager = SparkEnv.get.shuffleManager
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      return writer.stop(success = true).get
```
在计算shuffleRDD的时候，相关的类包括ShuffleManager，ShuffleDependency，ShuffleWriter，ShuffleReader以及他们的子类。

## ShuffleManager
- ShuffleManager是trait，在driver和executor当中都有，在driver当中注册shuffle，在executor当中调用创建ShuffleWriter和ShuffleReader。不同类型的shuffle分别实现不同类型的ShuffleManager。

## ShuffleReader
- ShuffleReader在reducer task当中，读取shuffle以后的结果。ShuffleReader存在于ShuffledRDD当中，当计算ShuffleRDD的时候，compute方法会获取一个ShuffleReader从blockmanager获取shuffle data。

## ShuffledRDD
ShuffleRDD的compute函数如下
```scala
  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
      .read()
      .asInstanceOf[Iterator[(K, C)]]
  }
```
在shuffleRDD计算partition的时候，会获取一个HashShuffleReader，读取ShuffleData。

## ShuffleWriter
- ShuffleWriter在shuffleMapTask当中，将计算出来的数据写入Shuffle的缓存。

### HashShuffleWriter
在HashShuffleWriter中，writer方法中通过调用blockmanager上的接口得到一个write group，进而将RDD中的KV对写入write group。
```scala
  override def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    val iter = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        dep.aggregator.get.combineValuesByKey(records, context)
      } else {
        records
      }
    } else if (dep.aggregator.isEmpty && dep.mapSideCombine) {
      throw new IllegalStateException("Aggregator is empty for map-side combine")
    } else {
      records
    }

    for (elem <- iter) {
      val bucketId = dep.partitioner.getPartition(elem._1)
      shuffle.writers(bucketId).write(elem)
    }
  }
```

## SortShuffleWriter
SortShuffleWriter在ShuffleMapper当中，对mapper输出的key-value对进行排序。然后输出到一个文件当中。一个mapper对应一个输出文件，在输出文件当中不同的block对应着不同的reducer。
write方法是Writer的核心逻辑
```scala
  override def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    if (dep.mapSideCombine) {
      if (!dep.aggregator.isDefined) {
        throw new IllegalStateException("Aggregator is empty for map-side combine")
      }
      // 使用externalSorter来进行数据的排序，在排序的过程中，如果有mapper端的combine，则需要有aggregator
      sorter = new ExternalSorter[K, V, C](
        dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
      sorter.insertAll(records)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      sorter = new ExternalSorter[K, V, V](
        None, Some(dep.partitioner), None, dep.serializer)
      sorter.insertAll(records)
    }

//生成一个文件来存储mapper的输出
    // Create a single shuffle file with reduce ID 0 that we'll write all results to. We'll later
    // serve different ranges of this file using an index file that we create at the end.
    val blockId = ShuffleBlockId(dep.shuffleId, mapId, 0)

    outputFile = blockManager.diskBlockManager.getFile(blockId)
    indexFile = blockManager.diskBlockManager.getFile(blockId.name + ".index")

// externalSorter将结果输出到outputFile当中
    val partitionLengths = sorter.writePartitionedFile(blockId, context)

    // Register our map output with the ShuffleBlockManager, which handles cleaning it over time
    blockManager.shuffleBlockManager.addCompletedMap(dep.shuffleId, mapId, numPartitions)

    mapStatus = new MapStatus(blockManager.blockManagerId,
      partitionLengths.map(MapOutputTracker.compressSize))
  }
```

## HashShuffleReader
HashShuffleReader在reducer task中，将数据从远端的接收到本地。然后构造一个iterator提供给reducer task做输入。

## ExternalSorter
ExternalSorter是一个外部排序的实现，在Shuffle的中间输出文件中，对key-value对进行排序，然后输出到指定的文件当中。外部排序使用mergeSort。
```scala
  private val bypassMergeThreshold = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
  private val bypassMergeSort =
    (numPartitions <= bypassMergeThreshold && aggregator.isEmpty && ordering.isEmpty)
```
ExternalSorter中，在满足一定条件的时候，sorter会bypass merge sort。在by pass merge sort的时候，不会对spill的文件进行排序，并且只是将每一个partition对应的数据输出到一个文件当中。由于reducer在获取shuffle data以后都必须进行merge排序，因此mapper端是可以省略merge排序的。
by pass merge的条件是同时满足以下条件
- partition数量较少
- 没有combiner函数
- 没有orderer函数

但是by pass merge以后，缺点是每一个mapper task会生成partition个文件。会导致文件数量较多。

## ShuffleMemoryManager
- ShuffleMemoryManager管理着一个executor当中不同的shuffletask的内存使用，保证内存在各个shuffle task之间可以公平的被使用。防止先启动的task将memory过度占用。MemoryManager保证了当有N个thread的时候，每一个thread只会被分配最多1/N * capacity的内存。但是，每个thread至少可以分配1/2N * capacity的内存。

## BlockManager
BlockManager是spark中很重要的一个类，它负责管理所有的数据存储相关的功能。例如：broadcast变量对应的数据存储，shuffle map输出的中间数据集的存储。
在shuffle的过程中，所有和local filesystem交互的时候，都需要调用BlockManager的接口生成ObjectWriter，用于将shuffle的中间数据写入磁盘。
```scala
  def getDiskWriter(
      blockId: BlockId,
      file: File,
      serializer: Serializer,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetrics): BlockObjectWriter = {
    val compressStream: OutputStream => OutputStream = wrapForCompression(blockId, _)
    val syncWrites = conf.getBoolean("spark.shuffle.sync", false)
    new DiskBlockObjectWriter(blockId, file, serializer, bufferSize, compressStream, syncWrites,
      writeMetrics)
  }
```

在Reducer task的Reader中，BlockManager负责从远端的BlockManager中读取相应的ShuffleData。HashShuffleReader将读取到的ShuffleData输入给Reducer Task。


## ShuffledBlockManager
在ShuffledBlockManager当中，forMapTask方法返回一个writer group，writer group会处理每一个Map Task生成的KV对。将他们写入BlockManager。共reducer后续使用。
```scala
  def forMapTask(shuffleId: Int, mapId: Int, numBuckets: Int, serializer: Serializer,
      writeMetrics: ShuffleWriteMetrics)
```
ShuffledBlockManager维护了各个Shuffle的状态。以及每个shuffle过程当中的文件信息。

## ShuffleFileGroup
ShuffleFileGroup是一组文件，其中文件的个数等于reducer的个数，每个File Group都属于某一个Shuffle过程，每一个文件包含了一个reducer的输入，其中文件分为若干个block。每个block对应一个map生成的数据。因此，在一个FileGroup中，给定一个二元组(ReducerID, MapID),就一定可以找到一个File Block对应。File Block由File，Offset，Length决定。
ShuffleFileGroup主要的目的在于减少Shuffle过程中的文件的数量。在不使用File Group的时候，每一个MapTask会生成R个文件，R为reducer的数量。因此，当mapper数量较大，且Reducer数量较大的时候，会导致磁盘文件太多，因此performance下降。使用FileGroup以后，多个Mapper的输出都会被整合到一个同一个FileGroup当中，有效的减少了文件的数量。

## DiskBlockObjectWriter
在Shuffle过程中，Writer Group中包含的writer都是DiskBlockObjectWriter实例。DiskBlockObjectWriter主要负责将mapper输出的object序列化以后持久化到File当中。DiskBlockObjectWriter可以在一个已经存在的File上面构建，一个writer完成一系列的写入之后，可以构成一个block。
一个writer只有在commitAndClose以后才可以知道一次写入的字节数。在每个mapper结束的时候，都会调用commitAndClose来提交一个mapper对应的block。
```scala
  override def commitAndClose(): Unit = {
    if (initialized) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      objOut.flush()
      bs.flush()
      updateBytesWritten()
      close()
    }
    finalPosition = file.length()
  }
```
write方法将MapperTask生成的object写入文件当中。
```Scala
  override def write(value: Any) {
    if (!initialized) {
      open()
    }

    objOut.writeObject(value)

    if (writesSinceMetricsUpdate == 32) {
      writesSinceMetricsUpdate = 0
      updateBytesWritten()
    } else {
      writesSinceMetricsUpdate += 1
    }
  }
```

## Aggregator
Aggregator完成数据的combine。它存在于ShuffleMap端，将数据做map end combine。
```scala
case class Aggregator[K, V, C] (
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C)
```
从aggregator的构造函数可以看出，它的功能主要是实现value的combine，已经combined value的merge。createCombiner是初始化combined value的函数，mergeValue是combine函数，mergeCombiners是merge combined value的函数。
combine的过程中，主要使用AppendOnlyMap和ExternalAppendOnlyMap两个数据结构来存储计算的中间状态。

## AppendOnlyMap
AppendOnlyMap实现了一个内存hash表，当数据量不大的时候，用内存当中的hash表来存储key-value对。AppendOnlyMap提供的changeValue方法是一个特殊的函数。类似的函数在ExternalAppendOnlyMap当中也存在。
```scala 
// 当map当中不存在key对应的value的时候，调用函数updateFunc(false, null)去计算
// 得到初始的value，如果key在map中存在，那么调用updateFunc(true, value)去更新
// map中的value。这个函数对于combiner的实现非常重要。
def changeValue(key: K, updateFunc: (Boolean, V) => V): V
```

## ExternalAppendOnlyMap
ExternalAppendOnlyMap用于收集key-value对，其中包含一个SizeTrackingAppendOnlyMap的对象，SizeTrackingAppendOnlyMap是一个在内存当中的map，所有append进来的对象都会先存储到这个map当中，当内存使用超过一定的阈值之后，内存当中的map会被spill到磁盘当中，map的数据是按照key的hash值排序以后输出到文件的。最后，在读取ExternalAppendOnlyMap的数据的时候，会将Spill以后的数据和在内存当中的数据做MergeSort以后输出。
```scala 
  private class HashComparator[K] extends Comparator[K] {
    def compare(key1: K, key2: K): Int = {
      val hash1 = hash(key1)
      val hash2 = hash(key2)
      if (hash1 < hash2) -1 else if (hash1 == hash2) 0 else 1
    }
  }
```





