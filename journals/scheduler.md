# Spark Scheduler代码阅读笔记

标签（空格分隔）： Spark

---

Spark的调度器主要分为三个层次。
- DAGScheduler，负责调度每个job生成的DAGstage执行
- TaskScheduler，负责调度Stage对应的taskset的执行
- BackEndScheduler，负责和后端的executor通信，维护executor的状态，以及分发task到executor

下面就分别来看三个层次的实现代码。


----------


## DAGScheduler

DAGScheduler 的核心逻辑都实现在DAGScheduler类当中。

在DAGScheduler当中，包含一个DAGSchedulerEventProcessActor，这个actor当中处理各种跟调度相关的事件，而事件的callback函数又是定义在DAGScheduler类当中的方法。

在DAGSchedulerEventProcessActor的receive方法中，包含了所有的事件处理的定义，下面是事件处理的代码。这个代码是看懂DAGScheduler的入口。每个事件的命名非常直观。

```scala
    case JobSubmitted(jobId, rdd, func, partitions, allowLocal, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, allowLocal, callSite,
        listener, properties)

    case StageCancelled(stageId) =>
      dagScheduler.handleStageCancellation(stageId)

    case JobCancelled(jobId) =>
      dagScheduler.handleJobCancellation(jobId)

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId) =>
      dagScheduler.handleExecutorLost(execId)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion @ CompletionEvent(task, reason, _, _, taskInfo, taskMetrics) =>
      dagScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
```

下面就按照事件来进一步分解DAGScheduler当中重要的一些代码路径。

- handleJobSubmitted是提交job的入口。简要的来看，JobSubmit之后，需要调用的函数如下：
```scala
DAGScheduler::handleJobSubmitted
finalStage = newStage(finalRDD, partitions.size, None, jobId, callSite)  // 计算这个job的最后一个stage，这个函数会递归的计算finalStage依赖的parent stage，并且创建这些依赖的stage

val job = new ActiveJob(jobId, finalStage, func, partitions, callSite, listener, properties) //新建一个activejob

submitStage(finalStage) //提交finalStage，这个函数会递归向前检查finalStage依赖的stage是否已经成功运行，如果没有，那么会先提交依赖的stage
```
如果一个stage依赖的stage都已经完成，那么调用函数``DAGScheduler::submitMissingTasks``将当前需要计算的stage提交，在提交stage的时候，会调用底层的TaskScheduler提交Tasks。

- ``handleStageCancellation``取消已经提交的Stage，如果Stage是已经提交或者已经完成的stage，则调用``handleJobCancellation``取消掉所有包含或者依赖这个stage的job。

- ``handleJobCancellation``取消已经提交或者完成的job。

- ``handleExecutorAdded``用于处理添加executor的事件

- ``handleExecutorLost``用于处理executor丢失的事件，当executor丢失的时候，需要更改scheduler内部的状态。

- ``handleTaskCompletion``在task完成的时候，用于更新scheduler内部的状态。TaskCompletion的状态可以有多种可能，在函数内部通过一个``match case``的分类来分别处理。
```scala
case Success  // 任务成功完成，其中会更新stage和job的状态
case Resubmitted // 重新提交的任务
case FetchFailed // 如果出现fetchFail，则会停止当前的reduce stage，重新提交相应的shuffleMapStage
```


----------


## TaskScheduler

TaskScheduler是任务调度器，当DAGScheduler将一个Stage转换成为一个TaskSet的时候，就会提交到TaskScheduler当中。目前TaskScheduler的大部分逻辑都存在于``TaskSchedulerImpl``类当中。
在TaskSchedulerImpl当中，比较重要的函数是
```scala
  override def submitTasks(taskSet: TaskSet) // 提交一个TaskSet到scheduler，然后scheduler会根据调度的策略，将task通过backendScheduler分发到各个executor当中执行。
  
  def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] // 根据cluster manager给出的一系列executor节点上面的可用资源offer，按照相应的调度算法FIFO/Fair，将activeTaskSet当中的task调度到相应的executor上，返回的TaskDescription包含了可以执行的task的信息以及task要执行的位置。返回值是一个list，list的index含义对应着输入的resourceOffer的index。
```

在Spark的实现当中，不同的调度算法（Fair/FIFO）是实现在TaskScheduler当中的。在``TaskSchedulerImpl``当中，用如下两个结构实现了对active task的排序调度。
```scala
  var schedulableBuilder: SchedulableBuilder = null
  var rootPool: Pool = null
```

### TaskSetManager
在TaskSchedulerImpl中，每个stage对应的taskset都会生成一个TaskSetManager对象实例，然后TaskSetManager会被放入调度池（Pool）当中。在TaskScheduler根据ResourceOffer调度task的时候，调用的正是TaskSetManager中的接口。

TaskManager的主要功能在于对Task的细粒度调度，比如
1. 为了达到Locality aware，将Task的调度做相应的延迟。
2. 当一个Task失败的时候，在约定的失败次数之内时，将Task重新提交。
3. Task speculative 执行。

下面是TaskSetManager中一些重要的方法。
```scala
  def handleSuccessfulTask(tid: Long, result: DirectTaskResult[_]) // 当一个Task执行成功以后，调用该函数更新Task状态，并且将Task成功的事件通知DAGScheduler
  
  def handleFailedTask(tid: Long, state: TaskState, reason: TaskEndReason) // 当一个Task失败之后，调用该函数修改Task的状态并且通知DAGScheduler这个失败的Task的状态
  
  override def executorLost(execId: String, host: String) // 当executor跟BackEndScheduler失去联系之后，调用这个方法，将Executor上面的pending task重新分配到不同的executor中，将Executor上已经执行完成的ShuffleMapTask重新提交执行，将正在Executor上执行的Task标记为Fail，重新执行
  
  override def checkSpeculatableTasks(): Boolean // 周期性的会被Scheduler调用的方法，如果TaskSet当中包含有长时间运行的Task，则对这些task启用speculative execution
  
  def resourceOffer(execId: String, host: String, maxLocality: TaskLocality.TaskLocality) : Option[TaskDescription] // Scheduler将ResourceOffer传给该方法，这个方法会根据ResourceOffer所在的executor和host，已经task的locality特性，决定那个Task会执行。在这个函数中，会通知DAGScheduler，一个task已经开始执行。


```

### SchedulableBuilder
SchedulableBuilder主要实现不同的调度算法，现在的算法包括FIFO和Fair。
因此，SchedulableBuilder有两个子类，FIFOSchedulableBuilder和FairSchedulableBuilder。他们分别实现了两个不同的调度算法。
SchedulableBuilder的定义如下，非常直观的包括3个很重要的方法。
```scala
/**
 * An interface to build Schedulable tree
 * buildPools: build the tree nodes(pools)
 * addTaskSetManager: build the leaf nodes(TaskSetManagers)
 */
private[spark] trait SchedulableBuilder {
  def rootPool: Pool

  def buildPools()  //Spark当中的调度实现跟Yarn当中的实现是类似的，也是采用树来表述调度的层级，其中叶子节点是可以调度的单元。

  def addTaskSetManager(manager: Schedulable, properties: Properties)
}
```

Spark中的FIFO调度实现的非常简单，就是将rootPool作为一个队列，根据TaskSetManager提交的先后顺序一次调度Task。
```scala
private[spark] class FIFOSchedulableBuilder(val rootPool: Pool)
  extends SchedulableBuilder with Logging {

  override def buildPools() {
    // nothing
  }

  override def addTaskSetManager(manager: Schedulable, properties: Properties) {
    rootPool.addSchedulable(manager)
  }
}
```

Spark的FairScheduler相对于Yarn来说也非常简单，rootPool下面只会包含一层节点，这层节点并非叶子节点，TaskSetManager作为叶子节点挂在第二层节点上面，因此，总共树的深度是3层。
```scala
  val schedulerAllocFile = conf.getOption("spark.scheduler.allocation.file")
  val DEFAULT_SCHEDULER_FILE = "fairscheduler.xml" // 跟Yarn的fairscheduler一样，spark通过加载一个xml配置文件来决定fairscheduler当中的队列。
  
```

```scala
    //在提交stage的时候，如果parent的pool不存在，那么会动态生成一个pool，stage提交到哪个pool由  val FAIR_SCHEDULER_PROPERTIES = "spark.scheduler.pool" 来决定

      if (parentPool == null) {
        // we will create a new pool that user has configured in app
        // instead of being defined in xml file
        parentPool = new Pool(poolName, DEFAULT_SCHEDULING_MODE,
          DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
        rootPool.addSchedulable(parentPool)
        logInfo("Created pool %s, schedulingMode: %s, minShare: %d, weight: %d".format(
          poolName, DEFAULT_SCHEDULING_MODE, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
      }
```

### Pool
Pool的定义类似Yarn当中的schedulerableQueue的定义。它可以包含一些子Pool或者是包含一些TaskSetManger的对象。
在Pool中，最核心的方法为
```scala
  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    val sortedSchedulableQueue =
      schedulableQueue.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
    }
    sortedTaskSetQueue
  }
```
这个方法将Pool当中可调度的对象做排序，排序的算法由taskSetSchedulingAlgorithm来实现。排序的算法决定了调度的算法。
```scala
  var taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()
    }
  }
```

----------


## BackendScheduler
 BackendScheduler主要负责跟cluster master通信，收集executor的信息，将上层TaskScheduler调度好的Task发送到相应的executor去执行，并且收集task执行的结果反馈给上层的TaskScheduler。
BackEndScheduler都是从一个叫``SchedulerBackend``的基类集成得到。
对于CoarseGrainMode的调度器，主要逻辑由``CoarseGrainedSchedulerBackend``实现。
先看``CoarseGrainedSchedulerBackend``中包含的DriverActor，这个Actor主要负责跟cluster manager通信以及跟后端的所有executor通信。
``DriverActor``中，最核心的方法是消息处理的函数。它能够处理的消息包括
```scala
      case RegisterExecutor(executorId, hostPort, cores) => // executor启动以后会向driver注册
      case StatusUpdate(executorId, taskId, state, data) => // 任务状态的更新
      case ReviveOffers =>  // ReviveOffers事件是被周期性触发的，默认1秒一次
      case KillTask(taskId, executorId, interruptThread) => // kill一个task
      case StopDriver => 
      case StopExecutors =>
      case RemoveExecutor(executorId, reason) =>  // 剔除一个executor，提供删除的原因
      case AddWebUIFilter(filterName, filterParams, proxyBase) =>
      case DisassociatedEvent(_, address, _) =>
      case RetrieveSparkProps =>
```
```scala
    def makeOffers() // make offers 非常重要，这个函数将cluster的资源以资源offer的方式发给上层的scheduler，获取调度得到的应该被执行的TaskDescription，然后调用launchTasks去将任务分发到相应的executor执行
    
    def launchTasks(tasks: Seq[Seq[TaskDescription]]) 
```
1. SparkDeploySchedulerBackend：继承自CoarseGrainedSchedulerBackend，负责在StandAlone Spark集群mode下执行调度。
2. YarnClusterSchedulerBackend：继承自CoarseGrainedSchedulerBackend，负责在yarn cluster mode下执行调度。
3. YarnClientSchedulerBackend：继承自CoarseGrainedSchedulerBackend，负责在yarn client mode下执行调度。
4. CoarseMesosSchedulerBackend：继承自CoarseGrainedSchedulerBackend，负责在mesos coarse grain mode下执行调度。
以上四个子类的核心逻辑都在父类中实现了，他们在实现start和stop的时候略有不同。



