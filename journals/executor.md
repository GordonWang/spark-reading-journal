# Spark Executor任务执行

标签（空格分隔）： Spark Reading-Journal

---

# Executor的工作流程

Spark executor是执行task的核心单元，每一个job后面都对应着若干个executor。这些executor在job执行的过程当中都一直存在。

Spark的Executor入口都实现了ExecutorBackend trait。
executor的主要逻辑都在类org.apache.spark.executor.Executor中。
Executor中以下函数分别对应launch/kill task。所有的Task都会被放入一个线程池来执行。```runningTasks```是用于记录已经被提交到线程池的task，是一个Map。
```scala
  def launchTask(
      context: ExecutorBackend, taskId: Long, taskName: String, serializedTask: ByteBuffer) {
    val tr = new TaskRunner(context, taskId, taskName, serializedTask)
    runningTasks.put(taskId, tr)
    threadPool.execute(tr)
  }

  def killTask(taskId: Long, interruptThread: Boolean) {
    val tr = runningTasks.get(taskId)
    if (tr != null) {
      tr.kill(interruptThread)
    }
  }
```


# Executor的入口

Executor的入口都在BackEndExecutor类当中。Spark的BackEndExecutor有两类，一类是Coarse Grain， 一类是Fine Grain。
1. Fine Grain Executor：```MesosExecutorBackend```
2. Goarse Grain Executor：```CoarseGrainedExecutorBackend```

- MesosExecutorBackend实现了Mesos的Executor接口，所有的Task是通过Mesos的Slave传递给Executor Driver。Task的状态更新也是通过Executor Driver发送给Slave，然后传递到Mesos Master，再传递回App Driver的。MesosExecutorBackend中包含一个Executor的对象，Task的执行都是在Executor对象中进行的。
```scala
  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    val mesosTaskId = TaskID.newBuilder().setValue(taskId.toString).build()
    driver.sendStatusUpdate(MesosTaskStatus.newBuilder()
      .setTaskId(mesosTaskId)
      .setState(TaskState.toMesos(state))
      .setData(ByteString.copyFrom(data))
      .build())
  }
  
    override def registered(
      driver: ExecutorDriver,
      executorInfo: ExecutorInfo,
      frameworkInfo: FrameworkInfo,
      slaveInfo: SlaveInfo) {
    logInfo("Registered with Mesos as executor ID " + executorInfo.getExecutorId.getValue)
    this.driver = driver
    val properties = Utils.deserialize[Array[(String, String)]](executorInfo.getData.toByteArray)
    executor = new Executor(
      executorInfo.getExecutorId.getValue,
      slaveInfo.getHostname,
      properties)
  }

  override def launchTask(d: ExecutorDriver, taskInfo: TaskInfo) {
    val taskId = taskInfo.getTaskId.getValue.toLong
    if (executor == null) {
      logError("Received launchTask but executor was null")
    } else {
      executor.launchTask(this, taskId, taskInfo.getName, taskInfo.getData.asReadOnlyByteBuffer)
    }
  }

```

- CoarseGrainedExecutorBackend在Stand alone，Yarn和Mesos的模式中都可以存在。CoarseGrainedExecutor通过AKKA直接和AppDriver通信，注册到AppDriver。CoarseGrainedExecutorBackend中也有一个Executor对象，Task的执行都在Executor对象当中。BackEndExecutor只是负责跟AppDriver通信。CoarseGrainedExecutorBackend是一个Actor。主要逻辑都在事件处理中。
```scala
  override def receiveWithLogging = {
    case RegisteredExecutor =>
      logInfo("Successfully registered with driver")
      // Make this host instead of hostPort ?
      executor = new Executor(executorId, Utils.parseHostPort(hostPort)._1, sparkProperties,
        false)

    case RegisterExecutorFailed(message) =>
      logError("Slave registration failed: " + message)
      System.exit(1)

    case LaunchTask(data) =>
      if (executor == null) {
        logError("Received LaunchTask command but executor was null")
        System.exit(1)
      } else {
        val ser = SparkEnv.get.closureSerializer.newInstance()
        val taskDesc = ser.deserialize[TaskDescription](data.value)
        logInfo("Got assigned task " + taskDesc.taskId)
        executor.launchTask(this, taskDesc.taskId, taskDesc.name, taskDesc.serializedTask)
      }

    case KillTask(taskId, _, interruptThread) =>
      if (executor == null) {
        logError("Received KillTask command but executor was null")
        System.exit(1)
      } else {
        executor.killTask(taskId, interruptThread)
      }

    case x: DisassociatedEvent =>
      logError(s"Driver $x disassociated! Shutting down.")
      System.exit(1)

    case StopExecutor =>
      logInfo("Driver commanded a shutdown")
      executor.stop()
      context.stop(self)
      context.system.shutdown()
  }
```
这部分代码可以结合CoarseGrainedSchedulerBackend来看。事件都是发送给CoarseGrainedSchedulerBackend来处理。

# TaskRunner
TaskRunner实现了Runnable接口，run是task执行的核心逻辑。在执行task的时候，TaskRunner会先下载缺失的文件和jar包。
```scala
  private def updateDependencies(newFiles: HashMap[String, Long], newJars: HashMap[String, Long])
```
然后反序列化Task，执行Task，收集Task执行的结果。
```scala
        // Run the actual task and measure its runtime.
        taskStart = System.currentTimeMillis()
        val value = task.run(taskId.toInt)
        val taskFinish = System.currentTimeMillis()
        ......
        ......
                val directResult = new DirectTaskResult(valueBytes, accumUpdates, task.metrics.orNull)
        val serializedDirectResult = ser.serialize(directResult)
        val resultSize = serializedDirectResult.limit

        // directSend = sending directly back to the driver
        val (serializedResult, directSend) = {
          if (resultSize >= akkaFrameSize - AkkaUtils.reservedSizeBytes) {
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId, serializedDirectResult, StorageLevel.MEMORY_AND_DISK_SER)
            (ser.serialize(new IndirectTaskResult[Any](blockId)), false)
          } else {
            (serializedDirectResult, true)
          }
        }
```
收集得到Task执行的结果，如果结果序列化之后大于AKKA framesize，则放入blockmanager当中，scheduler后续从blockmanager拿结果，否则结果直接返回给scheduler。