# StandAlone Deploy模式下的Master和Slave

标签（空格分隔）： Spark Reading-Journal

---

在Spark Stand alone的部署模式下，需要启动两类daemon程序
1. Spark master: 一个集群只有一个master，负责app的注册和接收
2. Spark slave: 一个集群有若干个slave，每个slave负责在App注册成功之后，启动BackEndExecutor

## Spark Master

org.apache.spark.deploy.master.Master是master进程的入口。
由于Master是一个Actor类，因此，所有的事件处理都在下面的函数中。
```scala
  override def receiveWithLogging 
```
下面分别看相应的事件处理，事件处理的逻辑都很直白。

- 处理LeaderElection相关的事件。由于Spark的Master提供了HA，状态持久化存储可以放到Zookeeper，也可以放到NFS当中。因此，有两个状态持久Engine，分别是```ZooKeeperPersistenceEngine```和```FileSystemPersistenceEngine```。Master将Driver的状态，Worker的状态和App的状态都存储下来。
```scala
    case ElectedLeader => 
```

- 如果master不再是leader，则退出。
```scala
    case RevokedLeadership => 
```

- 当slave节点上面的worker进程启动以后，会向master注册。
```scala
    case RegisterWorker(id, workerHost, workerPort, cores, memory, workerUiPort, publicAddress)
```

- 当client连接到master的时候，会向master注册driver程序。
```scala
case RequestSubmitDriver(description) =>
```

- kill一个已经提交的driver
```scala
case RequestKillDriver(driverId) =>
```

- driver 启动以后，会向master注册一个app
```scala
case RegisterApplication(description) =>
```

- 当一个executor的状态改变的时候，master会向对应的driver更新executor的状态。
```scala
case ExecutorStateChanged(appId, execId, state, message, exitStatus) =>
```
- driver退出或者被kill之后，会更新driver的状态。
```scala
case DriverStateChanged(driverId, state, exception) =>
```
- 处理worker进程和master之间的心跳。
```scala
case Heartbeat(workerId) =>
```

- 当worker reregister到master的时候，会向master重新发送当前worker上面包含的driver，executor的信息。
```scala
case WorkerSchedulerStateResponse(workerId, executors, driverIds) =>
```

- master 会周期性的检查worker，如果worker的心跳没有收到(默认timeout是60秒)，则剔除worker
```scala
    case CheckForWorkerTimeOut => {
      timeOutDeadWorkers()
    }
```
```scala
  /** Check for, and remove, any timed-out workers */
  def timeOutDeadWorkers() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT).toArray
    for (worker <- toRemove) {
      if (worker.state != WorkerState.DEAD) {
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          worker.id, WORKER_TIMEOUT/1000))
        removeWorker(worker)
      } else {
        if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT)) {
          workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
        }
      }
    }
  }
```

下面这个函数将在队列头部的waitingApp取出，根据目前cluster已有的cpu core的数量，在worker node上启动相应的executor。如果spark.deploy.spreadOut设置为true，那么将均匀的把executor分布到cluster中，否则，会尽可能将executor调度到少量节点。
schedule的时候，如果有supervised的driver，则优先在worker node上面启动driver进程。
```scala
private def schedule()
```

## Spark Master HA

Spark Master的HA由LeaderElectionAgent实现。一个Agent是一个actor(其实Agent没有必要是actor，因为它不需要处理任何的message)，agent通过给master actor发送ElectedLeader消息和RevokedLeadership消息来保证当前系统中仅有一个master是active的。

Spark Master的HA利用Zookeeper实现。代码都在类ZooKeeperLeaderElectionAgent中。Spark直接使用了[Curator][1]框架来完成Leader的选举。代码非常的简洁。

```scala
  override def isLeader() {
    synchronized {
      // could have lost leadership by now.
      if (!leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have gained leadership")
      updateLeadershipStatus(true)
    }
  }

  override def notLeader() {
    synchronized {
      // could have gained leadership by now.
      if (leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have lost leadership")
      updateLeadershipStatus(false)
    }
  }

  def updateLeadershipStatus(isLeader: Boolean) {
    if (isLeader && status == LeadershipStatus.NOT_LEADER) {
      status = LeadershipStatus.LEADER
      masterActor ! ElectedLeader
    } else if (!isLeader && status == LeadershipStatus.LEADER) {
      status = LeadershipStatus.NOT_LEADER
      masterActor ! RevokedLeadership
    }
  }
```

## Spark Slave
org.apache.spark.deploy.worker.Worker是worker daemon的入口。每一个slave节点上面运行一个worker daemon。
一个worker daemon中有一个actor，负责跟master的actor通信。

在worker actor启动的时候，会向所有的master注册。
```scala
  def tryRegisterAllMasters() {
    for (masterUrl <- masterUrls) {
      logInfo("Connecting to master " + masterUrl + "...")
      val actor = context.actorSelection(Master.toAkkaUrl(masterUrl))
      actor ! RegisterWorker(workerId, host, port, cores, memory, webUi.boundPort, publicAddress)
    }
  }
```

同样，在worker actor中，所有的事件都在```override def receiveWithLogging```中处理。
下面是一些重要的事件。

- worker registration的回复。worker将activeMaster的信息记录，设置自身的状态。
```scala
    case RegisteredWorker(masterUrl, masterWebUiUrl)
```
    
- 发送心跳。
```scala
case SendHeartbeat
```

- 当发生master的failover切换时，新的master会通知worker更新active master信息。
```scala
case MasterChanged(masterUrl, masterWebUiUrl)
```

- 启动一个executor。需要注意的是每一个app下面对应着多个executor。因此，executor的fullId是由"appID/executorID"构成。
```scala
case LaunchExecutor(masterUrl, appId, execId, appDesc, cores_, memory_)
```

- kill executor
```scala
case KillExecutor(masterUrl, appId, execId)
```

- 启动driver
```scala
case LaunchDriver(driverId, driverDesc)
```

- kill driver
```
case KillDriver(driverId)
```

- 返回worker的状态。用于webui获取worker的状态。
```scala
case RequestWorkerState
```

### executor 启动和运行监控
ExecutorRunner创建一个线程用于启动和监控executor。每个executor是在一个子进程中运行。

### driver 的启动和运行监控
DriverRunner创建一个线程用于启动driver program，driver在一个子进程中启动。stdout和stderr分别重定向到一个文件当中。
如果driver的supervise属性设置为true，那么DriverRunner会尝试重启driver直到driver成功或者被kill。
核心的函数就是start。driver的jar会在运行之前被worker download到相应的working dir当中。
```scala
  /** Starts a thread to run and manage the driver. */
  def start() = {
    new Thread("DriverRunner for " + driverId) {
      override def run() {
        try {
          val driverDir = createWorkingDirectory()
          val localJarFilename = downloadUserJar(driverDir)

          // Make sure user application jar is on the classpath
          // TODO: If we add ability to submit multiple jars they should also be added here
          val classPath = driverDesc.command.classPathEntries ++ Seq(s"$localJarFilename")
          val newCommand = Command(
            driverDesc.command.mainClass,
            driverDesc.command.arguments.map(substituteVariables),
            driverDesc.command.environment,
            classPath,
            driverDesc.command.libraryPathEntries,
            driverDesc.command.javaOpts)
          val command = CommandUtils.buildCommandSeq(newCommand, driverDesc.mem,
            sparkHome.getAbsolutePath)
          launchDriver(command, driverDesc.command.environment, driverDir, driverDesc.supervise)
        }
        catch {
          case e: Exception => finalException = Some(e)
        }

        val state =
          if (killed) {
            DriverState.KILLED
          } else if (finalException.isDefined) {
            DriverState.ERROR
          } else {
            finalExitCode match {
              case Some(0) => DriverState.FINISHED
              case _ => DriverState.FAILED
            }
          }

        finalState = Some(state)

        worker ! DriverStateChanged(driverId, state, finalException)
      }
    }.start()
  }
```


  [1]: http://curator.apache.org/