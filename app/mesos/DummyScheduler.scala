package mesos

import java.util
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConversions._
import ioc.modules.MesosConfig
import javax.inject.Inject
import org.apache.mesos.Protos._
import org.apache.mesos.{Protos, Scheduler, SchedulerDriver}

/**
  * Provides a set of mixins to calculate offer total amount of resources.
  */
trait OfferResourceCalculator {
  protected val CpuResource = "cpus"
  protected val DiskResource = "disk"
  protected val MemResource = "mem"

  def calculateCpuResource(offer: Protos.Offer): Double = calculateScalarResource(offer, CpuResource)

  def calculateCpuResource(offers: Seq[Protos.Offer]): Double = calculateScalarResource(offers, CpuResource)

  def calculateMemResource(offer: Protos.Offer): Double = calculateScalarResource(offer, MemResource)

  def calculateMemResource(offers: Seq[Protos.Offer]): Double = calculateScalarResource(offers, MemResource)

  def calculateDiskResource(offer: Protos.Offer): Double = calculateScalarResource(offer, DiskResource)

  def calculateDiskResource(offers: Seq[Protos.Offer]): Double = calculateScalarResource(offers, DiskResource)

  private def calculateScalarResource(offer: Protos.Offer, resourceType: String): Double = {
    offer.getResourcesList.filter(r => r.getName == resourceType).map(r => r.getScalar.getValue).sum
  }

  private def calculateScalarResource(offers: Seq[Protos.Offer], resourceType: String): Double = {
    offers.map(o => calculateScalarResource(o, resourceType)).sum
  }
}

/**
  * This schedules shell commands which outputs simple messages on the executor side (hello world).
  * The scheduler is adapted from OReilly book: https://www.oreilly.com/library/view/building-applications-on/9781491926543/ch04.html
  */
sealed class DummyScheduler @Inject() (mesosCfg: MesosConfig, frameworkInfo: FrameworkInfo,
                                       workProvider: WorkloadProvider)
  extends Scheduler with OfferResourceCalculator {
  private type WorkToBeScheduled = Map[(OfferID, SlaveID), Seq[Workload]]

  private var mutableProvider = workProvider
  private lazy val providerLock = new ReentrantLock()

  def resubmit: Unit = {
    providerLock.lock()
    mutableProvider = workProvider
    providerLock.unlock()
  }

  override def registered(driver: SchedulerDriver, frameworkId: Protos.FrameworkID, masterInfo: Protos.MasterInfo): Unit = {
    System.out.println(s"Registered framework ${frameworkId.getValue} to master ${masterInfo.getAddress.getIp}:${masterInfo.getAddress.getPort}")
  }

  override def reregistered(driver: SchedulerDriver, masterInfo: Protos.MasterInfo): Unit = {
    System.out.println(s"Re-registered framework to master ${masterInfo.getAddress.getIp}:${masterInfo.getAddress.getPort}")
  }

  override def resourceOffers(driver: SchedulerDriver, offers: util.List[Protos.Offer]): Unit = {
    val totalCpu = calculateCpuResource(offers)
    val totalDisk = calculateDiskResource(offers)
    val totalMem = calculateMemResource(offers)
    System.out.println(s"Received new offers: ${offers.size}, CPUs: ${totalCpu}, Memory: ${totalMem}, Disk: ${totalDisk}")

    if (mutableProvider.workloads.isEmpty) {
      declineAllOffers(driver, offers)
    } else {
      scheduleNextChunk(driver, offers)(scheduleWorkload)
    }
  }

  override def offerRescinded(driver: SchedulerDriver, offerId: Protos.OfferID): Unit = {
    System.out.println(s"Offer rescinded: ${offerId.getValue}")
  }

  override def statusUpdate(driver: SchedulerDriver, status: Protos.TaskStatus): Unit = {
    System.out.println(s"Status updated: taskId--->${status.getTaskId.getValue}, status: ${status.getState}")
  }

  override def frameworkMessage(driver: SchedulerDriver, executorId: Protos.ExecutorID, slaveId: Protos.SlaveID,
                                data: Array[Byte]): Unit = {
    System.out.println("Framework message received.")
  }

  override def disconnected(driver: SchedulerDriver): Unit = {
    System.out.println("Disconnected")
  }

  override def slaveLost(driver: SchedulerDriver, slaveId: Protos.SlaveID): Unit = {
    System.err.println(s"Slave lost ${slaveId.getValue}.")
  }

  override def executorLost(driver: SchedulerDriver, executorId: Protos.ExecutorID, slaveId: Protos.SlaveID,
                            status: Int): Unit = {
    System.err.println("Executor lost.")
  }

  override def error(driver: SchedulerDriver, message: String): Unit = {
    System.err.println(s"Unexpected error: ${message}")
  }

  private def declineAllOffers(driver: SchedulerDriver, offers: Seq[Protos.Offer]) =
    offers.foreach(o => driver.declineOffer(o.getId))

  private def scheduleNextChunk(driver: SchedulerDriver, offers: Seq[Protos.Offer])
                               (block: (SchedulerDriver, WorkToBeScheduled) => Unit) = {
    providerLock.lock()
    try {
      val (matchingWork, scheduledWork) = getWorkForOffers(offers, mutableProvider.workloads)
      mutableProvider -= scheduledWork
      providerLock.unlock()

      block(driver, matchingWork)
    } catch {
      case ex => {
        System.err.println(ex)
      }
    }
  }

  private def reserveWhileResourcesAvailable(workloads: Seq[Workload], availableCpu: Double, availableMem: Double,
                                             collectedWorkload: Seq[Workload]=Seq()): Seq[Workload] = {
    if (workloads.isEmpty) {
      collectedWorkload
    } else {
      val w = workloads.head
      if (w.matches(availableCpu, availableMem)) {
        val remCpu = availableCpu - w.requiredCpu
        val remMem = availableMem - w.requiredMem
        reserveWhileResourcesAvailable(workloads.tail, remCpu, remMem, collectedWorkload ++ List(w))
      } else {
        reserveWhileResourcesAvailable(workloads.tail, availableCpu, availableMem, collectedWorkload)
      }
    }
  }

  private def getWorkForOffers(offers: Seq[Protos.Offer], work: Seq[Workload]): (WorkToBeScheduled, Seq[Workload]) = {
    val matchingWork = offers.map(o => (o.getId, o.getSlaveId, calculateCpuResource(o), calculateMemResource(o)))
      .map {
        case (offerId, slaveId, cpu, mem) => {
          val work = reserveWhileResourcesAvailable(mutableProvider.workloads, cpu, mem)
          mutableProvider -= work
          (offerId, slaveId, work)
        }
      }
      .groupBy(t => (t._1, t._2))
      .flatMap {
        case ((offerId, slaveId), work) => Map((offerId, slaveId) -> work.map(_._3).filter(w => w != Workload.zero))
      }
      .flatMap {
        case ((offerId, slaveId), work) => Map((offerId, slaveId) -> work.flatten)
      }

    val scheduledWork = matchingWork.values.flatten.toSeq
    (matchingWork, scheduledWork)
  }

  private def scheduleWorkload(driver: SchedulerDriver, work: WorkToBeScheduled): Unit = {
    import mesos.Workload._

    work.foreach {
      case ((offerId, slaveId), tasks) => {
        val mesosTasks = tasks.map(w => {
          val taskBuilder: TaskInfo.Builder = w
          val taskId = Protos.TaskID.newBuilder.setValue(UUID.randomUUID.toString)
          taskBuilder.setTaskId(taskId)
            .setName(w.command)
            .setSlaveId(slaveId)
            .build
        })

        driver.launchTasks(
          util.Collections.singletonList(offerId),
          mesosTasks,
          Filters.newBuilder.build)
      }
    }
  }
}
