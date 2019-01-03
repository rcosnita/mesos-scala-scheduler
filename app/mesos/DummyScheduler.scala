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
  private var mutableProvider = workProvider
  private lazy val providerLock = new ReentrantLock()

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

  private def declineAllOffers(driver: SchedulerDriver, offers: Seq[Protos.Offer]) =
    offers.foreach(o => driver.declineOffer(o.getId))

  private def scheduleNextChunk(driver: SchedulerDriver, offers: Seq[Protos.Offer])
                               (block: (SchedulerDriver, Seq[Protos.Offer], Workload) => Unit) = {
    providerLock.lock()
    try {
      mutableProvider.reserve match {
        case None => {
          declineAllOffers(driver, offers)
          providerLock.unlock()
        }
        case Some((w: Workload, p: WorkloadProvider)) => {
          mutableProvider = p
          providerLock.unlock()
          block(driver, offers, w)
        }
      }
    } catch {
      case ex => {
        System.err.println(ex)
      }
    }
  }

  private def scheduleWorkload(driver: SchedulerDriver, offers: Seq[Protos.Offer], work: Workload): Unit = {
    import mesos.Workload._

    val matchingOffers = offers.map(o => (o.getId, o.getSlaveId, calculateCpuResource(o), calculateMemResource(o), calculateDiskResource(o)))
      .filter {
        case (_, _, cpu, mem, _) => cpu > work.requiredCpu && mem > work.requiredMem
      }
      .map {
        case (offerId, slaveId, _, _, _) => (offerId, slaveId)
      }

    if (matchingOffers.isEmpty) System.err.println("The workload is discarded ---> not enough resources")
    else {
      val (offerId, slaveId) = matchingOffers.head
      val taskId = Protos.TaskID.newBuilder.setValue(UUID.randomUUID.toString)
      val taskBuilder: TaskInfo.Builder = work
      val task = taskBuilder.setTaskId(taskId)
        .setName(work.command)
        .setSlaveId(slaveId)
        .build
      driver.launchTasks(
        util.Collections.singletonList(offerId),
        util.Collections.singletonList(task),
        Filters.newBuilder.build)
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
}
