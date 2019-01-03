package mesos

import java.util
import scala.collection.JavaConversions._

import ioc.modules.MesosConfig
import javax.inject.Inject
import org.apache.mesos.Protos.FrameworkInfo
import org.apache.mesos.{Protos, Scheduler, SchedulerDriver}

/**
  * Provides a set of mixins to calculate offer total amount of resources.
  */
trait OfferResourceCalculator {
  private val CpuResource = "cpus"
  private val DiskResource = "disk"
  private val MemResource = "mem"

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
sealed class DummyScheduler @Inject() (mesosCfg: MesosConfig, frameworkInfo: FrameworkInfo)
  extends Scheduler with OfferResourceCalculator {
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

    offers.foreach(o => driver.declineOffer(o.getId))
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
