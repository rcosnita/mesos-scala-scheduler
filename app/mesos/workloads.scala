package mesos

import javax.inject.Inject
import org.apache.mesos.Protos.{CommandInfo, Resource, TaskInfo, Value}

/**
  * Provides a simple contract for creating work which can be scheduled by the dummy scheduler.
  */
trait Workload { self =>
  def command: String

  def requiredCpu: Double

  def requiredMem: Double

  def requiredDisk: Option[Double]

  def matches(cpu: Double, mem: Double): Boolean = requiredCpu < cpu && requiredMem < mem

  def combine(w2: Workload) = new Workload {
    override def command: String = s"${self.command} && ${w2.command}"

    override def requiredCpu: Double = self.requiredCpu + w2.requiredCpu

    override def requiredMem: Double = self.requiredMem + w2.requiredMem

    override def requiredDisk: Option[Double] = {
      val disk1: Double = self.requiredDisk.getOrElse(0)
      val disk2: Double = w2.requiredDisk.getOrElse(0)
      Some(disk1 + disk2)
    }
  }
}

object Workload {
  implicit def workloadToTaskBuilder(work: Workload): TaskInfo.Builder = {
    TaskInfo.newBuilder
      .addResources(Resource.newBuilder.setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(work.requiredCpu)))
      .addResources(Resource.newBuilder.setName("mem")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder().setValue(work.requiredMem)))
      .setCommand(CommandInfo.newBuilder.setValue(work.command))
  }

  def zero: Workload = new Workload {
    override def command: String = ""

    override def requiredCpu: Double = 0

    override def requiredMem: Double = 0

    override def requiredDisk: Option[Double] = None

    override def combine(w2: Workload) = w2
  }
}

/**
  * Provides a simple abstraction for defining a set of workload tasks which must be scheduled.
  */
trait WorkloadProvider {
  def workloads: Seq[Workload]

  def -(workDone: Seq[Workload]): WorkloadProvider
}

/**
  * A basic in memory storage for workloads which must be scheduled.
  */
sealed class InMemoryWorkloadProvider @Inject() (val workloads: Seq[Workload]) extends WorkloadProvider {
  override def -(workDone: Seq[Workload]): WorkloadProvider = {
    new InMemoryWorkloadProvider(workloads diff workDone)
  }
}

/**
  * Provides an abstraction for defining new simple commands workloads.
  */
case class SimpleCommandWorkload(command: String, requiredCpu: Double = 0.1,
                              requiredMem: Double = 32,
                              requiredDisk: Option[Double] = None)
  extends Workload {
}