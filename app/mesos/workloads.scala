package mesos

import javax.inject.Inject
import org.apache.mesos.Protos.{CommandInfo, Resource, TaskInfo, Value}

/**
  * Provides a simple contract for creating work which can be scheduled by the dummy scheduler.
  */
trait Workload {
  def command: String

  def requiredCpu: Double

  def requiredMem: Double

  def requiredDisk: Option[Double]
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
}

/**
  * Provides a simple abstraction for defining a set of workload tasks which must be scheduled.
  */
trait WorkloadProvider {
  def workloads: Seq[Workload]

  def reserve: Option[(Workload, WorkloadProvider)]
}

/**
  * A basic in memory storage for workloads which must be scheduled.
  */
sealed class InMemoryWorkloadProvider @Inject() (val workloads: Seq[Workload]) extends WorkloadProvider {
  override def reserve: Option[(Workload, WorkloadProvider)] = workloads match {
    case Seq() => None
    case _ => Some((workloads.head, new InMemoryWorkloadProvider(workloads.tail)))
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