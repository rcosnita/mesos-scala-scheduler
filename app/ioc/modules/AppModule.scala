package ioc.modules

import com.google.inject.internal.SingletonScope
import com.google.inject.{AbstractModule, Provides}
import mesos._
import net.codingwell.scalaguice.ScalaModule
import org.apache.mesos.MesosSchedulerDriver
import org.apache.mesos.Protos.FrameworkInfo
import pureconfig._
import pureconfig.Derivation._
import pureconfig.generic.auto._

/**
  * Provides the configuration attributes for mesos framework.
  */
case class FrameworkConfig(name: String, user: String, numOfTasks: Int)

/**
  * Provides the configuration attributes for mesos.
  */
case class MesosConfig(master: String, framework: FrameworkConfig)

class AppModule extends AbstractModule with ScalaModule {
  override def configure(): Unit = {
    bind[DummyScheduler].self.asEagerSingleton
    bind[WorkloadProvider].to[InMemoryWorkloadProvider].in(new SingletonScope())
  }

  @Provides
  def frameworkInfo(mesosCfg: MesosConfig): FrameworkInfo = {
    val frameworkCfg = mesosCfg.framework

    FrameworkInfo.newBuilder()
      .setUser(frameworkCfg.user)
      .setName(frameworkCfg.name)
      .build
  }

  @Provides
  def frameworkConfig(mesosCfg: MesosConfig): FrameworkConfig = mesosCfg.framework

  @Provides
  def mesosConfig: MesosConfig = {
    pureconfig.loadConfigOrThrow[MesosConfig]("mesos")
  }

  @Provides
  def mesosSchedulingDriver(scheduler: DummyScheduler, frameworkInfo: FrameworkInfo,
                            mesosCfg: MesosConfig): MesosSchedulerDriver = {
    new MesosSchedulerDriver(scheduler, frameworkInfo, mesosCfg.master)
  }

  @Provides
  def workloads(frameworkConfig: FrameworkConfig): Seq[Workload] =
    for {
      idx <- 0 until frameworkConfig.numOfTasks
    } yield SimpleCommandWorkload(command = s"echo hello world ${idx} $${ENV1} $${ENV2}")
}
