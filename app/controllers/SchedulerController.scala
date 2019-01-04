package controllers

import java.util.concurrent.atomic.AtomicBoolean

import javax.inject.{Inject, Singleton}
import mesos.DummyScheduler
import org.apache.mesos.MesosSchedulerDriver
import play.api.mvc.{AbstractController, ControllerComponents}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Provides the http api for working with the new dummy framework.
  */
@Singleton()
class SchedulerController @Inject()(cc: ControllerComponents,
                                     driver: MesosSchedulerDriver,
                                     scheduler: DummyScheduler)(implicit ec: ExecutionContext)
  extends AbstractController(cc) {
  private var schedulerStarted: AtomicBoolean = new AtomicBoolean(false)

  /**
    * Starts the dummy framework by registering it to mesos master.
    */
  def start = Action.async {
    val previouslyStarted = schedulerStarted.getAndSet(true)

    if (!previouslyStarted) {
      Future {
        driver.start()
        driver.join
      }
    }

    Future.successful(Created)
  }

  /**
    * Provides useful information about the current framework.
    */
  def resubmit = Action.async {
    scheduler.resubmit
    Future.successful(Ok("all the tasks were resubmitted to mesos."))
  }
}
