package controllers

import com.typesafe.config.Config
import javax.inject.Inject
import play.api.mvc.{AbstractController, ControllerComponents}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Provides the http api for working with the new dummy framework.
  */
class SchedulerController @Inject() (cc: ControllerComponents, config: Config)(implicit ec: ExecutionContext)
  extends AbstractController(cc) {
  /**
    * Starts the dummy framework by registering it to mesos master.
    */
  def start = Action.async {
    Future.successful(Ok)
  }

  /**
    * Provides useful information about the current framework.
    */
  def status = Action.async {
    Future.successful(Ok("dummy content for now"))
  }
}
