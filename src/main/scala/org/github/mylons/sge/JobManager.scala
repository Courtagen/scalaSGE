package org.github.mylons.sge

/**
 * Author: Mike Lyons
 * Date: 4/19/13
 * Time: 3:59 PM
 * Description: 
 */

/**
 * actor and future notes
 *
 * Future.isSet() -- returns whether or not the output data is ready
 * val future = actor !! message
 *
 * idea: give futures to other actor(s) for processing
 */


import collection.mutable.ListBuffer
import scala.util.{Try, Success, Failure}
import org.ggf.drmaa.{JobInfo, JobTemplate, SessionFactory, Session}
import akka.actor.{ActorSystem, Props, ActorRef, Actor}
import akka.event.Logging
import akka.routing.RoundRobinRouter
import java.util.Collections
import collection.mutable


sealed trait ActorJob {
  val id: String
  val msg: String
  val s: Session
}

case object JobsSubmitted
case object JobsDone
case class QueuedJob( id: String, msg: String, s: Session ) extends ActorJob
case class RunningJob( id: String, msg: String, s: Session ) extends ActorJob
case class FailedJob( id: String, msg: String, s: Session ) extends ActorJob
case class EtcJob( id: String, msg: String, s: Session ) extends ActorJob
case class FinishedJob( id: String, msg: String, s: Session ) extends ActorJob

case class JobComplete( id: String, info: JobInfo)


class Master(nrOfWorkers: Int,
             nrOfMessages: Int,
             nrOfElements: Int,
             listener: ActorRef,
             s: Session) extends Actor {

  val log = Logging(context.system, this)

  var actualMessages: Int = nrOfMessages

  var nrOfResults: Int = _

  val completeMap = new mutable.HashMap[String, Boolean]()

  val workerRouter = context.actorOf(
    Props[JobHandler].withRouter(RoundRobinRouter(nrOfWorkers)), name = "MasterRouter"
  )

  def parseProgramStatus( id: String ): ActorJob = {
    s.getJobProgramStatus(id) match {
      case Session.UNDETERMINED => log.info(id +" job undetermined");  EtcJob(id, "undet", s)
      case Session.QUEUED_ACTIVE => log.info(id + " queued active");  QueuedJob(id, "queued", s)
      case Session.SYSTEM_ON_HOLD => log.info(id + " system on hold");  EtcJob(id, "sys hold", s)
      case Session.USER_ON_HOLD => log.info(id + " user on hold "); EtcJob(id, "usr hold", s)
      case Session.USER_SYSTEM_ON_HOLD => log.info(id + " user system on hold ");  EtcJob(id, "usr sys on hold", s)
      case Session.RUNNING => log.info(id + " job is running");  RunningJob(id, "running", s)
      case Session.SYSTEM_SUSPENDED => log.info(id + " system suspended"); EtcJob(id, "sys susp", s)
      case Session.USER_SUSPENDED => log.info(id + " user suspended "); EtcJob(id, "usr susp", s)
      case Session.DONE => log.info(id + " done");  FinishedJob(id, "finished", s)
      case Session.FAILED => log.info(id + " failed");  FailedJob(id, "failed", s)
    }

  }

  def receive = {
    case jobs: Seq[String] => {
      //setup complete map
      for (i <- 0 until jobs.length ){
        log.info("processing a job: %d of %d".format(i+1, jobs.length))
        workerRouter ! s.wait(Session.JOB_IDS_SESSION_ANY, Session.TIMEOUT_WAIT_FOREVER)
      }
    }
    case job: String => {
      log.info("sending jobid: %s back to handlers".format(job))
      workerRouter ! QueuedJob(job, "status_unknown", s)
      //this is an additional message over what we thought was going to happen --
      actualMessages += 1
    }
    case c: JobComplete => {
      nrOfResults += 1
      log.info(
        "master recieved complete job: %s nrOfResults=%d nrOfMessages=%d"
        .format(c.id, nrOfResults, actualMessages)
      )
      if (actualMessages == nrOfResults) {
        //send something to listener
        log.info("actors shutting down..")
        listener ! JobsDone
        context.stop(self)
      }
    }
    case j: ActorJob => log.info("job id:%s msg:%s".format(j.id, j.msg)); workerRouter ! j
    case wtf => log.warning("wtf -- outer: " + wtf)
  }

}


class JobHandler extends Actor {
  val log = Logging(context.system, this)

  def infoHandler(info: JobInfo) = {

    if (info.wasAborted()) log.info("job " + info.getJobId + " never ran")
    else if (info.hasExited) log.info("job " + info.getJobId + " finished regularly with status " + info.getExitStatus)
    else if (info.hasSignaled) log.info("job " + info.getJobId + " finished due to signal " + info.getTerminatingSignal)
    else log.info("job " + info.getJobId + " finished with unclear conditions")

    val rmap = info.getResourceUsage
    val itr = rmap.keySet().iterator()
    while (itr.hasNext){
      val name = itr.next()
      val value = rmap.get(name)
      //log.info("name=%s value=%s".format(name, value))
    }
    sender ! JobComplete(info.getJobId, info)
  }

  def receive = {
    case info: JobInfo => infoHandler(info)
    case qjob: QueuedJob => {
      val info = Try(qjob.s.wait(qjob.id, Session.TIMEOUT_NO_WAIT))
      info match {
        case Success(v) => infoHandler(v)
        case Failure(v) => {
          log.warning("failed to wait for job: %s exception:".format(qjob.id) + v)
          sender ! qjob.id
        }
      }
    }
    case err => log.warning("received err: " + err)
  }
}


class Listener extends Actor {
  val log = Logging(context.system, this)
  def receive = {
    case JobsDone => log.info("JobsDone received, shutting down actor system");context.system.shutdown()
    case any => log.info("getting any="+any)
  }
}

class JobManager( jobs: Seq[String] ) {

  val session: Session = SessionFactory.getFactory.getSession
  session.init(null)

  val system = ActorSystem("JobManagerSystem")

  val listener = system.actorOf(Props[Listener], name = "listener")
                                          //workers, messages, elements
  val master = system.actorOf(Props(new Master(2, jobs.length, 10000, listener, session )), name = "JobManagerMaster")

  def exit() {
    session.exit()
  }
  //def runnerType =

  //def create

  //def updateStatus

  def jobStatus(id: String) ={
    session.getJobProgramStatus(id) match {
      case Session.UNDETERMINED => println(id +" job undetermined")
      case Session.QUEUED_ACTIVE => println(id + " queued active")
      case Session.SYSTEM_ON_HOLD => println(id + " system on hold")
      case Session.USER_ON_HOLD => println(id + " user on hold ")
      case Session.USER_SYSTEM_ON_HOLD => println(id + " user system on hold ")
      case Session.RUNNING => println(id + " job is running")
      case Session.SYSTEM_SUSPENDED => println(id + " system suspended")
      case Session.USER_SUSPENDED => println(id + " user suspended ")
      case Session.DONE => println(id + " done")
      case Session.FAILED => println(id + " failed")
    }
  }

  def submitJob( jt: JobTemplate ): String = session.runJob(jt)

  def monitor( jobs: Seq[String] ) = for ( job <- jobs ) master ! job

  def monitorSession( ) = {
    /*session.synchronize(Collections.singletonList(Session.JOB_IDS_SESSION_ALL),
      Session.TIMEOUT_NO_WAIT, false)*/
    val ids = new ListBuffer[String]()
    for (job <- jobs) {
      val jt = session.createJobTemplate()
      jt.setNativeSpecification("-b no") //allows shell script to be submittable
      jt.setRemoteCommand(job)
      jt.setJobName(job.split('/').last.split('.').head)
      ids += submitJob(jt)
    }
    master ! ids

  }


}

object TestApp extends App {
  //setup manager
  val jobList = List("/home/sgeadmin/test-1.sh","/home/sgeadmin/test-2.sh","/home/sgeadmin/test-3.sh","/home/sgeadmin/test-4.sh","/home/sgeadmin/test-5.sh")
  val m = new JobManager(jobList)

  val jobMap = new mutable.HashMap[String, JobTemplate]()
  val ids = new ListBuffer[String]()
  /*
  for (job <- jobList) {
    val jt = m.session.createJobTemplate()
    jt.setNativeSpecification("-b no") //allows shell script to be submittable
    jt.setRemoteCommand(job)
    jt.setJobName(job.split('/').last.split('.').head)
    val id = m.submitJob(jt)
    jobMap += (id -> jt)
  }*/


  m.monitorSession
  while (!m.master.isTerminated) Thread.sleep(2000)
  println("exiting..")
  m.exit()

}
