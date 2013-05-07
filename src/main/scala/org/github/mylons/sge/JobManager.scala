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
import collection.mutable
import com.typesafe.scalalogging.log4j._

import org.ggf.drmaa.{JobInfo, JobTemplate, SessionFactory, Session}



class SleepJob(nameOfJob: String = "Sleep") extends Job {
  override val jobName = nameOfJob
  this.appendNameToScript()
  override val NUMBER_OF_CPUS = 1
  override val H_MEMORY = "256M"
  override val S_MEMORY = "128M"
  override val scriptFileName = jobName
  this.appendCommandToScript("sleep 25")

}

case class LifeOfJob(id: String, template: JobTemplate, numberOfSubmissions: Int, complete: Boolean)

class JobManager( jobs: Seq[Job], val resubmitAttempts: Int = 3 ) extends Logging {


  private val jobTemplateMap = new mutable.HashMap[String, LifeOfJob]()

  val session: Session = SessionFactory.getFactory.getSession
  session.init(null)

  def exit() {
    //clear job templates first
    //if you don't do this there is potential for memory leak, according to the doc
    //http://gridscheduler.sourceforge.net/howto/drmaa_java.html in Example 2
    for (loj <- jobTemplateMap.values ) session.deleteJobTemplate(loj.template)
    //this is how you close a session
    session.exit()
  }


  def submitJob( jt: JobTemplate ): String = session.runJob(jt)

  def createJobTemplate( job: Job ): JobTemplate = {
    val jt = session.createJobTemplate()
    jt.setNativeSpecification("-b no") //allows shell script to be submittable
    jt.setRemoteCommand(job.commandToSubmit)
    jt
  }

  def prepareJobs = {
    //write job scripts
    for (job <- jobs) job.writeScript
    //setup job templates
  }


  private def swapJobIdInMap(oldId: String, newId: String) = {
    if(jobTemplateMap.contains(oldId)){
      logger.debug("swapping id: oldId=%s newId=%s".format(oldId, newId))
      val loj = jobTemplateMap(oldId)
      //add new id, preserving references to job template, and # of submissions
      //println("about to add new item to template map cur size=%d oldId=%s newId=%s".format(jobTemplateMap.size, oldId, newId))
      jobTemplateMap.update(newId, new LifeOfJob(newId, loj.template, loj.numberOfSubmissions, loj.complete))
      //println("added new item to template map cur size=%d oldId=%s newId=%s".format(jobTemplateMap.size, oldId, newId))
      //remove old version
      jobTemplateMap.remove(oldId)
      //println("removed old id from template map cur size=%d oldId=%s newId=%s".format(jobTemplateMap.size, oldId, newId))
    } else {
      logger.debug("can't swap oldId: %s for newId: %s".format(oldId, newId))
    }
  }

  private def incrementJobSubmission( loj: LifeOfJob ): LifeOfJob = {
    new LifeOfJob(loj.id, loj.template, loj.numberOfSubmissions + 1, loj.complete)
  }

  def monitorSession: Boolean = {
    /*session.synchronize(Collections.singletonList(Session.JOB_IDS_SESSION_ALL),
      Session.TIMEOUT_NO_WAIT, false)*/

    def markJobComplete(info: JobInfo) = {
      if (jobTemplateMap.contains(info.getJobId)) {
        logger.debug("marking job: %s complete".format(info.getJobId))
        val loj = jobTemplateMap(info.getJobId)
        val newLoj = new LifeOfJob(info.getJobId, loj.template, loj.numberOfSubmissions, true)
        jobTemplateMap.update(info.getJobId, newLoj)
      } else {
        logger.debug("can't mark job complete for some reason: id=%s".format(info.getJobId))
      }
    }

    def jobsPastSubmissionLimit =
      jobTemplateMap.values.filter( lifeOfJob => lifeOfJob.numberOfSubmissions >= resubmitAttempts )

    def areThereJobsPastSubmissionLimit =
      jobTemplateMap.values.exists( lifeOfJob => lifeOfJob.numberOfSubmissions >= resubmitAttempts )

    def thereAreThereFailedJobs =
      jobTemplateMap.values.exists( lifeOfJob => lifeOfJob.complete == false )


    def failedJobHelper = {
      while( thereAreThereFailedJobs && !areThereJobsPastSubmissionLimit ) {
        //get the failed jobs
        val jobs = failedJobs
        logger.debug("top of failedJobHelper loop. jobTemplateMap size: %d failedJobs size: %d".format(jobTemplateMap.size, jobs.size))
        //setup ids collection
        val ids = new ListBuffer[String]()
        //resubmit jobs -- updates job map
        for ( lifeOfJob <- jobs ) {
          ids += resubmitJob(lifeOfJob.id)
        }
        //wait for jobs to finish
        for (i  <- 0 until ids.length){
          val info = session.wait(Session.JOB_IDS_SESSION_ANY, Session.TIMEOUT_WAIT_FOREVER)
          //infos += info
          infoHandler(info)
          if (info.hasExited) markJobComplete(info) //job should be complete
        }
      }
    }

    val ids = new ListBuffer[String]()
    for (job <- jobs) {
      val jt = createJobTemplate(job)
      ids += submitJob(jt)
      //put into map of id -> jt?
      jobTemplateMap.put(ids.last, new LifeOfJob(ids.last, jt, 0, false))
      //updateJobMap(ids.last, jt)
    }

    val infos = new ListBuffer[JobInfo]()
    for (i  <- 0 until ids.length){
      val info = session.wait(Session.JOB_IDS_SESSION_ANY, Session.TIMEOUT_WAIT_FOREVER)
      infos += info
      infoHandler(info)
      if (info.hasExited) markJobComplete(info)
    }

    //keep re-running jobs until they're done, or we've exceeded our execution threshold
    failedJobHelper

    for (job <- jobsPastSubmissionLimit )
      logger.info("job: %s failed after %d of submissions".format(job.id, job.numberOfSubmissions))

    if (areThereJobsPastSubmissionLimit) false
    else true
  }

  def infoHandler(info: JobInfo) = {
    if (info.wasAborted()) logger.warn("job " + info.getJobId + " never ran")
    else if (info.hasExited) logger.info("job " + info.getJobId + " finished regularly with status " + info.getExitStatus)
    else if (info.hasSignaled) logger.warn("job " + info.getJobId + " finished due to signal " + info.getTerminatingSignal)
    else logger.warn("job " + info.getJobId + " finished with unclear conditions")
  }

  def resubmitJob(id: String): String = {
    if (jobTemplateMap.contains(id)) {
      logger.info("resubmitting id=%s".format(id))
      //resubmit a job
      val loj = jobTemplateMap(id)
      val newId = submitJob(loj.template)
      //update number of submissions
      val newLoj = incrementJobSubmission(loj)
      //swap ids
      swapJobIdInMap(id, newId)
      jobTemplateMap.update(newId, newLoj)
      newId
    } else {
     //do nothing
      logger.warn("no record of job: %s and cannot resubmit".format(id))
      id
    }
  }

  /*def getSuccessfulJobs( infos: Seq[JobInfo]): Seq[JobInfo] = infos.filter( info => info.getExitStatus == 0 )

  def getFailedJobs( infos: Seq[JobInfo]): Seq[JobInfo] = {
    infos.filter( info => info.getExitStatus != 0 )
  }*/

  def successfulJobs =
    jobTemplateMap.values.filter( lifeOfJob => lifeOfJob.complete == true )

  def failedJobs =
    jobTemplateMap.values.filter( lifeOfJob => lifeOfJob.complete == false )


}


object TestApp extends Logging with App {
  //setup manager
  //set timeout condition -- infinity could be silly, but here we go




  val jobList = List(new SleepJob("test-1"), new SleepJob("test-2"), new SleepJob("test-3"), new SleepJob("test-4"))

  val m = new JobManager(jobList)

  val jobMap = new mutable.HashMap[String, JobTemplate]()
  val ids = new ListBuffer[String]()


  m.prepareJobs
  logger.debug("about to monitor session")
  val infoSeq = m.monitorSession //blocking
  logger.debug("done monitoring session")

  //val failedJobs = m.getFailedJobs(infoSeq)
  //val goodJobs = m.getSuccessfulJobs(infoSeq)

  logger.debug("infoSeq=%s exiting..".format(infoSeq))
  m.exit()

}
/*
*  filter Futures of jobs for failure
*  ex:
val future1 = Future.successful(4)
val future2 = future1.filter(_ % 2 == 0)

future2 foreach println

val failedFilter = future1.filter(_ % 2 == 1).recover {
  // When filter fails, it will have a java.util.NoSuchElementException
  case m: NoSuchElementException ⇒ 0
}

failedFilter foreach println


*/