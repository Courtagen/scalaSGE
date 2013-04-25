package org.github.mylons.sge

import org.github.mylons.sge.util.Resources
import collection.mutable.ArrayBuffer
import scala.sys.process._
import java.io.{File, PrintWriter}
import collection.mutable
import scala.util.{Try, Success, Failure}

/**
 * Author: Mike Lyons
 * Date: 9/7/12
 * Time: 1:29 PM
 * Description:
 */
trait SGE extends Resources {

  val header = "!#/bin/bash"
  val scriptFileName = "DefaultScriptName"
  var scriptPath = "/tmp/"
  val SGEOptions = new mutable.HashMap[String, String]()
  val environmentOptions = new ArrayBuffer[String]()

  val commands = new ArrayBuffer[String]()

  /**
   * Adds an SGE option to the
   * Deleterious to existing options in the map
   * @param qsubOption
   * @param optionValue
   */
  def addOption( qsubOption: String, optionValue: String = "") {
    SGEOptions += (qsubOption -> optionValue)
  }

  def addEnvironmentSetting( shellSetting: String ) {
    environmentOptions += shellSetting
  }

  def optionString: String = {
    SGEOptions.foldLeft("") { case (acc, (key, value)) => {
      if (key.contains("="))
        "%s#$ %s%s\n".format(acc, key, value)
      else
        "%s#$ %s %s\n".format(acc, key, value)
      }
    }
  }

  def defaultOptions = {
    addOption("-cwd", "") //use current working dir
    if (!SGEOptions.contains("-pe"))
      addOption("-pe" , new String( PE_TYPE + " " +NUMBER_OF_CPUS ) )//set number of cpus
    if (!SGEOptions.contains("-l s_vmem="))
      addOption("-l s_vmem=",  S_MEMORY)
    if (!SGEOptions.contains("-l h_vmem="))
      addOption("-l h_vmem=",  H_MEMORY)
    if (!SGEOptions.contains("-l h_rt="))
      addOption("-l h_rt=",  WALL_TIME)
  }

  /*
  ArrayBuffer containing each line of the script that eventually must be written
  to a file via writeScript()
   */
  def script( ): ArrayBuffer[String] = {
    val buff = new ArrayBuffer[String]()
    environmentOptions.foreach( buff += _ )
    defaultOptions //setup defaults if need be
    buff += "#Begin SGE Options"
    buff += optionString
    buff += "#End SGE Options"
    buff += "#Begin User Options"
    buff += "#End User Options"
    buff += "#Begin User Computation"
    for (command <- commands) buff += command
    buff += "#End User Computation"
    return buff
  }

  def writeScript = {
    val writer = new PrintWriter( commandToSubmit )
    val lines = script()
    for (line <- lines)
      writer.println(line)
    writer.close()
  }

  /*
    This is what actually gets submitted to the cluster
   */
  def commandToSubmit = "%s/%s.sh".format(scriptPath, scriptFileName)



}

trait QSUBStatus

case class QSUBFailure( msg: String ) extends QSUBStatus {
  override def toString = "[scalaSGE]: " + msg
}
case class QSUBSuccess( success: Boolean, msg: String = "" ) extends QSUBStatus{
  override def toString = "[scalaSGE]: qsub successful: %s".format(msg)
}

trait Job extends SGE {
  //qsub script basically
  val jobName = "DefaultJobName"
  val dependentJobs = new ArrayBuffer[Job]()

  def appendNameToScript() = SGEOptions += ("-N" -> jobName)
  def appendCommandToScript( command: String) = commands += command

  def submit(): QSUBStatus = {
    //"qsub " + scriptName !! //! is a shortcut to execute system process, !! returns the string
    val result = Try("qsub " + commandToSubmit !!)
    result match {
      case Success(v) => QSUBSuccess(true, v)
      case Failure(v) => {
        val failString: String = "qsub " + scriptPath
        return QSUBFailure("Could not submit job: " + failString + " [Fail]: " + result.failed)
      }
    }
  }

  def addDependency( job: Job ) = {
    if (SGEOptions.contains("-hold_jid"))
      SGEOptions.update("-hold_jid", SGEOptions.getOrElse("-hold_jid", "")+","+job.jobName)
    else
      SGEOptions += ("-hold_jid" ->  job.jobName )
  }

}


