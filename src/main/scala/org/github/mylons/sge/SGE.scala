package org.github.mylons.sge

import org.github.mylons.sge.util.Resources
import collection.mutable.ArrayBuffer
import scala.sys.process._
import java.io.{File, PrintWriter}
import collection.mutable

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

  def optionString: String = SGEOptions.foldLeft(""){ case (acc, (key, value)) => "%s# %s %s\n".format(acc, key, value) }

  def defaultOptions = {
    addOption("-cwd", "") //use current working dir
    addOption("-j","y") //join stdout and stderr
    if (!SGEOptions.contains("-pe"))
      addOption("-pe" , new String(PE_TYPE + " " +NUMBER_OF_CPUS ) )//set number of cpus
    if (!SGEOptions.contains("-l h_vmem"))
      addOption("-l h_vmem=",  MEMORY)
    if (!SGEOptions.contains("-l h_rt"))
      addOption("-l h_rt=",  WALL_TIME)
  }
  def script( ): ArrayBuffer[String] = {
    defaultOptions //setup defaults if need be
    val buff = new ArrayBuffer[String]()
    buff += header
    buff += "#Begin SGE Options"
    buff += optionString
    buff += "#End SGE Options"
    buff += "#Begin User Options"
    //for (content <- additionalContent) buff += content
    buff += "#End User Options"
    buff += "#Begin User Computation"
    for (command <- commands) buff += command
    buff += "#End User Computation"
    return buff
  }

  def writeScript( theScriptPath: String ) = {
    scriptPath = theScriptPath + "/" + scriptFileName + ".sh"
    val writer = new PrintWriter( scriptPath )
    val lines = script()
    for (line <- lines)
      writer.println(line)
    writer.close()
  }

}

case class QSUBStatus( )
case class QSUBFailure( msg: String ) extends QSUBStatus {
  override def toString = "[scalaSGE]: " + msg
}
case class QSUBSuccess( success: Boolean ) extends QSUBStatus{
  override def toString = "[scalaSGE]: qsub successful"
}

trait Job extends SGE {
  //qsub script basically
  val jobName = "DefaultJobName"

  val dependentJobs = new ArrayBuffer[Job]()

  def appendNameToScript() = SGEOptions += ("-N" -> jobName)
  def appendCommandToScript( command: String) = commands += command

  def submit(): QSUBStatus = {
    //"qsub " + scriptName !! //! is a shortcut to execute system process, !! returns the string
    try {
      "qsub " + scriptPath !!
    }
    catch {
      case failure => {
        val failString: String = "qsub " + scriptPath
        return QSUBFailure("Could not submit job: " + failString + " [Fail]: "+failure)
      }
    }
    return QSUBSuccess(true)
  }

  def addDependency( job: Job ) = {
    if (SGEOptions.contains("-hold_jid"))
      SGEOptions.update("-hold_jid", SGEOptions.getOrElse("-hold_jid", "")+","+job.jobName)
    else
      SGEOptions += ("-hold_jid" -> job.jobName )
  }


}


