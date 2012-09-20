package org.github.mylons.sge

import org.github.mylons.sge.util.Resources
import collection.mutable.ArrayBuffer
import scala.sys.process._
import java.io.{File, PrintWriter}

/**
 * Author: Mike Lyons
 * Date: 9/7/12
 * Time: 1:29 PM
 * Description:
 */
trait SGE extends Resources {

  val header = "!#/bin/bash"
  val scriptFileName = "DefaultScriptName"
  var scriptPath = "/tmp/superpipe/"
  val SGEOptions = new ArrayBuffer[String]()

  val commands = new ArrayBuffer[String]()

  def script( ): ArrayBuffer[String] = {
    //SGEOptions.clear()
    SGEOptions +=  "#$-cwd" //use current working dir
    SGEOptions +=  "#$-j y" //join stdout and stderr
    SGEOptions +=  "#$-pe " + PE_TYPE + " " +NUMBER_OF_CPUS  //set number of cpus
    SGEOptions +=  "#$-l h_vmem=" + MEMORY
    SGEOptions +=  "#$-S /bin/bash" //run through bash shell
    SGEOptions +=  "#$-l h_rt=" + WALL_TIME
    val buff = new ArrayBuffer[String]
    buff += header
    buff += "#Begin SGE Options"
    for (token <- SGEOptions) buff += token
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
      writer.write(line+"\n")
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

  def appendNameToScript() = SGEOptions += "#$-N " + jobName
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

  def addDependency( job: Job ) = (SGEOptions += "#$-hold_jid "+ job.jobName)


}

trait DependentJob extends Job {
//create new SGE options and override the script
  val dependentJobID = ""
}

