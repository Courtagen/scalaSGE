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
  val SGEOptions = new ArrayBuffer[String]()

  val commands = new ArrayBuffer[String]()

  def script( ): ArrayBuffer[String] = {
    SGEOptions.clear()
    SGEOptions +=  "#$-cwd" //use current working dir
    SGEOptions +=  "#$-j y" //join stdout and stderr
    SGEOptions +=  "#$-pe " + PE_TYPE + " " +NUMBER_OF_CPUS  //set number of cpus
    SGEOptions +=  "#$-mf=" + MEMORY
    SGEOptions +=  "#$-S /bin/bash" //run through bash shell
    SGEOptions +=  "#$-l h_rt=" + WALL_TIME
    val buff = new ArrayBuffer[String]
    buff += header
    buff += "#Begin SGE Options"
    for (token <- SGEOptions) buff += token
    buff += "#End SGE Options"
    buff += "#Begin User Options"
    for (content <- additionalContent) buff += content
    buff += "#End User Options"
    buff += "#Begin User Computation"
    for (command <- commands) buff += command
    buff += "#End User Computation"
    return buff
  }

  def writeScript( fileName: String ) = {
    val writer = new PrintWriter(new File(fileName) )
    val lines = script()
    for (line <- lines)
      writer.write(line)
    writer.close()
  }

}

trait Job extends SGE {
  //qsub script basically
  val scriptName = "DefaultScriptName"
  val jobName = "DefaultJobName"

  val dependentJobs = new ArrayBuffer[DependentJob]()

  def appendNameToScript() = SGEOptions += "#$-N " + jobName
  def appendCommandToScript( command: String) = commands += command

  def submit(): String = {
    "qsub " + scriptName !! //! is a shortcut to execute system process, !! returns the string
  }
  def addDependency( nameOfJob: String ) = (SGEOptions += "#$-hold_jid "+ nameOfJob)


}

trait DependentJob extends Job {
//create new SGE options and override the script
  val dependentJobID = ""
}

