package org.github.mylons.test

/*import org.github.mylons.sge.Job

/**
 * Author: Mike Lyons
 * Date: 9/10/12
 * Time: 1:56 PM
 * Description:
 */
class SGETest( nameOfJob: String ) extends Job {
  override val jobName = nameOfJob
  override val NUMBER_OF_CPUS = 10
  this.appendNameToScript()
  appendCommandToScript("""echo "hello world" """)
}

object SGETest extends App {
  val sge = new SGETest("test-name")
  println( sge.script().reduceLeft(_ + "\n" + _))
  println("\n\n")
  println( sge.script().reduceLeft(_ + "\n" + _))
}*/
