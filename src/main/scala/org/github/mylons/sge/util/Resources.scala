package org.github.mylons.sge.util

/**
 * Author: Mike Lyons
 * Date: 9/7/12
 * Time: 1:37 PM
 * Description:
 */
trait Resources {
  val PE_TYPE = "orte" //starcluster PE
  val NUMBER_OF_CPUS = 8
  val EMAIL = ""
  val S_MEMORY = "9G" //slightly more than 9G
  val H_MEMORY = "10G" //slightly more than 10G
  val WALL_TIME = "864000" //240 hour wall time in seconds (240 * 60 * 60)

  val DEPENDENT_JOB_ID = -1
}
