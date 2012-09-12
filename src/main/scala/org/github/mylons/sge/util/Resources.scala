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
  val MEMORY = "10256M" //slightly more than 10G
  val WALL_TIME = "24:0:0" //24 hour wall time

  val DEPENDENT_JOB_ID = -1
}
