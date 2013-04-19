package org.github.mylons.sge

/**
 * Author: Mike Lyons
 * Date: 4/19/13
 * Time: 3:59 PM
 * Description: 
 */

import org.ggf.drmaa.{SessionFactory, Session}

class JobManager {

  protected val session: Session = SessionFactory.getFactory.getSession

  def init = session.init("")


  def exit() {
    session.exit()
  }

  //def runnerType =

  //def create

  //def updateStatus

}
