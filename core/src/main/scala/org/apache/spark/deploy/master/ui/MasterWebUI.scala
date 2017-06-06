package org.apache.spark.deploy.master.ui

import org.apache.spark.Logging
import org.apache.spark.deploy.master.Master
import org.apache.spark.ui.WebUI

/**
 * Created by Administrator on 2017/6/6.
 */
class MasterWebUI(val master:Master, requestedPort:Int)
extends WebUI(master.securityMgr, requestedPort, master.conf) with Logging {

}
