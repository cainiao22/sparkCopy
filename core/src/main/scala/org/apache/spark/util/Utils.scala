package org.apache.spark.util

import java.io.{OutputStream, InputStream, IOException, File}
import java.net.{InetAddress, Inet4Address, NetworkInterface, URI, URL, URLConnection}
import java.util.{UUID, Locale}

import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{SparkException, Logging}
import org.apache.spark.executor.ExecutorUncaughtExceptionHandler

import scala.collection.JavaConversions._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Try
import scala.util.control.ControlThrowable

/**
 * Created by Administrator on 2017/5/23.
 */
private[spark] object Utils extends Logging {

  /**
   * Whether the underlying operating system is Windows.
   */
  val isWindows = SystemUtils.IS_OS_WINDOWS

  /**
   * Pattern for matching a Windows drive, which contains only a single alphabet character.
   */
  val windowsDriver = "(a-zA-Z)".r

  /**
   * Get the ClassLoader which loaded Spark.
   */
  def getSparkClassLoader = getClass.getClassLoader

  def getContextOrSparkClassLoader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  def classIsLoadable(clazz: String): Boolean = {
    Try {
      Class.forName(clazz, false, getContextOrSparkClassLoader)
    }.isSuccess
  }

  /**
   * Format a Windows path such that it can be safely passed to a URI.
   */
  def formatWindowsPath(path: String): String = path.replace("\\", "/")

  /**
   * Indicates whether Spark is currently running unit tests.
   */
  def isTesting = {
    sys.env.contains("SPARK_TESTING") || sys.props.contains("spark.testing")
  }

  /**
   * Strip the directory from a path name
   */
  def stripDirectory(path: String): String = {
    new File(path).getName
  }

  /**
   * Return a well-formed URI for the file described by a user input string.
   *
   * If the supplied path does not contain a scheme, or is a relative path, it will be
   * converted into an absolute path with a file:// scheme.
   */
  def resolveURI(path: String, testWindows: Boolean = false): URI = {

    // In Windows, the file separator is a backslash(反斜线), but this is inconsistent(不一致) with the URI format
    val windows = isWindows || testWindows
    val formattedPath = if (windows) formatWindowsPath(path) else path
    val uri = new URI(formattedPath)
    if (uri.getPath == null) {
      throw new IllegalArgumentException(s"Given path is malformed: $uri")
    }
    uri.getScheme match {
      case windowsDriver(d) if windows =>
        new URI("file:/" + uri.toString.stripPrefix("/"))
      case null =>
        val fragment = uri.getFragment
        val part = new File(uri.getPath).toURI
        new URI(part.getScheme, part.getPath, fragment)
      case _ =>
        uri
    }
  }

  /** Resolve a comma-separated list of paths. */
  def resolveURIs(paths: String, testWindows: Boolean = false): String = {
    if (paths == null || paths.trim.isEmpty) {
      ""
    } else {
      paths.split(",").map { p => Utils.resolveURI(p, testWindows) }.mkString(",")
    }
  }

  /** Return all non-local paths from a comma-separated list of paths. */
  def nonLocalPaths(paths: String, testWindows: Boolean = false): Array[String] = {
    val windows = isWindows || testWindows
    if (paths == null || paths.trim.isEmpty) {
      Array.empty
    } else {
      paths.split(",").filter { p =>
        val formattedPath = if (windows) formatWindowsPath(p) else p
        new URI(formattedPath).getScheme match {
          case windowsDriver(d) if windows => false
          case "local" | "file" | null => false
          case _ => true
        }
      }
    }
  }

  def checkHost(host: String, message: String = "") {
    assert(host.indexOf(':') == -1, message)
  }

  def checkHostPort(hostPort: String, message: String = "") {
    assert(hostPort.indexOf(':') != -1, message)
  }

  lazy val localIpAddress: String = findLocalIpAddress()
  lazy val localIpAddressHostname: String = getAddressHostName(localIpAddress)

  private def findLocalIpAddress(): String = {
    val defaultIpOverrde = System.getenv("SPARK_LOCAL_IP")
    if (defaultIpOverrde != null) {
      defaultIpOverrde
    } else {
      val address = InetAddress.getLocalHost
      if (address.isLoopbackAddress) {
        for (ni <- NetworkInterface.getNetworkInterfaces) {
          for (addr <- ni.getInetAddresses if !addr.isLinkLocalAddress
            && !addr.isLoopbackAddress && addr.isInstanceOf[Inet4Address]) {
            // We've found an address that looks reasonable!
            logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
              " a loopback address: " + address.getHostAddress + "; using " + addr.getHostAddress +
              " instead (on interface " + ni.getName + ")")
            logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
            return addr.getHostAddress
          }
        }
        logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
          " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
          " external IP address!")
        logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
      }
      address.getHostAddress
    }
  }

  private var customHostname: Option[String] = None

  def setCustomHostname(hostname: String): Unit = {
    customHostname = Some(hostname)
  }

  def getAddressHostName(address: String): String = {
    InetAddress.getByName(address).getHostName
  }

  def localHostName(): String = {
    customHostname.getOrElse(localIpAddressHostname)
  }

  /** Return the class name of the given object, removing all dollar signs */
  def getFormattedClassName(obj: AnyRef) = {
    obj.getClass.getSimpleName.replace("$", "")
  }

  /**
   * Execute a block of code that evaluates to Unit, forwarding any uncaught exceptions to the
   * default UncaughtExceptionHandler
   */
  def tryOrExit(block : => Unit): Unit ={
    try{
      block
    }catch {
      case e:Throwable => ExecutorUncaughtExceptionHandler.uncaughtException(e)
    }
  }


  def inShutdown():Boolean = {
    try{
      val hook = new Thread(){
        override def run(){}
      }
      Runtime.getRuntime.addShutdownHook(hook)
      //这里会返回true表示移除成功， 但是这时候代表的是运行中
      Runtime.getRuntime.removeShutdownHook(hook)
    }catch {
      case ise:IllegalStateException => return true
    }
    false
  }

  def megabytesToString(megabytes:Long):String = {
    bytesToString(megabytes * 1024L * 1024L)
  }

  def bytesToString(size:Long):String = {
    val TB = 1 << 40
    val GB = 1 << 30
    val MB = 1 << 20
    val KB = 1 << 10
    val (value, unit) = {
      if (size >= 2*TB) {
        (size.asInstanceOf[Double] / TB, "TB")
      } else if (size >= 2*GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (size >= 2*MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else if (size >= 2*KB) {
        (size.asInstanceOf[Double] / KB, "KB")
      } else {
        (size.asInstanceOf[Double], "B")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  private def listFilesSafely(file: File):Seq[File] = {
    val files = file.listFiles()
    if(files == null){
      throw new IOException("Failed to list files for dir: " + file)
    }
    files
  }

  /**
   * Delete a file or directory and its contents recursively.
   * Don't follow directories if they are symlinks.
   */
  def deleteRecursively(file:File): Unit ={
    if(file != null){
      if(file.isDirectory && !isSymlink(file)){
        for(child <- file.listFiles()){
          deleteRecursively(child)
        }
      }
      if(!file.delete()){
        // Delete can also fail if the file simply did not exist
        if(file.exists()){
          throw new IOException("Failed to delete: " + file.getAbsolutePath)
        }
      }
    }
  }

  /**
   *  Check to see if file is a symbolic link.
   */
  //todo ??? 怎么判断的？
  def isSymlink(file: File): Boolean = {
    if (file == null) throw new NullPointerException("File must not be null")
    if (isWindows) return false
    val fileInCanonicalDir = if (file.getParent() == null) {
      file
    } else {
      new File(file.getParentFile().getCanonicalFile(), file.getName())
    }

    if (fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile())) {
      return false
    } else {
      return true
    }
  }

  /**
   * Finds all the files in a directory whose last modified time is older than cutoff seconds.
   * @param dir  must be the path to a directory, or IllegalArgumentException is thrown
   * @param cutoff measured in seconds. Files older than this are returned.
   */
  def findOldestFiles(dir: File, cutoff: Long): Seq[File] = {
    val currentTimeMillis = System.currentTimeMillis()
    if(dir.isDirectory){
      val files = listFilesSafely(dir)
      files.filter{file => file.lastModified() < (currentTimeMillis - cutoff*1000)}
    }else{
      throw new IllegalArgumentException(dir + " is not a directory!")
    }
  }

  /**
   * Split a string of potentially quoted arguments from the command line the way that a shell
   * would do it to determine arguments to a command. For example, if the string is 'a "b c" d',
   * then it would be parsed as three arguments: 'a', 'b c' and 'd'.
   */
  def splitCommandString(s:String):Seq[String] = {
    val buf = new ArrayBuffer[String]()
    var inWord = false
    var inSingleQuote = false
    var inDoubleQuote = false
    val curWord = new StringBuilder
    def endWord(): Unit ={
      buf += curWord.toString
      curWord.clear()
    }

    var i = 0;
    while(i < s.length){
      val nextChar = s.charAt(i)
      if(inDoubleQuote){
        if(nextChar == '"'){
          inDoubleQuote = false
        }else if(nextChar == '\\'){
          if(i < s.length - 1){
            curWord.append(s.charAt(i+1))
            i += 1
          }
        }else{
          curWord.append(nextChar)
        }
      } else if(inSingleQuote) {
        if(nextChar == '\''){
          inSingleQuote = false
        }else {
          curWord.append(nextChar)
        }
      }else if(nextChar == '"'){
        inWord = true
        inDoubleQuote = true
      }else if(nextChar == '\''){
        inWord = true
        inSingleQuote = true
      }else if(isSpace(nextChar)){
        endWord()
        inWord = false
      }
      i += 1
    }

    if(inWord || inSingleQuote || inDoubleQuote){
      endWord()
    }
    buf
  }

  def isSpace(c:Char):Boolean = {
    "\t\r\n".indexOf(c) != -1
  }

  def excutaAndGetOutput(command:Seq[String], workDir:File = new File("."),
                          extraEnvironment:Map[String, String] = Map.empty):String = {
    val builder = new ProcessBuilder(command:_*)
      //工作目录：进行操作需要的资源，比如文件什么的，都会在这个目录进行查找
          .directory(workDir)
    val environment = builder.environment()
    for((k, v) <- extraEnvironment){
      environment.put(k, v)
    }

    val process = builder.start()
    new Thread("read stderr for " + command(0)){
      override def run(): Unit ={
        for(line <- Source.fromInputStream(process.getErrorStream).getLines()){
          System.err.println(line)
        }
      }
    }.start()

    val output = new StringBuffer
    val stdoutThread = new Thread("read stdout for " + command(0)){
      override def run: Unit ={
        for(line <- Source.fromInputStream(process.getInputStream).getLines()){
          output.append(line)
        }
      }
    }
    stdoutThread.start()
    val exitCode = process.waitFor()
    //process停止了，但是输出流缓冲区未必读取完毕
    stdoutThread.join()
    if (exitCode != 0) {
      throw new SparkException("Process " + command + " exited with code " + exitCode)
    }
    output.toString
  }

  /** Copy all data from an InputStream to an OutputStream */
  def coupStream(in:InputStream,
                 out:OutputStream,
                 closeStreams:Boolean = false): Unit ={
    val buf = new Array[Byte](8192)
    var n = 0
    while(n != -1){
      n = in.read(buf)
      if(n != -1){
        out.write(buf, 0, n)
      }
    }

    if(closeStreams){
      in.close()
      out.close()
    }
  }

  def logUncaughtExceptions[T](f: => T): T = {
    try{
      f
    }catch {
      case ct:ControlThrowable =>
        throw ct
      case t:Throwable =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        throw t
    }
  }

  /**
   * Return a Hadoop FileSystem with the scheme encoded in the given path.
   */
  def getHadoopFileSystem(path:URI):FileSystem = {
    FileSystem.get(path, SparkHadoopUtil.get.newConfiguration())
  }


  // Note: if file is child of some registered path, while not equal to it, then return true;
  // else false. This is to ensure that two shutdown hooks do not try to delete each others
  // paths - resulting in IOException and incomplete cleanup.
  def hasRootAsShutdownDeleteDir(dir:File):Boolean = {
    val absolutPath = dir.getAbsolutePath
    val retval = shutdownDeletePaths.synchronized{
      shutdownDeletePaths.exists{ path =>
        !absolutPath.equals(path) && absolutPath.startsWith(path)
      }
    }

    if (retval) {
      logInfo("path = " + absolutPath + ", already present as root for deletion.")
    }
    retval
  }

  def createTempDir(root:String = System.getProperty("java.io.tmpdir")):File = {
    var attempts = 0
    val maxAttempts = 10
    var dir:File = null
    while(dir == null){
      attempts += 1
      if(attempts > maxAttempts){
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try{
        dir = new File(root, "spark-" + UUID.randomUUID().toString)
        if(dir.exists() || !dir.mkdirs()){
          dir = null
        }
      }catch { case e: SecurityException => dir = null; }
    }

    registerShutdownDeleteDir(dir)
    Runtime.getRuntime.addShutdownHook(new Thread("delete Spark temp dir " + dir){
      override def run(): Unit ={
        // Attempt to delete if some patch which is parent of this is not already registered.
        if (! hasRootAsShutdownDeleteDir(dir)) Utils.deleteRecursively(dir)
      }
    })
  }
  private val shutdownDeletePaths = new scala.collection.mutable.HashSet[String]()

  def registerShutdownDeleteDir(file:File): Unit ={
    val absolutePath = file.getAbsolutePath
    synchronized{
      shutdownDeletePaths += absolutePath
    }
  }
}
