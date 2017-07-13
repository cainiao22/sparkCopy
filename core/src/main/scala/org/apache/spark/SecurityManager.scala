package org.apache.spark

import org.apache.spark.deploy.SparkHadoopUtil

/**
 * Spark class responsible for security.
 *
 * In general this class should be instantiated by the SparkEnv and most components
 * should access it from that. There are some cases where the SparkEnv hasn't been
 * initialized yet and this class must be instantiated directly.
 *
 * Spark currently supports authentication via a shared secret.
 * Authentication can be configured to be on via the 'spark.authenticate' configuration
 * parameter. This parameter controls whether the Spark communication protocols do
 * authentication using the shared secret. This authentication is a basic handshake to
 * make sure both sides have the same shared secret and are allowed to communicate.
 * If the shared secret is not identical they will not be allowed to communicate.
 *
 * The Spark UI can also be secured by using javax servlet filters. A user may want to
 * secure the UI if it has data that other users should not be allowed to see. The javax
 * servlet filter specified by the user can authenticate the user and then once the user
 * is logged in, Spark can compare that user versus the view acls to make sure they are
 * authorized to view the UI. The configs 'spark.ui.acls.enable' and 'spark.ui.view.acls'
 * control the behavior of the acls. Note that the person who started the application
 * always has view access to the UI.
 *
 * Spark does not currently support encryption after authentication.
 *
 * At this point spark has multiple communication protocols that need to be secured and
 * different underlying mechanisms are used depending on the protocol:
 *
 * - Akka -> The only option here is to use the Akka Remote secure-cookie functionality.
 * Akka remoting allows you to specify a secure cookie that will be exchanged
 * and ensured to be identical in the connection handshake between the client
 * and the server. If they are not identical then the client will be refused
 * to connect to the server. There is no control of the underlying
 * authentication mechanism so its not clear if the password is passed in
 * plaintext or uses DIGEST-MD5 or some other mechanism.
 * Akka also has an option to turn on SSL, this option is not currently supported
 * but we could add a configuration option in the future.
 *
 * - HTTP for broadcast and file server (via HttpServer) ->  Spark currently uses Jetty
 * for the HttpServer. Jetty supports multiple authentication mechanisms -
 * Basic, Digest, Form, Spengo, etc. It also supports multiple different login
 * services - Hash, JAAS, Spnego, JDBC, etc.  Spark currently uses the HashLoginService
 * to authenticate using DIGEST-MD5 via a single user and the shared secret.
 * Since we are using DIGEST-MD5, the shared secret is not passed on the wire
 * in plaintext.
 * We currently do not support SSL (https), but Jetty can be configured to use it
 * so we could add a configuration option for this in the future.
 *
 * The Spark HttpServer installs the HashLoginServer and configures it to DIGEST-MD5.
 * Any clients must specify the user and password. There is a default
 * Authenticator installed in the SecurityManager to how it does the authentication
 * and in this case gets the user name and password from the request.
 *
 * - ConnectionManager -> The Spark ConnectionManager uses java nio to asynchronously
 * exchange messages.  For this we use the Java SASL
 * (Simple Authentication and Security Layer) API and again use DIGEST-MD5
 * as the authentication mechanism. This means the shared secret is not passed
 * over the wire in plaintext.
 * Note that SASL is pluggable as to what mechanism it uses.  We currently use
 * DIGEST-MD5 but this could be changed to use Kerberos or other in the future.
 * Spark currently supports "auth" for the quality of protection, which means
 * the connection is not supporting integrity or privacy protection (encryption)
 * after authentication. SASL also supports "auth-int" and "auth-conf" which
 * SPARK could be support in the future to allow the user to specify the quality
 * of protection they want. If we support those, the messages will also have to
 * be wrapped and unwrapped via the SaslServer/SaslClient.wrap/unwrap API's.
 *
 * Since the connectionManager does asynchronous messages passing, the SASL
 * authentication is a bit more complex. A ConnectionManager can be both a client
 * and a Server, so for a particular connection is has to determine what to do.
 * A ConnectionId was added to be able to track connections and is used to
 * match up incoming messages with connections waiting for authentication.
 * If its acting as a client and trying to send a message to another ConnectionManager,
 * it blocks the thread calling sendMessage until the SASL negotiation has occurred.
 * The ConnectionManager tracks all the sendingConnections using the ConnectionId
 * and waits for the response from the server and does the handshake.
 *
 * - HTTP for the Spark UI -> the UI was changed to use servlets so that javax servlet filters
 * can be used. Yarn requires a specific AmIpFilter be installed for security to work
 * properly. For non-Yarn deployments, users can write a filter to go through a
 * companies normal login service. If an authentication filter is in place then the
 * SparkUI can be configured to check the logged in user against the list of users who
 * have view acls to see if that user is authorized.
 * The filters can also be used for many different purposes. For instance filters
 * could be used for logging, encryption, or compression.
 *
 * The exact mechanisms used to generate/distributed the shared secret is deployment specific.
 *
 * For Yarn deployments, the secret is automatically generated using the Akka remote
 * Crypt.generateSecureCookie() API. The secret is placed in the Hadoop UGI which gets passed
 * around via the Hadoop RPC mechanism. Hadoop RPC can be configured to support different levels
 * of protection. See the Hadoop documentation for more details. Each Spark application on Yarn
 * gets a different shared secret. On Yarn, the Spark UI gets configured to use the Hadoop Yarn
 * AmIpFilter which requires the user to go through the ResourceManager Proxy. That Proxy is there
 * to reduce the possibility of web based attacks through YARN. Hadoop can be configured to use
 * filters to do authentication. That authentication then happens via the ResourceManager Proxy
 * and Spark will use that to do authorization against the view acls.
 *
 * For other Spark deployments, the shared secret must be specified via the
 * spark.authenticate.secret config.
 * All the nodes (Master and Workers) and the applications need to have the same shared secret.
 * This again is not ideal as one user could potentially affect another users application.
 * This should be enhanced in the future to provide better protection.
 * If the UI needs to be secured the user needs to install a javax servlet filter to do the
 * authentication. Spark will then use that user to compare against the view acls to do
 * authorization. If not filter is in place the user is generally null and no authorization
 * can take place.
 */
private[spark] class SecurityManager(conf: SparkConf) extends Logging {

  // key used to store the spark secret in the Hadoop UGI
  private val sparkSecretLookupKey = "sparkCooike"

  private val authOn = conf.getBoolean("spark.authenticate", false)
  private val uiAclsOn = conf.getBoolean("spark.ui.acls.enable", false)

  private var viewAcls: Set[String] = _
  // always add the current user and SPARK_USER to the viewAcls
  private val defaultAclUsers = Seq[String](System.getProperty("user.name", ""),
  Option(System.getenv("SPARK_USER")).getOrElse(""))

  setViewAcls(defaultAclUsers, conf.get("spark.ui.view.acls", ""))

  private val secretKey = generateSecretKey()

  logInfo("SecurityManager: authentication " + (if (authOn) "enabled" else "disabled") +
    "; ui acls " + (if (uiAclsOn) "enabled" else "disabled") +
    "; users with view permissions: " + viewAcls.toString())

  private[spark] def setViewAcls(defaultUsers: Seq[String], allowedUsers: String): Unit = {
    viewAcls = (defaultAclUsers ++ allowedUsers.split(",")).map(_.trim).filterNot(_.isEmpty).toSet
    logInfo("Changing view acls to: " + viewAcls.mkString(","))
  }

  /**
   * Generates or looks up the secret key.
   *
   * The way the key is stored depends on the Spark deployment mode. Yarn
   * uses the Hadoop UGI.
   *
   * For non-Yarn deployments, If the config variable is not set
   * we throw an exception.
   */
  private def generateSecretKey(): String = {
    if (!isAuthenticationEnabled) return null
    // first check to see if the secret is already set, else generate a new one if on yarn
    val sCookie = if (SparkHadoopUtil.get.isYarnMode()) {
      val securityKey = SparkHadoopUtil.get.getSecretKeyFromUserCredentials(sparkSecretLookupKey)
      if (secretKey != null) {
        logDebug("in yarn mode, getting secret from credentials")
        return secretKey
      } else {
        logDebug("getSecretKey: yarn mode, secret key from credentials is null")
      }
      val cookie = akka.util.Crypt.generateSecureCookie
      // if we generated the secret then we must be the first so lets set it so t
      // gets used by everyone else
      SparkHadoopUtil.get.addSecretKeyToUserCredentials(sparkSecretLookupKey, cookie)
      logInfo("adding secret to credentials in yarn mode")
      cookie
    } else {
      // user must have set spark.authenticate.secret config
      conf.getOption("spark.authenticate.secret") match {
        case Some(value) => value
        case None => throw new Exception("Error: a secret key must be specified via the " +
          "spark.authenticate.secret config")
      }
    }

    sCookie
  }

  /**
   * Check to see if authentication for the Spark communication protocols is enabled
   * @return true if authentication is enabled, otherwise false
   */
  def isAuthenticationEnabled(): Boolean = authOn

  /**
   * Gets the user used for authenticating HTTP connections.
   * For now use a single hardcoded user.
   * @return the HTTP user as a String
   */
  def getHttpUser(): String = "sparkHttpUser"

  /**
   * Gets the user used for authenticating SASL connections.
   * For now use a single hardcoded user.
   * @return the SASL user as a String
   */
  def getSaslUser(): String = "sparkSaslUser"

  /**
   * Gets the secret key.
   * @return the secret key as a String if authentication is enabled, otherwise returns null
   */
  def getSecretKey(): String = secretKey
}
