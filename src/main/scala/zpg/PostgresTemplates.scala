package zpg

import zio._

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.DriverManager

final case class PgRoot(
  host: String,
  port: Int,
  user: String,
  password: String,
  adminDatabase: String
) {
  def jdbcUrl(database: String): String =
    s"jdbc:postgresql://$host:$port/$database"
}

final case class TestDatabase(
  name: String,
  jdbcUrl: String,
  host: String,
  port: Int,
  user: String,
  password: String
)

final case class TemplateKey(value: String) extends AnyVal

object TemplateKey {
  def fromContent(content: String): TemplateKey = {
    val digest = MessageDigest.getInstance("SHA-256")
    val bytes  = digest.digest(content.getBytes(StandardCharsets.UTF_8))
    val hex    = bytes.map("%02x".format(_)).mkString

    TemplateKey(hex.take(16))
  }
}

final case class PgTemplateConfig(
  key: TemplateKey,
  databasePrefix: String = "zpg",
  templateDatabasePrefix: String = "template",
  testDatabasePrefix: String = "test",
  rootTemplate: String = "template0",
  initialPoolSize: Int = 4,
  maxPoolSize: Int = 32,
  reuseExistingTemplate: Boolean = true
) {
  require(initialPoolSize >= 0, "initialPoolSize must be non-negative")
  require(maxPoolSize > 0, "maxPoolSize must be positive")
  require(initialPoolSize <= maxPoolSize, "initialPoolSize cannot exceed maxPoolSize")
}

object PgNames {
  def template(config: PgTemplateConfig) =
    s"${config.databasePrefix}_${config.templateDatabasePrefix}_${config.key.value}"

  def test(config: PgTemplateConfig, id: Int) =
    s"${config.databasePrefix}_${config.testDatabasePrefix}_${config.key.value}_${id.toString.reverse.padTo(6, '0').reverse}"
}

object PostgresAdmin {
  def prepareTemplate(pg: PgRoot, config: PgTemplateConfig, migrateTemplate: TestDatabase => Task[Unit]) =
    for {
      exists <- databaseExists(pg, PgNames.template(config))
      _      <- dropTestDatabases(pg, config)
      _      <- ZIO.when(!exists || !config.reuseExistingTemplate)(recreateTemplate(pg, config) *> migrateTemplate(templateDb(pg, config)))
    } yield ()

  private def recreateTemplate(pg: PgRoot, config: PgTemplateConfig) = {
    val template = dbIdentifier(PgNames.template(config))
    val owner    = dbIdentifier(pg.user)
    val source   = dbIdentifier(config.rootTemplate)

    adminStatement(pg, s"DROP DATABASE IF EXISTS $template") *>
      adminStatement(pg, s"CREATE DATABASE $template WITH OWNER $owner TEMPLATE $source")
  }

  private def templateDb(pg: PgRoot, config: PgTemplateConfig) =
    TestDatabase(
      PgNames.template(config),
      pg.jdbcUrl(PgNames.template(config)),
      pg.host,
      pg.port,
      pg.user,
      pg.password
    )

  def createTestDatabase(pg: PgRoot, config: PgTemplateConfig, id: Int) = {
    val name     = PgNames.test(config, id)
    val database = dbIdentifier(name)
    val owner    = dbIdentifier(pg.user)
    val template = dbIdentifier(PgNames.template(config))

    adminStatement(pg, s"DROP DATABASE IF EXISTS $database") *>
      adminStatement(pg, s"CREATE DATABASE $database WITH OWNER $owner TEMPLATE $template")
        .as(TestDatabase(name, pg.jdbcUrl(name), pg.host, pg.port, pg.user, pg.password))
  }

  def dropDatabase(pg: PgRoot, name: String) =
    adminStatement(pg, s"DROP DATABASE IF EXISTS ${dbIdentifier(name)}").orDie

  private def dropTestDatabases(pg: PgRoot, config: PgTemplateConfig) =
    for {
      names <- testDatabases(pg, config)
      _     <- ZIO.foreachDiscard(names)(dropDatabase(pg, _))
    } yield ()

  private def databaseExists(pg: PgRoot, name: String) =
    ZIO.attemptBlocking {
      val connection = DriverManager.getConnection(pg.jdbcUrl(pg.adminDatabase), pg.user, pg.password)
      try {
        val statement = connection.prepareStatement("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = ?)")
        try {
          statement.setString(1, name)
          val result = statement.executeQuery()
          result.next()
          result.getBoolean(1)
        } finally statement.close()
      } finally connection.close()
    }

  private def testDatabases(pg: PgRoot, config: PgTemplateConfig) =
    ZIO.attemptBlocking {
      val connection = DriverManager.getConnection(pg.jdbcUrl(pg.adminDatabase), pg.user, pg.password)
      try {
        val statement = connection.prepareStatement("SELECT datname FROM pg_database WHERE datname LIKE ?")
        try {
          statement.setString(1, s"${config.databasePrefix}_${config.testDatabasePrefix}_${config.key.value}_%")
          val result = statement.executeQuery()
          val names  = scala.collection.mutable.ListBuffer.empty[String]
          while (result.next()) names += result.getString(1)
          names.toList
        } finally statement.close()
      } finally connection.close()
    }

  private def adminStatement(pg: PgRoot, sql: String) =
    ZIO.attemptBlocking {
      val connection = DriverManager.getConnection(pg.jdbcUrl(pg.adminDatabase), pg.user, pg.password)
      try {
        connection.setAutoCommit(true)
        val statement = connection.createStatement()
        try statement.execute(sql)
        finally statement.close()
      } finally connection.close()
    }.unit

  private def dbIdentifier(name: String) = {
    require(name.matches("[a-z][a-z0-9_]*"), s"Invalid database name: $name")
    "\"" + name + "\""
  }
}

trait PgTemplateManager {
  def acquire: ZIO[Scope, Throwable, TestDatabase]
}

object PgTemplateManager {
  def live(
    config: PgTemplateConfig,
    migrateTemplate: TestDatabase => Task[Unit]
  ): ZLayer[PgRoot, Throwable, PgTemplateManager] =
    ZLayer.scoped {
      for {
        pg      <- ZIO.service[PgRoot]
        manager <- LivePgTemplateManager.make(pg, config, migrateTemplate)
      } yield manager
    }
}

final class LivePgTemplateManager private (
  pg: PgRoot,
  config: PgTemplateConfig,
  ready: Queue[TestDatabase],
  nextId: Ref[Int],
  total: Ref[Int],
  createLock: Semaphore
) extends PgTemplateManager {
  override def acquire =
    ZIO.acquireRelease(checkout)(db => release(db))

  private def checkout =
    ready.poll.flatMap {
      case Some(db) =>
        warmOne.forkDaemon.as(db)
      case None =>
        total.get.flatMap { current =>
          if (current < config.maxPoolSize) createLeased <* warmOne.forkDaemon
          else ready.take <* warmOne.forkDaemon
        }
    }

  private def release(db: TestDatabase) =
    PostgresAdmin.dropDatabase(pg, db.name) *>
      total.update(n => (n - 1).max(0)) *>
      warmOne.forkDaemon.unit

  private def createLeased: Task[TestDatabase] =
    createLock.withPermit {
      total.get.flatMap { current =>
        if (current < config.maxPoolSize) createNext
        else ready.take
      }
    }

  private[zpg] def warmPool =
    warmOne.repeatWhile(identity).unit

  private def warmOne: UIO[Boolean] =
    createLock.withPermit {
      (ready.size zip total.get).flatMap { case (readyCount, currentTotal) =>
        if (readyCount < config.initialPoolSize && currentTotal < config.maxPoolSize)
          createNext.flatMap(ready.offer).orDie
        else
          ZIO.succeed(false)
      }
    }

  private def createNext =
    for {
      id <- nextId.getAndUpdate(_ + 1)
      db <- PostgresAdmin.createTestDatabase(pg, config, id)
      _  <- total.update(_ + 1)
    } yield db
}

object LivePgTemplateManager {
  def make(
    pg: PgRoot,
    config: PgTemplateConfig,
    migrateTemplate: TestDatabase => Task[Unit]
  ): ZIO[Scope, Throwable, PgTemplateManager] =
    for {
      ready      <- Queue.unbounded[TestDatabase]
      nextId     <- Ref.make(0)
      total      <- Ref.make(0)
      createLock <- Semaphore.make(1)
      // PostgreSQL can clone a template database only when no other sessions are using it.
      // Run migrations through one short-lived connection, close it, then clone.
      _       <- PostgresAdmin.prepareTemplate(pg, config, migrateTemplate)
      manager = LivePgTemplateManager(pg, config, ready, nextId, total, createLock)
      _       <- manager.warmPool
    } yield manager
}
