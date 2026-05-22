package demo

import demo.PostgresTestAspect.migrate
import com.dimafeng.testcontainers.PostgreSQLContainer
import doobie._
import doobie.implicits._
import io.github.gaelrenoux.tranzactio.DbException
import io.github.gaelrenoux.tranzactio.doobie.{Database, tzio}
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.utility.DockerImageName
import zio._
import zio.interop.catz._
import zio.jdbc.{JdbcDecoder, ZConnection, ZConnectionPool, ZConnectionPoolConfig, transaction}
import zio.test._
import zio.test.TestAspect.PerTest
import zpg._

import java.util

type DoobieTransactor = Transactor.Aux[Task, Unit]
type ZioJdbcPool = ZConnectionPool

object TestContainer {
  val postgres =
    ZLayer.scoped {
      ZIO.acquireRelease {
        ZIO.attemptBlocking {
          val container = PostgreSQLContainer(
            dockerImageNameOverride = DockerImageName.parse("postgres:16-alpine")
          )

          container.container.withCommand(
            "postgres",
            "-c",
            "fsync=off",
            "-c",
            "synchronous_commit=off",
            "-c",
            "full_page_writes=off",
            "-c",
            "max_connections=200"
          )
          container.container.withTmpFs(util.Map.of("/var/lib/postgresql/data", "rw"))
          container.start()
          container
        }
      }(container => ZIO.attemptBlocking(container.stop()).orDie)
    }
}

object UnitTestEnvironment {
  val postgres: ZLayer[Any, Throwable, PgRoot] =
    TestContainer.postgres.map { env =>
      val container = env.get[PostgreSQLContainer]

      ZEnvironment(
        PgRoot(
          host = container.host,
          port = container.mappedPort(5432),
          user = container.username,
          password = container.password,
          adminDatabase = container.databaseName
        )
      )
    }
}

object Schema {
  val migrationSql =
    """
      CREATE TABLE user_accounts (
        id           BIGSERIAL PRIMARY KEY,
        email        TEXT NOT NULL UNIQUE,
        display_name TEXT NOT NULL,
        active       BOOLEAN NOT NULL DEFAULT FALSE,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        deleted_at   TIMESTAMPTZ
      );

      CREATE INDEX user_accounts_active_idx ON user_accounts(active);
    """

  val templateKey =
    TemplateKey.fromContent(migrationSql)

  def migrate(db: TestDatabase) = {
    val xa = Transactor.fromDriverManager[Task](
      "org.postgresql.Driver",
      db.jdbcUrl,
      db.user,
      db.password,
      None
    )

    Fragment.const(migrationSql).update.run.transact(xa).unit
  }
}

object Clients {
  def doobie(db: TestDatabase) =
    ZIO.succeed(transactor(db.jdbcUrl, db.user, db.password))

  def doobiePlaceholder =
    Transactor.fromDriverManager[Task](
      "org.postgresql.Driver",
      "jdbc:postgresql://localhost:1/placeholder",
      "placeholder",
      "placeholder",
      None
    )

  def tranzactio(db: TestDatabase) = {
    val ds = new PGSimpleDataSource()
    ds.setUrl(db.jdbcUrl)
    ds.setUser(db.user)
    ds.setPassword(db.password)

    (ZLayer.succeed[javax.sql.DataSource](ds) >>> Database.fromDatasource)
      .build
      .map(_.get[Database])
  }

  def tranzactioPlaceholder =
    Unsafe.unsafe { implicit unsafe =>
      val ds = new PGSimpleDataSource()
      ds.setUrl("jdbc:postgresql://localhost:1/placeholder")
      ds.setUser("placeholder")
      ds.setPassword("placeholder")

      Runtime.default.unsafe.run {
        (ZLayer.succeed[javax.sql.DataSource](ds) >>> Database.fromDatasource)
          .build
          .map(_.get[Database])
          .provideLayer(Scope.default)
      }.getOrThrowFiberFailure()
    }

  def zioJdbc(db: TestDatabase) =
    (ZLayer.succeed(ZConnectionPoolConfig.default.copy(minConnections = 1, maxConnections = 4)) >>>
      ZConnectionPool.postgres(
        db.host,
        db.port,
        db.name,
        Map("user" -> db.user, "password" -> db.password)
      )).build.map(_.get[ZConnectionPool])

  def zioJdbcPlaceholder =
    new ZConnectionPool {
      override def transaction =
        ZLayer.fail(new RuntimeException("placeholder zio-jdbc pool was used without @@ migrate"))

      override def invalidate(conn: ZConnection) =
        ZIO.unit
    }

  private def transactor(url: String, user: String, password: String) =
    Transactor.fromDriverManager[Task]("org.postgresql.Driver", url, user, password, None)
}

object PostgresTestAspect {
  def migrate[A: Tag](
    make: TestDatabase => ZIO[Scope, Throwable, A]
  ) =
    new PerTest.AtLeastR[PgTemplateManager & A] {
      override def perTest[R <: PgTemplateManager & A, E](
        test: ZIO[R, TestFailure[E], TestSuccess]
      )(implicit trace: Trace): ZIO[R, TestFailure[E], TestSuccess] = {
        val scoped =
          for {
            manager <- ZIO.service[PgTemplateManager]
            db      <- manager.acquire.mapError(TestFailure.die)
            client  <- make(db).mapError(TestFailure.die)
            result  <- test.provideSomeEnvironment[R](_.add[A](client))
          } yield result

        ZIO.scoped[R](scoped)
      }
    }
}

object PostgresTestBootstrap {
  val config =
    PgTemplateConfig(
      key = Schema.templateKey,
      initialPoolSize = 6,
      maxPoolSize = 24
    )

  val layer =
    UnitTestEnvironment.postgres >+> PgTemplateManager.live(config, Schema.migrate)
}

abstract class PostgresTemplateSpec extends ZIOSpec[TestEnvironment & PgRoot & PgTemplateManager] {
  override val bootstrap =
    testEnvironment ++ PostgresTestBootstrap.layer
}

object PostgresTemplateDemoSpec extends PostgresTemplateSpec {
  private val doobieLayer =
    ZLayer.succeed(Clients.doobiePlaceholder)

  private val tranzactioLayer =
    ZLayer.succeed(Clients.tranzactioPlaceholder)

  private val zioJdbcLayer =
    ZLayer.succeed(Clients.zioJdbcPlaceholder)

  final case class AccountRow(
    email: String,
    displayName: String,
    active: Boolean,
    deleted: Boolean
  )

  def createAccount(email: String, displayName: String): ConnectionIO[Long] =
    sql"""
      INSERT INTO user_accounts(email, display_name)
      VALUES ($email, $displayName)
      RETURNING id
    """.query[Long].unique

  def activate(id: Long): ConnectionIO[Int] =
    sql"""
      UPDATE user_accounts
      SET active = TRUE, updated_at = now()
      WHERE id = $id AND deleted_at IS NULL
    """.update.run

  def deactivate(id: Long): ConnectionIO[Int] =
    sql"""
      UPDATE user_accounts
      SET active = FALSE, updated_at = now()
      WHERE id = $id AND deleted_at IS NULL
    """.update.run

  def rename(id: Long, displayName: String): ConnectionIO[Int] =
    sql"""
      UPDATE user_accounts
      SET display_name = $displayName, updated_at = now()
      WHERE id = $id AND deleted_at IS NULL
    """.update.run

  def deleteAccount(id: Long): ConnectionIO[Int] =
    sql"""
      UPDATE user_accounts
      SET active = FALSE, deleted_at = now(), updated_at = now()
      WHERE id = $id AND deleted_at IS NULL
    """.update.run

  def account(email: String): ConnectionIO[Option[AccountRow]] =
    sql"""
      SELECT email, display_name, active, deleted_at IS NOT NULL
      FROM user_accounts
      WHERE email = $email
    """.query[AccountRow].option

  val countAccounts: ConnectionIO[Long] =
    sql"SELECT count(*) FROM user_accounts".query[Long].unique

  val activeAccounts: ConnectionIO[Long] =
    sql"SELECT count(*) FROM user_accounts WHERE active AND deleted_at IS NULL".query[Long].unique

  val currentDatabase: ConnectionIO[String] =
    sql"SELECT current_database()".query[String].unique

  def doobieSuite =
    (suite("doobie")(
      test("creates an inactive account") {
        for {
          xa  <- ZIO.service[DoobieTransactor]
          id  <- createAccount("alice@example.test", "Alice").transact(xa)
          row <- account("alice@example.test").transact(xa)
        } yield assertTrue(
          id > 0L,
          row.contains(AccountRow("alice@example.test", "Alice", active = false, deleted = false))
        )
      },
      test("uses deterministic database names") {
        for {
          xa   <- ZIO.service[DoobieTransactor]
          name <- currentDatabase.transact(xa)
        } yield assertTrue(name.matches("zpg_test_[a-f0-9]{16}_[0-9]{6}"))
      },
      test("activates and deactivates an account") {
        for {
          xa          <- ZIO.service[DoobieTransactor]
          id          <- createAccount("bob@example.test", "Bob").transact(xa)
          activated   <- activate(id).transact(xa)
          active1     <- activeAccounts.transact(xa)
          deactivated <- deactivate(id).transact(xa)
          active2     <- activeAccounts.transact(xa)
        } yield assertTrue(
          activated == 1,
          active1 == 1L,
          deactivated == 1,
          active2 == 0L
        )
      },
      test("edits an account profile") {
        for {
          xa  <- ZIO.service[DoobieTransactor]
          id  <- createAccount("carol@example.test", "Carol").transact(xa)
          _   <- rename(id, "Carol Danvers").transact(xa)
          row <- account("carol@example.test").transact(xa)
        } yield assertTrue(row.exists(_.displayName == "Carol Danvers"))
      },
      test("soft deletes an account") {
        for {
          xa  <- ZIO.service[DoobieTransactor]
          id  <- createAccount("dave@example.test", "Dave").transact(xa)
          _   <- activate(id).transact(xa)
          _   <- deleteAccount(id).transact(xa)
          row <- account("dave@example.test").transact(xa)
        } yield assertTrue(row.contains(AccountRow("dave@example.test", "Dave", active = false, deleted = true)))
      },
      test("starts from an empty cloned database") {
        for {
          xa <- ZIO.service[DoobieTransactor]
          n  <- countAccounts.transact(xa)
        } yield assertTrue(n == 0L)
      }
    ) @@ TestAspect.parallel @@ migrate(Clients.doobie))
      .provideSomeLayerShared[TestEnvironment & PgRoot & PgTemplateManager](doobieLayer)

  def managerSuite =
    suite("manager")(
      test("reuses an existing template for the same key") {
        val config =
          PgTemplateConfig(
            key = TemplateKey.fromContent("cache-reuse-demo"),
            initialPoolSize = 0,
            maxPoolSize = 4
          )

        def migrate(counter: Ref[Int])(db: TestDatabase) =
          counter.update(_ + 1) *> {
            val xa = Transactor.fromDriverManager[Task](
              "org.postgresql.Driver",
              db.jdbcUrl,
              db.user,
              db.password,
              None
            )

            sql"CREATE TABLE cache_marker (id BIGSERIAL PRIMARY KEY)".update.run.transact(xa).unit
          }

        for {
          pg      <- ZIO.service[PgRoot]
          counter <- Ref.make(0)
          _       <- ZIO.scoped(LivePgTemplateManager.make(pg, config, migrate(counter)))
          _       <- ZIO.scoped(LivePgTemplateManager.make(pg, config, migrate(counter)))
          n       <- counter.get
        } yield assertTrue(n == 1)
      }
    )

  object ZioJdbcAccounts {
    import zio.jdbc._

    implicit val accountRowDecoder: JdbcDecoder[AccountRow] =
      JdbcDecoder[(String, String, Boolean, Boolean)].map { case (email, displayName, active, deleted) =>
        AccountRow(email, displayName, active, deleted)
      }

    def create(email: String, displayName: String): ZIO[ZConnection, Throwable, Long] =
      sql"""
        INSERT INTO user_accounts(email, display_name)
        VALUES ($email, $displayName)
        RETURNING id
      """.insertReturning[Long].map(_.updatedKeys.head)

    def activate(id: Long): ZIO[ZConnection, Throwable, Long] =
      sql"""
        UPDATE user_accounts
        SET active = TRUE, updated_at = now()
        WHERE id = $id AND deleted_at IS NULL
      """.update

    def rename(id: Long, displayName: String): ZIO[ZConnection, Throwable, Long] =
      sql"""
        UPDATE user_accounts
        SET display_name = $displayName, updated_at = now()
        WHERE id = $id AND deleted_at IS NULL
      """.update

    def account(email: String): ZIO[ZConnection, Throwable, Option[AccountRow]] =
      sql"""
        SELECT email, display_name, active, deleted_at IS NOT NULL
        FROM user_accounts
        WHERE email = $email
      """.query[AccountRow].selectOne

    val count: ZIO[ZConnection, Throwable, Long] =
      sql"SELECT count(*) FROM user_accounts".query[Long].selectOne.map(_.getOrElse(0L))
  }

  def zioJdbcSuite =
    (suite("zio-jdbc")(
      test("creates and activates an account") {
        (for {
          id  <- ZioJdbcAccounts.create("gina@example.test", "Gina")
          _   <- ZioJdbcAccounts.activate(id)
          row <- ZioJdbcAccounts.account("gina@example.test")
        } yield assertTrue(row.contains(AccountRow("gina@example.test", "Gina", active = true, deleted = false))))
          .provideLayer(transaction)
      },
      test("edits an account profile") {
        (for {
          id  <- ZioJdbcAccounts.create("hank@example.test", "Hank")
          _   <- ZioJdbcAccounts.rename(id, "Henry")
          row <- ZioJdbcAccounts.account("hank@example.test")
        } yield assertTrue(row.exists(_.displayName == "Henry")))
          .provideLayer(transaction)
      },
      test("starts from an empty cloned database") {
        ZioJdbcAccounts.count
          .map(n => assertTrue(n == 0L))
          .provideLayer(transaction)
      }
    ) @@ TestAspect.parallel @@ migrate(Clients.zioJdbc))
      .provideSomeLayerShared[TestEnvironment & PgRoot & PgTemplateManager](zioJdbcLayer)

  def countTranzactio: ZIO[Database, DbException, Long] =
    ZIO.serviceWithZIO[Database](
      _.autoCommitOrWiden[Any, DbException, Long](
        tzio(countAccounts)
      )
    )

  def createTranzactio(email: String, displayName: String): ZIO[Database, DbException, Long] =
    ZIO.serviceWithZIO[Database](
      _.autoCommitOrWiden[Any, DbException, Long](
        tzio(createAccount(email, displayName))
      )
    )

  def activateTranzactio(id: Long): ZIO[Database, DbException, Int] =
    ZIO.serviceWithZIO[Database](
      _.autoCommitOrWiden[Any, DbException, Int](
        tzio(activate(id))
      )
    )

  def deleteTranzactio(id: Long): ZIO[Database, DbException, Int] =
    ZIO.serviceWithZIO[Database](
      _.autoCommitOrWiden[Any, DbException, Int](
        tzio(deleteAccount(id))
      )
    )

  def accountTranzactio(email: String): ZIO[Database, DbException, Option[AccountRow]] =
    ZIO.serviceWithZIO[Database](
      _.autoCommitOrWiden[Any, DbException, Option[AccountRow]](
        tzio(account(email))
      )
    )

  def tranzactioSuite =
    (suite("tranzactio")(
      test("uses an empty cloned database") {
        countTranzactio.map(n => assertTrue(n == 0L)).orDie
      },
      test("creates and activates an account") {
        (for {
          id  <- createTranzactio("erin@example.test", "Erin")
          _   <- activateTranzactio(id)
          row <- accountTranzactio("erin@example.test")
        } yield assertTrue(row.contains(AccountRow("erin@example.test", "Erin", active = true, deleted = false)))).orDie
      },
      test("deletes without leaking into another clone") {
        (for {
          id  <- createTranzactio("frank@example.test", "Frank")
          _   <- deleteTranzactio(id)
          row <- accountTranzactio("frank@example.test")
        } yield assertTrue(row.exists(_.deleted))).orDie
      },
      test("gets a clean database after another test wrote rows") {
        countTranzactio.map(n => assertTrue(n == 0L)).orDie
      }
    ) @@ TestAspect.parallel @@ migrate(Clients.tranzactio))
      .provideSomeLayerShared[TestEnvironment & PgRoot & PgTemplateManager](tranzactioLayer)

  override def spec =
    suite("postgres template demo")(
      managerSuite,
      doobieSuite,
      zioJdbcSuite,
      tranzactioSuite
    ) @@ TestAspect.parallel
}
