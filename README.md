# ZIO Test Postgres Template Demo

Scala 3 demo for an in-process, ZIO-native version of the IntegreSQL pattern.

The implementation starts one PostgreSQL Testcontainer in `ZIOSpec.bootstrap`, prepares or reuses a hash-named template database, keeps a warm pool of cloned databases, and checks out one isolated database per test. Tests run in parallel and exercise a small user-account lifecycle.

Demonstrated clients:

- Doobie, using `ConnectionIO` and `Transactor[Task]`
- zio-jdbc, using `ZConnectionPool` and `transaction`
- TranzactIO, using `io.github.gaelrenoux.tranzactio.doobie.Database`

Run:

```bash
scala-cli test . --server=false
```

Docker must be running.

The key pieces are:

- `src/main/scala/zpg/PostgresTemplates.scala`: reusable ZIO service that owns hash-based template reuse, stale clone cleanup, deterministic names, warm pool creation, checkout, and release.
- `PgTemplateManager`: the service API used by tests to acquire scoped isolated databases.
- `PostgresTemplateSpec`: `ZIOSpec` base class whose `bootstrap` starts the container and manager once for all suites.
- `@@ migrate(Clients.doobie)`: optional ZIO Test aspect that checks out a database and swaps the app-facing client before each test.

Database names are deterministic and identifier-safe:

```text
zpg_template_<sha256-prefix>
zpg_test_<sha256-prefix>_<zero-padded-id>
```

The template key is derived from migration content in the demo. In a real project, hash the migration and fixture files that define the database shape.

If the template database already exists for the same key, startup skips migration and reuses it. Stale test databases for that key are dropped before the warm pool is recreated.

## Lifecycle

The implementation follows the useful parts of IntegreSQL's lifecycle, but keeps it in-process:

1. Start one PostgreSQL Testcontainer from `ZIOSpec.bootstrap`.
2. Compute a `TemplateKey` from the inputs that define the database shape.
3. Prepare or reuse `zpg_template_<sha256-prefix>`.
4. Drop stale `zpg_test_<sha256-prefix>_*` databases from previous runs.
5. Warm a pool of ready test databases.
6. Check out one database per test using `Scope`.
7. Drop the checked-out database on release and refill the pool in the background.

`PgTemplateManager` deliberately has a small public API:

```scala
trait PgTemplateManager {
  def acquire: ZIO[Scope, Throwable, TestDatabase]
}
```

The optional `PostgresTestAspect.migrate` aspect is only test integration glue. It asks the manager for a database, builds the app-facing client for that database, and replaces that client in the test environment.

## Template Preparation

PostgreSQL can clone a template database only when no other sessions are connected to it. Prepare the template through a short-lived connection that is not the application pool:

```scala
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
```

Flyway or another migration tool can replace the raw SQL migration. Keep the same property: open one migration connection to the template, migrate, close it, then clone.

## Testcontainer Flags

The demo starts PostgreSQL with test-only settings:

```scala
container.container.withCommand(
  "postgres",
  "-c", "fsync=off",
  "-c", "synchronous_commit=off",
  "-c", "full_page_writes=off",
  "-c", "max_connections=200"
)
container.container.withTmpFs(java.util.Map.of("/var/lib/postgresql/data", "rw"))
```

These trade durability for speed in a disposable container. If your CI environment does not allow tmpfs, remove the tmpfs line first.

## Client Wiring

The manager hands out connection details as `TestDatabase`, so it is not tied to one database library.

Doobie:

```scala
def doobie(db: TestDatabase) =
  ZIO.succeed {
    Transactor.fromDriverManager[Task](
      "org.postgresql.Driver",
      db.jdbcUrl,
      db.user,
      db.password,
      None
    )
  }
```

zio-jdbc:

```scala
def zioJdbc(db: TestDatabase) =
  (ZLayer.succeed(ZConnectionPoolConfig.default.copy(minConnections = 1, maxConnections = 4)) >>>
    ZConnectionPool.postgres(
      db.host,
      db.port,
      db.name,
      Map("user" -> db.user, "password" -> db.password)
    )).build.map(_.get[ZConnectionPool])
```

TranzactIO:

```scala
def tranzactio(db: TestDatabase) = {
  val ds = new PGSimpleDataSource()
  ds.setUrl(db.jdbcUrl)
  ds.setUser(db.user)
  ds.setPassword(db.password)

  (ZLayer.succeed[javax.sql.DataSource](ds) >>> Database.fromDatasource)
    .build
    .map(_.get[Database])
}
```
