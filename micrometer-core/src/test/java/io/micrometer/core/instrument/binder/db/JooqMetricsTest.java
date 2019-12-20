/**
 * Copyright 2019 Pivotal Software, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micrometer.core.instrument.binder.db;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.jetbrains.annotations.NotNull;
import org.jooq.*;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static io.micrometer.core.instrument.binder.db.JooqMetrics.timing;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.jooq.impl.DSL.asterisk;

class JooqMetricsTest {
  private MeterRegistry meterRegistry = new SimpleMeterRegistry();

  @Test
  void timeFluentSelectStatement() throws SQLException {
    try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:fluentSelect")) {
      DSLContext jooq = createDatabase(conn);

      Result<Record> result = timing(jooq.select(asterisk()).from("author"), "selectAllAuthors").fetch();
      assertThat(result.size()).isEqualTo(1);

      // intentionally don't time this operation to demonstrate that the configuration hasn't been globally mutated
      jooq.select(asterisk()).from("author").fetch();

      assertThat(meterRegistry.get("jooq.query")
        .tag("name", "selectAllAuthors")
        .tag("type", "read")
        .timer().count())
        .isEqualTo(1);
    }
  }

  @Test
  void timeParsedSelectStatement() throws SQLException {
    try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:parsedSelect")) {
      DSLContext jooq = createDatabase(conn);
      timing(jooq, "selectAllAuthors").fetch("SELECT * FROM author");

      // intentionally don't time this operation to demonstrate that the configuration hasn't been globally mutated
      jooq.fetch("SELECT * FROM author");

      assertThat(meterRegistry.get("jooq.query")
        .tag("name", "selectAllAuthors")
        .tag("type", "read")
        .timer().count())
        .isEqualTo(1);
    }
  }

  @Test
  void timeFaultySelectStatement() throws SQLException {
    try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:faultySelect")) {
      DSLContext jooq = createDatabase(conn);
      timing(jooq, "selectAllAuthors").fetch("SELECT non_existent_field FROM author");

      failBecauseExceptionWasNotThrown(DataAccessException.class);
    } catch (DataAccessException ignored) {
    }

    assertThat(meterRegistry.get("jooq.query")
      .tag("name", "selectAllAuthors")
      .tag("type", "read")
      .tag("exception", "c42 syntax error or access rule violation")
      .tag("exception.subclass", "none")
      .timer().count())
      .isEqualTo(1);
  }

  @NotNull
  private DSLContext createDatabase(Connection conn) {
    Configuration configuration = new DefaultConfiguration()
      .set(conn)
      .set(SQLDialect.H2)
      .set(new JooqMetrics(meterRegistry, Tags.empty()));

    DSLContext jooq = DSL.using(configuration);

    jooq.execute("CREATE TABLE author (" +
      "  id int NOT NULL," +
      "  first_name varchar(255) DEFAULT NULL," +
      "  last_name varchar(255) DEFAULT NULL," +
      "  PRIMARY KEY (id)" +
      ")");

    jooq.execute("INSERT INTO author VALUES(1, 'jon', 'schneider')");
    return jooq;
  }
}
