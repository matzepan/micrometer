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

import io.micrometer.core.annotation.Incubating;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.util.StringUtils;
import org.jooq.*;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultExecuteListener;

/**
 * Time SQL queries passing through JOOQ.
 *
 * @author Jon Schneider
 * @since 1.4.0
 */
@Incubating(since = "1.4.0")
public class JooqMetrics extends DefaultExecuteListener {
  private static final String CONFIGURATION_KEY = "__micrometer_tag";
  private static final String TIMER_SAMPLE_KEY = "__micrometer_timer_sample";

  private final MeterRegistry registry;
  private final Iterable<Tag> tags;

  /**
   * @param registry Registry to accumulate timings to.
   */
  public JooqMetrics(MeterRegistry registry) {
    this(registry, Tags.empty());
  }

  /**
   * @param registry Registry to accumulate timings to.
   * @param tags     Tags to apply to all recorded metrics.
   */
  public JooqMetrics(MeterRegistry registry, Iterable<Tag> tags) {
    this.registry = registry;
    this.tags = tags;
  }

  @Override
  public void start(ExecuteContext ctx) {
    if (ctx.configuration().data(CONFIGURATION_KEY) != null) {
      ctx.query().attach(ctx.configuration().derive());
      ctx.query().configuration().data(TIMER_SAMPLE_KEY, Timer.start(registry));
    }
  }

  @Override
  public void end(ExecuteContext ctx) {
    Timer.Sample sample = (Timer.Sample) ctx.query().configuration().data(TIMER_SAMPLE_KEY);
    if (sample != null) {
      String exceptionName = "none";
      String exceptionSubclass = "none";

      Exception exception = ctx.exception();
      if (exception != null) {
        if (exception instanceof DataAccessException) {
          DataAccessException dae = (DataAccessException) exception;
          exceptionName = dae.sqlStateClass().name().toLowerCase().replace('_', ' ');
          exceptionSubclass = dae.sqlStateSubclass().name().toLowerCase().replace('_', ' ');
          if (exceptionSubclass.contains("no subclass")) {
            exceptionSubclass = "none";
          }
        } else {
          String simpleName = exception.getClass().getSimpleName();
          exceptionName = StringUtils.isNotBlank(simpleName) ? simpleName : exception.getClass().getName();
        }
      }

      sample.stop(Timer.builder("jooq.query")
        .description("Execution time of a SQL query performed with JOOQ")
        .tag("name", (String) ctx.configuration().data(CONFIGURATION_KEY))
        .tag("type", ctx.type().name().toLowerCase())
        .tag("exception", exceptionName)
        .tag("exception.subclass", exceptionSubclass)
        .tags(tags)
        .register(registry));
    }
  }

  /**
   * Mark a JOOQ query for metrics instrumentation.
   *
   * @param q         The query to time.
   * @param queryName A human-readable name for the query that will be added as the 'name' tag to the 'jooq.query' timer.
   * @param <Q>       The query type.
   * @return The original query, with its configuration derived and the query name added to the derived configuration.
   */
  public static <Q extends Attachable> Q timing(Q q, String queryName) {
    q.attach(q.configuration().derive());
    q.configuration().data(CONFIGURATION_KEY, queryName);
    return q;
  }

  public static DSLContext timing(DSLContext dslContext, String queryName) {
    Configuration derivedConfiguration = dslContext.configuration().derive();
    derivedConfiguration.data(CONFIGURATION_KEY, queryName);
    return DSL.using(derivedConfiguration);
  }
}
