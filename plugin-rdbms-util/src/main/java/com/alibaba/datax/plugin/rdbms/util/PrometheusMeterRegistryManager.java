package com.alibaba.datax.plugin.rdbms.util;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.exporter.HTTPServer;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

public class PrometheusMeterRegistryManager {
  private static final PrometheusMeterRegistry prometheusRegistry;
  private static final Map<String, EntityMeters> entityMeters = new HashMap<>();

  public static final String SOURCE_ENTITY_NAME_TAG_KEY = "entity-name";

  static {
    prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    try {
      new HTTPServer(new InetSocketAddress(6667), prometheusRegistry.getPrometheusRegistry(), true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static EntityMeters meters(String table) {
    if (entityMeters.containsKey(table)) {
      return entityMeters.get(table);
    } else {
      EntityMeters entityMeter = new EntityMeters(table);
      entityMeters.put(table, entityMeter);
      return entityMeter;
    }
  }

  public static Timer timer(String subName, String description, List<Tag> tags) {
    return Timer.builder(subName).description(description).tags(tags).register(prometheusRegistry);
  }

  public static Gauge gauge(
      String subName, String description, List<Tag> tags, Supplier<Number> f) {
    return Gauge.builder(subName, f)
        .description(description)
        .tags(tags)
        .register(prometheusRegistry);
  }

  public static Counter counter(String subName, String description, List<Tag> tags) {
    return Counter.builder(subName)
        .description(description)
        .tags(tags)
        .register(prometheusRegistry);
  }

  public static <T> FunctionCounter functionCounter(
      String subName, String description, List<Tag> tags, T t, ToDoubleFunction<T> f) {
    return FunctionCounter.builder(subName, t, f)
        .description(description)
        .tags(tags)
        .register(prometheusRegistry);
  }

  public static List<Tag> getTags(String entityName) {
    List<Tag> tags = new ArrayList<>();

    if (entityName != null) {
      tags.add(new ImmutableTag(SOURCE_ENTITY_NAME_TAG_KEY, entityName));
    }
    return tags;
  }
}
