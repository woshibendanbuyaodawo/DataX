package com.alibaba.datax.plugin.rdbms.util;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.Tag;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EntityMeters {
  private List<Tag> tags;
  private long taskTotalTime;
  private long buildRecordTime;
  private Map<String, Long> typeConverts = new HashMap<>();

  private Gauge taskTotalGauge;
  private Gauge buildRecordGauge;
  private Map<String, Gauge> typeConvertsGauge = new HashMap<>();

  public EntityMeters(String table) {
    tags = PrometheusMeterRegistryManager.getTags(table);
    taskTotalGauge =
        PrometheusMeterRegistryManager.gauge(
            "task-total-time", "task total time", tags, () -> taskTotalTime);
    buildRecordGauge =
        PrometheusMeterRegistryManager.gauge(
            "build-record-time", "build record time", tags, () -> buildRecordTime);
  }

  public void addTypeTime(String type, long time) {
    if (typeConvertsGauge.containsKey(type)) {
      typeConverts.put(type, typeConverts.get(type) + time);
    } else {
      typeConverts.put(type, time);
      List<Tag> tags = new ArrayList<>(this.tags);
      tags.add(new ImmutableTag("type", type));
      typeConvertsGauge.put(
          type,
          PrometheusMeterRegistryManager.gauge(
              "type-convert-time", "type convert time", tags, () -> taskTotalTime));
    }
  }

  public void addTaskTotalTime(long time) {
    this.taskTotalTime += time;
  }

  public void addBuildRecordTime(long time) {
    this.buildRecordTime += time;
  }
}
