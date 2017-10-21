/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import java.util.concurrent.TimeUnit

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.utils.Time


class ClientRequestQuotaManager(private val config: ClientQuotaManagerConfig,
                         private val metrics: Metrics,
                         private val time: Time) extends ClientQuotaManager(config, metrics, QuotaType.Request, time) {
  val maxThrottleTimeMs = TimeUnit.SECONDS.toMillis(this.config.quotaWindowSizeSeconds)
  val exemptSensor = createSensor(exemptSensorName, exemptMetricName)

  def recordExempt(value: Double) {
    exemptSensor.record(value)
  }

  override protected def throttleTime(clientMetric: KafkaMetric, config: MetricConfig): Long = {
    math.min(super.throttleTime(clientMetric, config), maxThrottleTimeMs)
  }

  override protected def clientRateMetricName(sanitizedUser: String, clientId: String): MetricName = {
    metrics.metricName("request-time", QuotaType.Request.toString,
                   "Tracking request-time per user/client-id",
                   "user", sanitizedUser,
                   "client-id", clientId)
  }

  private def exemptMetricName: MetricName = {
    metrics.metricName("exempt-request-time", QuotaType.Request.toString,
                   "Tracking exempt-request-time utilization percentage")
  }

  private def exemptSensorName: String = "exempt-" + QuotaType.Request

}
