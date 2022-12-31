/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * Skeleton code for the data stream walkthrough
 * The FraudDetectionJob class defines the data flow of the application
 */
public class FraudDetectionJob {
	/**
	 * In the main method we start by describing how the job is assembled
	 *
	 */
	public static void main(String[] args) throws Exception {
		/**
		 * We start by setting our stream execution environment, The execution environment is how we set the job properties
		 * For instance:
		 * 	here we start by creating our data sources and finally triggering the job
		 */
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/**
		 * Here we create our data source
		 * Sources ingest data from external systems, such as ApacheKafka, Rabbit MQ, or Apache Pulsar into Flink Jobs.
		 */
		DataStream<Transaction> transactions = env
			.addSource(new TransactionSource())
			.name("transactions"); // the name attached to this transaction is just for debugging purposes

		/**
		 * The transactions stream contains a lot of transactions from a large number of users,
		 * Therefore, it needs to be processed in parallel by multiple fraud detection tasks
		 * Since fraud occurs on per-account basis, you must ensure that all transactions from the same account are processed by the same parallel task of the fraud detector operator
		 *
		 * To ensure that the same physical tasks processes all records for a particular key,
		 * we can partition a stream using DataStream#keyBy function.
		 * The process() call then adds an operator that applies a function to eash partitioned element in the stream.
		 */
		DataStream<Alert> alerts = transactions
			.keyBy(Transaction::getAccountId)
			.process(new FraudDetector())
			.name("fraud-detector");
		/**
		 * A sink writes a DataStream to an external system; such as Apache Kafka, cassandra, and AWS Kinesis.
		 * The AlertSink logs each alert record with a log level INFO, instead of writing it to persistent storage, so you can easily see your results.
		 */
		alerts
			.addSink(new AlertSink())
			.name("send-alerts");
		/**
		 * Triggering the Job
		 */
		env.execute("Fraud Detection");
	}
}
