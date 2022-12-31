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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;


/**
 * Skeleton code for implementing a fraud detector.
 * The FraudDetector class defines the business logic of the function that detects fraudulent transactions
 * The FraudDetector is implemented as a KeyedProcessFunction.
 * It's method KeyedProcessFunction#processElement is called for every transaction event
 */

public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

	private static final long serialVersionUID = 1L;

	private transient ValueState<Boolean> flagState; // ValueState is always scoped to the current key
	private transient ValueState<Long> timerState;

	private static final double SMALL_AMOUNT = 1.00;
	private static final double LARGE_AMOUNT = 500.00;
	private static final long ONE_MINUTE = 60 * 1000;


	@Override
	public void open(Configuration parameters){
		ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
				"flag",
				Types.BOOLEAN
		);
		flagState = getRuntimeContext().getState(flagDescriptor);


		ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
				"timer-state",
				Types.LONG
		);

		timerState = getRuntimeContext().getState(timerDescriptor);
	}
	@Override
	public void processElement(
			Transaction transaction,
			Context context,
			Collector<Alert> collector) throws Exception {

		// Get the current state for the current key
		Boolean lastTransactionWasSmall = flagState.value();

		// Check if the flag is set
		if(lastTransactionWasSmall != null){
			// Check if the amount is greater than the large amount threshold
			if(transaction.getAmount() > LARGE_AMOUNT){
				// Output an alert downstream
				Alert alert = new Alert();
				alert.setId(transaction.getAccountId());

				collector.collect(alert);
			}

			// Clean up our state
			flagState.clear();
		}

		// Check if flagState is less than the small amount threshold
		if (transaction.getAmount() < SMALL_AMOUNT){
			// Set the flag to true
			flagState.update(true);

			// set the timer and timer state
			long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
			context.timerService().registerEventTimeTimer(timer);
			timerState.update(timer);
		}
	}

	/***
	 * Implementing callback to reset the timer
	 */
	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out){
		// remove flag after 1 minute
		timerState.clear();
		flagState.clear();
	}

	/**
	 * Canceling the timer by deleting the registered timer and the timer state
	 */
	private void cleanUp(Context ctx) throws Exception{
		// delete timer
		Long timer = timerState.value();
		ctx.timerService().deleteProcessingTimeTimer(timer);

		// clean up all state
		timerState.clear();
		flagState.clear();
	}

}
