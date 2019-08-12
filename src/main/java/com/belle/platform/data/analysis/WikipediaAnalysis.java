package com.belle.platform.data.analysis;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * @description flink官方指导示例代码demo
 *
 * @author xiaoTaoShi
 * @date 2019/7/25 14:26
 * @version 1.0.0
 * @copyright (C) 2013 WonHigh Information Technology Co.,Ltd 
 *  All Rights Reserved. 
 *
 * The software for the WonHigh technology development, without the 
 * company's written consent, and any other individuals and 
 * organizations shall not be used, Copying, Modify or distribute 
 * the software.
 */

public class WikipediaAnalysis {
	
	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		// data source from Wikipedia IRC log
		DataStream<WikipediaEditEvent> edits = env.addSource(new WikipediaEditsSource());
		
		KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
				.keyBy((KeySelector<WikipediaEditEvent, String>) WikipediaEditEvent::getUser);
		
		DataStream<Tuple2<String, Long>> result = keyedEdits
				.timeWindow(Time.seconds(5)).aggregate(
						new AggregateFunction<WikipediaEditEvent, Tuple2<String, Long>, Tuple2<String, Long>>() {
							
							private static final long serialVersionUID = 464958762405412080L;
							
							@Override
							public Tuple2<String, Long> createAccumulator() {
								return new Tuple2<>("", 0L);
							}
							
							@Override
							public Tuple2<String, Long> add(WikipediaEditEvent value,
									Tuple2<String, Long> accumulator) {
								return new Tuple2<>(value.getUser(), value.getByteDiff() + accumulator.f1);
							}
							
							@Override
							public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
								return accumulator;
							}
							
							@Override
							public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
								return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
							}
						});
		
		result.print();
		
		env.execute();
		
	}
}
