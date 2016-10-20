package cndw.jstorm.task;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public abstract class AbstractSplitTask implements ISplitTask  {
	
	private OutputCollector collector;
	protected Tuple input;
	protected String word;
	public AbstractSplitTask(OutputCollector collector,Tuple input) {
		this.collector = collector;
		this.input = input;
	}
	@Override
	public void execute() {
		split();
		collector.emit(input, new Values(word,1));
		collector.ack(input);
	}
	/**
	 * 拆分出要计算的word
	 * @return
	 */
	protected abstract void split();
}
