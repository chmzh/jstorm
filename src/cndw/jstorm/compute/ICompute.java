package cndw.jstorm.compute;

import java.util.List;

import cndw.jstorm.DvObject;

public interface ICompute {
	/**
	 * 1分钟指标
	 * @param cols
	 * @param vals
	 * @param type
	 * @return
	 */
	public List<DvObject> execute(String logType,String[] cols,String[] vals,int type);
}
