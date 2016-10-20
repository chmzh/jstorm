/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cndw.jstorm.bolt;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cndw.jstorm.util.HBaseUtil;
import clojure.main;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * 将数据存入hbase
 * @author chenmingzhou
 *
 */
public class DvHBaseBolt  extends DvAbstractHBaseBolt {
    public DvHBaseBolt() {
		super();
		// TODO Auto-generated constructor stub
	}

	private static final Logger LOG = LoggerFactory.getLogger(DvHBaseBolt.class);


    @Override
    public void execute(Tuple tuple) {   	
    	String gameId = tuple.getStringByField("gameId");
    	String logType = tuple.getStringByField("logType");
    	String rowKey = tuple.getStringByField("rowKey");
    	String col = tuple.getStringByField("col");
    	long val = tuple.getLongByField("val");
    	try {
			if(!hBaseUtil.existsTable(gameId)){
				hBaseUtil.createTable(gameId, "info");
			}
			hBaseUtil.incVal(gameId, rowKey, "info", col, val);
			
			
			//测试用
			Result result = hBaseUtil.get(gameId, rowKey);
			byte[] b = result.getValue(Bytes.toBytes("info"), Bytes.toBytes(col));
			long v = Bytes.toLong(b);
			LOG.info("====="+gameId+"======="+rowKey+"========="+col+"===================="+v);
			//测试用
			
			//hBaseUtil.put(gameId, rowKey, "info", col, val+v);
			collector.ack(tuple);
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	
    }

    

}
