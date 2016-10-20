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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cndw.jstorm.util.HBaseUtil;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;

/**
 * HBaseBolt抽象类
 * @author chenmingzhou
 *
 */
public abstract class DvAbstractHBaseBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(DvAbstractHBaseBolt.class);
    
    protected OutputCollector collector;
    protected Connection connection;
    protected HBaseUtil hBaseUtil;
    public DvAbstractHBaseBolt() {
    	
    }
    
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		collector = arg2;
		
		Configuration configuration = new Configuration();
    	configuration.set("hbase.zookeeper.quorum","192.168.23.91,192.168.21.7,192.168.23.207");
    	configuration.set("hbase.zookeeper.property.clientPort","2181");
    	Configuration cfg = HBaseConfiguration.create(configuration);
    	try {
			connection = ConnectionFactory.createConnection(cfg);
			hBaseUtil = new HBaseUtil(connection);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
