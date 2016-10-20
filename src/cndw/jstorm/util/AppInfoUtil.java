package cndw.jstorm.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cndw.jstorm.bolt.DvBaseSpout;

public class AppInfoUtil {
	private final static Logger LOG = LoggerFactory.getLogger(AppInfoUtil.class);
	public static String getWorkHome(){
		
		String home = System.getProperty("user.dir");
		LOG.info("=====================user.dir======================"+home);
		return home;
	}
}
