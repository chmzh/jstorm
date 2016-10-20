package cndw.jstorm.redis;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.redisson.core.RMap;

public class DvCache {
	private final static ConcurrentMap<String, ConcurrentMap<String, Object>> CACHE = new ConcurrentHashMap<String,ConcurrentMap<String,Object>>();
	
	public static void putLocalCache(String gameId,String key,Object obj){
		ConcurrentMap<String, Object> map;
		//写入本地内存
		map = new ConcurrentHashMap<String,Object>();
		map.put(key, obj);
		CACHE.put(gameId, map);
	}
	
	public static void putRedisCache(String gameId,String key,Object obj){
		DvRedisClient.getInstance().put2Map(gameId, key, obj);
	}
	
//	public static boolean exists(String gameId,String key){
//		if(localExists(gameId, key) == null){
//			return false;
//		}else if(redistExists(gameId, key)==null){
//			return false;
//		}
//		return true;
//	}
	
	/**
	 * 本地是否已经存在
	 * @param gameId
	 * @param key
	 * @return
	 */
	public static ConcurrentMap<String, Object> localExists(String gameId,String key){
		ConcurrentMap<String, Object> map = CACHE.get(gameId);
		if(map == null){
			return null;
		}else{
			if(map.get(key)==null){
				return null;
			}
		}
		return map;
	}
	/**
	 * redis是否已经存在
	 * @param gameId
	 * @param key
	 * @return
	 */
	public static ConcurrentMap<String, Object> redistExists(String gameId,String key){
		ConcurrentMap<String, Object> map = DvRedisClient.getInstance().getMap(gameId);
		if(map.get(key) == null){
			return null;
		}
		return map;
	}
}
