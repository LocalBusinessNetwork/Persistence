package com.rw.persistence;

import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

public class JedisMT {

	static final Logger log = Logger.getLogger(JedisMT.class.getName());
	
	public static final String MASTER_CHANNEL_NAME = "RW_MASTER_CHANNEL";
	public static final String ASYNC_API_CHANNEL = "RWENGINE1.0-AsyncAPI";
	public static final String ASYNC_EMAIL_CHANNEL = "RWENGINE1.0-AsyncEmail";
	public static final String PARTY_ENRICHMENT_CHANNEL = "RWENGINE1.0-PartyEnrichent";
	public static final String CONTACT_IMPORT_CHANNEL = "RWENGINE1.0-ContactImport";
	public static final String CONTACT_DEDUP_CHANNEL = "RWENGINE1.0-ContactDedup";
	public static final String PARTY_TRIANGULATE_CHANNEL = "RWENGINE1.0-PartyTriangulate";
	public static final String CRON = "RWENGINE1.0-Cron";
	
	public static JedisPool pool;
	static {
	    JedisPoolConfig config = new JedisPoolConfig();
	    config.setMaxActive(10000);
	    config.setMaxIdle(10);
	    config.setMaxWait(10000);
	    pool = new JedisPool(config, System.getProperty("PARAM1") == null ? "localhost" : System.getProperty("PARAM1") );
	}

	String tenant = null;
	
	public JedisMT(){
		try {
			dataMT t = (dataMT)Thread.currentThread();
			tenant = t.tenant.get();
		}
		catch(Exception e){
			// We need this for Demo Setup
			tenant = System.getProperty("PARAM3");	
		}		
	}
	
	public JedisMT(String tenantStr) throws Exception{
		if ( tenantStr == null) throw new Exception("Tenant Can't be null");
		tenant = tenantStr;
	}
	
	public void setTenant(String tenantStr)  {
		if ( tenantStr == null )
			tenant = System.getProperty("PARAM3");
		else
			tenant = tenantStr;
	}
	
	public static Jedis getJedis(){
		
	    Jedis jedis = null;
	    jedis = pool.getResource();
	    jedis.connect();
	    return jedis;
	}
	
	public String hget(String hash, String key) {
		Jedis jp = getJedis();
		try {
			return jp.hget(tenant+hash, key);
		}
		finally {
			pool.returnResource(jp);
		}
	}
	
	public Long hset(String hash, String key, String value) {
		Jedis jp = getJedis();
		try {
			return jp.hset(tenant+hash, key, value);
		}
		finally {
			pool.returnResource(jp);
		}
	}
	public String set(String key, String value) {
		Jedis jp = getJedis();
		try 
		{
			if ( tenant == null )
				tenant = "master";
			
			return jp.set(tenant+ key, value);
		}
		finally {
			pool.returnResource(jp);
		}
	}
	
	public String get(String key) {
		Jedis jp = getJedis();
		try {
			if ( tenant == null )
				tenant = "master";
			
			return jp.get(tenant+key);
		}
		finally {
			pool.returnResource(jp);
		}
	}

	public Long sadd2(String key, String member) {
		Jedis jp = getJedis();
		try {
			return jp.sadd(key, member);
		}
		finally {
			pool.returnResource(jp);
		}
	}
	
	public Long sadd(String key, String member) {
		Jedis jp = getJedis();
		try {
			return jp.sadd(tenant+key, member);
		}
		finally {
			pool.returnResource(jp);
		}
	}
	
	public Set<String> sinter(String SetOne, String SetTwo) {
		Jedis jp = getJedis();
		try {
			return jp.sinter(tenant+SetOne, tenant+SetTwo);
		}
		finally {
			pool.returnResource(jp);
		}
	}
	
	public Boolean sismember(String setkey, String memberVal) {
		Jedis jp = getJedis();
		try {
			return jp.sismember(tenant+setkey, memberVal);
		}
		finally {
			pool.returnResource(jp);
		}
	}
	
	public Long publish(String channel, String message) {
		Jedis jp = getJedis();
		try {
			// You always publish to master channel.
			// RWENGINE distributes the calls based on the tenancy
			return jp.publish(JedisMT.MASTER_CHANNEL_NAME, tenant +":"+ channel + ":" + message);
		}
		finally {
			pool.returnResource(jp);
		}
	}
	
	public Long expireAt(String key, Long time) {
		Jedis jp = getJedis();
		try {
			return jp.expireAt(tenant+key, time);
		}
		finally {
			pool.returnResource(jp);
		}
	}
	public Long del(String key) {
		Jedis jp = getJedis();
		try {
			return jp.del(tenant+key);
		}
		finally {
			pool.returnResource(jp);
		}
	}
	
	public Boolean exists(String key) {
		Jedis jp = getJedis();
		try {
			return jp.exists(tenant+key);
		}
		finally {
			pool.returnResource(jp);
		}
	}
	
	public void subscribe(JedisPubSub jedisPubSub, String channel){
		Jedis jp = getJedis();
			try {
				jp.subscribe(jedisPubSub, tenant+channel);
			}
			finally {
				pool.returnResource(jp);
			}
	}
	
	public void putCache(String base, String token, String data) {
		Jedis jp = getJedis();
		try {
			jp.set(tenant + "METADATA_" + token, data);	
		}
		finally {
			pool.returnResource(jp);
		}
	}

	public String getCache(String base, String token) {
		Jedis jp = getJedis();
		try {
			return jp.get(tenant +  "METADATA_" + token);
		}
		finally {
			pool.returnResource(jp);
		}
	}
	public long delCache(String base, String token) {
		Jedis jp = getJedis();
		try {
			return jp.del(tenant +  "METADATA_" + token);
		}
		finally {
			pool.returnResource(jp);
		}
	}

	public long getCounter(String base, String counterName) {
		Jedis jp = getJedis();
		try {
			return jp.incr(tenant + base + counterName);	
		}
		finally {
			pool.returnResource(jp);
		}
	}
	
	public long resetCounter(String base, String counterName) {
		Jedis jp = getJedis();
		try {
			return jp.del(tenant + base  + counterName);	
		}
		finally {
			pool.returnResource(jp);
		}
	}
	
	public void cacheFaceBookIds(Boolean reset) throws Exception {
		Jedis jp = getJedis();

		// It gotta be a  rare case we reset this set.
		// This set could grow to few hundred elements 
		// We would then have to apply map/reduce across 
		// a cluster of redis servers.
		
		if ( reset == true ) jp.del(tenant + "FACEBOOK_MEMBERS");
		else  if ( jp.exists(tenant + "FACEBOOK_MEMBERS") ) return;
		
		mongoStore m = new mongoStore(tenant);
		List<String> ids = m.getColl("rwParty").distinct("FaceBookId");		
		for ( String id : ids ) {
			if ( !id.isEmpty())
				jp.sadd(tenant + "FACEBOOK_MEMBERS", id );			
		}
		return;
	}

	public void cacheLinkedInIds(Boolean reset) throws Exception {
		Jedis jp = getJedis();
		
		// It gotta be a  rare case we reset this set.
		// This set could grow to few hundred elements 
		// We would then have to apply map/reduce across 
		// a cluster of redis servers.
		
		if ( reset == true )  jp.del(tenant + "LINKEDIN_MEMBERS");
		else if ( jp.exists(tenant + "LINKEDIN_MEMBERS") ) return;
		
		mongoStore m = new mongoStore(tenant);
		List<String> ids = m.getColl("rwParty").distinct("LNProfileId");		
		for ( String id : ids ) {
			if ( !id.isEmpty())
				jp.sadd(tenant + "LINKEDIN_MEMBERS", id );			
		}		
	}
	
	public void pushAsync(JSONObject data) {
		
	}
	
	public Long lpush(String key, String value) {
		Jedis jp = getJedis();
		try {
			return jp.lpush(tenant+key, value);
		}
		finally {
			pool.returnResource(jp);
		}
	}
	
	public String lpop(String key) {
		Jedis jp = getJedis();
		try {
			return jp.lpop(tenant+key);
		}
		finally {
			pool.returnResource(jp);
		}
	}
	
	public Long rpush(String key, String value) {
		Jedis jp = getJedis();
		try {
			return jp.rpush(tenant+key, value);
		}
		finally {
			pool.returnResource(jp);
		}
	}
	
	public String rpop(String key) {
		Jedis jp = getJedis();
		try {
			return jp.rpop(tenant+key);
		}
		finally {
			pool.returnResource(jp);
		}
	}
}
