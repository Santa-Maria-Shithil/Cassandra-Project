package org.apache.cassandra.predictor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Predictor {
	private final ConcurrentHashMap<InetAddressAndPort,AtomicInteger> queuesize =new ConcurrentHashMap<>();
	private final HashMap<InetAddressAndPort,Long> servicetime = new HashMap<>();
	private final HashMap<InetAddressAndPort,Long> latency=new HashMap<>();
	private static final Logger logger=LoggerFactory.getLogger(Predictor.class);
	public boolean containsKey(InetAddressAndPort key)
	{
		if(queuesize.containsKey(key))
		{
			return true;
		}
		else 
			return false;
	}
	 public AtomicInteger put(InetAddressAndPort key, AtomicInteger value)
	 {
		 return queuesize.put(key, value);
	 }
	 public AtomicInteger get(InetAddressAndPort key)
	 {
		 return queuesize.get(key);
	 }
	public AtomicInteger getPendingRequestCounter(InetAddressAndPort key)
	{
		AtomicInteger counter = queuesize.get(key);
		if(counter==null)
		{
			queuesize.put(key,new AtomicInteger(0));
			counter=queuesize.get(key);
		}
		return counter;
	}
	public void updateMetrices(InetAddressAndPort key,int qsize, long l, long stime, String tag)
	{
		
		latency.put(key, l);
		servicetime.put(key, stime);
		//int qsize=get(key).decrementAndGet();
	   	logger.info("decrementing pending job inside predictor");
		String data = key.toString() + " " + Integer.toString(qsize) + " " +l + " " + stime+" "+tag+"\n";
		File file =new File("data.txt");
		FileWriter fr = null;
		try
		{
			fr = new FileWriter(file,true);
			fr.write(data);
		}catch (IOException e)
		{
			e.printStackTrace();
		}finally {
			try
			{
				fr.close();
			}catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	} 
	
	public void updateMetrices2(InetAddressAndPort key, long l, long stime, String tag)
	{
		
		latency.put(key, l);
		servicetime.put(key, stime);
		//int qsize=get(key).decrementAndGet();
	   	logger.info("decrementing pending job inside predictor");
		String data = key.toString() + " "+l + " " + stime+" "+tag+"\n";
		File file =new File("datatwo.txt");
		FileWriter fr = null;
		try
		{
			fr = new FileWriter(file,true);
			fr.write(data);
		}catch (IOException e)
		{
			e.printStackTrace();
		}finally {
			try
			{
				fr.close();
			}catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	} 
}
