package org.apache.kafka.clients.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * 
 * Custom hack to get HighWater mark
 * 
 * Used in org.apache.kafka.clients.consumer.internals.Fetcher, line 861.
 * 
 * 
 * @author tcrown
 *
 */
public class LagHack
{
	private static Map<String, Map<Integer, long[]>> map1 = new HashMap<String, Map<Integer, long[]>>();

	static private long[] get( String topic, int partition )
	{
		Map<Integer, long[]> map2 = map1.get( topic );

		if( map2 == null )
		{
			map2 = new HashMap<Integer, long[]>();
			map1.put( topic, map2 );
		}

		long[] v = map2.get( partition );

		if( v == null )
		{
			v = new long[] { 0, 0 };
			map2.put( partition, v );
		}

		return v;
	}

	static public void putHighWatermark( String topic, int partition, long highWatermark )
	{
		synchronized( map1 )
		{
			long[] v = get( topic, partition );
			v[0] = highWatermark;
		}
	}

	static public long getHighWatermark( String topic, int partition )
	{
		synchronized( map1 )
		{
			long l = map1.get( topic ).get( partition )[0];
			return l;
		}
	}

	/*
	 * gets the total lag for this topic across all partitions
	 */
	static public long getTotalLag( String topic, int partition, long offset )
	{
		synchronized( map1 )
		{
			long[] v0 = get( topic, partition );
			v0[1] = offset;

			long sum = 0;
			Map<Integer, long[]> map2 = map1.get( topic );
			for( long[] v : map2.values() )
			{
				if( v[1] > 0 )
				{
					sum += (v[0] - v[1] - 1);
				}
			}
			return sum;
		}
	}
}
