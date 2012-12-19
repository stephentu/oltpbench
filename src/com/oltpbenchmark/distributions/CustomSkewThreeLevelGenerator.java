package com.oltpbenchmark.distributions;


/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

import java.util.Random;

/**
 * This random number generator generates a custom skew with according to two parameters, data skew and access skew. Data skew represents what percentage of 
 * the total range of numbers is the "hot" part. Access skew represents what percentage of the numbers will be generated for the "hot" part of the range. For
 * example, an 80/20 skew means that 80% of the numbers will be generated from 20% of the total range of values. In this implementation, the hot range
 * is the range [0, data skew ceiling], i.e. the lower end of the range. 
 */

public class CustomSkewThreeLevelGenerator extends IntegerGenerator
{
	private Random rand; 
	
	int hot_data_access_skew; 
	int warm_data_access_skew; 
	int hot_data_size; 
	int warm_data_size; 
	
	// the max of the hot/warm/cold ranges, where hot_data_max < warm_data_max < max
	int max; 
	int hot_data_max; 	// integers in the range 0 < x < hot_data_max will represent the "hot" numbers getting hot_data_access_skew% of the accesses 
	int warm_data_max;  // integers in the range hot_data_max < x < warm_data_max will represent the "warm" numbers
	
  // generates #s from [0, _max)
	public CustomSkewThreeLevelGenerator(int _max, int _hot_data_access_skew, int _hot_data_pct, int _warm_data_access_skew, int _warm_data_pct)
	{
		assert(_hot_data_access_skew + _warm_data_access_skew <= 100) : "Workload skew cannot be more than 100%."; 
    assert(_hot_data_pct + _warm_data_pct <= 100) : "XXX";
		
		rand = new Random(); 
				
		hot_data_access_skew = _hot_data_access_skew; 
		warm_data_access_skew = _warm_data_access_skew; 

		hot_data_size = (int)(((double)_max) * ((double)_hot_data_pct / 100.0));
		warm_data_size = (int)(((double)_max) * ((double)_warm_data_pct / 100.0));
		
		max = _max;  
		
		hot_data_max = hot_data_size; 
		warm_data_max = hot_data_max + warm_data_size; 
	}
	
	public int nextInt()
	{
		int key = 0; 
		int access_skew_rand = rand.nextInt(100); 
		
		if(access_skew_rand < hot_data_access_skew)  // generate a number in the "hot" data range, 0 <= x < hot_data_max
		{
			key = rand.nextInt(hot_data_max); 
		}
		else if(access_skew_rand < (hot_data_access_skew + warm_data_access_skew)) // generate a key in the "warm" data range, hot_data_max <= x < warm_data_max
		{
			key = rand.nextInt(warm_data_max - hot_data_max) + hot_data_max; 
		}
		else  // generate a number in the "cold" data range, warm_data_max <= x < max
		{
			key = rand.nextInt(max - warm_data_max) + warm_data_max; 
		}
		
		return key; 
	}
	
	public double mean()
	{
		return 0; 
	}
}




