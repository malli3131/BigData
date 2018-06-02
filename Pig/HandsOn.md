## Pig HandsOn....

* Apache Pig is client to Hadoop
* Pig is allowed run anywhere i.e., on cluster node, edge node or individual work station
* Pig doesn't have any service component in cluster.
* So installing and running Pig is always on Client nodes.

#### Pig Installation



```
package malli.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class Addition extends EvalFunc<Integer>{

	@Override
	public Integer exec(Tuple input) throws IOException {
		if(input == null || input.size() == 0)
		{
			return null;
		}
		Integer number1 = (Integer) input.get(0);
		Integer number2 = (Integer) input.get(1);
		Integer sum = number1 + number2;
		return sum;
	}
}



Input Data
----------------
p1        201.23
p1        200.50
p2        123.45
p3        99.99
p3        98.88
p2        125.67
p3        122.56
p4        50.50
p3        120.55
p4        55.55
p4        54.44

PigScript:
----------
register /home/hadoopz/naga/bigdata/pig-0.10.0/pigscripts/avgbag.jar
products = load '/pig/products' using PigStorage() as (product:chararray, price:double);
grpproducts = group products by product;
proavg = foreach grpproducts generate group, com.pig.udf.AvgBag(products.price) as sum;
dump proavg;

UDF
---
package com.pig.udf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class AvgBag extends EvalFunc<Double> {

	@Override
	public Double exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		DataBag myBag = (DataBag) input.get(0);
		Iterator<Tuple> itr = myBag.iterator();
		Tuple tuple = null;
		Double sum = 0.0d;
		Double avg = 0.0d;
		int counter = 0;
		while (itr.hasNext()) {
			tuple = itr.next();
			Double price = (Double) tuple.get(0);
			sum = sum + price;
			counter++;
		}
		avg = sum / counter;
		return Math.round(avg * 100.0) / 100.0;
	}
}


Output
------
(p1,200.87)
(p2,124.56)
(p3,110.5)
(p4,53.5)



package malli.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class Concat extends EvalFunc<String>{

	@Override
	public String exec(Tuple input) throws IOException {
		if(input == null || input.size() == 0)
		{
			return null;
		}
		String fullName = "";
		String firstName = (String) input.get(0);
		String lastName = (String) input.get(1);
		fullName = firstName + " " + lastName;
		return fullName;
	}

}


Input Data
----------------
p1 201.23
p1 200.50
p2 123.45
p3 99.99
p3 98.88
p2 125.67
p3 122.56
p4 50.50
p3 120.55
p4 55.55
p4 54.44


PigScript
---------
register /home/hadoopz/naga/bigdata/pig-0.10.0/pigscripts/maxbag.jar                    
products = load '/pig/products' using PigStorage() as (product:chararray, price:double);
grpproducts = group products by product;                                                
promax = foreach grpproducts generate group, com.pig.udf.MaxBag(products.price) as max; 
dump promax;

UDF
---
package com.pig.udf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class MaxBag extends EvalFunc<Double> {

	@Override
	public Double exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		DataBag myBag = (DataBag) input.get(0);
		Iterator<Tuple> itr = myBag.iterator();
		Tuple tuple = null;
		Double max = 0.0d;
		while (itr.hasNext()) {
			tuple = itr.next();
			Double price = (Double) tuple.get(0);
			max = (max < price) ? price : max;
		}
		return max;
	}
}

Output
------
(p1,201.23)
(p2,125.67)
(p3,122.56)
(p4,55.55)


Input Data
----------------
p1 201.23
p1 200.50
p2 123.45
p3 99.99
p3 98.88
p2 125.67
p3 122.56
p4 50.50
p3 120.55
p4 55.55
p4 54.44


PigScript
---------
register /home/hadoopz/naga/bigdata/pig-0.10.0/pigscripts/minbag.jar                    
products = load '/pig/products' using PigStorage() as (product:chararray, price:double);
grpproducts = group products by product;                                                
promin = foreach grpproducts generate group, com.pig.udf.MinBag(products.price) as min; 
dump promin;

UDF
---
package com.pig.udf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class MinBag extends EvalFunc<Double> {

	@Override
	public Double exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		DataBag myBag = (DataBag) input.get(0);
		Iterator<Tuple> itr = myBag.iterator();
		Tuple tuple = null;
		Double min = Double.MAX_VALUE;
		while (itr.hasNext()) {
			tuple = itr.next();
			Double price = (Double) tuple.get(0);
			min = (min > price) ? price : min;
		}
		return min;
	}
}

Output
------
(p1,200.5)
(p2,123.45)
(p3,98.88)
(p4,50.5)


Input Data
--------------
p1	201.23
p1	200.50
p2	123.45
p3	99.99
p3	98.88
p2	125.67
p3	122.56
p4	50.50
p3	120.55
p4	55.55
p4	54.44

PigScript:
----------
register /home/hadoopz/naga/bigdata/pig-0.10.0/pigscripts/sumbag.jar
products = load '/pig/products' using PigStorage() as (product:chararray, price:double);
grpproducts = group products by product;
prosum = foreach grpproducts generate group, com.pig.udf.SumBag(products.price) as sum;
dump prosum;

UDF
---
package com.pig.udf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class SumBag extends EvalFunc<Double> {

	@Override
	public Double exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		DataBag myBag = (DataBag) input.get(0);
		Iterator<Tuple> itr = myBag.iterator();
		Tuple tuple = null;
		Double sum = 0.0d;
		while (itr.hasNext()) {
			tuple = itr.next();
			Double price = (Double) tuple.get(0);
			sum = sum + price;
		}
		return Math.round(sum * 100.0) / 100.0;
	}
}

Output
-------
(p1,401.73)
(p2,249.12)
(p3,441.98)
(p4,160.49)




package malli.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class Upper extends EvalFunc<String>{

	@Override
	public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{
            String str = (String)input.get(0);
            return str.toUpperCase();
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
		return null;
	}
}

Input Data:
-----------
DocName	Tokens
------- ------
cricket	sachin,sehwag,dravid,dhoni
movie	amir,salman,hruthik,ranveer
cricket	sachin,ganguly,rohit,dhoni
cricket	sehwag,sachin,dravid,kohli
movie	salman,amir,sharukh

===================================================
Pig UDF
--------
package com.pig.udf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class WordBag extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		DataBag myBag = (DataBag) input.get(0);
		String frequency = "";
		Iterator<Tuple> itr = myBag.iterator();
		Tuple tuple = null;
		Map<String, Integer> wordcount = new HashMap<String, Integer>();
		while (itr.hasNext()) {
			tuple = itr.next();
			DataBag tokens = (DataBag) tuple.get(0);
			Iterator<Tuple> it = tokens.iterator();
			while(it.hasNext())
			{
				tuple = it.next();
				String token = (String) tuple.get(0);
				if (wordcount.containsKey(token)) {
					int count = wordcount.get(token);
					count++;
					wordcount.put(token, count);
				} else {
					wordcount.put(token, 1);
				}
			}
		}
		Set<String> keys = wordcount.keySet();
		for (String key : keys) {
			frequency = frequency + " " + key + ":" + wordcount.get(key);
		}
		return frequency;
	}
}

Build a jar for the above UDF and add it to pig script;

========================================================================================
PigScript:
----------
register /home/hadoopz/naga/bigdata/pig-0.10.0/pigscripts/wordbag.jar
news = load '/pig/news' using PigStorage() as (doc:chararray, content:chararray);
words = foreach news generate doc, TOKENIZE(content, ',') as mywords;
describe words;
wordcount = foreach grpwords generate group, com.pig.udf.WordBag(words.mywords);
dump wordcount;

==========================================================================================
Output
------
docName	Tokens and their Frequency
------- --------------------------
(movie, sharukh:1 salman:2 ranveer:1 hruthik:1 amir:2)
(cricket, sehwag:2 kohli:1 rohit:1 ganguly:1 sachin:3 dhoni:2 dravid:2)


--This pig script is used to aggregated the top 10 volumes of each and every stock
--Load the data from HDFS and Delimiter is ",".
stocks = load '/nyse/stocks' using PigStorage(',') as (ex:chararray, stock:chararray, sdate:chararray, open:double, high:double, low:double, close:double, volume:long, adj_close:double);
--Project the stock and volume columns...
reqitems = foreach stocks generate stock, volume;
--group reqitems by stock
grpstocks = group reqitems by stock;
--Sort the stocks data and limit top10 for every stock...
--It is using nested foreach.....
top10 = foreach grpstocks { 
  sortorder = order reqitems by volume desc; 
  top = limit sortorder 10; 
  generate group, flatten(top);
}
--again group the top10 by stock
mygroup = group top10 by top::stock;
--aggregate the every 10 volumes of each and every stock
mysum = foreach mygroup generate group, SUM(top10.top::volume);
dump mysum;



```
