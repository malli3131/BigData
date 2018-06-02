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



assert operator example

sample data set is: 

1,2,3
4,2,1
8,3,4
4,3,3
7,2,5
8,4,3

mydata = load '/pig/nums' using PigStorage(',') as (n1:int, n2:int, n3:int);
ASSERT mydata by n1 > 2, 'n1 should be greater than 2';

In general most of the input data fits with scalar data types, but some times we have to deal with complex input data sets like

(3,8,9) (4,5,6)
(1,4,7) (3,7,5)
(2,5,8) (9,5,8)

mydata = LOAD '/pig/data' AS (col1:tuple(n11:int, n12:int,n13:int),col2:tuple(n21:int,t22:int,t23:int));
projdata = foreach mydata generate col1.n11 as num1, col2.n21 as num2;


Base ball data example
a sample record is:

Jorge Posada    New York Yankees        {(Catcher),(Designated_hitter)} [games#1594,hit_by_pitch#65,on_base_percentage#0.379,grand_slams#7,home_runs#243,at_bats#5365,sacrifice_flies#43,gdb#163,sacrifice_hits#1,ibbs#71,base_on_balls#838,hits#1488,rbis#964,slugging_percentage#0.48,batting_average#0.277,triples#9,doubles#342,strikeouts#1278,runs#817]

baseball = load '/pig/baseball' as (name:chararray, team:chararray, postition:bag{t:(p:chararray)}, bat:map[]);
mydata = foreach baseball generate name, bat#'base_on_balls' - bat#'ibbs'

cube operator example

sample data:

car, 2012, midwest, ohio, columbus, 4000
car, 2013, midwest, texas, austin, 5000
car, 2014, midwest, ohio, columbus, 6000
car, 2011, midwest, california, palo alto, 14000
car, 2012, midwest, ohio, columbus, 3000

example:

cars = load '/pig/cars' as (product:chararray, year:int, region:chararray, state:chararray, city:chararray, sales:long);
cubedata = CUBE cars BY CUBE(product,year);
result = FOREACH cubedata GENERATE FLATTEN(group), SUM(cube.sales) AS totalsales;

define dividend_analysis (daily, year, daily_symbol, daily_open, daily_close)
returns analyzed {
	divs          = load 'pig/divs' as (exchange:chararray,
						symbol:chararray, date:chararray, dividends:float);
	divsthisyear  = filter divs by date matches '$year-.*';
	dailythisyear = filter $daily by date matches '$year-.*';
	jnd           = join divsthisyear by symbol, dailythisyear by $daily_symbol;
	$analyzed     = foreach jnd generate dailythisyear::$daily_symbol, $daily_close - $daily_open;
};

daily 	= load '/pig/stocks' as (exchange:chararray, symbol:chararray,
			date:chararray, open:float, high:float, low:float, close:float,
			volume:int, adj_close:float);
results = dividend_analysis(daily, '2009', 'symbol', 'open', 'close');
dump results;

stocks = load '/pig/stocks' as (market:chararray, stock:chararray, sdate:chararray, open:double, high:double, low:double, close:double, volume:long, adj_close:double);
aggdata = mapreduce '/home/naga/bigdata/volume.jar' store stocks into '/pig/mystocks' load '/pig/aggvolume' as (stock:chararray, aggvolume:long) `StockTotalVolume /pig/mystocks /pig/aggvolume`;
dump aggdata

mapreduce operator example:
----------------------------
Download the data NYSE_daily from alangates programming pig data folder:
https://github.com/alanfgates/programmingpig/blob/c77436c2d0416574966a2e9925b4ee4b6eb5caca/data/NYSE_daily

We have already a MapReduce program for computing aggregated volume for stock i.e volume.jar

Use this MapReduce program inside pig script and no need to develop the same logic in PigLatin


example:

stocks = load '/pig/stocks' as (market:chararray, stock:chararray, sdate:chararray, open:double, high:double, low:double, close:double, volume:long, adj_close:double);

aggdata = mapreduce '/home/naga/bigdata/volume.jar' store stocks into '/pig/mystocks' load '/pig/aggvolume' as (stock:chararray, aggvolume:long) `StockTotalVolume /pig/mystocks /pig/aggvolume`;


The above pig latin statement broken as follows:

1. store stocks into '/pig/mystocks'
2. hadoop jar /home/naga/bigdata/volume.jar StockTotalVolume /pig/mystocks /pig/aggvolume
3. aggdata = load '/pig/aggvolume' as (stock:chararray, aggvolume:long);

In simple words.. PigLatin statements ----> MapReduce Job ---> PigLatin statements



stream operator example:

Download the dataset NYSE_dividends from alangates programming pig data folder:
https://github.com/alanfgates/programmingpig/blob/c77436c2d0416574966a2e9925b4ee4b6eb5caca/data/NYSE_dividends


write a perl program to enable control: "highdiv.pl"

#!/usr/bin/perl

use strict;

while (<>) {
	my @fields = split;
	if ($fields[3] > 1.0) {
		print join("\t", @fields) . "\n"
	}
}

change the file permission of perl script:
chmod 755 <path>/highdiv.pl

divs = load '/pig/divs' as (exchange, symbol, date, dividends);
highdivs = stream divs through `/home/naga/bigdata/highdiv.pl` as (exchange, symbol, date, dividends);
dump highdivs;


we can also ship the external program using the following statement:

define highdivs `/home/naga/bigdata/highdiv.pl` ship('/home/naga/bigdata/highdiv.pl');

divs = load '/pig/divs' as (exchange, symbol, date, dividends);
highdivs = stream divs through highdivs as (exchange, symbol, date, dividends);
dump highdivs;


data = load '/pig/news';
words = foreach data generate flatten(TOKENIZE((chararray)$0)) as word;
tokens = filter words by word matches '\\w+';
gprd = group tokens by word;
cntd = foreach gprd generate group, COUNT(tokens) as fre;
ord = order cntd by fre desc;
lmt = limit ord 10;
dump lmt;

names = load '/pig/names' using PigStorage() as (fname:chararray, lname:chararray);

ages = load '/pig/ages' using PigStorage() as (age:int);

record = union names, ages;

dump record;

data = load '/pig/stocks' using PigStorage()as (market: chararray, stockname : chararray, date:chararray, open:float, high:float, low:float, close:float, volume:long,overall:float);
project = foreach data generate stockname, volume;
gprd = group project by stockname;
cntd = foreach gprd generate group, COUNT(project.stockname);
volume = foreach gprd generate group, SUM(project.volume);
store cntd into '/pig/counts' using PigStorage();
store volume into '/pig/volume' using PigStorage();


ages = load '/pig/ages' using PigStorage() as (age:int);
split ages into young if age <25, middle if (age >25 AND age <30), old if age >30;
dump young;
dump middle;
dump old;

data = load '/daily' using PigStorage();
test = sample data 0.01;
dump test;

ages = load '/pig/ages' using PigStorage() as (age:int);                           
data = order ages by age desc;
ltd = limit data 3;
dump data;

data = load '/pig/cross' using PigStorage() as (fname:chararray,lname:chararray,age:int);
age24 = filter data by age == 24;                                                        
dump age24

ranks = load '/pig/ranks' using PigStorage() as (player:chararray,con:chararray,rank:int);
country = load '/pig/country' using PigStorage() as (country:chararray, con:chararray);
--inner join
rankings = join ranks by con, country by con;
--right outer join
rankings = join ranks by con right, country by con;
--full outer join
rankings = join ranks by con full, country by con; 
--left outer join
rankings = join ranks by con left, country by con
dump rankings

names = load '/pig/names' using PigStorage() as (fname:chararray, lname:chararray);       
ages = load '/pig/ages' using PigStorage() as (age:int);                                  
crs = cross names, ages;
dump crs

nums = load '/pig/numbers' using PigStorage() as (num1:int, num2:int);
arth = foreach nums generate num1, num2, num1 + num2 as add, num2 - num1 as sub, num1 * num2 as mul;
dump arth;

MATH:

mydata = load 'number' as (num:double);
ab = foreach mydata generate ABS(num);
dump ab;

mydata = load 'number' as (num:double);
acos = foreach mydata generate ACOS(num);
dump acos;

mydata = load '/stats/number' as (num:double);
asin = foreach mydata generate ASIN(num);
dump asin;

mydata = load '/stats/number' as (num:double);
atan = foreach mydata generate ATAN(num);
dump atan;

mydata = load '/stats/number' as (num:double);
ceil = foreach mydata generate CEIL(num);
dump ceil;

mydata = load '/stats/number' as (num:double);
cos = foreach mydata generate COS(num);
dump cos;

mydata = load '/stats/number' as (num:double);
cosh = foreach mydata generate COSH(num);
dump cosh;

mydata = load '/stats/number' as (num:double);
round = foreach mydata generate ROUND(num);
dump round;

mydata = load '/stats/number' as (num:double);
croot = foreach mydata generate CBRT(num);
dump croot;

mydata = load '/stats/number' as (num:double);
exp = foreach mydata generate EXP(num);
dump exp;

mydata = load '/stats/number' as (num:double);
floor = foreach mydata generate FLOOR(num);
dump floor;

mydata = load '/stats/number' as (num:double);
log = foreach mydata generate LOG(num);
dump log;

mydata = load '/stats/number' as (num:double);
gprd = group mydata by num;
agg = foreach gprd generate group, AVG(mydata.num);
dump agg;

mydata = load '/stats/info' using PigStorage() as (company:chararray, sales:double);
uniq = distinct mydata;
gprd = group uniq by company;
cntd = foreach gprd generate group, COUNT(uniq.sales) as total;
ord = order cntd by total desc;
dump ord; 

stats = foreach gprd generate group, MIN(uniq.sales) as min, MAX(uniq.sales) as max, MEAN(uniq.sales);


```
