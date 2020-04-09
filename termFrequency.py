import  sys
from pyspark import  SparkContext
import  string
from operator import add


def words(l):
	tup = ()
	count = [(elem, 1) for elem in l]

	return count


def removepunc(l):
	elem = ""
	li =[]
	for elem in l:
		for s in string.punctuation:
			if s in elem:
				elem = elem.replace(s, "")
				li.append(elem)
		li.append(elem)
	final = list(set(li))
	return final


if __name__ == "__main__":
	sc = SparkContext(appName="termFrequency")
	lines = sc.textFile("abc.txt").map(lambda l:l.split()).filter(lambda x:len(x)>1).map(lambda punc:removepunc(punc))\
			.map(lambda c: words(c))
	fi = []
	for elem in lines.collect():
		fi.extend(elem)
	rdd = sc.parallelize(fi).reduceByKey(add)
	print(rdd.collect())