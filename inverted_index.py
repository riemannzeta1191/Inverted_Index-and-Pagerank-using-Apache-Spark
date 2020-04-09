import sys,re
from pyspark import SparkContext
from bs4 import BeautifulSoup




def parse(elem):
	soup = BeautifulSoup(elem)
	title = soup.find("title").text
	nodes = re.findall(r'\[\[([^]]*)\]\]', elem)
	return [title,nodes]


def removedups(element):
	
	li = list(set(element[1]))
	return [element[0],li]


def mapper(li):
	g = []
	for elem in li[1]:
		g.append((elem,li[0]))
	return g


if __name__=="__main__":
	sc = SparkContext(appName="inverted_index")
	
	lines = sc.textFile(sys.argv[1]).map(lambda x:parse(x)).map(lambda p:removedups(p)).map(lambda l:mapper(l)).cache()
	Map = lines.flatMap(lambda line:line)
	index = Map.groupByKey().mapValues(set)
	print(index.collect())


