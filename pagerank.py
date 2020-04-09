import sys, re
from pyspark import SparkContext
from bs4 import BeautifulSoup
from operator import add


def parse(elem):
	soup = BeautifulSoup(elem)
	title = re.findall(r'<title>(.*?)</title>', elem)
	nodes = re.findall(r'\[\[([^]]*)\]\]', elem)
	return title[0],nodes


def initial_rank(element,count):
	n = len(element[1])
	for el in element[1]:
		return (el,1/count)


def rank_fractions(nodes):
	n = len(nodes[1])
	for node in nodes[1]:
		return (node,1/n)


if __name__ == "__main__":
	sc = SparkContext(appName="pagerank")
	lines = sc.textFile(sys.argv[1])
	count = lines.count()
	N = 1/count
	links = lines.map(lambda x:parse(x)).cache()
	# print(links.collect())
	# fractionals = lines.map(lambda f:parse(f)).map(lambda z:rank_fractions(z)).filter(lambda fg:(fg!=None)).mapValues(lambda pr:pr*N)
	# fractionals = lines.map(lambda f:parse(f)).map(lambda p:initial_rank(p,count))


	#

	ranks = sc.emptyRDD()
	contribs = sc.emptyRDD()
	#
	initial_ranks = links.map(lambda r:initial_rank(r,count))
	frac = initial_ranks.map(lambda p: p).filter(lambda f: (f != None))
	# print(frac.collect())
	# print(initial_ranks.collect())
	for i in range(10):
		fractionals =  initial_ranks.map(lambda p: rank_fractions(p)).filter(lambda f:(f!=None))
		contribs = fractionals.reduceByKey(add)
		ranks = contribs.mapValues(lambda v:.15 + .85 * v)
		print(ranks.collect())
		joined = links.join(frac)

	print(joined.collect())

	pageRanksOrdered = ranks.takeOrdered(count, key = lambda x: -x[1])
	pageRanksOrderedRDD = sc.parallelize(pageRanksOrdered)
	pageRanksOrderedRDD.saveAsTextFile("pageRanks_wiki.txt")
