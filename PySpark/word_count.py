from pyspark import SparkConf
from pyspark import SparkContext

if __name__=="__main__":

    conf = SparkConf().setAppName('word_cound').setMaster('local[3]')
    sc = SparkContext(conf=conf)

    # read data from text file and split each line into words
    textFile = sc.textFile("file:///home/saif/LFS/datasets/wordcount.txt")
    print(textFile.collect())
    split_words = textFile.flatMap(lambda line:line.split(" "))
    print(split_words.collect())
    word_Assign = split_words.map(lambda word:(word,1))
    print(word_Assign.collect())
    pycharmWordCount = word_Assign.reduceByKey(lambda x,y:x+y)
    print(pycharmWordCount.collect())
    pycharmWordCount.saveAsTextFile('hdfs://localhost:9000/user/saif/HFS/Output/PycharmWordCount')