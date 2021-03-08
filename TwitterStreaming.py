from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
sc = SparkContext(conf=conf)
# Create a streaming context with batch interval of 10 sec
ssc = StreamingContext(sc, 10)
  

def main():
    ssc.checkpoint("checkpoint")
    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")

def load_wordlist(filename):
    
    words = sc.textFile(filename)
    wordlist = words.flatMap(lambda x:x.encode("utf-8").split('\n'))
    return wordlist.collect()

def stream(ssc, pwords, nwords, duration):
    kafkastream = KafkaUtils.createDirectStream(ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kafkastream.map(lambda x: x[1].encode("ascii","ignore"))
    words = tweets.flatMap(lambda x:x.split(' '))
    
    def feel(word):
        if word in pwords:
            return "positive"
        if word in nwords:
            return "negative"
        else:
            return "neutral"
    
    pairs = words.map(lambda x: (feel(x.lower()),1))
    count = pairs.reduceByKey(lambda x,y:x+y)
    count = count.filter(lambda x: (x[0]=="positive" or x[0]=="negative"))

    def updateFunction(newValues, runningCount):
        if runningCount is None:
            runningCount = 0
        return sum(newValues, runningCount)  

    runningCounts = count.updateStateByKey(updateFunction)
    runningCounts.pprint()

    counts = []
    count.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    # Start the computation
    ssc.start()
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts

if __name__=="__main__":
    main()
