
import pykafka
from kafka import KafkaProducer, KafkaClient
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# Kafka settings
topic = 'twitterstream'
# setting up Kafka producer
kafka = KafkaClient('localhost:9092')
producer = KafkaProducer(kafka)


class TweeterStreamListener(StreamListener):
    """ A class to read the twiiter stream and push it to Kafka"""
    
    def __init__(self):
        self.api = api
        super(tweepy.StreamListener, self).__init__()
        self.client = pykafka.KafkaClient("localhost:9092")
        self.producer = self.client.topics[bytes('twitter','ascii')].get_producer()
    
    def on_data(self, data):
        producer.send_messages(topic, data.encode('utf-8'))
        #print data
        return True

    def on_error(self, status_code):
        print("Error received in kafka producer")
        return True

    def on_timeout(self):
        return True

if __name__ == '__main__':
    
    
    consumer_key    = 'n6T1OKV0v9i3UYY5jNfYhvBWp'
    consumer_secret = 'mIqxDjH3Ik0q335lqNYpiveqbWL07iJeNtzNOxr7Nh5cSXiovI'
    access_token    = '1603771112-2fDsRiXRqP9UpOCVk2PZdZtaPHZvTZUdWtQvtCX'
    access_secret   = 'oUNZitGZKSbXrjjkAer3TtzshIjcc2Cm2LCXwYbfDGm6r'
    
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)

    # Create stream and bind the listener to it
    #stream = tweepy.Stream(auth, listener = TweeterStreamListener(api))
    stream = Stream(auth, TweeterStreamListener())

    #Custom Filter rules pull all traffic for those filters in real time.
    #stream.filter(track = ['love', 'hate'], languages = ['en'])
    stream.filter(languages = ['en'])
