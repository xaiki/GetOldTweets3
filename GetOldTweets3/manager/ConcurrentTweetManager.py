# Author: Giulio Neusch-Frediani - www.github.com/giulionf
from datetime import datetime
from datetime import timedelta
from threading import Thread
import copy

from GetOldTweets3.manager.TweetManager import TweetManager

DATE_FORMAT = "%Y-%m-%d"
FIRST_TWEET_DATE = datetime.strptime("2006-03-21", DATE_FORMAT)


class ConcurrentTweetManager:

    @staticmethod
    def getTweets(tweetCriteria, receiveBuffer=None, bufferLength=100, proxy=None, debug=False, worker_count=1,
                  forceMaxTweets=False):

        if worker_count < 1:
            raise ValueError("At least one worker is needed")

        if tweetCriteria.maxTweets != 0 and not forceMaxTweets:
            raise ValueError("Max Tweets is not supported by parallel downloading, since the results can not be ordered"
                             " by time. If you do not care, you can set forceMaxTweets=True!")

        # Init the queues
        time_spans = []
        tweets = []
        workers = []

        # Split the date in smaller parts
        since_date = datetime.strptime(tweetCriteria.since, DATE_FORMAT) if hasattr(tweetCriteria, "since") else FIRST_TWEET_DATE
        until_date = datetime.strptime(tweetCriteria.until, DATE_FORMAT) if hasattr(tweetCriteria, "until") else datetime.now()
        date_diff_per_worker = (until_date - since_date) / worker_count

        if date_diff_per_worker < timedelta(days=1):
            max_workers = int((until_date - since_date) / timedelta(days=1))
            raise ValueError("Too many workers for the time span. Each worker needs at least one day for himself, or"
                             " some workers will have the same results leading to inconsistencies."
                             "For your case, the max worker count is {} workers".format(max_workers))

        # Create a TweetCriteria that can be cloned as Model by the Workers
        criteria = copy.deepcopy(tweetCriteria)
        if forceMaxTweets:
            criteria.setMaxTweets(tweetCriteria.maxTweets / worker_count)

        # Create a time span for each of the splitted parts and a corresponding worker
        for i in range(1, worker_count+1):
            from_time = since_date + (i-1) * date_diff_per_worker
            to_time = since_date + i * date_diff_per_worker
            time_spans.append((from_time, to_time))

        for i in range(0, worker_count):
            w = WorkerThread(copy.deepcopy(criteria), time_spans[i], tweets, receiveBuffer, bufferLength,
                             proxy, debug)
            w.start()
            workers.append(w)

        # Wait for the workers to finish, then return the results
        for worker in workers:
            worker.join()

        return sorted(tweets, key=lambda r: r.date)


class WorkerThread(Thread):

    def __init__(self, tweetCriteria, time_span, tweets,  receiveBuffer=None, bufferLength=100, proxy=None,
                 debug=False):
        super().__init__()
        self.stopped = False
        self.manager = TweetManager()
        self.tweetCriteria = tweetCriteria
        self.receiveBuffer = receiveBuffer
        self.bufferLength = bufferLength
        self.proxy = proxy
        self.debug = debug
        self.time_span = time_span
        self.tweets = tweets

    def run(self) -> None:
        self.tweetCriteria.setSince(datetime.strftime(self.time_span[0], "%Y-%m-%d"))
        self.tweetCriteria.setUntil(datetime.strftime(self.time_span[1], "%Y-%m-%d"))
        search_results = self.manager.getTweets(self.tweetCriteria, self.receiveBuffer, self.bufferLength, self.proxy,
                                                self.debug)
        self.tweets.extend(search_results)
