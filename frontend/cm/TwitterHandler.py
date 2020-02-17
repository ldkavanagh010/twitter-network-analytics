import tweepy


class TwitterHandler:


	def __init__(self):
		consumer_key = cfg['twitter']['consumer']
		consumer_secret = cfg['twitter']['consumer_secret']
		access_token = cfg['twitter']['api_key']
		access_token_secret = cfg['twitter']['secret_key']
		try:
			auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
			auth.set_access_token(access_token, access_token_secret)
			self.client = tweepy.API(auth)
			if not self.client.verify_credentials():
				raise tweepy.TweepError
		except tweepy.TweepError as e:
			print('ERROR : connection failed. Check your OAuth keys.')



	def get_user_profile(self, screen_name):
		return self.client.get_user(screen_name)

	def get_user_timeline(self, user_id):
		return self.client.user_timeline(user_id)[:10]

	def get_retweeters(self, tweet_id):
		return self.client.retweeters(tweet_id)