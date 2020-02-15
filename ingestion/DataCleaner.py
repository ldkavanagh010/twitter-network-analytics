from pyspark.sql import DataFrame


class DataCleaner:

	def _read_data(self) -> DataFrame:
		"""Read all raw data files from s3 and return them to a spark dataframe. """
		s3_path = 's3a://{}/{}/*'.format(cfg['s3']['bucket'], cfg['s3']['input'])
		return sqlctx.read.option('encoding', 'UTF-8').json(s3_path)

	def _write_data(self, dataframe: DataFrame, s3_path: str) -> None:
		dataframe.write.mode('overwrite').parquet(s3_path)
		
	def _write_log(self, entry):


	def _clean_users(self, dataframe) -> None:
		"""find all user data dictionaries, extract relevant features for redis cache
		   FINAL SCHEMA: {id: str, screen_name: str, statuses_count: int, followers_count: int, description: str}
		"""
		s3_path = 's3a://{}/{}'.format(cfg['s3']['bucket'], cfg['s3']['users'])
		dataframe.registerTempTable('tweets')

		# find all authors of tweets captured in the data set
		tweeters = sqlctx.sql("""SELECT user.id, user.screen_name,
									 user.statuses_count, user.followers_count, user.friends_count,
									 user.description
							  FROM tweets
							  WHERE lang = 'en';
						    """)


		# find the authors of quoted tweets captured in the dataset
		quoted_users = sqlctx.sql("""SELECT user.id, user.screen_name,
										 user.statuses_count, user.followers_count, user.friends_count,
										 user.description
								  FROM ( SELECT quoted_status.user
								  		 FROM tweets
								  		 WHERE lang = 'en'
								  		 	   AND quoted_status IS NOT NULL;)
								""")

		#find the authors of retweeted tweets captured in the dataset
		retweeted_users = sqlctx.sql("""SELECT user.id, user.screen_name,
								 		 user.statuses_count, user.followers_count, user.friends_count,
								 		 user.description
							   	   FROM ( SELECT retweeted_status.user
									  	  FROM tweets
									  	  WHERE lang = 'en'
									  		 	AND retweeted_status IS NOT NULL;)
								""")


		# combine all user dataframes into one table
		users = tweeters.union(quoted_users).union(retweeted_users)
		users.registerTempTable('users')

		# Select unique entry for each user
		unique_users = sqlctx.sql("""SELECT id, screen_name, statuses_count, followers_count, friends_count, description
						   			 FROM (SELECT *,
						   		 		   ROW_NUMBER() OVER  (PARTITION BY id ORDER BY screen_name DESC) as rn
						   				   FROM users) as rows
						   			WHERE rn = 1 
						   				  AND lang = 'en';""")

		# write all combined user data to s3
		self._write_data(unique_users, s3_path)



	
	def _gather_replies(self, dataframe) -> None:
		"""find all tweet data dictionaries, extract relevant features for graphing algorithms
		   FINAL SCHEMA: {src: str, dst: str}
		"""
		s3_path = 's3a://{}/{}'.format(cfg['s3']['bucket'], cfg['s3']['retweets'])
		dataframe.registerTempTable('tweets')

		# find all instances of retweets and capture their relationships
		retweets = sqlctx.sql("""SELECT user.id as src, retweeted_status.user.id as dst
								 FROM tweets
								 WHERE retweeted_status IS NOT NULL 
								 	   AND lang = 'en' """)

		# find all instances of quote tweets and capture their relationships
		quotetweets = sqlctx.sql("""SELECT user.id as src, quoted_status.user.id as dst
								    FROM tweets
								    WHERE quoted_status IS NOT NULL 
								    	  AND lang = 'en' """)

		references = retweets.union(quotetweets)
		self._write_data(references, s3_path)

	def clean():
		""" This is the public interface for the DataCleaner class. Please call clean in order to clean and perform
			feature felection on all the raw data for the ConstituentMapper.
		"""
		tweets = self._read_data()
		self._clean_users(tweets)
		self._gather_replies(tweets)




