### ConstituentMapper
> Find Your Voters, Find Their Cliques

ConstituentMapper is a web application that aims to help political campaigns find real time metrics for their campaign's twitter usage and decide on the strategy they need to take to maximize their reach online. Due to the polarization of political communities in America today, ConstituentMapper finds communities within the larger Twitterverse and charts their most influential members. Then users can see in real time the reach they have within those communities. 

This project was made as part of the Insight Data Engineering Program in New York (Winter 2020). Visit [ConsituentMapper](digitalanalytics.life "ConsituentMapper") on the web and check it out. You can also watch a short demo here.


### Data Sources
	Tweets of Verified Users accounting for the second half of 2019 (May - December), consisting of all statuses posted, retweets and quote tweets. (~2TB JSON raw uncompressed )
	 Tweets gathered from the twitter api by the folks at archiveteam at          archive.org, in the same form as the pushshift data, but including deletes as well.  (>1TB JSON raw uncompressed)

More Information about the twitter api and the definition of tweets, quote tweets, retweets and replies can be found in the twitter documentation at:
> https://developer.twitter.com/en.html

### Pipeline
[![Architecture](https://i.imgur.com/dNRgQmF.png "Architecture")](Architecture "Architecture")

**Amazon S3 - Raw Data Storage and Data Intermediary**
The raw data is in json form and the cleaned data is written out to parquet.

**Apache Spark - Data Cleaning and Processing.**
First Spark is used for feature selection, followed by processing the data into graph form. Afterwards, the data is run through the Label Propagation Algorithm and separated into clusters. The clusters are then run through PageRank to determine who are the influencers in that community.

** Redis **
Database for the front end. Data is dumped to it by apache spark, and made available to be queried by the front end.

** Flask **
The front-end for the the application. Maintains a connection to both the redis database and the twitter api for querying.

### Installation and Setup

1. Download and Configure [Pegasus](https://github.com/InsightDataScience/pegasus "Pegasus"), a command-line tool used to set-up and configure EC2 Clusters. Included are yaml files, which you can use to replicate my environment, by way of `peg up *.yml`

2. Clone this github repo onto your local machine with:
`git clone github.com/PKjamoo/twitter-network-analytics.git`

3. Fill in the stubbed `config.yml` file provided with the necessary information on your spark-cluster, database and frontend instances. 

4. Run the provided shell script `cluster_setup.sh`, to install all necessary software onto the different EC2 instances.

5. Follow the instructions provided in `redis_setup.txt` to configure and run the database instance, you spun up in step 1.

6. Likewise follow the instructions in `frontend_setup.txt` to configure the frontend instance, you spun up in step 1. 

### Running ConstituentMapper

1.  ** Running the Batch Job**
Run the provided shell script `ingest.sh`, from the master node of your spark-cluster, which can be accessed with `peg fetch spark-cluster` and `peg ssh spark-cluster 1`

2. **Running the Graph Algorithms **
Run the provided shell script `graph.sh` accessed from the master node in the same way as in the previous step.

3. **Run the Frontend **
ssh into your frontend EC2 instance and run the provided shell script `cm.sh`. Then go to the public ip of your EC2 and try it out!




