import random
import os
import yaml
from flask import Flask, render_template, request, jsonify
from flask_redis import FlaskRedis
from twitter_handler import TwitterHandler
from redis_handler import decode_redis


with open('config.yml', 'r') as ymlfile:
    cfg = yaml.safe_load(ymlfile)

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
    SECRET_KEY='dev',
    DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    REDIS_URL = 'redis://:{}@{}:{}/0'.format(cfg['redis']['pwd'], cfg['redis']['host'], cfg['redis']['port'])
    )
    app.static_folder = 'static'
    redis_client = FlaskRedis(app)
    twitter_client = TwitterHandler()


    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    @app.route('/')
    def index():
        return render_template('base.html')

    @app.route('/user/', methods=['GET'])
    def user():
        if request.method == "GET":
            user = twitter_client.get_user_profile(screen_name=request.args.get('username'))
            timeline = twitter_client.get_user_timeline(user.id)[:5]

        return render_template('user.html', user=user, timeline=timeline)

    @app.route('/background_process', methods=['GET'])
    def background_process():
        try:
            tweet_id = request.args.get("tweet_id")
            community_size = {}

            retweeters = twitter_client.get_retweeters(tweet_id)
            for user_id in retweeters:
                label_value = redis_client.hget(('user:' + user_id), "label")
                if label_value is None:
                    continue
                label_value = label_value.decode('utf-8')
                if label_value not in community_size:
                    community_size[label_value] = 1
                else:
                    community_size[label_value] += 1
            communities = []
            for key, value in community_size.items():
                community = {"name": key, "size": value}
                communities.append(community)

            communities = sorted(communities, key = lambda i: (i['size']), reverse=True)

            return render_template('communities.html', communities=communities)
        except Exception as e:
            return str(e)

    @app.route('/background_process_2', methods=['GET'])
    def background_process_2():
        try:
            community = request.args.get('name')
            community_members = []
            members = redis_client.hget(('communities:' + community) , 'influencers')
            members = members.decode('utf-8')[13:-1]
            members = members.split(', ')
            return render_template('community.html', members=members)


        except Exception as e:
            return str(e)


    return app

if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port='80')
