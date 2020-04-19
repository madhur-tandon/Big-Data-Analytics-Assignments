from flask import Flask, render_template, Response
from datetime import datetime
import redis
import json

app = Flask(__name__)
r = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)
total_users = 0

def event_stream():
    global total_users
    pubsub = r.pubsub()
    pubsub.subscribe('WordCountTopology')
    for message in pubsub.listen():
        data = message['data']
        if type(data) != int:
            word, count = data.split(b'|')
            # json_data = json.dumps(
                # {'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'word': str(word),'value': int(count)})
            total_users += int(count)
            json_data = json.dumps(
                {'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'word': str(word),'value': int(total_users)}
            )
            yield f"data:{json_data}\n\n"


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/stream')
def stream():
    return Response(event_stream(), mimetype="text/event-stream")


if __name__ == '__main__':
    app.run(debug=True,
    threaded=True,
    host='0.0.0.0'
)
