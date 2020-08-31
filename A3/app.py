from flask import Flask, render_template, Response
from datetime import datetime
import redis
import json

app = Flask(__name__)
r = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)

def event_stream():
    pubsub = r.pubsub()
    pubsub.subscribe('SurpriseNumberTopology')
    for message in pubsub.listen():
        print(message)
        data = message['data']
        if type(data) != int:
            surprise_number, num_unique_users, stream_counter = data.decode().split("|")
            json_data = json.dumps(
                {'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'surprise_number': float(surprise_number), 'num_unique_users': int(num_unique_users), 'stream_count': int(stream_counter)})
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
