# BDA Assignment 3

1. Run the following commands in separate terminals
    - `storm dev-zookeeper`
    - `storm nimbus`
    - `storm supervisor`
    - `storm ui`

2. Start Redis in yet another terminal
    - `redis-server`

3. Clear the data present in Redis
    - `redis-cli`
    - `FLUSHALL`
    - `EXIT`

4. Go to the project folder using `cd`

4. Submit the Topology to Apache Storm
    - `sparse run -n surprise_number_twitter`

5. Run the Flask server
    - `python app.py`

6. Open up `localhost:8772` and `localhost:5000` in separate tabs in the browser

7. Enjoy the Live Chart