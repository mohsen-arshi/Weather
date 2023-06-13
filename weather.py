from requests import get
from redis import Redis
from celery import Celery

app = Celery('task110', backend='redis://localhost', broker='redis://localhost:6379/0')
app.config_from_object('task111')


def requesting(city):
    red = Redis(host="localhost", port=6379, db=0, decode_responses=True, charset='UTF-8')
    city_temp = red.get(f'temp_{city}')
    if city_temp is not None:
        return city_temp
    try:
        response = get('https://api.openweathermap.org/data/2.5/weather',
                       {"q": {city}, "appid": {"435744b930e3d41b754c08cedd23c364"}})
        r = int(response.json()['main']['temp']) - 273
        red.set(f'temp_{city}', r, ex=60)
        return r
    except ConnectionError:
        return 'you may have problem with connecting please check and try again!!!'


@app.task
def ten_city(*args):
    cities = ['tehran', 'joybar', 'gorgan', 'mashhad', 'tabriz',
              'london', 'liverpool', 'madrid', 'munich', 'shiraz',
              'ardebil']

    mapping = list(zip(cities, list(map(requesting, cities))))
    return mapping