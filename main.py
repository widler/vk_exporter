import argparse
import json
import os
import threading
from datetime import datetime, timedelta

import jsonschema
import requests
from flask import Flask, redirect, request, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from prometheus_client.core import GaugeMetricFamily, REGISTRY

config = {}
oauth_url = "https://oauth.vk.com"
vk_method_url = "https://api.vk.com/method"

SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["group_id", "app_id", "interval_hours", "interval_ranges", "renew_interval_seconds", "port", "host"],
    "properties": {
        "group_id": {"type": "number"},
        "app_id": {"type": "number"},
        "interval_hours": {"type": "number"},
        "interval_ranges": {"type": "number"},
        "renew_interval_seconds": {"type": "number"},
        "port": {"type": "number"},
        "host": {"type": "string"}
    }
}


class VKMetrics:
    """
    Экспортер метрик группы вконтакте,
    На данный момет отображаются только количество лайков, репостов и просмотров сообщества
    """
    metrics = {}

    def collect(self):
        group_activity_views = GaugeMetricFamily(
            'views_value',
            'Group views value',
            labels=['group_name']
        )
        group_activity_reposts = GaugeMetricFamily(
            'reposts_value',
            'Group reposts value',
            labels=['group_name']
        )
        group_activity_likes = GaugeMetricFamily(
            'likes_value',
            'Group likes value',
            labels=['group_name']
        )
        group_activity_comments = GaugeMetricFamily(
            'comments_value',
            'Group comments value',
            labels=['group_name']
        )
        group_activity_subscribed = GaugeMetricFamily(
            'subscribed_value',
            'Group subscribed value',
            labels=['group_name']
        )
        group_activity_unsubscribed = GaugeMetricFamily(
            'unsubscribed_value',
            'Group unsubscribed value',
            labels=['group_name']
        )

        for group, metrics in self.metrics.items():
            group_activity_views.add_metric([group], metrics['views'])
            yield group_activity_views
            group_activity_reposts.add_metric([group], metrics['reposts'])
            yield group_activity_reposts
            group_activity_likes.add_metric([group], metrics['likes'])
            yield group_activity_likes
            group_activity_comments.add_metric([group], metrics['comments'])
            yield group_activity_comments
            group_activity_subscribed.add_metric([group], metrics['subscribed'])
            yield group_activity_subscribed
            group_activity_unsubscribed.add_metric([group], metrics['unsubscribed'])
            yield group_activity_unsubscribed


app = Flask(__name__)
REGISTRY.register(VKMetrics())


# Эта часть кода взята вотот сюда, понравилась за обработку SIGTERM SIGKILL
# https://medium.com/greedygame-engineering/an-elegant-way-to-run-periodic-tasks-in-python-61b7c477b679
class ProgramKilled(Exception):
    pass


def signal_handler(signum, frame):
    raise ProgramKilled


class Job(threading.Thread):
    def __init__(self, interval, execute, *args, **kwargs):
        threading.Thread.__init__(self)
        self.daemon = False
        self.stopped = threading.Event()
        self.interval = interval
        self.execute = execute
        self.args = args
        self.kwargs = kwargs

    def stop(self):
        self.stopped.set()
        self.join()

    def run(self):
        while not self.stopped.wait(self.interval.total_seconds()):
            self.execute(*self.args, **self.kwargs)


@app.route('/')
def get_auth_token():
    """
    Запрос кода для получения токена авторизации
    При первом входе потребует дать права на доступ к сообществам пользователя
    При последующих заходах автоматически переходит на /auth
    """
    if config['token'] is None:
        url = f"{oauth_url}/authorize?client_id={config['app_id']}&display=page&redirect_uri={config['host']}:{config['port']}/auth&scope=stats&response_type=code&v=5.131"
        return redirect(url, 302)
    return redirect('/info')


@app.route('/auth')
def auth():
    """
    Второй шаг авторизации пользователя, по полученному в функции get_auth_token коду
    Получаем access_token, необходимый для всех дальнейших операций с VK API
    """
    code = request.args['code']
    if code is not None:
        url = f"{oauth_url}/access_token?client_id={config['app_id']}&client_secret={config['app_secret_key']}&redirect_uri={config['host']}:{config['port']}/auth&code={code}"
        res = requests.get(url)
        if res.status_code == 200:
            js = res.json()
            config['token'] = js['access_token']
    return redirect('/info')


@app.route('/info')
def group_info():
    """
    получение информации о группе для которой собраем статистику, в данный момент метод "декоративный"
    Но в дальнейшем хочу сделать возможность одним exporter-ом получать информацию о нескольких группах
    и приделть веб интерфейс. Тогда информация о сообществе может оказаться нужной
    """
    # https://dev.vk.com/method/groups.getById
    url = f"{vk_method_url}/groups.getById?access_token={config['token']}&v=5.131&group_id={config['group_id']}&fields=description"
    params = {
        "group_id": config['group_id'],
        "access_token": config['token'],
        "fields": "description",
        "v": "5.131"
    }
    r = requests.get(url, params=params)
    if r.status_code == 200:
        js = r.json()
        config["screen_name"] = js["response"][0]["screen_name"]
        return js, 200
    else:
        return f"Error <strong>{r.status_code}</strong> <br /> {r.text}", 500


@app.route('/stat')
def stat():
    """
    получение статистики сообщества и отправка её в прометеус
    """
    # https://dev.vk.com/method/stats.get
    if config['token'] is None:
        print("no token")
        return ""

    url = f"{vk_method_url}/stats.get"
    now = datetime.now()
    from_time = now.replace(second=0, microsecond=0, minute=0, hour=now.hour) - timedelta(
        hours=config['interval_hours'])
    to_time = now.replace(second=0, microsecond=0, minute=0, hour=now.hour) + timedelta(
        hours=1
    )
    params = {
        "group_id": config['group_id'],
        "timestamp_from": str(from_time.timestamp()),
        "timestamp_to": str(to_time.timestamp()),
        "intervals_count": config['interval_ranges'],
        "extended": "true",
        "access_token": config['token'],
        "v": 5.131
    }
    r = requests.get(url, params=params)
    if r.status_code == 200:
        js = r.json()
        response = js["response"][0]
        visitors = response.setdefault('visitors', {})
        activity = response.setdefault('activity', {})
        metrics = VKMetrics.metrics.setdefault(config["screen_name"], {})
        metrics["views"] = visitors.get("views", 0)
        metrics["likes"] = activity.get("likes", 0)
        metrics["reposts"] = activity.get("copies", 0)
        metrics["comments"] = activity.get("comments", 0)
        metrics["subscribed"] = activity.get("subscribed", 0)
        metrics["unsubscribed"] = activity.get("unsubscribed", 0)
        return js, 200
    else:
        return f"Error <strong>{r.status_code}</strong> <br /> {r.text}", 500


@app.route("/metrics")
def metrics():
    """
    Метод для прометеуса, откуда он будет забирать статистику
    """
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


def prepare_configuration(filename: str):
    """
    Функция для получения словаря с параметрами конфигурации из файла конфигурации
    и обогащение его данными из переменных окружения
    """
    with open(filename, 'r') as file:
        cfg = json.load(file)
    jsonschema.validate(instance=cfg, schema=SCHEMA)
    secret_key = os.environ.get('VKEXP_APPLICATION_SECRET_KEY')
    if secret_key is None:
        print('No VKEXP_APPLICATION_SECRET_KEY available')
        exit(1)
    cfg['app_secret_key'] = secret_key
    cfg['token'] = None
    return cfg


def parse_command_line():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='./config.json', type=str)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_command_line()
    try:
        config = prepare_configuration(args.config)
    except BaseException as err:
        print(f"Can't prepare configuration {err}")
        exit(1)

    job = Job(interval=timedelta(seconds=config['renew_interval_seconds']), execute=stat)
    job.start()
    app.run(host='0.0.0.0', port=config['port'])
    job.stop()
    print("graceful shutdown")
