import redis
import string
import pandas as pd
from rq import Queue, Connection
from flask import render_template, Blueprint, jsonify, request, current_app

from project.server.main.orcid import get_links_from_orcid
from project.server.main.tasks import create_task_dois, create_task_public_dump
from project.server.main.utils_swift import download_object
from project.server.main.parse import parse_all

main_blueprint = Blueprint("main", __name__,)
from project.server.main.logger import get_logger

logger = get_logger(__name__)

MOUNTED_VOLUME = '/upw_data/'

@main_blueprint.route("/", methods=["GET"])
def home():
    return render_template("main/home.html")


@main_blueprint.route("/public_dump", methods=["POST"])
def run_task_public_dump():
    args = request.get_json(force=True)
    if args.get('download') or args.get('uncompress'):
        with Connection(redis.from_url(current_app.config["REDIS_URL"])):
            q = Queue("harvest-orcid", default_timeout=216000)
            task = q.enqueue(create_task_public_dump, args)
            response_object = {
                "status": "success",
                "data": {
                    "task_id": task.get_id()
                }
            }
            return jsonify(response_object), 202
    if args.get('parse') and args.get('filename'):
        filename = args.get('filename')
        prefixes = []
        for a in list(string.digits):
            for b in list(string.digits):
                for c in list(string.digits)+['X']:
                    prefix = f'{a}{b}{c}'
                    prefixes.append(prefix)
        for prefix in prefixes:
            with Connection(redis.from_url(current_app.config["REDIS_URL"])):
                q = Queue("harvest-orcid", default_timeout=216000)
                task = q.enqueue(parse_all, f'{MOUNTED_VOLUME}{filename}/{prefix}/', True)
                response_object = {
                    "status": "success",
                    "data": {
                        "task_id": task.get_id()
                    }
                }
        return jsonify(response_object), 202

@main_blueprint.route("/publications", methods=["POST"])
def run_task_download():
    args = request.get_json(force=True)
    if args.get('links'):
        download_object('misc', 'vip.jsonl', f'{MOUNTED_VOLUME}vip.jsonl')
        df_vip = pd.read_json(f'{MOUNTED_VOLUME}vip.jsonl', lines=True)
        vips = df_vip.to_dict(orient='records')
        links = []
        for vip in vips:
            person_id = vip['id']
            orcid = None
            for ext in vip.get('externalIds'):
                if ext.get('type') == 'orcid':
                    orcid = ext['id'].upper()
                    break
            if orcid is None:
                continue
            with Connection(redis.from_url(current_app.config["REDIS_URL"])):
                q = Queue("harvest-orcid", default_timeout=216000)
                task = q.enqueue(get_links_from_orcid, orcid, person_id, orcid )
    if args.get('dois'):
        with Connection(redis.from_url(current_app.config["REDIS_URL"])):
            q = Queue("harvest-orcid", default_timeout=216000)
            task = q.enqueue(create_task_dois, args)
    response_object = {
        "status": "success",
        "data": {
            "task_id": task.get_id()
        }
    }
    return jsonify(response_object), 202

@main_blueprint.route("/tasks/<task_id>", methods=["GET"])
def get_status(task_id):
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("harvest-orcid")
        task = q.fetch_job(task_id)
    if task:
        response_object = {
            "status": "success",
            "data": {
                "task_id": task.get_id(),
                "task_status": task.get_status(),
                "task_result": task.result,
            },
        }
    else:
        response_object = {"status": "error"}
    return jsonify(response_object)
