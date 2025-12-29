import redis
from rq import Queue, Connection
from flask import render_template, Blueprint, jsonify, request, current_app

from project.server.main.tasks import create_task_update, create_task_update_v2

main_blueprint = Blueprint("main", __name__,)
from project.server.main.logger import get_logger

logger = get_logger(__name__)
queue_name = "harvest-anr"


@main_blueprint.route("/", methods=["GET"])
def home():
    return render_template("main/home.html")

@main_blueprint.route("/update", methods=["POST"])
def run_task_update():
    args = request.get_json(force=True)
    if args.get('v2'):
        with Connection(redis.from_url(current_app.config["REDIS_URL"])):
            q = Queue(queue_name, default_timeout=2160000)
            task = q.enqueue(create_task_update_v2, args)
    else:
        with Connection(redis.from_url(current_app.config["REDIS_URL"])):
            q = Queue(queue_name, default_timeout=2160000)
            task = q.enqueue(create_task_update, args)
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
        q = Queue(queue_name)
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
