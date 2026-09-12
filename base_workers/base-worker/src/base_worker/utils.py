import os
import requests
import traceback

def ds_error_notifier(context):
    """Discord notifier wrapper for task execution errors"""
    ti = context['task_instance']

    task_name = ti.task_id
    dag_id = ti.dag_id
    run_date = context.get('logical_date') or ti.logical_date
    content = (f"# Task execution error\n\n"
               f"Task {task_name} failed at {dag_id}")

    if context.get('exception'):
        embeds = [
            {
                "description": "".join(traceback.format_exception(context.exception)),
                "color": 16711680,
                "footer": f"{run_date}"
            }
        ]
    else:
        embeds = []

    __notify_mambo(content,embeds)

def notify_to_ds(title, text, origin):
    """Mambo notifier helper.
    :param title: message title
    :param text: body text
    :param origin: message source"""
    content = (f"# {title}\n\n"
             f"{text}\n\n"
             f"{origin}, Mambo! ")

    __notify_mambo(content,[])


def __notify_mambo(content,embeds):
    payload = {
        "content": content,
        "embeds": embeds,
        "tts": True,
    }

    requests.post(
        os.environ['DS_MAMBO_WHOOK_URL'],
        json=payload
    )