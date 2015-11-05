from fluent import sender
from fluent import event

def send_buffer(fluent_host, fluent_tag, level):
    tags = fluent_tag.split('.', 1)
    sender.setup(tags[0], host=fluent_host, port=24224)
    with open('/tmp/pyxtra_fluent.buffer') as fd:
        msg = fd.read()
    event.Event(tags[1], {
        'message': str(msg),
        'level': str(level)
    })
