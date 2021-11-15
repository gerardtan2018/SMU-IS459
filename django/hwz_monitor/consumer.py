from kafka import KafkaConsumer
import json
import os
import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning) 
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "hwz_monitor.settings")
from django.core.wsgi import get_wsgi_application

application = get_wsgi_application()
from dashboard.models import PostCount

def main():
    consumer = KafkaConsumer('dashboard_author',
                                group_id='dashboard_author',
                                bootstrap_servers=['localhost:9092'],
                                auto_offset_reset='latest',
                                value_deserializer=lambda v:json.loads(v.decode('utf-8'))
                            )
    PostCount.objects.all().delete()
    for record in consumer:
        entry = record.value
        timestamp = record.key.decode("utf-8").split('.')[0]
        postCount = PostCount(timestamp=timestamp, user_name=entry['author'], post_count=int(entry['count']))
        print(postCount)
        postCount.save()

if __name__ == "__main__":
    main()