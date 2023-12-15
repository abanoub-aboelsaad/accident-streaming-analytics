from kafka import KafkaProducer
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic = 'test'

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)


class FileEventHandler(FileSystemEventHandler):
    def on_created(self, event):

        with open(event.src_path, 'r') as file:
            lines = file.readlines()
            for line in lines:
                producer.send(topic, value=line.encode())

# Watchdog observer to monitor the directory for file events
observer = Observer()
event_handler = FileEventHandler()
directory_to_watch = 'C:/Users/Abanoub/Desktop/bigdata/project/Section 7/project14/data'  

observer.schedule(event_handler, directory_to_watch)
observer.start()

try:
    while True:
        pass  
except KeyboardInterrupt:
    observer.stop()

observer.join()
