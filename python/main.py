import os
# Protobuf C-extension hatasını önlemek için (Özellikle Python 3.14'te) saf Python implementasyonunu kullanmaya zorluyoruz:
os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"

import pika
from config import *

from google import genai

client = genai.Client(api_key=GEMINI_API_KEY)

# RabbitMQ configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
INPUT_QUEUE = "task_queue"
OUTPUT_QUEUE = "ready_queue"

## geminaya istek atıp cevap alma fonksiyonu
def get_gemini_response(prompt):
    """Calls Gemini API to generate content based on the prompt."""
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt
        )
        return response.text
    except Exception as e:
        print(f"Error calling Gemini API: {e}")
        return f"Error: {str(e)}"


def callback(ch, method, properties, body):
    try:
        # Decode the message
        message = body.decode('utf-8')
        print(f" [x] Received request: {message}")
        
        # Call Gemini API
        print(" [x] Processing with Gemini API...")
        gemini_response = get_gemini_response(message)
        
        # Publish to ready queue
        ch.basic_publish(
            exchange='',
            routing_key=OUTPUT_QUEUE,
            body=gemini_response,
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            )
        )
        print(f" [x] Sent response to '{OUTPUT_QUEUE}' queue.")
        
        # Acknowledge the message so RabbitMQ knows it's processed
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        print(f" [!] Error processing message: {e}")
        # Reject message and don't requeue if there's a fatal error
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    # Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    # Declare the input queue (durable=True survives RabbitMQ restarts)
    channel.queue_declare(queue=INPUT_QUEUE, durable=True)
    
    # Declare the output queue
    channel.queue_declare(queue=OUTPUT_QUEUE, durable=True)

    # Fair dispatch: don't give more than one message to a worker at a time
    channel.basic_qos(prefetch_count=1)
    
    # Start consuming from the input queue
    channel.basic_consume(queue=INPUT_QUEUE, on_message_callback=callback)

    print(f" [*] Waiting for messages in '{INPUT_QUEUE}'. To exit press CTRL+C")
    channel.start_consuming()



if __name__ == '__main__':
    print(get_gemini_response("Hello, which model are you, version"))
    exit()
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted by user')
        try:
            import sys
            sys.exit(0)
        except SystemExit:
            os._exit(0)
