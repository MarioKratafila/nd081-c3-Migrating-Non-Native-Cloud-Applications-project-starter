import azure.functions as func
import logging
import psycopg2
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from datetime import datetime

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="notificationqueue",
                               connection="techconf_SERVICEBUS") 
def servicebus_queue_trigger(azservicebus: func.ServiceBusMessage):
    logging.info('Python ServiceBus Queue trigger processed a message: %s',
                azservicebus.get_body().decode('utf-8'))
    
    try:
        conn = psycopg2.connect(user="postgres", password=os.getenv('POSTGRES_PASSWORD'), host="techconfdb.postgres.database.azure.com", port=5432, database="techconfdb")
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM public.notification WHERE id = %s", (azservicebus.get_body().decode('utf-8'),))
        notification = cursor.fetchone()
        logging.info('Fetched notification: %s', notification)
        message_body = notification[2]
        message_subject = notification[5]
        sender_email = 'info@techconf.com'
        sg = SendGridAPIClient(os.getenv('SENDGRID_API_KEY'))

        logging.info('Fetching attendee list...')
        cursor.execute("SELECT first_name, last_name, email FROM public.attendee")
        attendees = cursor.fetchall()
        logging.info('Fetched attendee list: %s', len(attendees))
   
        for attendee in attendees:
            first_name = attendee[0]
            last_name = attendee[1]
            recipient_email = attendee[2]
            customized_subject = message_subject + " - " + first_name + " " + last_name

            mail = Mail(
                from_email=sender_email,
                to_emails=recipient_email,
                subject=customized_subject,
                html_content=message_body
            )

            sg.send(mail)

        current_timestamp = datetime.now()
        cursor.execute("UPDATE public.notification SET status = %s WHERE id = %s", (f'Notified {len(attendees)} attendees', notification[0]))
        conn.commit()
        cursor.execute("UPDATE public.notification SET completed_date = %s WHERE id = %s", (current_timestamp, notification[0]))
        conn.commit()
        logging.info('Record updated successfully')

        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(error)
