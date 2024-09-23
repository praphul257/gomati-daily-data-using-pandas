import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os

def send_email(record_date):
    smtp_server = os.getenv('SMTP_SERVER')
    smtp_port = os.getenv('SMTP_PORT')
    smtp_user = os.getenv('SMTP_USER')
    smtp_password = os.getenv('SMTP_PASSWORD')
    from_addr = os.getenv('FROM_ADDR')
    to_addrs = os.getenv('TO_ADDRS').split(',')
    
    subject = f"Daily Data for Gomati Meters: {record_date}"
    body = f"""Please find the attached file (Block load, Daily load, Push events, Pull events and Monthly Billing data).
Date: {record_date}

This is an auto-generated email.
Please do not reply.
Thank you
    """
    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = ", ".join(to_addrs)
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    attachment_path = f'{record_date}_Gomati_Data.csv'
    with open(attachment_path, 'rb') as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename= {os.path.basename(attachment_path)}')
        msg.attach(part)

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(from_addr, to_addrs, msg.as_string())
    
    print(f"Email sent to {to_addrs} with attachment file : {attachment_path}")
