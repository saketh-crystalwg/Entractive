import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
from babel.numbers import format_currency
import numpy as np

import mysql.connector
from mysql.connector import Error

from sqlalchemy import create_engine

import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders


connection = mysql.connector.connect(host='206.189.96.57',
                                         database='platform',
                                         user='PlatBI',
                                         password='BIAIPass!2019204PurumPum')

Entractive_3 = pd.read_sql_query("with txn_base  as  ( \
select customer_fk, max(date(c_date)) as last_dpst_date  from platform.customer_transactions ct \
where status in ('APPROVED','SUCCESSFUL') \
and trx_type = 'DEPOSIT' \
group by 1) \
 \
select c.name as brand_name, a.customer_fk as customer_id, a.referral_info, a.first_name, a.last_name, \
b.login as user_name, email, a.phone,  city, d.country_iso_code, date(b.c_date) as registration_date, \
a.accept_marketing_offer as email_consent, a.accept_marketing_offer_sms as sms_consent  \
from  platform.customer_attributes as a \
left join platform.customers  as b \
on a.customer_fk = b.id \
left join platform.merchants as c \
on b.merchant_fk = c.id \
left join platform.countries as d \
on b.country_fk = d.id \
left join txn_base as e \
on e.customer_fk = a.customer_fk \
where d.id = 75 \
and c.id in (53,59) \
and e.last_dpst_date is null", con = connection)

Entractive_3['brand_name'] = ['PLAYDINGO' if x == 'PDIN' \
                                  else x for x in Entractive_3['brand_name']]

Entractive_3['SMS Consent'] = ['TRUE' if x == 1 \
                                  else 'FALSE' for x in Entractive_3['sms_consent']]

Entractive_3['Email Consent'] = ['TRUE' if x == 1 \
                                  else 'FALSE' for x in Entractive_3['email_consent']]

Entractive_3.rename(columns={'referral_info': 'Affiliate Info'}, inplace=True)

Entractive_3 = Entractive_3[['brand_name','customer_id','Affiliate Info','first_name','last_name','user_name','email','phone','city',\
             'country_iso_code','registration_date','Email Consent','SMS Consent']].reset_index(drop=True)

date = dt.datetime.today()
date_1 = date.strftime("%m-%d-%Y")

filename = f'Entractive_Segment_2_{date_1}.xlsx'

with pd.ExcelWriter(filename) as writer:
    Entractive_3.reset_index(drop=True).to_excel(writer, sheet_name="Entractive_3", index=False)
    
sub = f'Entractive_Segment_2 - {date_1}'

def send_mail(send_from, send_to, subject, text, server, port, username='', password=''):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = ', '.join(recipients)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    msg.attach(MIMEText(text))

    part = MIMEBase('application', "octet-stream")
    part.set_payload(open(filename, "rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename={filename}')
    msg.attach(part)

    # context = ssl.SSLContext(ssl.PROTOCOL_SSLv3)
    # SSL connection only working on Python 3+
    smtp = smtplib.SMTP_SSL(server, port)
    smtp.login(username, password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()


subject = sub
body = f"Hi,\n\n Attached contains list of customers for Entractive  Campaign Activity for {date_1}.\n\nThanks,\nSaketh"
sender = "sakethg250@gmail.com"
recipients = ["sakethg250@gmail.com"]
password = "xjyb jsdl buri ylqr"
send_mail(sender, recipients, subject, body, "smtp.gmail.com", 465, sender, password)