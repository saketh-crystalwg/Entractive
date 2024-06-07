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

Entractive_2 = pd.read_sql_query("with txn_base  as  (\
select customer_fk, max(date(c_date)) as last_dpst_date  from platform.customer_transactions ct \
where status in ('APPROVED','SUCCESSFUL') \
and trx_type = 'DEPOSIT' \
group by 1), \
 \
30_days_txn_base  as  ( \
select customer_fk, max(date(c_date)) as 30_dpst_date  from platform.customer_transactions ct \
where status in ('APPROVED','SUCCESSFUL')  \
and trx_type = 'DEPOSIT' \
and DATEDIFF(SYSDATE(),date(c_date)) <= 30 \
group by 1), \
 \
RND as (select b.name as brand_name, a.id as customer_id, date(a.last_logged_activity_date) as last_log_date,  \
c.accept_marketing_offer_sms as sms_consent, c.accept_marketing_offer as email_consent, a.email, \
c.referral_info, c.first_name, c.last_name, a.login as user_name, c.phone, c.city,  d.country_iso_code, date(a.c_date) as registration_date \
from platform.customers as a  \
left join platform.merchants as b \
on a.merchant_fk = b.id \
left join platform.customer_attributes as  c \
on a.id = c.customer_fk \
left join platform.countries as d  \
on a.country_fk = d.id \
where a.country_fk in (75) \
and DATEDIFF(SYSDATE(),a.c_date) <= 120 and DATEDIFF(SYSDATE(),a.c_date) >  7 \
and a.id not in (select distinct customer_fk from platform.customer_transactions where status in ('APPROVED','SUCCESSFUL') \
and trx_type = 'DEPOSIT' )  \
)  \
 \
select c.name as brand_name, a.customer_fk as customer_id, a.referral_info, a.first_name, a.last_name, \
b.login as user_name, email, a.phone,  city, d.country_iso_code, date(b.c_date) as registration_date, last_dpst_date, \
a.accept_marketing_offer as email_consent, a.accept_marketing_offer_sms as sms_consent  \
from  platform.customer_attributes as a \
left join platform.customers  as b \
on a.customer_fk = b.id \
left join platform.merchants as c \
on b.merchant_fk = c.id \
left join platform.countries as d \
on b.country_fk = d.id \
inner join txn_base as e  \
on e.customer_fk = a.customer_fk \
left join 30_days_txn_base  as f \
on a.customer_fk = f.customer_fk \
where d.id = 75 \
and f.30_dpst_date is null  \
 \
union all  \
 \
select brand_name, customer_id,referral_info,first_name,last_name, user_name, \
email, phone, city, country_iso_code, registration_date, \
''  as last_dpst_date, email_consent,sms_consent from RND",con = connection)


Entractive_2["email"] = Entractive_2["email"].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)

Entractive_2["user_name"] = Entractive_2["user_name"].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)


Entractive_2['brand_name'] = ['PLAYDINGO' if x == 'PDIN' \
                                  else x for x in Entractive_2['brand_name']]

Entractive_2['SMS Consent'] = ['TRUE' if x == 1 \
                                  else 'FALSE' for x in Entractive_2['sms_consent']]

Entractive_2['email_consent_1'] = ['TRUE' if x == 1 \
                                  else 'FALSE' for x in Entractive_2['email_consent']]

Entractive_2.rename(columns={'referral_info': 'Affiliate Info'}, inplace=True)

Entractive_2 = Entractive_2[['brand_name','customer_id','Affiliate Info','first_name','last_name','user_name','email','phone','city',\
             'country_iso_code','registration_date','last_dpst_date','email_consent_1','SMS Consent']].reset_index(drop=True)

Entractive_1 = pd.read_sql_query("with txn_base  as  ( \
select customer_fk, max(date(c_date)) as last_dpst_date  from platform.customer_transactions ct \
where status in ('APPROVED','SUCCESSFUL') \
and trx_type = 'DEPOSIT' \
group by 1), \
 \
RND as (select b.name as brand_name, a.id as customer_id, date(a.last_logged_activity_date) as last_log_date, \
c.accept_marketing_offer_sms as sms_consent, c.accept_marketing_offer as email_consent, a.email \
from platform.customers as a \
left join platform.merchants as b \
on a.merchant_fk = b.id \
left join platform.customer_attributes as  c \
on a.id = c.customer_fk \
where a.country_fk in (75) \
and DATEDIFF(SYSDATE(),a.c_date) <= 120 and DATEDIFF(SYSDATE(),a.c_date) >  7 \
and a.id not in (select distinct customer_fk from platform.customer_transactions where status in ('APPROVED','SUCCESSFUL') \
and trx_type = 'DEPOSIT' ) \
) \
\
\
select c.name as brand_name, a.customer_fk as customer_id, d.last_dpst_date, date(b.last_logged_activity_date) as last_log_date, \
a.accept_marketing_offer_sms as sms_consent, a.accept_marketing_offer as email_consent, b.email \
from  platform.customer_attributes as a \
left join platform.customers  as b \
on a.customer_fk = b.id \
left join platform.merchants as c \
on b.merchant_fk = c.id \
inner join txn_base as d \
on a.customer_fk = d.customer_fk \
where b.country_fk = 75 \
\
union all \
 \
select brand_name, customer_id, ''  as last_dpst_date, last_log_date, sms_consent, email_consent, email from RND", con = connection)

Entractive_1["email"] = Entractive_1["email"].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)

Entractive_1['brand_name'] = ['PLAYDINGO' if x == 'PDIN' \
                                  else x for x in Entractive_1['brand_name']]

Entractive_1['Eligible'] = Entractive_1['email'].str.contains('blocked').apply(lambda x: not x if x else True)

Entractive_1['sms_consent_1'] = ['TRUE' if x == 1 \
                                  else 'FALSE' for x in Entractive_1['sms_consent']]

Entractive_1['Email Consent'] = ['TRUE' if x == 1 \
                                  else 'FALSE' for x in Entractive_1['email_consent']]

Entractive_1  = Entractive_1[['brand_name','customer_id','Eligible','last_dpst_date','last_log_date','sms_consent_1','Email Consent']].reset_index(drop=True)

Entractive_2_BR = pd.read_sql_query("with txn_base  as  ( \
select customer_fk, max(date(c_date)) as last_dpst_date  from platform.customer_transactions ct \
where status in ('APPROVED','SUCCESSFUL') \
and trx_type = 'DEPOSIT' \
group by 1 \
having DATEDIFF(SYSDATE(),last_dpst_date) <= 500 \
), \
30_days_txn_base  as  ( \
select customer_fk, max(date(c_date)) as 30_dpst_date  from platform.customer_transactions ct \
where status in ('APPROVED','SUCCESSFUL')  \
and trx_type = 'DEPOSIT' \
and DATEDIFF(SYSDATE(),date(c_date)) <= 30 \
group by 1), \
 \
RND as (select b.name as brand_name, a.id as customer_id, date(a.last_logged_activity_date) as last_log_date, \
c.accept_marketing_offer_sms as sms_consent, c.accept_marketing_offer as email_consent, a.email, \
c.referral_info, c.first_name, c.last_name, a.login as user_name, c.phone, c.city,  d.country_iso_code, date(a.c_date) as registration_date \
from platform.customers as a \
left join platform.merchants as b \
on a.merchant_fk = b.id \
left join platform.customer_attributes as  c \
on a.id = c.customer_fk \
left join platform.countries as d \
on a.country_fk = d.id \
where a.country_fk in (32) and a.merchant_fk = 77 \
and DATEDIFF(SYSDATE(),a.c_date) <= 120 and DATEDIFF(SYSDATE(),a.c_date) >  7 \
and a.id not in (select distinct customer_fk from platform.customer_transactions where status in ('APPROVED','SUCCESSFUL') \
and trx_type = 'DEPOSIT' ) \
) \
\
select c.name as brand_name, a.customer_fk as customer_id, a.referral_info, a.first_name, a.last_name, \
b.login as user_name, email, a.phone,  city, d.country_iso_code, date(b.c_date) as registration_date, last_dpst_date, \
a.accept_marketing_offer as email_consent, a.accept_marketing_offer_sms as sms_consent  \
from  platform.customer_attributes as a \
left join platform.customers  as b \
on a.customer_fk = b.id \
left join platform.merchants as c \
on b.merchant_fk = c.id \
left join platform.countries as d \
on b.country_fk = d.id \
inner join txn_base as e \
on e.customer_fk = a.customer_fk \
left join 30_days_txn_base  as f \
on a.customer_fk = f.customer_fk \
where d.id = 32 \
and f.30_dpst_date is null and b.merchant_fk = 77 \
 \
union all \
 \
select brand_name, customer_id,referral_info,first_name,last_name, user_name, \
email, phone, city, country_iso_code, registration_date, \
''  as last_dpst_date, email_consent,sms_consent from RND", con = connection)

Entractive_2_BR["email"] = Entractive_2_BR["email"].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)

Entractive_2_BR["user_name"] = Entractive_2_BR["user_name"].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)

Entractive_2_BR['brand_name'] = ['PLAYDINGO' if x == 'PDIN' \
                                  else x for x in Entractive_2_BR['brand_name']]

Entractive_2_BR['SMS Consent'] = ['TRUE' if x == 1 \
                                  else 'FALSE' for x in Entractive_2_BR['sms_consent']]

Entractive_2_BR['email_consent_1'] = ['TRUE' if x == 1 \
                                  else 'FALSE' for x in Entractive_2_BR['email_consent']]

Entractive_2_BR.rename(columns={'referral_info': 'Affiliate Info'}, inplace=True)

Entractive_2_BR = Entractive_2_BR[['brand_name','customer_id','Affiliate Info','first_name','last_name','user_name','email','phone','city',\
             'country_iso_code','registration_date','last_dpst_date','email_consent_1','SMS Consent']].reset_index(drop=True)


Entractive_1_BR = pd.read_sql_query("with txn_base  as  ( \
select customer_fk, max(date(c_date)) as last_dpst_date  from platform.customer_transactions ct \
where status in ('APPROVED','SUCCESSFUL') \
and trx_type = 'DEPOSIT' \
group by 1 \
having DATEDIFF(SYSDATE(),last_dpst_date) <= 500 ), \
 \
RND as (select b.name as brand_name, a.id as customer_id, date(a.last_logged_activity_date) as last_log_date, \
c.accept_marketing_offer_sms as sms_consent, c.accept_marketing_offer as email_consent, a.email \
from platform.customers as a \
left join platform.merchants as b \
on a.merchant_fk = b.id \
left join platform.customer_attributes as  c \
on a.id = c.customer_fk \
where a.country_fk in (32) and a.merchant_fk = 77 \
and DATEDIFF(SYSDATE(),a.c_date) <= 120 and DATEDIFF(SYSDATE(),a.c_date) >  7 \
and a.id not in (select distinct customer_fk from platform.customer_transactions where status in ('APPROVED','SUCCESSFUL') \
and trx_type = 'DEPOSIT' ) \
) \
 \
select c.name as brand_name, a.customer_fk as customer_id, d.last_dpst_date, date(b.last_logged_activity_date) as last_log_date,  \
a.accept_marketing_offer_sms as sms_consent, a.accept_marketing_offer as email_consent, b.email \
from  platform.customer_attributes as a \
left join platform.customers  as b \
on a.customer_fk = b.id \
left join platform.merchants as c \
on b.merchant_fk = c.id \
inner join txn_base as d \
on a.customer_fk = d.customer_fk \
where b.country_fk = 32 and b.merchant_fk = 77 \
 \
union all \
 \
select brand_name, customer_id, ''  as last_dpst_date, last_log_date, sms_consent, email_consent, email from RND", con = connection)

Entractive_1_BR["email"] = Entractive_1_BR["email"].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)

Entractive_1_BR['brand_name'] = ['PLAYDINGO' if x == 'PDIN' \
                                  else x for x in Entractive_1_BR['brand_name']]

Entractive_1_BR['Eligible'] = Entractive_1_BR['email'].str.contains('blocked').apply(lambda x: not x if x else True)

Entractive_1_BR['sms_consent_1'] = ['TRUE' if x == 1 \
                                  else 'FALSE' for x in Entractive_1_BR['sms_consent']]

Entractive_1_BR['Email Consent'] = ['TRUE' if x == 1 \
                                  else 'FALSE' for x in Entractive_1_BR['email_consent']]

Entractive_1_BR  = Entractive_1_BR[['brand_name','customer_id','Eligible','last_dpst_date','last_log_date','sms_consent_1','Email Consent']].reset_index(drop=True)


date = dt.datetime.today()
date_1 = date.strftime("%m-%d-%Y")

filename = f'Entractive_{date_1}.xlsx'

with pd.ExcelWriter(filename) as writer:
    Entractive_1.reset_index(drop=True).to_excel(writer, sheet_name="Entractive_1_FI", index=False)
    Entractive_2.reset_index(drop=True).to_excel(writer, sheet_name="Entractive_2_FI", index=False)
    Entractive_1_BR.reset_index(drop=True).to_excel(writer, sheet_name="Entractive_1_BR", index=False)
    Entractive_2_BR.reset_index(drop=True).to_excel(writer, sheet_name="Entractive_2_BR", index=False)


sub = f'Entractive Summary - {date_1}'

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
recipients = ["sakethg250@gmail.com","sebastian@crystalwg.com","saketh@crystalwg.com","alin@crystalwg.com"]
password = "xjyb jsdl buri ylqr"
send_mail(sender, recipients, subject, body, "smtp.gmail.com", 465, sender, password)