import imaplib
import email
import os
import sys
import time
import pp
import conn_redis
import datetime


def record_hdfs(path_record, append):
    os.popen("hdfs dfs -put /home/cloudera/Desktop/Accounts/" + path_record + " /accounts")
    os.popen("echo \"" + append + "\" | hdfs dfs -appendToFile - /accounts/" + path_record)
    os.popen("hdfs dfs -put /home/cloudera/Desktop/Attachments/* /attachments")


def get_user_pwd(path):
    with open(path, "r") as emailList:
        lines = emailList.read().split("\n")
        print(lines)
        for line in lines:
            fix = line.split(";")
            userx = fix[0]
            passw = fix[1]
            dir = "/home/cloudera/Desktop/Attachments/"
            job1 = job_server.submit(download_email, (userx, passw, dir), (extract_body, record_hdfs), ("imaplib", "email", "datetime", "conn_redis"))
            result = job1()
        return userx, passw


def extract_body(payload):
    if isinstance(payload, str):
        return payload
    else:
        return '\n'.join([extract_body(part.get_payload()) for part in payload])


def download_email(user, pwd, dir):
    conn = imaplib.IMAP4_SSL("imap.gmail.com", 993)
    conn.login(user, pwd)
    conn.select("Inbox")

    c = conn_redis.conn_redis(host='localhost', port=6379)
    now_date = datetime.date.today().strftime('%Y%m%d%H%M')
    user_mail = user

    redis_date = conn_redis.get_email_last_date(email=user_mail, conn=c)
    date_compared = conn_redis.compare_dates(now_date, redis_date)

    if redis_date is None:
        resp, items = conn.search(None, 'ALL')
        items = items[0].split()
    else:
        date_compared_fmt = datetime.datetime.strptime(date_compared, '%Y%m%d').strftime('%d-%b-%Y')
        resp, items = conn.search(None, '(SINCE "%s")' % date_compared_fmt)
        items = items[0].split()

    for emailid in items:
        resp, data = conn.fetch(emailid, "(RFC822)")
        email_body = data[0][1]  # getting the mail content
        mail = email.message_from_string(email_body)  # parsing the mail content to get a mail object
        global body
        body = ""
        if mail.is_multipart():
            for part in mail.walk():
                c_type = part.get_content_type()
                c_dispo = str(part.get('Content-Disposition'))

                # skip any text/plain (txt) attachments
                if c_type == 'text/plain' and 'attachment' not in c_dispo:
                    body = part.get_payload(decode=True)  # decode
                    break

        else:
            body = mail.get_payload(decode=True)

        # conn_redis.set_email_date(email=user_mail, last_date_value=now_date, conn=c)

        if mail.get_content_maintype() != 'multipart':
            continue

        path_dir = "/home/cloudera/Desktop/Accounts/" + str(user) + ".txt"
        with open(path_dir, "a") as result:
            result.write("Date: " + mail["Date"] + "\n")
            result.write("[From: " + mail["From"] + "] :" + "\n")
            result.write("[To: " + mail["To"] + "\n")
            result.write("Subject: " + mail["Subject"] + "\n")
            result.write("Body: " + body + "\n")
            result.close()

        append = "Date: " + mail["Date"] + "\n" + "[From: " + mail["From"] + "] :" + "\n" + "[To: " + mail["To"]
        append += "\n" + "Subject: " + mail["Subject"] + "\n" + "Body: " + body + "\n"

        record_hdfs(str(user) + ".txt", append)

        lista = mail["Date"].split(' ')
        lists = [lista[1], lista[2], lista[3], lista[4]]
        lista2 = [lists[0] + ' ' + lists[1] + ' ' + lists[2] + ' ' + lists[3]]
        lista_3 = []

        for item in lista2:
            lista_3.append(datetime.datetime.strptime(item, '%d %b %Y %H:%M:%S').strftime('%Y%m%d'))
            conn_redis.set_email_date(email=user_mail, last_date_value=lista_3[0], conn=c)

        # Check if any attachments at all
        if mail.get_content_maintype() != "multipart":
            continue

        for part in mail.walk():
            # multipart are just containers, so we skip them
            if part.get_content_maintype() == "multipart":
                continue

            # is this part an attachment ?
            if part.get("Content-Disposition") is None:
                continue

            filename = part.get_filename()
            counter = 1

            # if there is no filename, we create one with a counter to avoid duplicates
            if not filename:
                filename = 'part-%03d%s' % (counter, 'bin')
                counter += 1

            att_path = os.path.join(dir, filename)

            if not os.path.isfile(att_path):
                fp = open(att_path, 'wb')
                fp.write(part.get_payload(decode=True))
                fp.close()

            conn.close()
            conn.logout()


ppservers = ()


if len(sys.argv) > 1:
    ncpus = int(sys.argv[1])
    # Creates jobserver with ncpus workers
    job_server = pp.Server(ncpus, ppservers=ppservers)
else:
    # Creates jobserver with automatically detected number of workers
    job_server = pp.Server(ppservers=ppservers)

print "Starting pp with", job_server.get_ncpus(), "workers"

get_user_pwd("/home/cloudera/Desktop/email.csv")

start_time = time.time()
print "Time elapsed: ", time.time() - start_time, "s"
job_server.print_stats()

#record_hdfs()
