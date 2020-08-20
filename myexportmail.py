#!/usr/bin/python2.7
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
import tempfile
import click
import smtplib
import string
import ConfigParser
import syslog
import re
from email.header import Header

def column_name(col):
    cs = col.strip().split(' ')
    return cs[-1].strip("'")


def column_names(query):
    query = re.sub(r'\n',' ', query)
    while re.search(r'\([^\(\)]+\)', query):
        query = re.sub(r'(\([^\(\)]+\))', '', query)

    match = re.match(r'^SELECT (.*) FROM .*', query, re.I)
    field_list = []

    if match:
        columns = match.group(1)

        for c in columns.split(','):
            field_list.append(column_name(c))

    return field_list


def format_email(mail):
    return "%s <%s>" % (Header(mail[0], 'utf-8'), mail[1])


def command_line():
    cl = ''
    for v in sys.argv:
        if v != sys.argv[0]:
            cl += v
            cl += ' '

    return cl.strip()


def logger(level, message):
    syslog.syslog(level, "\"%s\"; %s" % (command_line(), message))
    if level == syslog.LOG_INFO:
        print "INFO: %s" % message
    else:
        print "ERROR: %s" % message


class my_mail:
    """
    execute mysql script, send mail results.
    """

    def __init__(self, configure):
        self.dir = os.path.join(tempfile.gettempdir(), sys.argv[0].split('/')[-1])
        self.day = datetime.datetime.now().strftime('%Y%m%d')
        self.rows = 0
        self.conf = {}
    
        if not os.path.isfile(configure):
            logger(syslog.LOG_ERR, 'The configure file "%s" is not exist' % configure)
            sys.exit(1)

        self.config_file = configure
        self.event_id = "%s_%s" % (os.path.basename(self.config_file).split('.')[0], self.day)

        if not os.path.isdir(self.dir):
            os.mkdir(self.dir)
        else:
            os.system(
                'find %s -type f -ctime +30 \( -name \*.csv -o -name \*.zip -o -name \*.smtp \) -exec rm -f {} +' % self.dir)

        if os.path.isfile(os.path.join(self.dir, "%s.smtp" % self.event_id)):
            logger(syslog.LOG_INFO, "The mail has been send, don't do this.")
            sys.exit(1)


    def check_configure(self):
        cp = ConfigParser.ConfigParser()
        try:
            cp.read(self.config_file)
            sections = cp.sections()
        except ConfigParser.ParsingError:
            logger(syslog.LOG_ERR, 'The configure file "%s" is parsererror, ParsingError' % self.config_file)
            sys.exit(1)
        except ConfigParser.NoSectionError:
            logger(syslog.LOG_ERR, 'The configure file "%s" is invaild, NoSectionError' % self.config_file)
            sys.exit(1)
        except BaseException:
            logger(syslog.LOG_ERR, 'The configure file "%s" is parsererror' % self.config_file)
            sys.exit(1)
        else:
            self.conf['default'] = {}
            section = 'default'

            for option in ['smtpsubject', 'smtpbodytext', 'smtphost', 'smtpssl', 'smtpport', 'smtpuser', 'smtppass', 'mailto']:
                if not cp.has_option(section, option):
                    logger(syslog.LOG_ERR, 'The configure file "%s" section "%s" option "%s" is missing' % (self.config_file, section, option))
                    sys.exit(1)
                item = cp.get(section, option).strip()

                if not item:
                    logger(syslog.LOG_ERR, 'The configure file "%s" section "%s" option "%s" is not setting' % (self.config_file, section, option))
                    sys.exit(1)

                if option[-6:] == 'mailto':
                    self.conf[section][option] = item.split(';')
                else:
                    if option == 'smtpuser':
                        self.conf[section][option] = item.strip('>').split('<')[-1]
                        self.conf[section][option + '_msg'] = item.strip('>').split('<')
                    else:
                        self.conf[section][option] = item

            for option in ['ccmailto', 'bccmailto', 'dbhost', 'dbcharset', 'dbuser', 'dbpass', 'dbname', 'query']:
                if cp.has_option(section, option):
                    item = cp.get(section, option).strip()
                    if item:
                        if option[-6:] == 'mailto':
                            self.conf[section][option] = item.split(';')
                        else:
                            self.conf[section][option] = item

            for section in sections:
                if section == 'default':
                    continue

                self.conf[section] = {}

                if not cp.has_option(section, 'title'):
                    self.conf[section]['title'] = section
                else:
                    self.conf[section]['title'] = cp.get(section, 'title').strip()

                for option in ['dbhost', 'dbcharset', 'dbuser', 'dbpass', 'dbname', 'query']:
                    if not cp.has_option(section, option):
                        if not self.conf['default'][option]:
                            logger(syslog.LOG_ERR, 'The configure file "%s" section "%s" option "%s" is missing' % (self.config_file, section, option))
                            sys.exit(1)
                        cp.set(section, option, self.conf['default'][option])

                    item = cp.get(section, option).strip()

                    if not item:
                        logger(syslog.LOG_ERR, 'The configure file "%s" section "%s" option "%s" is not setting' % (self.config_file, section, option))
                        sys.exit(1)

                    self.conf[section][option] = item

                self.conf[section]['rows'] = 0
                self.conf[section]['filename'] = ''

    def exports(self):
        import csv
        import codecs
        import MySQLdb

        for key in self.conf.keys():
            if key == 'default':
                continue

            config = self.conf[key]

            columns = column_names(config['query'])
            if len(columns) == 0:
               logger(syslog.LOG_ERR, 'query "%s" is invalid.' % config['query'])
               sys.exit(1)

            # connect to the database
            db = MySQLdb.connect(host=config['dbhost'], user=config['dbuser'], passwd=config['dbpass'], db=config['dbname'], charset=config['dbcharset'])
            cursor = db.cursor()
            
            try:
                self.conf[key]['rows'] = cursor.execute(config['query'])
            except BaseException:
                logger(syslog.LOG_ERR, 'sql "%s" execute error' % config['query'])
                cursor.close()
                db.close()
                continue

            if self.conf[key]['rows'] == 0:
                continue

            filename = os.path.join(self.dir, "%s_%s.csv" % (self.event_id, config['title']))
            with open(filename, 'wb') as csv_file:
                csv_file.write(codecs.BOM_UTF8)
                f = csv.writer(csv_file)

                try:
                    f.writerow(columns)
                except csv.Error as e:
                    logger(syslog.LOG_ERR, 'file %s, %s' % (filename, e))
                    sys.exit(1)
    
                self.rows += self.conf[key]['rows']

                try:
                    f.writerows(cursor)
                except csv.Error as e:
                    logger(syslog.LOG_ERR, 'write rows "%s" file execute error "%s"' % (filename, e))
                    sys.exit(1)

            cursor.close()
            db.close()
            self.conf[key]['filename'] = filename


    def create_zip(self):
        import zipfile

        if self.rows == 0:
            logger(syslog.LOG_INFO, "all sql execute return nothing, don't send mail.")
            sys.exit(1)

        zipname = os.path.join(self.dir, '%s.zip' % self.event_id)
        with zipfile.ZipFile(zipname, 'w', zipfile.ZIP_DEFLATED) as myzip:
            for key in self.conf.keys():
                if key == 'default':
                    continue

                if self.conf[key]['rows'] > 0:
                    myzip.write(self.conf[key]['filename'], os.path.basename(self.conf[key]['filename']))
                    os.remove(self.conf[key]['filename'])

            myzip.close()


    def send_email_with_attachment(self):
        from email import Encoders
        from email.mime.text import MIMEText
        from email.MIMEBase import MIMEBase
        from email.MIMEMultipart import MIMEMultipart
        from email.Utils import formatdate
        from email.utils import formataddr

        attach_file = os.path.join(self.dir, '%s.zip' % self.event_id)

        my_conf = self.conf['default']

        # create the message
        msg = MIMEMultipart()
        msg["From"] = format_email(my_conf['smtpuser_msg'])
        msg["Subject"] = Header("%s%s" % (self.day, my_conf['smtpsubject']), 'utf-8')
        msg["Date"] = formatdate(localtime=True)

        mailto = []
        msgto = []
        for mc in my_conf['mailto']:
            if re.match(r'^.*<.*@.*>$', mc, re.I):
                mail_mc = mc.strip('>').split('<')
                mailto.append(mail_mc[-1])
                msgto.append(format_email(mail_mc))
            else:
                mailto.append(mc)
                msgto.append(mc)

        msg["To"] = ','.join(msgto)
        emails = mailto

        if 'ccmailto' in my_conf:
            if len(my_conf['ccmailto']):
                ccmailto = []
                ccmsgto = []
                for cc in my_conf['ccmailto']:
                    if re.match(r'^.*<.*@.*>$', cc, re.I):
                        mail_cc = cc.strip('>').split('<')
                        ccmailto.append(mail_cc[-1])
                        ccmsgto.append(format_email(mail_cc))
                    else:
                        ccmailto.append(cc)
                        ccmsgto.append(cc)

                msg["CC"] = ','.join(ccmsgto)
                emails += ccmailto

        if 'bccmailto' in my_conf:
            if len(my_conf['bccmailto']):
                bccmailto = []
                bccmsgto = []
                for bcc in my_conf['bccmailto']:
                    if re.match(r'^.*<.*@.*>$', bcc, re.I):
                        mail_bcc = bcc.strip('>').split('<')
                        bccmailto.append(mail_bcc[-1])
                        bccmsgto.append(format_email(mail_bcc))
                    else:
                        bccmailto.append(bcc)
                        bccmsgto.append(bcc)

                msg["BCC"] = ','.join(bccmsgto)
                emails += bccmailto

        if my_conf['smtpbodytext']:
            msg.attach(MIMEText(my_conf['smtpbodytext'], 'plain', 'utf-8'))

        attachment = MIMEBase('application', "octet-stream")

        try:
            with open(attach_file, "rb") as fh:
                data = fh.read()
            attachment.set_payload(data)
            Encoders.encode_base64(attachment)
            header = 'Content-Disposition', 'attachment; filename="%s"' % ('%s.zip' % self.event_id)
            attachment.add_header(*header)
            msg.attach(attachment)
        except IOError:
            logger(syslog.LOG_ERR, "Error opening attachment file %s" % attach_file)
            sys.exit(1)

        try:
            if my_conf['smtpssl'] == '0':
                server = smtplib.SMTP(my_conf['smtphost'], my_conf['smtpport'])
            else:
                server = smtplib.SMTP_SSL(my_conf['smtphost'], my_conf['smtpport'])

            server.login(my_conf['smtpuser'], my_conf['smtppass'])
            server.sendmail(my_conf['smtpuser'], emails, msg.as_string())
            server.quit()
            logger(syslog.LOG_INFO, "Successfully sent email")
            os.mknod(os.path.join(self.dir, "%s.smtp" % self.event_id))
        except smtplib.SMTPException, e:
            logger(syslog.LOG_ERR, e)
        except BaseException, e:
            logger(syslog.LOG_ERR, "unable to send email, %s" % e)

    def main(self):
        self.check_configure()
        self.exports()
        self.create_zip()
        self.send_email_with_attachment()


@click.command()
@click.option('-c', '--configure', help='Configure File', required=True)
def main(configure):
    reload(sys)  
    sys.setdefaultencoding('utf8')
    my = my_mail(configure)
    my.main()

if __name__ == "__main__":
    main()
