# coding: utf-8

import smtplib
from email.mime.text import MIMEText
# email 用于构建邮件内容
from email.header import Header


# 用于构建邮件头
class EmailSmtp:
    def __init__(self):
        pass

    def send(self, target_email='', content={}):
        if not target_email:
            return {
                'status': 2,
                'message': 'target_email is empty',
            }

        if not content:
            return {
                'status': 3,
                'message': 'content is empty',
            }

        if 'title' not in content:
            return {
                'status': 4,
                'message': 'content.title not exists',
            }

        if 'content' not in content:
            return {
                'status': 5,
                'message': 'content.content not exists',
            }

        if not content['title']:
            return {
                'status': 4,
                'message': 'content.title is empty',
            }

        # 发信方的信息：发信邮箱，QQ 邮箱授权码
        from_addr = '1719285365@qq.com'
        password = 'qxljlmhvmdatcdgj'

        # 收信方邮箱
        to_addr = str(target_email)

        # 发信服务器
        smtp_server = 'smtp.qq.com'

        # 邮箱正文内容，第一个参数为内容，第二个参数为格式(plain 为纯文本)，第三个参数为编码

        text = "From: 张滨彬的邮件服务\r\n\r\n"+str(content['content'])
        msg = MIMEText(text, 'plain', 'utf-8')

        # 邮件头信息
        msg['From'] = Header(from_addr)
        msg['To'] = Header(to_addr)
        msg['Subject'] = Header(str(content['title']))

        # 开启发信服务，这里使用的是加密传输
        server = smtplib.SMTP_SSL(smtp_server)
        server.connect(smtp_server, 465)
        # 登录发信邮箱
        server.login(from_addr, password)
        # 发送邮件
        server.sendmail(from_addr, to_addr, msg.as_string())
        # 关闭服务器
        server.quit()

        return {
            'status': 1,
            'message': 'success',
        }


if __name__ == '__main__':
    pass
    # try:
    #     start = time.perf_counter()
    #
    #     Temp = Temp()
    #     Temp.main()
    # except BaseException as err:
    #     print('__main__ exception: ')
    #     print(err)
    # finally:
    #     elapsed = (time.perf_counter() - start)
    #     print("Python Complete, Time used:", elapsed)
