
import pymqi
import datetime
import os
import requests

# Параметры подключения IBM MQ
queue_manager = 'RGSMGR_PROD'
channel = 'SYSTEM.DEF.SVRCONN'
host = '10.221.11.108'
port = '1414'
user = 'mq_adm'
password = 'shuTh8ooyohk'
bytes_encoding = 'utf-8'
default_ccsid = 819

# Параметры подключения SPLUNK
url_splunk = '10.99.16.77'
port_splunk = '8088'
securitytoken_splunk = """5af3ea57-bca1-45cf-ae82-2275d038de58"""


#содаем сообщение для записи в SPLUNK
def create_splunk_message(url,queue, time_oldest_MSG,delay_msg,depth):
    dictjson = {
        "index": 'int_status',
        "sourcetype": "JSON",
        "event": {
            "url": url,
            "Message": 'Completed',
            "Queue": queue,
            "Action": 'Read_Oldest_MSG_Time',
            "Time_Oldest_MSG":time_oldest_MSG,
            "Delay_MSG_Delivery":delay_msg,
            "System_Create_Log":'Python_script',
            "Depth":depth
        }
    }
    return dictjson


#Запись соообщения в SPLUNK
def writejsontosplunk (jsontext,adr,port_,token_):
    url =adr
    port = port_
    securitytoken = token_
    os.environ['NO_PROXY'] = url
    # authHeader = {'Authorization': 'Splunk {}'.format(securitytoken)}

    authHeader = {'Authorization': 'Splunk {}'.format(securitytoken),'Content-type': 'application/json; profile=urn:splunk:event:1.0; charset=utf-8'}

    #print (authHeader)
    jsonDict = jsontext
    try:
     #print('''https://''' + url + ':' + port + '/services/collector/event''')
     r1 = requests.post('''http://''' + url + ':' + port + '/services/collector/event', headers=authHeader,json=jsonDict, verify=False)
     #print ('''https://''' + url + ':' + port + '/services/collector/event''')
     d = eval(r1.text)
     r = (d["code"])
    except:
        r=777
    return r



#Проверка наличия в названии очереди слова
def is_part_in_list(check_str, words):
    if words in check_str:
            return True
    return False

# подключаемся в IBM MQ, считываем перечень бизнес очередей и по каждой очереди записываем в SPLUNK дату-время самого старого сообщения
def qinfo():
    queue_manager = 'RGSMGR_PROD'
     # channel = 'SYSTEM.DEF.SVRCONN'
    channel = 'RSU.DEF.SVRCONN'
    host = 'esb-prod-mq-01.rgs.ru'
    port = '1414'
    conn_info = '{}({})'.format(host, port)
    user = 'mq_adm'
    password = 'shuTh8ooyohk'
    bytes_encoding = 'utf-8'
    default_ccsid = 819
    qmgr = pymqi.connect(queue_manager, channel, conn_info,user, password,bytes_encoding=bytes_encoding, default_ccsid=default_ccsid)
    pcf = pymqi.PCFExecute(qmgr,response_wait_interval=30000)
    c = 0
    attrs = {
        pymqi.CMQC.MQCA_Q_NAME :'*'
    }
    result = pcf.MQCMD_INQUIRE_Q(attrs)


    dict_queue={}
    for queue_info in result:
        try:

            if int(queue_info[3])>=0:


                qname=str(queue_info[2016].decode(('utf-8').replace(" ",""))).strip()
            #print(str(queue_info[2016].decode(('utf-8').replace(" ","")))+': '+str(queue_info[3]))
                dict_queue.update({qname:int(str(queue_info[3]).strip())})

        except:
            a=1

    keys_bad=[]
    for key in dict_queue:
        if    is_part_in_list   (key,'DEAD')==True \
           or is_part_in_list(key,'SPLUNK')==True or is_part_in_list(key,'SYSTEM')==True \
                or is_part_in_list(key,'PYMQPCF')==True or is_part_in_list(key,'AMQ.MQEXPLORER'): \

           #or is_part_in_list(key,'DIASREINS')==True: # ставим очереди которые хотим исключить например сейчасисключаем все очереди со словом DEAD в названии
            keys_bad.append(key)

    for key in keys_bad:
        del dict_queue[key]
    qmgr.disconnect()
    return  dict_queue
    #return '<b>'+'не удалось считать список очередей IBM MQ.'+'</b>'


dict =qinfo()
for key in qinfo():
    depth = str (dict [key])
    queue_name = key
# Создание MQ объекта
    qmgr = pymqi.connect(queue_manager, channel, f"{host}({port})", user , password)
    queue = pymqi.Queue(qmgr, queue_name,pymqi.CMQC.MQOO_BROWSE)


# Получение самого старого сообщения
    try:
        md = pymqi.MD()
        gmo = pymqi.GMO()
        gmo.Options = pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING | pymqi.CMQC.MQGMO_BROWSE_FIRST
        queue.get(None,md,gmo)


    # Рассчет времени создания сообщения
        put_date = md['PutDate']
        put_time = md ['PutTime']

        time_str = put_time.decode('utf-8')
        time = datetime.datetime.strptime(time_str, '%H%M%S%f').time()
        new_time = (datetime.datetime.combine(datetime.date.today(), time) + datetime.timedelta(hours=3)).time()
        formatted_time_str = new_time.strftime('%H:%M:%S')
        date_str = put_date.decode('utf-8')
        formatted_date_str = '-'.join([date_str[:4], date_str[4:6], date_str[6:]])


        formatted_dt_obj = formatted_date_str+' '+formatted_time_str
        datetime_object = datetime.datetime.strptime(formatted_dt_obj, '%Y-%m-%d %H:%M:%S')
        print ('Очередь',queue_name)
        print ('Дата поступления самого старого сообщения:',formatted_dt_obj)
        delay = str(datetime.datetime.now()-datetime_object)
        #print (type(delay))
        #print (delay)
        msg_splunk = create_splunk_message(host,queue_name,formatted_dt_obj, delay,depth)
        #print (msg_splunk)
        writejsontosplunk(msg_splunk,url_splunk,port_splunk,securitytoken_splunk)
    except:
        datetime_object = '1900-01-01 00:00:01'
        # print ('Очередь:',queue_name)
        # print ('Дата поступления самого старого сообщения:','N/D')
        # print ('Задержка в доставке составляет:','N/D')
        msg_splunk = create_splunk_message(host, queue_name, str(datetime.datetime.now()), '00:00:00',depth)
        writejsontosplunk(msg_splunk, url_splunk, port_splunk, securitytoken_splunk)

    qmgr.disconnect()


