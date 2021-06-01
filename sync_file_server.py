import asyncio
import pyinotify
import aiofiles
import sqlite3
import time
import datetime
import os
from sys import argv
import signal
from yaml import load
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

db = None
counter = 1
timer = 0
queue_list = [] #队列, 用以通知client True: notify触发, False: keepalive
client_name = {} #字典, 让client name不重复, client_name[name] = (timer,counter)
default_name = 'root@@123_'

def create_db():
    global db

    cursor = db.cursor()

    # 数据传递到操作系统层后不同步
    cursor.execute('PRAGMA synchronous = OFF;')
    # 关闭日志，因此不进行回滚或原子提交
    cursor.execute('PRAGMA journal_mode = OFF;')

    sql = 'CREATE TABLE IF NOT EXISTS FILE_INFO (FILE_NAME TEXT NOT NULL, FILE_TIME INTEGER NOT NULL, COUNTER INTEGER NOT NULL UNIQUE, PRIMARY KEY(FILE_NAME));'
    print(sql)
    cursor.execute(sql)
    sql = 'CREATE INDEX IF NOT EXISTS COUNTER_INDEX ON FILE_INFO (COUNTER);'
    print(sql)
    cursor.execute(sql)
    sql = 'CREATE INDEX IF NOT EXISTS FILE_TIME_INDEX ON FILE_INFO (FILE_TIME);'
    print(sql)
    cursor.execute(sql)
    sql = 'CREATE TABLE IF NOT EXISTS USER (NAME TEXT NOT NULL, FILE_TIME INTEGER, COUNTER INTEGER, PRIMARY KEY(NAME));'
    print(sql)
    cursor.execute(sql)
    db.commit()
    cursor.close()
    pass

def update_db_file(filename,file_time,counter):
    global db
    rowcount = 0
    cursor = db.cursor()

    sql_insert = 'INSERT INTO FILE_INFO (FILE_NAME,FILE_TIME,COUNTER) VALUES("{filename}",{file_time},{counter});'.format(filename=filename,file_time=file_time,counter=counter)
    sql_update = 'UPDATE FILE_INFO set FILE_TIME={file_time},COUNTER={counter} where FILE_NAME="{filename}";'.format(filename=filename,file_time=file_time,counter=counter)
    #print('update_db_file','sql_update',sql_update)
    #print('update_db_file','sql_insert',sql_insert)
    try:
        cursor.execute(sql_update)
        db.commit()
        rowcount = cursor.rowcount
    except Exception as e:
        #print(e)
        rowcount=0
        pass

    if rowcount==1:
        cursor.close()
        return cursor
    
    try:
        cursor.execute(sql_insert)
        db.commit()
        rowcount = cursor.rowcount
    except Exception as e:
        #print(e)
        rowcount=0
        pass
    
    cursor.close()
    return cursor

def select_db_file_next(counter):
    global db
    cursor = db.cursor()

    sql = 'SELECT FILE_NAME, FILE_TIME, COUNTER FROM FILE_INFO WHERE COUNTER > {counter} ORDER BY COUNTER LIMIT 1;'.format(counter=counter)
    #print('select_db_next',sql)
    cursor.execute(sql)
    column = cursor.fetchone()
    cursor.close()
    return column
    pass

def delete_db_file_timeout(timer):
    global db
    cursor = db.cursor()

    sql = 'DELETE FROM FILE_INFO WHERE FILE_TIME < {timer};'.format(timer=timer)
    #print('delete_db_timeout',sql)
    cursor.execute(sql)
    db.commit()
    cursor.close()
    pass

def select_db_name(name):
    global db
    cursor = db.cursor()

    sql = 'SELECT COUNTER,FILE_TIME FROM USER WHERE NAME = "{name}";'.format(name=name)
    cursor.execute(sql)
    column = cursor.fetchone()
    cursor.close()
    if column == None:
        return 0,0
    return column

def update_db_name_counter(name,file_time,counter):
    global db
    rowcount = 0
    cursor = db.cursor()

    sql_insert = 'INSERT INTO USER (NAME,FILE_TIME,COUNTER) VALUES("{name}",{file_time},{counter});'.format(name=name,file_time=file_time,counter=counter)
    sql_update = 'UPDATE USER set FILE_TIME={file_time}, COUNTER={counter} where NAME="{name}";'.format(name=name,file_time=file_time,counter=counter)
    #print('update_db_name_counter','sql_update',sql_update)
    #print('update_db_name_counter','sql_insert',sql_insert)
    try:
        cursor.execute(sql_update)
        db.commit()
        rowcount = cursor.rowcount
    except Exception as e:
        #print(e)
        rowcount=0
        pass

    if rowcount==1:
        cursor.close()
        return rowcount

    try:
        cursor.execute(sql_insert)
        db.commit()
        rowcount = cursor.rowcount
    except Exception as e:
        #print(e)
        rowcount=0
        pass

    cursor.close()
    return rowcount
    pass

class MyEventHandler(pyinotify.ProcessEvent):
    def process_IN_MODIFY(self,event):
        #print('MODIFY',event.pathname)
        if not os.path.isfile(event.pathname):
            return
        
        global counter,timer
        file_name = event.pathname
        timer = round(os.stat(event.pathname).st_mtime * 1000000000)
        update_db_file(file_name,timer,counter)
        counter+=1
        client_name[default_name] = (timer,counter)
        try:
            for queue in queue_list:
                queue.put_nowait(True)
        except Exception as e:
            pass

async def client_connected_cb(reader,writer):
    global client_name,queue_list
    name = '' # 客户端名字
    queue = asyncio.Queue(maxsize = 2)
    file_name = ''
    file_time = 0
    file_size = 0
    counter = 0
    flag = True

    try:
        name = await reader.readline()
        name = name.decode().strip()
        print('name {name} start sync'.format(name=name))
    except Exception as e:
        writer.close()
        return

    if name in client_name:
        await asyncio.sleep(5)
        writer.close()
        return
    
    # 通过客户端名字找到数据库中的记录
    counter,file_time = select_db_name(name)
    client_name[name] = (file_time,counter)
    queue_list.append(queue)

    try:
        while True:
            while True:
                column = select_db_file_next(counter)
                # 数据库中无更新的数据, 则等待队列
                if column == None:
                    break
    
                file_name,file_time,counter = column
                #print('name {name}, file_name {file_name}, file_time {file_time}'.format(name=name,file_name=file_name,file_time=file_time))
                file_size = os.stat(file_name).st_size
                data = '{0}\r\n{1}\r\n{2}\r\n'.format(file_name,file_time,file_size).encode()
                if file_size>0:
                    '''
                    async with aiofiles.open(file_name,'rb') as fd:
                        data += await fd.read()
                    '''
                    with open(file_name,'rb') as fd:
                        data += fd.read(file_size)
                data += b'\r\n'
                writer.write(data)
                await writer.drain()
                await reader.readline()
                await reader.readline()

                #update_db_name_counter(name,file_time,counter)
                client_name[name] = (file_time,counter)

            while True:
                # 队列返回True, 说明是inotify
                # 队列返回False, 说明是keepalive
                flag = await queue.get()
                if flag:
                    break
                # keepalive
                writer.write(b'\r\n0\r\n0\r\n\r\n')
                await writer.drain()

                await reader.readline()
                await reader.readline()

    except Exception as e:
        print('client_connected_cb',e)
        pass

    writer.close()
    update_db_name_counter(name,file_time,counter)
    del client_name[name]
    queue_list.remove(queue)
    print('name {name} finish sync'.format(name=name))
    pass

async def server_handler(port):
    server = await asyncio.start_server(client_connected_cb,'0.0.0.0',port)
    await server.serve_forever()

async def keepalive(seconds):
    # 定时做心跳检测
    while True:
        await asyncio.sleep(seconds)
        try:
            for queue in queue_list:
                queue.put_nowait(False)
        except Exception as e:
            pass

async def clean_db_timeout():
    # 定时清除数据库中过期数据
    # 默认是凌晨2点执行
    # 清除1天前的数据
    while True:
        t = time.time()
        t_datetime = datetime.datetime.fromtimestamp(t)
        t2 = t + 86400
        t2_datetime = datetime.datetime.fromtimestamp(t2)
        t3_datetime = datetime.datetime(year=t2_datetime.year,month=t2_datetime.month,day=t2_datetime.day,hour=2)
        diff_datetime = t3_datetime-t_datetime
        diff_second = diff_datetime.total_seconds()
        #等到凌晨2点执行
        await asyncio.sleep(diff_second)
        now = time.time_ns()
        #删除1天前的db记录
        now -= 86400000000000
        delete_db_file_timeout(now)

    pass

async def db_dump():
    # 定时持久化
    global db,client_name
    while True:
        await asyncio.sleep(1)
        for name in client_name:
            timer,counter = client_name[name]
            update_db_name_counter(name,timer,counter)

def signal_handler(signum, frame):
    global db,client_name
    for name in client_name:
        timer,counter = client_name[name]
        update_db_name_counter(name,timer,counter)
    db.close()
    exit(0)

if __name__ == '__main__':
    if len(argv) < 2:
        print("python3 sync_file_server.py conf.yaml")
        exit(0)

    conf = argv[1]
    conf_data = {}
    with open(conf,'r',encoding='UTF-8') as conf_fd:
        conf_data = load(conf_fd.read(),Loader=Loader)

    db = conf_data['db']
    port = conf_data['port']
    notify_path = conf_data['notify_path']
    second = conf_data['keep_alive']

    print('start server port{port}, watch path{notify_path}, db{db}, keep_alive{second}'.
    format(port=port,notify_path=notify_path,db=db,second=second))

    db = sqlite3.connect(db)

    create_db()

    #获取数据库default_name的counter,timer
    counter,timer = select_db_name(default_name)
    if counter==0:
        counter += 1
    client_name[default_name] = (timer,counter)
    print('default_name={default_name}, counter={counter}, timer={timer}'.format(default_name=default_name,counter=counter,timer=timer))

    signal.signal(signal.SIGTERM,signal_handler)
    signal.signal(signal.SIGINT,signal_handler)

    #判断是否有新文件未记录到数据库
    timer_tmp = timer
    for file in os.listdir(notify_path):
        file = notify_path + '/' + file
        if not os.path.isfile(file):
            continue
        file_time = round(os.stat(file).st_mtime * 1000000000)
        if file_time > timer:#大于, 极端情况下会出现两个文件有相同时间
            update_db_file(file,file_time,counter)
            counter += 1
            timer_tmp = max(timer_tmp,file_time)
    timer = timer_tmp
    client_name[default_name] = (timer,counter)

    # 获取loop
    loop = asyncio.get_event_loop()

    # 注册inotify事件到loop
    wm = pyinotify.WatchManager()
    notifier = pyinotify.AsyncioNotifier(wm, loop, default_proc_fun=MyEventHandler())
    wm.add_watch(notify_path, pyinotify.IN_MODIFY)

    # 注册server, 定时任务到loop
    tasks = [
        server_handler(port),
        keepalive(second),
        clean_db_timeout(),
        db_dump(),
    ]

    loop.run_until_complete(asyncio.wait(tasks))