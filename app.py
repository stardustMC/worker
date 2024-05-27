import threading

import requests
import time
import settings
import redis
import random
import datetime
import json
import pymysql
from pymysql.cursors import DictCursor
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import parse_qs, urlparse, urlencode
import ctypes
import binascii

# Python调用JavaScript代码
#   - 安装nodejs + 配置环境变量
#   - 安装pyexecjs模块  pip install pyexecjs
import execjs
# pip install pycryptodome
from Crypto.Cipher import AES

ERROR_COUNT = 0
ERROR_LOCK = threading.RLock()

javascript_file = execjs.compile("""
function createGUID(e) {
    e = e || 32;
    for (var t = "", r = 1; r <= e; r++) {
        t += Math.floor(16 * Math.random()).toString(16);
    }
    return t;
}
""")


class Connect(object):
    def __init__(self):
        self.conn = conn = pymysql.connect(**settings.MYSQL_CONN_PARAMS)
        self.cursor = conn.cursor(pymysql.cursors.DictCursor)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()

    def exec(self, sql, **kwargs):
        self.cursor.execute(sql, kwargs)
        self.conn.commit()

    def fetch_one(self, sql, **kwargs):
        self.cursor.execute(sql, kwargs)
        result = self.cursor.fetchone()
        return result

    def fetch_all(self, sql, **kwargs):
        self.cursor.execute(sql, kwargs)
        result = self.cursor.fetchall()
        return result


class DbRow(object):
    def __init__(self, id, oid, status, url, count):
        self.id = id
        self.oid = oid
        self.status = status
        self.url = url
        self.count = count


def get_redis_task():
    # 直接连接redis
    conn = redis.Redis(**settings.REDIS_PARAMS)
    oid = conn.brpop(settings.QUEUE_TASK_NAME, timeout=5)
    if not oid:
        return
    # # (b'CRC_TASK_QUEUE', b'2022081409424192778452289117')
    return oid[1].decode('utf-8')


def get_order_info_by_id(oid):
    with Connect() as conn:
        row_dict = conn.fetch_one(
            "select id,oid,status,url,count from web_order where oid=%(oid)s and status=1",
            oid=oid
        )
    if not row_dict:
        return
    row_object = DbRow(**row_dict)
    return row_object


def update_order_status(oid, status):
    with Connect() as conn:
        conn.exec("update web_order set status=%(status)s where oid=%(oid)s", status=status, oid=oid)


def create_qa(data_string):
    """
    string = "|d000035rirv|1622526980|mg3c3b04ba|1.3.2|df553a055bb06eda3653173ee5a010bf|4330701|https://w.yangshipin.cn/|mozilla/5.0 (macintosh; ||Mozilla|Netscape|MacIntel|"
    原算法
        Aa = "|d000035rirv|1622526980|mg3c3b04ba|1.3.2|df553a055bb06eda3653173ee5a010bf|4330701|https://w.yangshipin.cn/|mozilla/5.0 (macintosh; ||Mozilla|Netscape|MacIntel|"
        wl = -5516
        $a=0
        for (Se = 0; Se < Aa[St]; Se++)
                Ma = Aa[bt](Se), Ae["charCodeAt"]()
                $a = ($a << wl + 1360 + 9081 - 4920) - $a + Ma,
                $a &= $a;
            qa = $a
    """

    a = 0
    for i in data_string:
        _char = ord(i)
        a = (a << 5) - a + _char
        a &= a & 0xffffffff
    return ctypes.c_int32(a).value


def aes_encrypt(text):
    """
    AES加密
    """
    # "4E2918885FD98109869D14E0231A0BF4"
    # "16B17E519DDD0CE5B79D7A63A4DD801C"

    key = binascii.a2b_hex('4E2918885FD98109869D14E0231A0BF4')
    iv = binascii.a2b_hex('16B17E519DDD0CE5B79D7A63A4DD801C')
    pad = 16 - len(text) % 16
    text = text + pad * chr(pad)
    text = text.encode()
    cipher = AES.new(key, AES.MODE_CBC, iv)
    encrypt_bytes = cipher.encrypt(text)
    return binascii.b2a_hex(encrypt_bytes).decode()


def create_wt():
    """
    h5_plugins.js文件
    for (Wt = "",
        Kt = xc + yc + -7598 + 4607,
        zt = cs + "৮঺৪঺৫হঽ৫২"; Kt < zt.length; Kt++)
            Wt += String["f" + ls + "de"](-1746 + Hc + 14157 ^ zt[ps + us + "CodeAt"](Kt));
    """
    return "mg3c3b04ba"


def create_ckey(vid, tt, version, platform, guid):
    wt = create_wt()
    ending = "https://w.yangshipin.cn/|mozilla/5.0 (macintosh; ||Mozilla|Netscape|MacIntel|"

    data_list = ["", vid, tt, wt, version, guid, platform, ending]
    string = "|".join(data_list)
    qa = create_qa(string)
    encrypt_string = "|{}{}".format(qa, string)
    ckey = "--01" + aes_encrypt(encrypt_string).upper()
    return ckey


def fetch_vkey(session, vid, rnd, app_ver, platform, flow_id, guid, ckey):
    params = {
        "callback": "txplayerJsonpCallBack_getinfo_711482",
        "charge": "0",
        "defaultfmt": "auto",
        "otype": "json",
        "guid": guid,
        "flowid": flow_id,
        "platform": platform,
        "sdtfrom": "v7007",
        "defnpayver": "0",
        "appVer": app_ver,
        "host": "w.yangshipin.cn",
        "ehost": "https://w.yangshipin.cn/video",
        "refer": "w.yangshipin.cn",
        "sphttps": "1",
        "_rnd": rnd,  # _rnd: x.getTimeStampStr(),
        "spwm": "4",
        "vid": vid,
        "defn": "auto",
        "show1080p": "false",
        "dtype": "1",
        "clip": "4",
        "fmt": "auto",
        "defnsrc": "",
        "fhdswitch": "",
        "defsrc": "1",
        "sphls": "",
        "encryptVer": "8.1",
        "cKey": ckey,
    }

    headers = {
        'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, '
                      'like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1',
        'referer': 'https://m.yangshipin.cn/',
    }
    url = "https://playvv.yangshipin.cn/playvinfo"

    res = session.get(url=url, params=params, headers=headers)
    text = res.text.strip("txplayerJsonpCallBack_getinfo_711482")[1:-1]
    res_dict = json.loads(text)
    return res_dict


def txplayerJsonpCallBack_getinfo_711482(session, response, video_url, vid, guid, pid):
    download_params = {
        "sdtfrom": "v7007",
        "guid": guid,
        "vkey": response["vl"]['vi'][0]['fvkey'],
        "platform": "2",
    }
    # 视频下载连接视频 # FlOO10002
    download_url = "https://mp4playcloud-cdn.ysp.cctv.cn/{}.iHMg10002.mp4?{}".format(vid, urlencode(download_params))

    # 播放视频
    params = {
        "BossId": 2865,
        "Pwd": 1698957057,
        "_dc": random.random()  # "&_dc=".concat(Math.random()))
    }
    data = {
        "uin": "",
        "vid": vid,
        "coverid": "",
        "pid": pid,
        "guid": guid,
        "unid": "",
        "vt": "0",
        "type": "3",
        # "url": "https://w.yangshipin.cn/video?type=0&vid=d000035rirv",
        "url": video_url,
        "bi": "0",
        "bt": "0",
        "version": "1.3.2",
        "platform": "4330701",
        "defn": "0",
        # "ctime": "2021-06-02 09:30:01",
        "ctime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ptag": "",
        "isvip": "-1",
        "tpid": "13",
        "pversion": "h5",
        "hc_uin": "",
        "hc_vuserid": "",
        "hc_openid": "",
        "hc_appid": "",
        "hc_pvid": "0",
        "hc_ssid": "",
        "hc_qq": "",
        "hh_ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML  like Gecko) Chrome/90.0.4430.212 Safari/537.36",
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML  like Gecko) Chrome/90.0.4430.212 Safari/537.36",
        "ckey": "",
        "iformat": "0",
        "hh_ref": video_url,
        "vuid": "",
        "vsession": "",
        "format_ua": "other",
        "common_rcd_info": "",
        "common_ext_info": "",
        "v_idx": "0",
        "rcd_info": "",
        "extrainfo": "",
        "c_channel": "",
        "vurl": download_url,
        "step": "6",
        "val": "164",
        "val1": "1",
        "val2": "1",
        "idx": "0",
        "c_info": "",
        "isfocustab": "0",
        "isvisible": "0",
        "fact1": "",
        "fact2": "",
        "fact3": "",
        "fact4": "",
        "fact5": "",
        "cpay": "0",
        "tpay": "0",
        "dltype": "1"
    }
    url = "https://btrace.yangshipin.cn/kvcollect"
    session.post(
        url=url,
        params=params,
        data=data,
        headers={
            'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, '
                          'like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1',
            'referer': 'https://m.yangshipin.cn/',
        }
    )


def task(video_url):
    """ 爬虫 + 逆向 """
    # video_url = "https://w.yangshipin.cn/video?type=0&vid=f0000711h22"
    for i in range(5):
        try:
            session = requests.Session()
            platform = "4330701"
            app_ver = "1.3.2"
            rnd = str(int(time.time()))
            vid = parse_qs(urlparse(video_url).query)['vid'][0]
            guid = javascript_file.call('createGUID')
            pid = javascript_file.call('createGUID')
            flow_id = "{}_{}".format(pid, platform)

            ckey = create_ckey(vid, rnd, app_ver, platform, guid)
            vkey_info = fetch_vkey(session, vid, rnd, app_ver, platform, flow_id, guid, ckey)
            txplayerJsonpCallBack_getinfo_711482(session, vkey_info, video_url, vid, guid, pid)
            session.close()
            return
        except Exception as e:
            pass

    with ERROR_LOCK:
        global ERROR_COUNT
        ERROR_COUNT += 1


def run():
    while True:
        # 1.去redis的队列中获取待执行的订单号
        oid = get_redis_task()
        if not oid:
            time.sleep(3)
            continue

        # 2.连接数据库获取订单信息
        order_object = get_order_info_by_id(oid)
        if not order_object:
            continue

        # 3.更新订单状态-正在执行
        update_order_status(oid, 2)

        # 4.执行订单-线程池 20
        pool = ThreadPoolExecutor(20)
        print(order_object.url)
        for i in range(order_object.count):  # 100
            pool.submit(task, order_object.url)
        pool.shutdown()  # 等待20线程把100个人任务执行完成

        # 5.如果有错误，就继续执行
        global ERROR_COUNT
        while ERROR_COUNT:
            print("有错误：", ERROR_COUNT)
            run_count = ERROR_COUNT
            ERROR_COUNT = 0
            pool = ThreadPoolExecutor(20)
            for i in range(run_count):
                pool.submit(task, order_object.url)
            pool.shutdown()  # 等待20线程把100个人任务执行完成

        # 6.更新订单状态-已完成
        update_order_status(oid, 3)

        print("执行完毕：", oid)


if __name__ == '__main__':
    run()
