# 叮当快药   app
# 1  没有总销,
# 2  可以拿到准确月销
# 2.1  处方药拿不到月销
# 3  参数可能在图片上也可能没有
# 4  封ip
# 5  可批量爬取


import requests, time, hashlib

def GetMd5(String):
    """
    :param String: 需要计算MD5的字符串
    :return: MD5值
    """
    if isinstance(String, str):
        String = String.encode("utf-8")
    md = hashlib.md5()
    md.update(String)
    return md.hexdigest()


def sousuo():
    url = "https://api.ddky.com/cms/rest.htm"

    headers = {
        'X-Tingyun-Id': 'p35OnrDoP8k;c=2;r=2063229955;',
        'Connection': 'close',
        'Charset': 'utf-8',
        'http.agent': 'com.ddsy (Android 6.0; Le X620 Build/HEXCNFN6003009092S)',
        'Accept-Encoding': 'gzip,deflate',
        'screenWidth': '1080',
        'city': '%E6%9D%AD%E5%B7%9E%E5%B8%82',
        'channelName': 'xiaomi',
        'lng': '120.179563',
        'uDate': '',
        'versionName': '5.6.0',
        'platform': 'android6.0',
        'imei': 'AndroidIDfe9eb22c2ccdf954',
        'loginToken': '',
        'screenHeight': '1920',
        'macid': '02%3A00%3A00%3A00%3A00%3A00',
        'uid': '',
        'imei0': '862380035985053',
        'language': 'zh',
        'lat': '30.191414',
        'imsi': 'NaN',
        'model': 'Le+X620',
        'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 6.0; Le X620 Build/HEXCNFN6003009092S)',
        'Host': 'api.ddky.com'
    }

    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    data = {
        'v': '1.0',
        'suite': '0',
        't': t,
        'method': 'ddky.cms.app.search.get.by550',
        'pageSize': '20',
        'searchPanel': '1',
        'searchType': 'b2c',
        'pageNo': '1',
        'orderTypeId': '0',
        'wd': '999小儿清热止咳'
    }

    k = ""
    ss = sorted(data.keys())
    for i in ss:
        k += i + str(data[i])

    k = data["method"] + k + "6C57AB91A1308E26B797F4CD382AC79D"
    sign = GetMd5(k).upper()
    data["sign"] = sign

    s = requests.get(url, headers=headers, params=data)
    print(s.text)


def xiangqing():
    url = "https://api.ddky.com/cms/rest.htm"

    headers = {
        'X-Tingyun-Id': 'p35OnrDoP8k;c=2;r=2063229955;',
        'Connection': 'close',
        'Charset': 'utf-8',
        'http.agent': 'com.ddsy (Android 6.0; Le X620 Build/HEXCNFN6003009092S)',
        'Accept-Encoding': 'gzip,deflate',
        'screenWidth': '1080',
        'city': '%E6%9D%AD%E5%B7%9E%E5%B8%82',
        'channelName': 'xiaomi',
        'lng': '120.179563',
        'uDate': '',
        'versionName': '5.6.0',
        'platform': 'android6.0',
        'imei': 'AndroidIDfe9eb22c2ccdf954',
        'loginToken': '',
        'screenHeight': '1920',
        'macid': '02%3A00%3A00%3A00%3A00%3A00',
        'uid': '',
        'imei0': '862380035985053',
        'language': 'zh',
        'lat': '30.191414',
        'imsi': 'NaN',
        'model': 'Le+X620',
        'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 6.0; Le X620 Build/HEXCNFN6003009092S)',
        'Host': 'api.ddky.com'
    }
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    data = {
        'v': '1.0',
        'skuId': '50005443',
        'suite': '2',
        'shopId': '-1',
        't': t,
        'method': 'ddky.cms.product.detailfps.get'
    }
    k = ""
    ss = sorted(data.keys())
    for i in ss:
        k += i + str(data[i])
    k = data["method"] + k + "6C57AB91A1308E26B797F4CD382AC79D"
    sign = GetMd5(k).upper()
    data["sign"] = sign
    s = requests.get(url, headers=headers, params=data)
    print(s.text)


for i in range(100):
    xiangqing()
    time.sleep(1)

# for i in range(50):
#     s = requests.get(url, headers=headers)
#     print(s.text)
#     if "12.50" in s.text:
#         print(1)
#     else:
#         print(2)