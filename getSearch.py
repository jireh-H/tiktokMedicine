import multiprocessing

import requests
import pymongo
import time
import hashlib
import json
import redis
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

baseurl = 'https://api.ddky.com/cms/rest.htm'


# 破解MD5加密
def get_md5(string):
    if isinstance(string, str):
        string = string.encode('utf-8')
    md5 = hashlib.md5()
    md5.update(string)
    return md5.hexdigest()


# 链接Redis
def connect_redis():
    redis_pool = redis.ConnectionPool(host='192.168.1.150', port=6379, db=1, decode_responses=True)
    # redis_pool = redis.ConnectionPool(host='localhost', port=6379, db=1, decode_responses=True)
    # redis_pool2 = redis.ConnectionPool(host='localhost', port=6379, db=12, decode_responses=True)
    redis_pool2 = redis.ConnectionPool(host='192.168.1.150', port=6379, db=12, decode_responses=True)
    redis_connect = redis.Redis(connection_pool=redis_pool)
    redis_connect2 = redis.Redis(connection_pool=redis_pool2)
    return redis_connect, redis_connect2


# 链接MongoDB
def connect_mongo():
    client = pymongo.MongoClient('192.168.1.150:27017')
    db = client['tiktokmedicine']
    return db


# 获取药品详情的sign
def get_detail_parameter(medicine_id):
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    params = {
        'suite': '2',
        'method': 'ddky.cms.product.detailfps.get',
        't': t,
        'v': '1.0',
        'shopId': '-1',
        'skuId': '%s' % medicine_id
    }
    k = ""
    ss = sorted(params.keys())
    for i in ss:
        k += i + str(params[i])
    k = params["method"] + k + "6C57AB91A1308E26B797F4CD382AC79D"
    sign = get_md5(k).upper()
    params["sign"] = sign
    return params


def get_search_data(keywords, page):
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    data = {
        'v': '1.0',
        'suite': '0',
        't': t,
        'method': 'ddky.cms.app.search.get.by550',
        'pageSize': '20',
        'searchPanel': '1',
        'searchType': 'b2c',
        'pageNo': str(page),
        'orderTypeId': '0',
        'wd': keywords
    }
    k = ""
    ss = sorted(data.keys())
    for i in ss:
        k += i + str(data[i])
    k = data["method"] + k + "6C57AB91A1308E26B797F4CD382AC79D"
    sign = get_md5(k).upper()
    data["sign"] = sign
    return data


def get_search_page_info(non_param):
    con_redis, con_redis2 = connect_redis()
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
    while True:
        x = con_redis.brpop('tiktok_keywords', timeout=10)
        if x is None:
            break
        keywords = x[1]
        data = get_search_data(keywords, 1)
        proxy = con_redis2.randomkey()
        proxies = {"http": f"http{proxy}", "https": f"http{proxy}"}
        try:
            response = requests.get(baseurl, headers=headers, params=data, proxies=proxies, timeout=(10, 7)).json()
            content = response['result']
            products = content['productMap']['o2oTab']
            total_page = products['totalPageNo']
            for page in range(1, total_page + 1):
                add_dic = {
                    'keywords': keywords,
                    'page': page
                }
                con_redis.lpush('tiktok_search_page_info', json.dumps(add_dic, ensure_ascii=False))
                con_redis.lpush('tiktok_search_page_info_copy', json.dumps(add_dic, ensure_ascii=False))
        except Exception as e:
            print(e)
            con_redis.lpush('tiktok_keywords', keywords)


def get_search_list(non_param):
    con_redis, con_redis2 = connect_redis()
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
    while True:
        x = con_redis.brpop('tiktok_search_page_info', timeout=10)
        if x is None:
            break
        search_info = eval(x[1])
        keywords = search_info['keywords']
        page = search_info['page']
        data = get_search_data(keywords, page)
        proxy = con_redis2.randomkey()
        proxies = {"http": f"http{proxy}", "https": f"http{proxy}"}
        try:
            response = requests.get(baseurl, headers=headers, params=data, proxies=proxies, timeout=(10, 7)).json()
            content = response['result']
            products = content['productMap']['o2oTab']
            product_list = products['productList']
            for product_item in product_list:
                medicine_id = product_item['skuId']
                medicine_name = product_item['name']
                add_dic = {
                    'medicine_id': medicine_id,
                    'medicine_name': medicine_name
                }
                con_redis.lpush('tiktok_search_medicine_id', json.dumps(add_dic, ensure_ascii=False))
                con_redis.lpush('tiktok_search_medicine_id_copy', json.dumps(add_dic, ensure_ascii=False))
        except Exception as e:
            print(e)
            con_redis.lpush('tiktok_search_page_info', json.dumps(search_info, ensure_ascii=False))


# 更新id总表
def update_all_id(medicine_id):
    db = connect_mongo()
    record = db['all_id']
    result = record.find_one({'medicine_id': medicine_id})
    if result:
        record.update({'medicine_id': medicine_id}, {'$set': {'crawl_date': time.strftime('%Y-%m-%d')}})
        record.update({'medicine_id': medicine_id}, {'$set': {'crawl_time': time.strftime('%H:%M:%S')}})
    else:
        add_dic = {
            'medicine_id': medicine_id,
            'into_db_time': time.strftime('%H:%M:%S'),
            'into_db_date': time.strftime('%Y-%m-%d'),
            'crawl_time': time.strftime('%H:%M:%S'),
            'crawl_date': time.strftime('%Y-%m-%d')
        }
        record.insert(add_dic)


def get_medicine_detail(non_param):
    con_redis, con_redis2 = connect_redis()
    db = connect_mongo()
    url = baseurl
    headers = {
        'X-Tingyun-Id': 'p35OnrDoP8k;c=2;r=487631737;',
        'Connection': 'close',
        'Charset': 'utf-8',
        'http.agent': 'com.ddsy (Android 10; Pixel Build/QP1A.191005.007.A3)',
        'Accept-Encoding': 'gzip,deflate',
        'screenWidth': '1080',
        'lng': '120.1796',
        'city': '%E6%9D%AD%E5%B7%9E%E5%B8%82',
        'macid': '02%3A00%3A00%3A00%3A00%3A00',
        'screenHeight': '1794',
        'loginToken': '',
        'language': 'zh',
        'imsi': 'NaN',
        'versionName': '5.9.3',
        'platform': 'android10',
        'uid': '',
        'imei0': '',
        'uDate': '',
        'imei': 'AndroidID669e84a123091a56',
        'model': 'Pixel',
        'channelName': 'zhongzhuanye',
        'lat': '30.191399',
        'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 10; Pixel Build/QP1A.191005.007.A3)',
        'Host': 'api.ddky.com'
    }
    while True:
        try:

            date = time.strftime('%Y_%m_%d')
            details = db['medicine_detail_' + date]
            prox = con_redis2.randomkey()
            proxies = {"http": f"http{prox}", "https": f"http{prox}"}
            x = con_redis.brpop('tiktok_search_medicine_id', timeout=10)
            if x is None:
                break
            medicine_info = eval(x[1])
            medicine_id = medicine_info['medicine_id']
            medicine_name = medicine_info['medicine_name']
            result = details.find_one({'medicine_id': medicine_id})
            if result:
                update_all_id(medicine_id)
            else:
                params = get_detail_parameter(medicine_id)
                try:
                    response = json.loads(requests.get(url, headers=headers, params=params, proxies=proxies, timeout=(10, 7)).text)
                    data = response['data']
                    medicine_detail = data['detail']
                    medicine_name = medicine_detail['commonName']
                    title = medicine_detail['name']
                    approval_num = medicine_detail['approvalNumber']
                    img = medicine_detail['imgUrls']
                    brand = medicine_detail['name'].split(']')
                    enterprise = medicine_detail['manufacturers']
                    price = medicine_detail['priceL'] / 100
                    original_price = medicine_detail['originalPriceL'] / 100
                    if_otc = medicine_detail['otcCanBuy']
                    description = medicine_detail['productDescription']
                    specification = medicine_detail['productSpecifications']
                    stock = medicine_detail['productStock']
                    coupons = medicine_detail['productDetailVoucherInfos']
                    dic_coupons = {}
                    if len(coupons) > 0:
                        for coupons_item in coupons:
                            name = coupons_item['name']
                            coupons_content = coupons_item['productListDatailComment']
                            limit_time = coupons_item['limitDateStr']
                            coupons_id = coupons_item['id']
                            coupons_quantity = coupons_item['quantity']
                            add_dic_coupons = {
                                name: {
                                    'coupons_content': coupons_content,
                                    'limit_time': limit_time,
                                    'coupons_id': coupons_id,
                                    'coupons_quantity': coupons_quantity
                                }
                            }
                            dic_coupons.update(add_dic_coupons)
                    else:
                        dic_coupons = {}
                    promote = medicine_detail['promotionsList']
                    sale_num = medicine_detail['saleVolume']
                    dic_promote = {}
                    for promote_item in promote:
                        promote_name = promote_item['promotionsType']
                        promote_des = promote_item['promotionsDesc']
                        promote_buy_count = promote_item['totalBuyCount']
                        promote_buy_money = promote_item['totalBuyMoney']
                        promote_id = promote_item['promotionId']
                        add_dic = {
                            promote_name: {
                                'promote_id': promote_id,
                                'promote_des': promote_des,
                                'promote_buy_count': promote_buy_count,
                                'promote_buy_money': promote_buy_money
                            }
                        }
                        dic_promote.update(add_dic)
                    dic_promote.update(dic_coupons)
                    quantity_limit = medicine_detail['quantityLimit']
                    total_comment = medicine_detail['totalCommentCount']
                    instructions = data['instructionsImg']['instruction']
                    if len(instructions) > 0:
                        instructions = instructions[0]['imgs']
                        img_instruct = []
                        for instructions_item in instructions:
                            instructions_img = instructions_item['imgUrl']
                            img_instruct.append(instructions_img)
                    else:
                        img_instruct = []
                    if len(brand) > 1:
                        brand = brand[0].replace('[', '')
                    else:
                        brand = ''
                    parameters = medicine_detail['parameters']
                    dic_parameters = {}
                    if len(parameters) > 0:
                        for parameters_item in parameters:
                            add_dic_parameters = {
                                parameters_item['text']: parameters_item['value']
                            }
                            dic_parameters.update(add_dic_parameters)
                    else:
                        dic_parameters = {}
                    dic_detail = {
                        'medicine_id': medicine_id,
                        'medicine_name': medicine_name,
                        'title': title,
                        'approval_num': approval_num,
                        'img': img,
                        'enterprise': enterprise,
                        'price': price,
                        'original_price': original_price,
                        'sale_num': sale_num,
                        'description': description,
                        'specification': specification,
                        'if_otc': if_otc,
                        'stock': stock,
                        'quantity_limit': quantity_limit,
                        'total_comment': total_comment,
                        'brand': brand,
                        'promote': promote,
                        'parameters': dic_parameters,
                        'instruction': img_instruct
                    }
                    details.insert(dic_detail)
                    update_all_id(medicine_id)
                except Exception as e:
                    print('-----------------------' + medicine_name + str(medicine_id) + '------------------------')
                    print(e)
                    con_redis.lpush('tiktok_search_medicine_id', json.dumps(medicine_info, ensure_ascii=False))
        except Exception as e:
            print('--------------------------------------------------------------')
            print(e)
            print('--------------------------------------------------------------')


if __name__ == '__main__':

    with ThreadPoolExecutor(5) as t1:
        t1.map(get_search_page_info, [i for i in range(5)])

    with ThreadPoolExecutor(10) as t2:
        t2.map(get_medicine_detail, [i for i in range(10)])

