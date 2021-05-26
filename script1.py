import random

import requests
import json
import pymongo
import hashlib
import time
import redis  # 版本号 2.10.6
from retrying import retry
import ssl

baseurl = 'http://api.ddky.com/cms/rest.htm'


def get_ip():
    url = 'http://api.66daili.cn/API/GetSecretProxy/?orderid=1422048292101310432&num=20&token=66daili&format=text&line_separator=win&protocol=http&region=all'
    response = requests.get(url).text
    response = response.split('\n')
    lis_ip = []
    for i in response[:-1]:
        i = i.replace('\r', '')
        _ip = '://' + i
        lis_ip.append(_ip)
    return lis_ip




# 破解MD5加密
def get_md5(string):
    if isinstance(string, str):
        string = string.encode('utf-8')
    md5 = hashlib.md5()
    md5.update(string)
    return md5.hexdigest()


# 链接MongoDB
def connect_mongo():
    # client = pymongo.MongoClient('192.168.1.150:27017')
    client = pymongo.MongoClient('localhost:27017')
    db = client['tiktokmedicine']
    return db


# 链接Redis
def connect_redis():
    # redis_pool = redis.ConnectionPool(host='192.168.1.150', port=6379, db=1, decode_responses=True)
    # redis_pool2 = redis.ConnectionPool(host='192.168.1.150', port=6379, db=12, decode_responses=True)
    redis_pool = redis.ConnectionPool(host='localhost', port=6379, db=1, decode_responses=True)
    redis_pool2 = redis.ConnectionPool(host='localhost', port=6379, db=12, decode_responses=True)
    redis_connect = redis.Redis(connection_pool=redis_pool)
    redis_connect2 = redis.Redis(connection_pool=redis_pool2)
    return redis_connect, redis_connect2


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


# 获取二级分类的sign
def get_category2_parameter(page, search_id):
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    params = {
        'searchPanel': '2',
        'suite': '2',
        'method': 'ddky.cms.all.search.spells.blend.by574',
        't': t,
        'searchType': 'b2c',
        'pageNo': '%s' % page,
        'v': '1.0',
        'b2cDirectoryId': '%s' % search_id,
        'orderTypeId': '0',
        'pageSize': '20'
    }
    k = ""
    ss = sorted(params.keys())
    for i in ss:
        k += i + str(params[i])
    k = params["method"] + k + "6C57AB91A1308E26B797F4CD382AC79D"
    sign = get_md5(k).upper()
    params["sign"] = sign
    return params


def _result(result):
    return result is None


# 获取分类下全部药品id
@retry(stop_max_attempt_number=10, wait_random_min=10, wait_random_max=20, retry_on_result=_result)
def get_medicine_id(url, page, search_id):
    headers = {
        'versionName': '5.9.3',
        'platform': 'android10',
    }
    con_redis, con_redis2 = connect_redis()
    prox = con_redis2.randomkey()
    proxies = {"http": f"http{prox}", "https": f"http{prox}"}
    params = get_category2_parameter(page, search_id)
    # response = json.loads(requests.get(url, params=params, proxies=proxies, headers=headers).text)
    response = json.loads(requests.get(url, params=params, headers=headers).text)
    result = response['result']
    total_page = result['totalPageNo']
    product_list = result['productList']
    dic_medicine_id = {}
    for product_item in product_list:
        medicine_name = product_item['name']
        medicine_id = product_item['skuId']
        add_dic = {
            medicine_name: medicine_id
        }
        dic_medicine_id.update(add_dic)
    return dic_medicine_id, total_page


# 获取商品详情信息
def get_medicine_detail(url, medicine_id):
    con_redis, con_redis2 = connect_redis()
    insert_time = time.strftime('%H:%M:%S')
    date = time.strftime('%Y-%m-%d')
    headers = {
        'X-Tingyun-Id': 'p35OnrDoP8k;c=2;r=487631737;',
        'Connection': 'keep-alive',
        'Charset': 'utf-8',
        'http.agent': 'com.ddsy (Android 10; Pixel Build/QP1A.191005.007.A3)',
        'Accept-Encoding': 'gzip,deflate',
        'screenWidth': '1080',
        'lng': '120.1796',
        'city': '%E6%9D%AD%E5%B7%9E%E5%B8%82',  # 杭州
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
        'channelName': 'Xiaomi',
        'lat': '30.191399',
        'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16',
        'Host': 'api.ddky.com'
    }
    params = get_detail_parameter(medicine_id)
    # prox = con_redis2.randomkey()
    # lis_ip = get_ip()
    # prox = random.choice(lis_ip)
    # prox = '://114.217.51.81:11098'
    # proxies = {"http": f"http{prox}", "https": f"http{prox}"}

    # response = json.loads(requests.get(url, headers=headers, params=params, proxies=proxies, timeout=(10, 7)).text)

    response = requests.get(url, headers=headers, params=params, timeout=(10, 7))
    print(response.text)

    # res = requests.get(url, params=params)
    # print(res.text)
    # data = response['data']
    # medicine_detail = data['detail']
    # medicine_name = medicine_detail['commonName']
    # title = medicine_detail['name']
    # approval_num = medicine_detail['approvalNumber']
    # img = medicine_detail['imgUrls']
    # brand = medicine_detail['name'].split(']')
    # enterprise = medicine_detail['manufacturers']
    # price = medicine_detail['priceL'] / 100
    # original_price = medicine_detail['originalPriceL'] / 100
    # if_otc = medicine_detail['otcCanBuy']
    # description = medicine_detail['productDescription']
    # specification = medicine_detail['productSpecifications']
    # stock = medicine_detail['productStock']
    # coupons = medicine_detail['productDetailVoucherInfos']
    # dic_coupons = {}
    # if len(coupons) > 0:
    #     for coupons_item in coupons:
    #         name = coupons_item['name']
    #         coupons_content = coupons_item['productListDatailComment']
    #         limit_time = coupons_item['limitDateStr']
    #         coupons_id = coupons_item['id']
    #         coupons_quantity = coupons_item['quantity']
    #         add_dic_coupons = {
    #             name: {
    #                 'coupons_content': coupons_content,
    #                 'limit_time': limit_time,
    #                 'coupons_id': coupons_id,
    #                 'coupons_quantity': coupons_quantity
    #             }
    #         }
    #         dic_coupons.update(add_dic_coupons)
    # else:
    #     dic_coupons = {}
    # promote = medicine_detail['promotionsList']
    # sale_num = medicine_detail['saleVolume']
    # dic_promote = {}
    # for promote_item in promote:
    #     promote_name = promote_item['promotionsType']
    #     promote_des = promote_item['promotionsDesc']
    #     promote_buy_count = promote_item['totalBuyCount']
    #     promote_buy_money = promote_item['totalBuyMoney']
    #     promote_id = promote_item['promotionId']
    #     add_dic = {
    #         promote_name: {
    #             'promote_id': promote_id,
    #             'promote_des': promote_des,
    #             'promote_buy_count': promote_buy_count,
    #             'promote_buy_money': promote_buy_money
    #         }
    #     }
    #     dic_promote.update(add_dic)
    # dic_promote.update(dic_coupons)
    # quantity_limit = medicine_detail['quantityLimit']
    # total_comment = medicine_detail['totalCommentCount']
    # instructions = data['instructionsImg']['instruction']
    # if len(instructions) > 0:
    #     instructions = instructions[0]['imgs']
    #     img_instruct = []
    #     for instructions_item in instructions:
    #         instructions_img = instructions_item['imgUrl']
    #         img_instruct.append(instructions_img)
    # else:
    #     img_instruct = []
    # if len(brand) > 1:
    #     brand = brand[0].replace('[', '')
    # else:
    #     brand = ''
    # parameters = medicine_detail['parameters']
    # dic_parameters = {}
    # if len(parameters) > 0:
    #     for parameters_item in parameters:
    #         add_dic_parameters = {
    #             parameters_item['text']: parameters_item['value']
    #         }
    #         dic_parameters.update(add_dic_parameters)
    # else:
    #     dic_parameters = {}
    # dic_detail = {
    #     'medicine_id': medicine_id,
    #     'medicine_name': medicine_name,
    #     'title': title,
    #     'approval_num': approval_num,
    #     'img': img,
    #     'enterprise': enterprise,
    #     'price': price,
    #     'original_price': original_price,
    #     'sale_num': sale_num,
    #     'description': description,
    #     'specification': specification,
    #     'if_otc': if_otc,
    #     'stock': stock,
    #     'quantity_limit': quantity_limit,
    #     'total_comment': total_comment,
    #     'brand': brand,
    #     'promote': promote,
    #     'parameters': dic_parameters,
    #     'instruction': img_instruct,
    #     'time': insert_time,
    #     'date': date
    # }
    # return dic_detail


if __name__ == '__main__':
    date2 = time.strftime('%Y_%m_%d')
    db = connect_mongo()
    details = db['medicine_detail_' + date2]
    # details = db['medicine_detail_2021_05_01']
    conn_redis, conn_redis2 = connect_redis()
    record = db['all_id']

    a = get_medicine_detail(baseurl, 15049501)


