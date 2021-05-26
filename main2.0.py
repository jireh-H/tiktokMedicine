import requests
import json
import pymongo
import hashlib
import time
import redis  # 版本号 2.10.6
from concurrent.futures import ThreadPoolExecutor
import traceback

baseurl = 'https://api.ddky.com/cms/rest.htm'
date = time.strftime('%Y_%m_%d')

# 链接MongoDB
client = pymongo.MongoClient('192.168.1.150:27017')
# client = pymongo.MongoClient('localhost:27017')
db = client['tiktokmedicine']
client_local = pymongo.MongoClient('localhost:27017')
db_local = client_local['tiktokmedicine']

# 链接Redis
redis_pool = redis.ConnectionPool(host='192.168.1.150', port=6379, db=1, decode_responses=True)
redis_pool2 = redis.ConnectionPool(host='192.168.1.150', port=6379, db=12, decode_responses=True)
# redis_pool = redis.ConnectionPool(host='localhost', port=6379, db=1, decode_responses=True)
# redis_pool2 = redis.ConnectionPool(host='localhost', port=6379, db=12, decode_responses=True)
con_redis = redis.Redis(connection_pool=redis_pool)
con_redis2 = redis.Redis(connection_pool=redis_pool2)


# 破解MD5加密
def get_md5(string):
    if isinstance(string, str):
        string = string.encode('utf-8')
    md5 = hashlib.md5()
    md5.update(string)
    return md5.hexdigest()


# 获取一级分类的sign
def get_category1_parameter(category_id):
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    params = {
        'method': 'ddky.cms.illnessByMedicinal.get',
        't': t,
        'v': '1.0',
        'id': '%s' % category_id
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


# 获取一级分类id
def get_category_id(url, category_id):
    params = get_category1_parameter(category_id)
    try:
        response = requests.get(url, params=params)
        response_json = json.loads(response.text)
        result = response_json.get('result', '')
        category = result.get('categories', '')
        lis_category = []
        for category_item in category:
            category_id = category_item.get('id', '')
            category_name = category_item.get('name', '')
            dic_add = {
                'category_id': category_id,
                'category_name': category_name,
            }
            lis_category.append(dic_add)
        return lis_category
    except Exception as e:
        print(e)
        get_category_id(url, category_id)


# 获取二级分类参数
def get_category2_id(url, category_id):
    try:
        params = get_category1_parameter(category_id)
        response = requests.get(url, params=params)
        response_json = json.loads(response.text)
        result = response_json.get('result', '')
        key_word = result.get('keyWords', '')[0]
        search_id = key_word.get('searchIds', '')
        return search_id
    except Exception as e:
        print(e)
        get_category2_id(url, category_id)


# 获取分类页码
def get_category_info(non_param):
    headers = {
        'versionName': '5.9.3',
        'platform': 'android10',
        'city': '%E4%B8%8A%E6%B5%B7%E5%B8%82'
    }
    while True:
        # 随机取一个ip代理
        prox = con_redis2.randomkey()
        proxies = {"http": f"http{prox}", "https": f"http{prox}"}
        # 从任务队列中取一条数据
        x = con_redis.brpop('tiktok_medicine_category_id', timeout=10)
        # 当任务队列中为空后 终止循环
        if x is None:
            break
        category_info = eval(x[1])
        category_id = category_info['category_id']
        category_name = category_info['category_name']
        search_id = category_info['search_id']
        params = get_category2_parameter(1, search_id)
        try:
            # 获取每个分类的总页数
            response = requests.get(baseurl, params=params, proxies=proxies, headers=headers, timeout=(10, 7))
            response_json = json.loads(response.text)
            result = response_json.get('result', '')
            total_page = result.get('totalPageNo', '')
            for page in range(1, total_page + 1):
                add_dic = {
                    'category_name': category_name,
                    'category_id': category_id,
                    'page': page,
                    'search_id': search_id
                }
                # 将每个分类的全部页插入任务队列中
                con_redis.lpush('tiktok_medicine_page_info', json.dumps(add_dic, ensure_ascii=False))
                con_redis.lpush('tiktok_medicine_page_info_copy', json.dumps(add_dic, ensure_ascii=False))
        except Exception as e:
            # 当发生报错将该任务插回任务队列尾
            con_redis.lpush('tiktok_medicine_category_id', json.dumps(category_info, ensure_ascii=False))
            with open(date + '_category_error.txt', 'a') as f:
                f.write(str(x[1]) + '\n' + traceback.format_exc() + '\n')


# 更新id总表
def update_all_id(medicine_id):
    record = db['all_id']
    # 查看当前药品id是否已经在总表中
    result = record.find_one({'medicine_id': medicine_id})
    # 如果已经在总表中就更改当前药品的最后爬取时间
    if result:
        record.update({'medicine_id': medicine_id}, {'$set': {'crawl_date': time.strftime('%Y-%m-%d')}})
        record.update({'medicine_id': medicine_id}, {'$set': {'crawl_time': time.strftime('%H:%M:%S')}})
    # 不在就插入当前药品id以及入库时间、最后爬取时间
    else:
        add_dic = {
            'medicine_id': medicine_id,
            'into_db_time': time.strftime('%H:%M:%S'),
            'into_db_date': time.strftime('%Y-%m-%d'),
            'crawl_time': time.strftime('%H:%M:%S'),
            'crawl_date': time.strftime('%Y-%m-%d')
        }
        record.insert(add_dic)


# 获取全部药品id
def get_medicine_id(non_param):
    headers = {
        'versionName': '5.9.3',
        'platform': 'android10',
        'city': '%E4%B8%8A%E6%B5%B7%E5%B8%82'
    }
    while True:
        # 随机获取一个代理ip
        prox = con_redis2.randomkey()
        proxies = {"http": f"http{prox}", "https": f"http{prox}"}
        # 从任务队列中获取一条数据
        x = con_redis.brpop('tiktok_medicine_page_info', timeout=10)
        # 如果任务队列为空则停止循环
        if x is None:
            break
        category_info = eval(x[1])
        category_name = category_info['category_name']
        search_id = category_info['search_id']
        page = category_info['page']
        params = get_category2_parameter(page, search_id)
        try:
            # 获取当前页中全部药品id
            response = requests.get(baseurl, params=params, proxies=proxies, headers=headers, timeout=(10, 7))
            response_json = json.loads(response.text)
            result = response_json.get('result', '')
            product_list = result.get('productList', '')
            for product_item in product_list:
                medicine_name = product_item.get('name', '')
                medicine_id = product_item.get('skuId', '')
                dic_add = {
                    'medicine_name': medicine_name,
                    'category_name': category_name,
                    'medicine_id': medicine_id
                }
                # 将药品id信息插入任务队列中
                con_redis.lpush('tiktok_medicine_medicine_id', json.dumps(dic_add, ensure_ascii=False))
                con_redis.lpush('tiktok_medicine_medicine_id_copy', json.dumps(dic_add, ensure_ascii=False))
        except Exception as e:
            with open(date + '_id_error.txt', 'a') as f:
                f.write(str(x[1]) + '\n' + traceback.format_exc() + '\n')
            # 发生错误将该页重新插入任务队列尾
            con_redis.lpush('tiktok_medicine_page_info', json.dumps(category_info, ensure_ascii=False))


# 获取药品详情
def get_medicine_detail(non_param):
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
            details = db['medicine_detail_' + date]
            # 随机获取一个代理ip
            prox = con_redis2.randomkey()
            proxies = {"http": f"http{prox}", "https": f"http{prox}"}
            # 从任务队列中获取一条数据
            x = con_redis.brpop('tiktok_medicine_medicine_id', timeout=10)
            # 如果任务队列为空 则停止循环
            if x is None:
                break
            medicine_info = eval(x[1])
            medicine_id = medicine_info['medicine_id']
            medicine_name = medicine_info['medicine_name']
            # 检查当前药品是否已在当天的表中
            result = details.find_one({'medicine_id': medicine_id})
            # 药品在表中则更改id总表
            if result:
                update_all_id(medicine_id)
            # 药品不在表中，插入当前药品详细信息
            else:
                params = get_detail_parameter(medicine_id)
                try:
                    response = requests.get(baseurl, headers=headers, params=params, proxies=proxies, timeout=(10, 7))
                    response_json = json.loads(response.text)
                    data = response_json.get('data', '')
                    medicine_detail = data.get('detail', '')
                    medicine_name = medicine_detail.get('commonName', '')
                    title = medicine_detail.get('name', '')
                    approval_num = medicine_detail.get('approvalNumber', '')
                    img = medicine_detail.get('imgUrls', '')
                    enterprise = medicine_detail.get('manufacturers', '')
                    price = medicine_detail.get('priceL', 0) / 100
                    original_price = medicine_detail.get('originalPriceL', 0) / 100
                    if_otc = medicine_detail.get('otcCanBuy', None)
                    description = medicine_detail.get('productDescription', '')
                    specification = medicine_detail.get('productSpecifications', '')
                    stock = medicine_detail.get('productStock', -1)
                    coupons = medicine_detail.get('productDetailVoucherInfos', [])
                    dic_coupons = {}
                    if len(coupons) > 0:
                        for coupons_item in coupons:
                            name = coupons_item.get('name', '')
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
                    sale_num = medicine_detail['saleVolume']
                    promote = medicine_detail.get('promotionsList', [])
                    dic_promote = {}
                    for promote_item in promote:
                        promote_name = promote_item.get('promotionsType', '')
                        promote_des = promote_item.get('promotionsDesc', '')
                        promote_buy_count = promote_item.get('totalBuyCount', -1)
                        promote_buy_money = promote_item.get('totalBuyMoney', '')
                        promote_id = promote_item.get('promotionId', -1)
                        add_dic = {
                            promote_name.replace('.', ''): {
                                'promote_id': promote_id,
                                'promote_des': promote_des,
                                'promote_buy_count': promote_buy_count,
                                'promote_buy_money': promote_buy_money
                            }
                        }
                        dic_promote.update(add_dic)
                    dic_promote.update(dic_coupons)
                    quantity_limit = medicine_detail.get('quantityLimit', -1)
                    total_comment = medicine_detail.get('totalCommentCount', '')
                    instructions_img = data.get('instructionsImg', [])
                    instruction = instructions_img.get('instruction', [])
                    if len(instruction) > 0:
                        instructions = instruction[0].get('imgs', [])
                        lis_instruct = []
                        for instructions_item in instructions:
                            instructions_img = instructions_item.get('imgUrl', '')
                            lis_instruct.append(instructions_img)
                    else:
                        lis_instruct = []

                    brand = medicine_detail.get('name', '').split(']')
                    if len(brand) > 1:
                        brand = brand[0].replace('[', '')
                    else:
                        brand = ''
                    parameters = medicine_detail.get('parameters', [])
                    dic_parameters = {}
                    if len(parameters) > 0:
                        for parameters_item in parameters:
                            add_dic_parameters = {
                                parameters_item.get('text', '').replace('.', ''): parameters_item.get('value', '')
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
                        'promote': dic_promote,
                        'parameters': dic_parameters,
                        'instruction': lis_instruct
                    }
                    details.insert(dic_detail)
                    update_all_id(medicine_id)
                except Exception as e:
                    with open(date + '_detail_error.txt', 'a') as f:
                        f.write(medicine_name + ' ' + str(medicine_id) + '\n' + traceback.format_exc() + '\n')
                    con_redis.lpush('tiktok_medicine_medicine_id', json.dumps(medicine_info, ensure_ascii=False))
        except Exception as e:
            print('--------------------------------------------------------------')
            print(e)
            print('--------------------------------------------------------------')


if __name__ == '__main__':
    details = db['medicine_detail_' + date]
    # details = db['medicine_detail_2021_05_2245345']
    detail_local = db_local['medicine_detail_2021_05_22']
    record = db['all_id']

    # result = detail_local.find()
    # for i in result:
    #     details.insert(i)

    # 插入所有种类id
    lis_category = get_category_id(baseurl, 1)
    for category_item in lis_category:
        print('-----------------正在获取： ' + category_item['category_name'] + '------------------')
        search_id = get_category2_id(baseurl, category_item['category_id'])
        category_item['search_id'] = search_id
        con_redis.lpush('tiktok_medicine_category_id', json.dumps(category_item, ensure_ascii=False))

    # 获取分类页码信息
    print('---------------开始插入分类页码信息------------------')
    with ThreadPoolExecutor(5) as t1:
        t1.map(get_category_info, [i for i in range(5)])
    print('---------------分类页码信息插入完成------------------')

    # 插入所有药品id
    print('---------------开始插入全部药品id-------------------')
    with ThreadPoolExecutor(15) as t2:
        t2.map(get_medicine_id, [i for i in range(15)])
    print('---------------完成插入全部药品id-------------------')

    # 插入所有药品详情
    # print('---------------开始插入全部药品详情------------------')
    # with ThreadPoolExecutor(20) as t3:
    #     t3.map(get_medicine_detail, [i for i in range(20)])
    # print('---------------全部药品详情插入完毕------------------')
