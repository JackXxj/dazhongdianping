# coding:utf-8
__author__ = 'xxj'

import requests
import lxml.etree
import time
import os
import Queue
from copy import deepcopy
from rediscluster import StrictRedisCluster
import threading
from requests.exceptions import ReadTimeout, ConnectionError, ConnectTimeout, ProxyError, ChunkedEncodingError
from Queue import Empty
import json
import re
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

CITY_CY_QUEUE = Queue.Queue()    # 数据源队列
DATA_QUEUE = Queue.Queue()    # 数据存储队列
PROXY_IP_Q = Queue.Queue()    # 代理IP队列
THREAD_PROXY_MAP = {}    # 线程与代理关系
THREAD_CITY_CY = {}    # 线程与城市关系
CITY_LIST = ['呼和浩特', '南昌', '武汉', '大连', '太原', '苏州', '上饶', '宜春', '杭州', '眉山', '吉安', '哈尔滨', '新余', '巴彦淖尔', '聊城', '汉中', '泸州', '九江', '周口', '雅安', '亳州', '淮北', '株洲', '泉州', '上海', '郑州', '临汾', '厦门', '佳木斯', '淮南', '鹤岗', '黄冈', '昭通', '邯郸', '景德镇', '齐齐哈尔', '徐州', '衢州', '赤峰', '乐山', '滁州', '拉萨', '曲靖', '芜湖', '宁德', '牡丹江', '绥化', '鹰潭', '成都', '兴安盟', '赣州', '南通', '六盘水', '大庆', '抚州', '西昌', '深圳', '丽水', '沧州', '北京', '海口', '鄂州', '宜昌', '运城', '南平', '宿迁', '重庆', '朔州', '洛阳', '台州', '东莞']


def get_redis_proxy():
    '''
    从redis相应的key中获取代理ip
    :return:
    '''
    startup_nodes = [{'host': 'redis2', 'port': '6379'}]
    r = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    da_zhong_dian_ping_proxy_length = r.llen('spider:da_zhong_dian_ping:proxy')  # da_zhong_dian_ping
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中da_zhong_dian_ping的代理ip长度：', da_zhong_dian_ping_proxy_length
    if da_zhong_dian_ping_proxy_length == 0:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中的代理ip数量为0，等待60s'
        time.sleep(60)
        return get_redis_proxy()
    for i in xrange(da_zhong_dian_ping_proxy_length):
        ip = r.lpop('spider:da_zhong_dian_ping:proxy')
        proxies = {
            'http': "http://8c84700fa7d2:kgvavaeile@{ip}".format(ip=ip),
            'https': "http://8c84700fa7d2:kgvavaeile@{ip}".format(ip=ip)
        }
        PROXY_IP_Q.put(proxies)


def city_cy():
    '''
    生成(城市--城市链接字典)以及(城市--cy值字典)
    :return:
    '''
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'
        }
        url = 'https://m.dianping.com/citylist'
        print time.strftime('[%Y-%m-%d %H:%M:%S] '), '城市列表页：', url
        response = requests.get(url=url, headers=headers, timeout=10)
        # response = get(url, 3, headers)
        response_obj = lxml.etree.HTML(response.text)
        char_list = response_obj.xpath('//div[@class="home-place-list letter-list"]/ul/li/a/text()')    # 字母列表（具有以该字母为首字母的城市）
        # print '具有城市的首字母：', len(char_list)
        city_dict_link = {}  # 城市--城市链接 字典
        city_dict_cy = {}  # 城市--cy值 字典
        for char in char_list:
            url = 'https://m.dianping.com/citylist?c={char}&returl=&type='.format(char=char)    # 每个首字母城市的url
            print time.strftime('[%Y-%m-%d %H:%M:%S] '), '城市详情页：', url
            try:
                response = requests.get(url=url, headers=headers, timeout=10)
            except BaseException as e:
                print '城市详情页异常：', e.message
            # response = get(url, 3, headers)
            response_obj = lxml.etree.HTML(response.text)
            city_list = response_obj.xpath('//ul[@class="J_citylist"]/li/a')    # 首字母下的城市
            for city in city_list:
                city_name = city.xpath('./text()')[0]    # 城市名称
                city_link = city.xpath('./@href')[0]    # 城市link
                cy = city.xpath('./@data-id')[0]    # 城市cy
                for city in CITY_LIST:
                    if city in city_name:
                        city_dict_link[city_name] = city_link
                        city_dict_cy[city_name] = cy
        print '所有城市的数量：', len(city_dict_cy)
        return city_dict_link, city_dict_cy
    except BaseException as e:
        print '城市列表页异常：', e.message
        return city_cy()



def index_parse(lock):
    '''
    索引页
    :return:
    '''
    while not CITY_CY_QUEUE.empty():
        try:
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                # 'Cookie': 'cy=255',    # 在索引页中不同城市相对应的cookie需要改变
                'Host': 'm.dianping.com',
                'Pragma': 'no-cache',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'
            }

            thread_name = threading.currentThread().name  # 当前线程名
            if not THREAD_PROXY_MAP.get(thread_name):    # 每个线程对应一个代理ip、一个city，一个cy，一个page
                THREAD_PROXY_MAP[thread_name] = PROXY_IP_Q.get(False)
            proxies = THREAD_PROXY_MAP.get(thread_name)

            city_cy_page = CITY_CY_QUEUE.get(False)
            city, cy, page = city_cy_page
            # url = 'https://m.dianping.com/tuan/ajax/moreAggregatedItemList?guidanceParameter=1_0_9_0&version=2&currentPage={page}'.format(page=page)    # 美食系列（按评价排序,部分城市不存在按评价排序的url）
            url = 'https://m.dianping.com/tuan/ajax/moreAggregatedItemList?guidanceParameter=&version=2&currentPage={page}'.format(page=page)    # 美食系列（按智能排序）
            headers['Cookie'] = 'cy={cy}'.format(cy=cy)
            print time.strftime('[%Y-%m-%d %H:%M:%S] '), thread_name, '优惠券索引页url：', url
            response = requests.get(url=url, headers=headers, proxies=proxies, timeout=5)
            # response = get(url, 3, headers)
            response_obj = lxml.etree.HTML(response.text)

            store_list = response_obj.xpath('//div[@class="content group-list J_group_list J_seeMore_box"]')  # 商店列表（有两种页面格式）
            if store_list:
                # print '链接下的商家数量：', len(store_list)  # 页面下商家的数量(每一页下的商家数量是不固定的)
                for store in store_list:
                    store_name = store.xpath('.//span[@class="tit"]/text()')[0].strip()  # 商店名称
                    # print '商店名称：', store_name
                    # 一个商店下可能具有多个优惠券
                    coupon_list = store.xpath('.//a[contains(@class, "table-cell Fix")]')  # 一个商家下面所有的优惠券
                    for coupon in coupon_list:
                        crawl_time = time.strftime('%Y-%m-%d %H:%M:%S')  # 数据抓取时间
                        store_link = coupon.xpath('./@href')[0]  # 优惠券详情页链接
                        img_url = coupon.xpath('.//img/@lazy-src')[0]    # 图片链接
                        img_url = img_url.split('%')[0]    # 该图片链接下的图片更加清晰
                        dealGroupId = store_link.replace('/tuan/deal/', '')     # 为area()的url提供相关参数
                        area_url = 'https://m.dianping.com/tuan/ajax/dealShop?dealGroupId={dealGroupId}&lat=&lng=&userCityId={cy}'.format(dealGroupId=dealGroupId, cy=cy)    # 地址数据获取相关url
                        store_link = 'https://m.dianping.com{store_link}'.format(store_link=store_link)    # 优惠券详情页url(优惠券领取链接)
                        # print '优惠券详情页链接：', store_link
                        discount_title = coupon.xpath('.//h3[@class="aggr"]/text()')[0]  # 优惠券标题
                        # print '优惠券标题：', discount_title

                        price_after = coupon.xpath('.//span[@class="price"]/strong/text()')[0].strip()  # 优惠后的价格
                        price_before = coupon.xpath('.//span[@class="o-price"]/text()')    # 优惠前的价格
                        if price_before:
                            price_before = price_before[0].replace('¥', '').strip()
                        else:
                            price_before = ''

                        number = coupon.xpath('.//div[@ class="count"]/text()')  # 优惠券已售数量(优惠券)
                        if number:
                            number = number[0].replace('已售', '')
                        else:
                            number = ''
                        # print '已售数量：', number
                        # old_price, new_price = coupon_detail(store_link)  # 获取优惠券详情页的相关信息（获取优惠前以及优惠后的价格，以及购买须知）
                        address, shop_power = area(area_url, proxies, cy, store_link)    # 获取相关地理位置信息以及评分
                        detail_address = city+address    # 获取详细的地理位置（以便获取经纬度）
                        # lng_lat = baidu_map_api(detail_address)    # 百度地图获取获取经纬度
                        lng_lat = gaode_map_api(detail_address)  # 高德地图获取获取经纬度
                        if lng_lat:
                            # 城市、商店名称、商店url、优惠券标题、优惠券出售数量、优惠前的价格、优惠后的价格，商店地址
                            # content_list = [city, store_name, store_link, discount_title, number, old_price, new_price, address]
                            # 优惠券类型、抓取时间、城市、店名、优惠券领取链接（详情页链接）、素材链接（图片url）、优惠券标题、优惠券出售数量、商店地址、经纬度、评分、优惠前的价格，优惠后的价格
                            content_list = ['美食', crawl_time.replace('\t', ''), city.replace('\t', ''), store_name.replace('\t', ''), store_link.replace('\t', ''), img_url.replace('\t', ''), discount_title.replace('\t', ''), number.replace('\t', ''), address.replace('\t', ''), lng_lat.replace('\t', ''), shop_power.replace('\t', ''), price_before, price_after]
                            content_str = '\t'.join(content_list)
                            # print content_str
                            DATA_QUEUE.put(content_str)
                            # print '测试时数据队列长度：', DATA_QUEUE.qsize()
                            # f.write(content_str)
                            # f.write('\n')
                            # f.flush()

            else:
                store_list = response_obj.xpath('//div[@id="list"]/a')  # 商家列表
                # print '链接下的商家数量：', len(store_list)  # 页面下商家的数量(每一页下的商家数量是不固定的)
                for store in store_list:
                    crawl_time = time.strftime('%Y-%m-%d %H:%M:%S')    # 数据抓取时间
                    store_link = store.xpath('./@href')[0]  # 优惠券详情页url（优惠券领取链接）
                    img_url = store.xpath('.//img/@lazy-src')[0]  # 图片链接
                    img_url = img_url.split('%')[0]  # 该图片链接下的图片更加清晰
                    dealGroupId = store_link.replace('/tuan/deal/', '')  # 为area()的url提供相关参数
                    area_url = 'https://m.dianping.com/tuan/ajax/dealShop?dealGroupId={dealGroupId}&lat=&lng=&userCityId={cy}'.format(
                        dealGroupId=dealGroupId, cy=cy)  # 地址数据获取相关url
                    store_link = 'https://m.dianping.com{store_link}'.format(store_link=store_link)    # 优惠券详情页url
                    store_name = store.xpath('.//div[@class="shopName"]/text()')[0]  # 商店名称
                    # print 'other商店名称：', store_name
                    discount_title = store.xpath('.//div[@class="aggr"]/text()')[0]  # 优惠券标题
                    # print '优惠券标题：', discount_title

                    price_after = store.xpath('.//span[@class="price"]/strong/text()')[0].strip()  # 优惠后的价格
                    price_before = store.xpath('.//span[@class="o-price"]/text()')  # 优惠前的价格
                    if price_before:
                        price_before = price_before[0].replace('¥', '').strip()
                    else:
                        price_before = ''

                    number = store.xpath('.//div[@class="count"]/text()')  # 优惠券已售数量
                    if number:
                        number = number[0].replace('已售', '')
                    else:
                        number = ''
                    # old_price, new_price = coupon_detail(store_link)    # 优惠前价格、优惠后价格
                    # if old_price == '' and new_price == '':    # ----有待修改-----
                    #     continue
                    address, shop_power = area(area_url, proxies, cy, store_link)  # 获取相关地理位置信息
                    detail_address = city + address  # 获取详细的地理位置（以便获取经纬度）
                    # lng_lat = baidu_map_api(detail_address)  # 百度地图获取经纬度
                    lng_lat = gaode_map_api(detail_address)  # 高德地图获取获取经纬度
                    if lng_lat:
                        # content_list = [city, store_name, store_link, discount_title, number, old_price, new_price, address]
                        # 优惠券类型、抓取时间、城市、店名、优惠券领取链接（详情页链接）、素材链接（图片url）、优惠券标题、优惠券出售数量、商店地址,经纬度， 评分、优惠前的价格、优惠后的价格
                        content_list = ['美食', crawl_time.replace('\t', ''), city.replace('\t', ''), store_name.replace('\t', ''), store_link.replace('\t', ''), img_url.replace('\t', ''), discount_title.replace('\t', ''), number.replace('\t', ''), address.replace('\t', ''), lng_lat.replace('\t', ''), shop_power.replace('\t', ''), price_before, price_after]
                        content_str = '\t'.join(content_list)
                        # print content_str
                        DATA_QUEUE.put(content_str)
                        # print '测试时数据队列长度：', DATA_QUEUE.qsize()
                        # f.write(content_str)
                        # f.write('\n')
                        # f.flush()

        except ReadTimeout as e:
            print 'ReadTimeout：', thread_name, city_cy_page

        except ProxyError as e:
            with lock:
                print 'ProxyError：', thread_name, city_cy_page
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                print '新的代理IP：', proxies
                THREAD_PROXY_MAP[thread_name] = proxies

        except ConnectionError as e:
            with lock:
                print 'ConnectionError：', thread_name
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                print '新的代理IP：', proxies
                THREAD_PROXY_MAP[thread_name] = proxies

        except Empty as e:
            print 'Empty:', '异常'

        except BaseException as e:
            with lock:
                print 'BaseException：',  thread_name,
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                print '新的代理IP：', proxies
                THREAD_PROXY_MAP[thread_name] = proxies


def coupon_detail(store_link):
    '''
    优惠券详情页的相关信息（优惠前、后的价格，，后续需要填补的抓取字段）
    :param store_link:优惠券详情页url
    :return:
    '''
    try:
        headers = {
            'Referer': 'https://m.dianping.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'
        }
        print time.strftime('[%Y-%m-%d %H:%M:%S]：'), '优惠券详情页：', store_link
        response = requests.get(url=store_link, headers=headers, timeout=5)
        # response = get(store_link, 3, headers)
        response_obj = lxml.etree.HTML(response.text)
        old_price = response_obj.xpath('//div[@class="t"]/span/text()')[0].replace('¥', '')  # 优惠前的价格
        print '优惠前的价格：', old_price
        new_price = response_obj.xpath('//div[@class="price sum"]/text()')[0]  # 优惠后的价格
        print '优惠后的价格：', new_price
        return old_price, new_price
    except BaseException as e:    # ---有待修改----
        time.sleep(10)
        print '异常：', e.message
        # proxies = get_proxy_ip()
        proxies = ''
        print '异常出现后的代理ip切换：', proxies
        # print response.text
        return '', ''


def area(area_url, proxies, cy, store_link):
    '''
    用户商店地址接口
    :param area_url:
    :param proxies:
    :return:
    '''
    headers = {
    	'Referer': store_link,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'
    }
    headers['Cookie'] = 'cy={cy}'.format(cy=cy)
    print time.strftime('[%Y-%m-%d %H:%M:%S] '),  '地址详情页：', area_url
    response = requests.get(url=area_url, headers=headers, proxies=proxies, timeout=5)
    # response = get(area_url, 3, headers)
    content = response.json()
    dealGroupShop = content.get('dealGroupShop')
    if dealGroupShop:
        address = dealGroupShop.get('address')  # 地址
        # print '商店位置：', address
        shop_power = dealGroupShop.get('shopPower')    # 评分
        shop_power = str(shop_power)
        # print '评分：', shop_power
    else:
        address = ''
        shop_power = ''
    return address, shop_power


def baidu_map_api(detail_address):
    '''
    百度地图api接口
    将详细地址转换为经纬度
    :return:
    '''
    global AK_LIST_COUNT
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'
    }
    ak_list = ['GE70D0HQiCirDMQzfhsgrQTBVF9WpM3o']    # 百度地图接口ak（每天）
    ak = ak_list[AK_LIST_COUNT]    # 根据断点获取有效ak
    url = 'http://api.map.baidu.com/geocoder/v2/?address={address}&output=json&ak={ak}&callback='.format(address=detail_address, ak=ak)
    response = requests.get(url=url, headers=headers, timeout=5)
    content = response.json()
    status = content.get('status')    # 返回数据的状态码（0表示正常）
    if status == 302:     # 当该ak的使用量达到上限的时候，切换ak（302状态码表示天配额超限，限制访问）
        print '302：', status
        AK_LIST_COUNT += 1
        # if AK_LIST_COUNT > len(ak_list):    # 当ak列表遍历完
        #     pass
        lng_lat = ''
    elif status == 0:
        result = content.get('result')
        location = result.get('location')
        lat = location.get('lat')    # 纬度
        lng = location.get('lng')    # 经度
        lng_lat = '{lng},{lat}'.format(lng=lng, lat=lat)
        print lng_lat
    else:
        print 'baidu_map_api other status：', status
        lng_lat = ''
    return lng_lat


def gaode_map_api(detail_address):
    '''
    高德地图api接口
    将详细地址转换为经纬度
    :return:
    '''
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'
    }
    key = 'ebd39e965853b17cfbf03cfa2060079c'
    url = 'https://restapi.amap.com/v3/geocode/geo?address={address}&output=JSON&key={key}'.format(address=detail_address,
                                                                                                   key=key)
    response = requests.get(url=url, headers=headers, timeout=10)
    content_json = response.json()
    status = content_json.get('status')
    if status == '1':
        geocodes = content_json.get('geocodes')
        if geocodes:
            location = geocodes[0].get('location')  # 经纬度
        else:
            location = ''
    else:
        infocode = content_json.get('infocode')    # 出现错误状态时的消息code
        print '高德地图接口问题：', infocode
        # if infocode == '10003':    # 日访问量超过限定量
        #     location = infocode
        location = ''
    return location


def file_write(f, lock):
    '''
    写入文件
    :param f:
    :return:
    '''
    while (not CITY_CY_QUEUE.empty()) or (not DATA_QUEUE.empty()):
        try:
            content = DATA_QUEUE.get(False)
            with lock:
                f.write(content)
                f.write('\n')
                f.flush()
        except Empty as e:
            pass


def file_write_other(f):
    '''
    写入文件
    :param f:
    :return:
    '''
    while not DATA_QUEUE.empty():
        try:
            content = DATA_QUEUE.get(False)
            f.write(content)
            f.write('\n')
            f.flush()
        except Empty as e:
            pass


def main():
    lock = threading.Lock()
    print time.strftime('[%Y-%m-%d %H:%M:%S]：'), 'start'
    date = time.strftime('%Y%m%d')

    city_dict_link, city_dict_cy = city_cy()  # city---cy
    for city, cy in city_dict_cy.items():
        city_cy_page_list = []
        city_cy_page_list.append(city)
        city_cy_page_list.append(cy)
        city_cy_page_list.append(0)
        for page in xrange(100):
            city_cy_page_list[2] = page
            # time.sleep(1)
            data = deepcopy(city_cy_page_list)
            # print '数据源：',data
            CITY_CY_QUEUE.put(data)  # 数据源队列中的数据格式  [city, cy, page_num]
    print '队列长度：', CITY_CY_QUEUE.qsize()

    # dest_path = os.getcwd()  # 本机及测试环境上数据文件存储目录
    dest_path = '/ftp_samba/112/spider/python/dazhongdianping/'    # 线上数据文件存储目录
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
    dest_file_name = os.path.join(dest_path, 'dazhongdianping_' + date)    # 目标文件
    tmp_file_name = os.path.join(dest_path, 'dazhongdianping_' + date + '.tmp')    # 中间文件
    fileout = open(tmp_file_name, 'a')

    get_redis_proxy()  # 将redis中的代理ip放入到PROXY_IP_Q队列中
    proxy_count = PROXY_IP_Q.qsize()  # 根据代理队列中代理的数量来决定线程数
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '代理ip队列中的ip数量：', proxy_count

    threads = []
    for i in range(50):
        t = threading.Thread(target=index_parse, args=(lock,))
        t.start()
        threads.append(t)

    data_threads = []
    for i in xrange(50):
        t = threading.Thread(target=file_write, args=(fileout, lock))
        t.start()
        data_threads.append(t)

    for t in threads:
        t.join()

    for t in data_threads:
        t.join()

    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '数据存储队列的长度：', DATA_QUEUE.qsize()

    if not DATA_QUEUE.empty():
        file_write_other(fileout)

    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '筛选之后数据队列的数量', DATA_QUEUE.qsize()

    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '抓取结束'
    fileout.flush()
    fileout.close()
    os.rename(tmp_file_name, dest_file_name)


if __name__ == '__main__':
    main()
