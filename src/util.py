import os
import shutil
from datetime import date
from concurrent.futures import ThreadPoolExecutor
import xml.etree.ElementTree as et
import xml.dom.minidom as MD
import requests
import time
from building_util import clean_xml
from tag_strings import Tags as tags


def manage_folders(path):
    if os.path.exists(path):
        try:
            shutil.rmtree(path)
        except OSError as e:
            print(f"ERROR: {e.filename} - {e.strerror}")
    try:
        os.mkdir(path)
    except OSError:
        print(f"Creation of the directory {path} failed, it probably already exists")


def get_date():
    return date.today().strftime("%Y-%m-%d")


def get_policy_proposer(policy_drivers):
    for policy_holder in policy_drivers:
        if str(int(policy_holder[tags.DRIVERNUMBER.value])) == '1':
            return policy_holder


def get_items_with_agghub_id(list_of_items, quote):
    quote_item_list = []
    for i in list_of_items:
        if i[tags.AGGHUBID.value] == quote:
            quote_item_list.append(i)
    return quote_item_list


def get_items_with_quote_ref(list_of_items, quote):
    quote_item_list = []
    for i in list_of_items:
        if i[tags.QUOTEREFERENCE.value] == quote:
            quote_item_list.append(i)
    return quote_item_list


def find_additional_drivers(d):
    additional_driver_list = []
    if len(d) > 1:
        for i in range(0, len(d)):
            if len(additional_driver_list) > 0:
                does_exsist = True
                for x in additional_driver_list:
                    if d[i][tags.DRIVERNUMBER.value] == x[tags.DRIVERNUMBER.value] or str(
                            int(d[i][tags.DRIVERNUMBER.value])) == "1":
                        does_exsist = False
                        break

                if does_exsist:
                    additional_driver_list.append(d[i])
            else:
                if str(int(d[i][tags.DRIVERNUMBER.value])) != "1":
                    additional_driver_list.append(d[i])
    return additional_driver_list


def xml_response(body):
    root = body.getroot()
    body = et.tostring(root, encoding="utf-8").decode("utf-8")
    url = "https://ad1-prd-pcclus-qh.network.uk.ad/pc/ws/com/hastings/integration/aggs/commercialvan/qcore/CVQuoteEngineAPI?wsdl"
    headers = {"content-type": "text/xml"}
    response = requests.post(url, data=body, headers=headers, verify=False)
    content = MD.parseString(response.text).toprettyxml()
    content = et.fromstring(content)
    return content


def deal_with_writing_req(data, filename, date_folder):
    try:
        start_time = time.time()
        s = xml_response(et.ElementTree(et.fromstring(clean_xml(data))))
        executionTime1 = (time.time() - start_time)
        print('api call: ' + str(executionTime1))
        tree = et.ElementTree(s)
        tree.write(f"../responses/{date_folder}/" + filename)
    except requests.exceptions.RequestException as e:
        return e


def get_store_responses():
    path = f"../responses/{get_date()}"
    manage_folders(path)
    threads = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        path_of_the_directory = f'../requests/{get_date()}'
        print("Files and directories in a specified path:")
        for filename in os.listdir(path_of_the_directory):
            f = os.path.join(path_of_the_directory, filename)
            if os.path.isfile(f):
                c = et.parse(f).getroot()
                data = et.tostring(c, encoding="utf-8").decode("utf-8")
                threads.append(executor.submit(deal_with_writing_req, data, filename, get_date()))
        return threads
