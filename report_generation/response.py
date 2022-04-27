import os
import xml.etree.ElementTree as et
from src.building_util import get_tree_tags
from src.building_util import get_multiple_tree_tags
import csv
from pathlib import Path
import datetime
from datetime import timedelta


def get_right_branch(tree, brand_needed):
    quotes = get_multiple_tree_tags(tree, "quote")
    for tags in quotes:
        brand = get_tree_tags(tags, "brandID").text
        if brand == brand_needed:
            return tags


def check_valid_response(tree):
    txt = get_tree_tags(tree, "pncdIndicator")
    if txt is not None:
        return True
    else:
        return False


def get_dict():
    dict = {}
    with open(Path("../results/RenewalBreakdown.csv"), "r", encoding="utf-8", newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        for row in reader:
            brand = row[4]
            qr = row[9]
            dict[qr] = brand
    return dict


def get_dict_s4():
    dict = {}
    with open(Path("../results/RenewalBreakdown.csv"), "r", encoding="utf-8", newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        for row in reader:
            brand = row[18]
            qr = row[19]
            dict[qr] = brand
    return dict


def extract_values_s4(my_dict):
    get_date = datetime.date.today().strftime("%Y-%m-%d")
    path_of_the_dir = f"../responses/{get_date}"
    fileNam = "../results/nets.csv"
    print("write to csv started")
    header = ['RN_SUBMISSION', 'NB_SUBMISSION', 'RN_DATE_CREATED', 'NB_DATE_CREATED']
    with open(fileNam, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=",")
        # write the header
        writer.writerow(header)
        for filename in os.listdir(path_of_the_dir):
            f = os.path.join(path_of_the_dir, filename)
            nam = filename.split('.')[1]
            #rn_quote_num = f'="{nam}"'
            rn_quote_num = nam
            if os.path.isfile(f):
                tree = et.parse(f)
                if rn_quote_num in my_dict and check_valid_response(tree):
                    brand_needed = my_dict[rn_quote_num]
                    # right_branch = get_right_branch(tree, brand_needed)
                    txt = get_tree_tags(tree, "pncdIndicator")
                    if txt is not None:
                        quote_ref = get_tree_tags(tree, "quoteReference").text
                        #quote_ref_num = f'="{quote_ref}"'
                        quote_ref_num = quote_ref
                        data = [rn_quote_num, str(quote_ref_num), get_date, get_date]
                        # write the data
                        writer.writerow(data)
                    else:
                        continue


def get_full_report_s4():
    values = get_dict_s4()
    extract_values_s4(values)


if __name__ == '__main__':
    get_full_report_s4()
