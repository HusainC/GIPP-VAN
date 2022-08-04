from lxml import etree
from tag_strings import Tags as tags
from mappings.mappings_van import *
import math
import uuid
import datetime as dt


def get_tree_tags(tree, tree_item):
    """This method is used to get all the tags with with the parameter name eg- car would return abicode, regno
    input - a tag whose inner tags are needed
    output - a tag that contains all the inner tags needed"""
    for tag in tree.iter(tree_item):
        return tag


def remove_extra_elements(items, element, length_used):
    counter = 0
    for occu in items.findall(element):
        if counter >= length_used:
            items.remove(occu)
        counter = counter + 1


def claims_and_convictions(driver_items, list_of_dict, item, con_clai):
    """This method handles the multiple claims in concerns with adding it to the correct xml tree item"""
    for i in range(0, len(list_of_dict)):
        desc = [tags.TYPEDESC.value, tags.CONVICTIONCODEDESC.value, tags.USEOTHERVEHICLEDESC.value]
        list_of_dict[i][tags.SOURCE.value] = "Priorloss"
        list_of_dict[i]["claimSettled"] = "true"
        if tags.COST.value in list_of_dict[i]:
            if (list_of_dict[i][tags.COST.value] is None) or (math.isnan(list_of_dict[i][tags.COST.value])):
                list_of_dict[i][tags.COST.value] = "0"
        if tags.CONVICTIONCODE.value in list_of_dict[i]:
            if len(list_of_dict[i][tags.CONVICTIONCODE.value].split("-")) > 1:
                list_of_dict[i][tags.CONVICTIONCODE.value] = list_of_dict[i][tags.CONVICTIONCODE.value].split("-")[
                    0].replace(" ", "")

        add_desc_and_write(driver_items[i], desc, list_of_dict[i])
    counter = 0
    # root = et.fromstring(item)
    for claim_conviction in item.findall(con_clai):
        if con_clai == tags.CONVICTIONS.value:
            for n in claim_conviction.findall(tags.CONVICTION.value):
                if counter >= len(list_of_dict):
                    claim_conviction.remove(n)
                counter = counter + 1
        else:
            for n in claim_conviction.findall(tags.CLAIM.value):
                if counter >= len(list_of_dict):
                    claim_conviction.remove(n)
                counter = counter + 1


def get_driver_convictions(driver_prn, con_list, renewal_date):
    convictions_for_driver = []
    for i in con_list:
        # conviction_years = five_years(str(i[tags.DATE.value]), str(pd.to_datetime(renewal_date).date()))
        # if str(int(i[tags.DRIVERNUMBER.value])) == driver_prn and conviction_years:
        #     convictions_for_driver.append(i)
        if str(int(i[tags.DRIVERNUMBER.value])) == driver_prn:
            convictions_for_driver.append(i)

    return convictions_for_driver


def get_driver_claims(driver_prn, claims_list, renewal_date):
    claims_for_driver = []
    c1 = 0  # No claims in the last year
    c3 = 0  # No claims in the last 3 years
    # for i in claims_list:
    #     claim_years = five_years(str(i["date"]), str(pd.to_datetime(renewal_date).date()))
    #     if str(int(i['DRIVER_PRN'])) == driver_prn and claim_years:
    #         claims_for_driver.append(i)
    #     if one_year(str(i["date"]), str(pd.to_datetime(renewal_date).date())):
    #         c1 += 1
    #     if three_years(str(i["date"]), str(pd.to_datetime(renewal_date).date())):
    #         c3 += 1
    # return claims_for_driver, c1, c3
    for i in claims_list:
        if str(int(i['DRIVER_PRN'])) == driver_prn:
            claims_for_driver.append(i)
    return claims_for_driver, c1, c3


def get_driver_occupations(quote_occupations, driver_prn):
    occupation_list = []
    for i in quote_occupations:
        if str(int(i[tags.DRIVERNUMBER.value])) == driver_prn:
            occupation_list.append(i)
    return occupation_list


def recursively_empty(xml_element):
    ignore = [tags.ADDRESSLINE2.value, tags.ADDRESSLINE3.value, tags.TOWN.value, tags.COUNTY.value]
    if xml_element.tag in ignore:
        return False
    if xml_element.text:
        if xml_element.text == "?":
            return all((recursively_empty(xe) for xe in xml_element.iterchildren()))
        elif xml_element.text == "nan":
            return all((recursively_empty(xe) for xe in xml_element.iterchildren()))
        else:
            return False
    return all((recursively_empty(xe) for xe in xml_element.iterchildren()))


def check_for_decimal(number):
    fin = number.split(".")[0]
    return fin


def ncd_zero_check(ncd_dict_add):
    if ncd_dict_add[tags.YEARSNAMEDDRIVEREXP.value] is None:
        ncd_dict_add[tags.NAMEDDRIVEREXPFLAG.value] = 'false'
        ncd_dict_add[tags.YEARSNAMEDDRIVEREXP.value] = '0'
    elif math.isnan(ncd_dict_add[tags.YEARSNAMEDDRIVEREXP.value]):
        ncd_dict_add[tags.NAMEDDRIVEREXPFLAG.value] = 'false'
        ncd_dict_add[tags.YEARSNAMEDDRIVEREXP.value] = '0'
    else:
        ncd_dict_add[tags.NAMEDDRIVEREXPFLAG.value] = 'true'


def change_aggs_timestamp(tree):
    """This method populates the time stamp and aggsid for the xml
    input - xml tree"""
    aggs = get_tree_tags(tree, tags.AGGSID.value)
    aggs.text = str(uuid.uuid4())
    xml_time_stamp = get_tree_tags(tree, tags.TIMESTAMP.value)
    xml_time_stamp.text = (dt.datetime.now() + dt.timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")


def add_modifications(tree_items, modifications_list, main_tree):
    """This method handles the multiple claims in concerns with adding it to the correct xml tree item"""
    for i in range(0, len(modifications_list)):
        desc = [tags.MODIFICATIONABIDESC.value]

        add_desc_and_write(tree_items[i], desc, modifications_list[i])
    counter = 0
    # root = et.fromstring(item)
    for mods in main_tree.findall(tags.MODIFICATIONS.value):
        if counter >= len(modifications_list):
            main_tree.remove(mods)
        counter = counter + 1


def add_to_xml(tree_sub, final_mapping):
    """This class takes in the tree subitems and adds the appropriate text to the tags
    input - takes a tree subitem and a dictionary
    result should be adding text to the tree subitem"""
    for driver_tags in tree_sub:
        if driver_tags.tag in final_mapping:
            if len(driver_tags) == 1:
                for i in driver_tags:
                    # i.text = str(int(final_mapping[driver_tags.tag]))
                    i.text = check_for_decimal(str(final_mapping[driver_tags.tag]))
            elif len(driver_tags) == 0:
                # driver_tags.text = str(int(final_mapping[driver_tags.tag]))
                driver_tags.text = check_for_decimal(str(final_mapping[driver_tags.tag]))


def add_to_xml_address(tree_sub, final_mapping):
    """This class takes in the tree subitems and adds the appropriate text to the tags
    input - takes a tree subitem and a dictionary
    result should be adding text to the tree subitem"""
    for driver_tags in tree_sub:
        if driver_tags.tag in final_mapping:
            if len(driver_tags) == 1:
                for i in driver_tags:
                    i.text = str(final_mapping[driver_tags.tag])
            elif len(driver_tags) == 0:
                driver_tags.text = str(final_mapping[driver_tags.tag])


def clean_xml(data):
    """
  Input: XML string, Output: XML string
  Removes empty tags not specified in ignore and element.text = "?"
  ignore is stored in the function, "recursively_empty"
  """
    root = etree.fromstring(data)
    xml_root = etree.iterwalk(root)
    for action, xml_element in xml_root:
        parent = xml_element.getparent()
        if recursively_empty(xml_element):
            parent.remove(xml_element)
    return etree.tostring(root, pretty_print=True).decode("utf-8")


def deal_with_case(tag_name):
    """This method deals with descriptor tag names that donot match with their parent tag name with the code
    input - takes the tag name to check if it is one of those special case tags"""
    special_cases = {
        tags.COVERLEVELDESC.value: tags.COVERTYPE.value,
        tags.PARKEDOVERNIGHTDESC.value: tags.PARKEDOVERNIGHT.value,
        tags.EMPLOYMENTSTATUSDESC.value: tags.EMPLOYMENTSTATUSCODE.value,
        tags.EMPLOYMENTOCCUPATIONDESC.value: tags.EMPLOYMENTOCCUPATIONCODE.value,
        tags.EMPLOYMENTBUSINESSDESC.value: tags.EMPLOYMENTBUSINESSCODE.value,
        tags.MAKE.value: tags.ABICODE.value,
        tags.MODIFICATIONABIDESC.value: tags.MODIFICATIONABICODE.value
    }
    if tag_name in special_cases:
        return special_cases[tag_name]
    else:
        return tag_name


def add_desc_map(final_map, description_list):
    """This method takes care of the description tags in the sub-tree as these can only be detected after the sql
    is run.
    input - a dict with the final mappings that needed to be added to the xml.
    the method does not return anything but adds to the map that was passed as a parameter"""
    for i in description_list:
        tag = deal_with_case(i)
        if tag.find("Desc") != -1:
            tag = tag[:-4]
        map_needed = code_desc[tag]
        if final_map is not None:
            if tag in final_map and isinstance(final_map[tag], float):
                # final_map[tag] = str(int(final_map[tag])) # fairly sure it has to be a string
                final_map[tag] = check_for_decimal(str(final_map[tag]))  # old working method
            if tag in final_map and final_map[tag] in map_needed:
                final_map[i] = map_needed[final_map[tag]]


def add_database_values_to_dict(extracted_values, desc_list):
    """This methods add all the car values to the dict to then add to the respective tags in the xml
    input - takes the extracted values from the snowflake database"""
    if len(desc_list) > 0:
        add_desc_map(extracted_values, desc_list)


def add_desc_and_write(item, desc_items, final_map):
    """This method adds in the descriptions in the xml and writes the new values to it
    input - xml, descriptions and the final map which contains all the values to be added """
    add_database_values_to_dict(final_map, desc_items)
    add_to_xml(item, final_map)


def get_multiple_tree_tags(tree, tree_item):
    """This method is used to get all the tags with with the parameter name eg- car would return abicode, regno
    input - a tag whose inner tags are needed
    output - a tag that contains all the inner tags needed"""
    my_tree_items = []
    for tag in tree.iter(tree_item):
        my_tree_items.append(tag)
    return my_tree_items
