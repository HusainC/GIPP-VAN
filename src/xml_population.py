from tag_strings import Tags as tags
from building_util import *
import pandas as pd
from mappings import mappings_van as m


def main_driver(tree, fin_dict, convics, claims, renew_date, quote_occupations):
    """This method populates the driver risk details"""
    driver_items = get_tree_tags(tree, tags.POLICYHOLDER.value)
    policy_holder_items = driver_items
    driver_prn = "1"
    if fin_dict['number'] is None:
        fin_dict['number'] = '?'

    # desc = ["maritalStatusDesc", "relationshipDesc"]
    desc = [tags.MARITALSTATUSDESC.value, tags.USEOTHERVEHICLEDESC.value, tags.MEDICALCONDITIONSDESC.value]
    add_desc_and_write(driver_items, desc, fin_dict)

    driver_items = get_tree_tags(policy_holder_items, tags.FULLNAME.value)
    desc = [tags.TITLEDESC.value]
    add_desc_and_write(driver_items, desc, fin_dict)

    driver_items = get_multiple_tree_tags(policy_holder_items, tags.EMPLOYMENT.value)
    driver_occupations = get_driver_occupations(quote_occupations, driver_prn)
    for i in range(0, len(driver_occupations)):
        desc = [tags.EMPLOYMENTSTATUSDESC.value, tags.EMPLOYMENTOCCUPATIONDESC.value, tags.EMPLOYMENTBUSINESSDESC.value]
        add_desc_and_write(driver_items[i], desc, driver_occupations[i])
    remove_extra_elements(policy_holder_items, tags.EMPLOYMENT.value, len(driver_occupations))

    driver_items = get_tree_tags(policy_holder_items, tags.ADDRESS.value)
    desc = []
    add_to_xml_address(driver_items, fin_dict)
    # add_desc_and_write(driver_items, desc, fin_dict)

    driver_items = get_tree_tags(policy_holder_items, tags.LICENCE.value)
    desc = [tags.TYPEDESC.value]
    #if fin_dict[tags.TYPE.value] is None:
    fin_dict[tags.TYPE.value] = "F_FM"
    if len(fin_dict[tags.TYPE.value]) > 4 and fin_dict[tags.TYPE.value] in m.type_of_licence:
        fin_dict[tags.TYPE.value] = m.type_of_licence[fin_dict[tags.TYPE.value]]
    add_desc_and_write(driver_items, desc, fin_dict)

    driver_items = get_tree_tags(policy_holder_items, tags.MARKETINGPREFERENCE.value)
    desc = []
    add_desc_and_write(driver_items, desc, fin_dict)

    driver_items = get_multiple_tree_tags(policy_holder_items, tags.CONVICTION.value)
    convic_list = get_driver_convictions(driver_prn, convics, renew_date)
    claims_and_convictions(driver_items, convic_list, policy_holder_items, tags.CONVICTIONS.value)

    driver_items = get_multiple_tree_tags(policy_holder_items, tags.CLAIM.value)
    claim_list = get_driver_claims(driver_prn, claims, renew_date)[0]
    claims_and_convictions(driver_items, claim_list, policy_holder_items, tags.CLAIMS.value)


def main_additional(tree, dict_list, convic, claim, renew_date, quote_occupations):
    """This method populates the xml with the additional driver risk details if any."""
    item = get_tree_tags(tree, tags.DRIVERS.value)
    additional_driver_sample_list = get_multiple_tree_tags(item, tags.ADDITIONALDRIVER.value)
    for i in range(0, len(dict_list)):
        fin_dict = dict_list[i]
        if fin_dict['number'] is None:
            fin_dict['number'] = '?'
        driver_prn = str(int(fin_dict[tags.DRIVERNUMBER.value]))
        policy_holder_items = additional_driver_sample_list[i]
        desc = [tags.MARITALSTATUSDESC.value, tags.RELATIONSHIPDESC.value, tags.MEDICALCONDITIONSDESC.value]
        add_desc_and_write(additional_driver_sample_list[i], desc, fin_dict)

        driver_items = get_tree_tags(policy_holder_items, tags.FULLNAME.value)
        desc = [tags.TITLEDESC.value]
        add_desc_and_write(driver_items, desc, fin_dict)

        # driver_items = get_tree_tags(policy_holder_items, "employment")
        driver_occupations = get_driver_occupations(quote_occupations, driver_prn)
        # desc = ["employmentStatusDesc", "employmentOccupationDesc", "employmentBusinessDesc"]
        # add_desc_and_write(driver_items, desc, dict_list[i])
        driver_items = get_multiple_tree_tags(policy_holder_items, tags.EMPLOYMENT.value)
        for j in range(0, len(driver_occupations)):
            desc = [tags.EMPLOYMENTSTATUSDESC.value, tags.EMPLOYMENTOCCUPATIONDESC.value,
                    tags.EMPLOYMENTBUSINESSDESC.value]
            add_desc_and_write(driver_items[j], desc, driver_occupations[j])
        remove_extra_elements(policy_holder_items, tags.EMPLOYMENT.value, len(driver_occupations))

        driver_items = get_tree_tags(policy_holder_items, tags.LICENCE.value)
        desc = [tags.TYPEDESC.value]
        #if fin_dict[tags.TYPE.value] is None:
        fin_dict[tags.TYPE.value] = "F_FM"
        if len(fin_dict[tags.TYPE.value]) > 4 and fin_dict[tags.TYPE.value] in m.type_of_licence:
            fin_dict[tags.TYPE.value] = m.type_of_licence[fin_dict[tags.TYPE.value]]
        add_desc_and_write(driver_items, desc, fin_dict)

        driv_prn_number = str(i + 2)

        driver_items = get_multiple_tree_tags(policy_holder_items, tags.CONVICTION.value)
        conviction_list = get_driver_convictions(driv_prn_number, convic, renew_date)
        claims_and_convictions(driver_items, conviction_list, policy_holder_items, tags.CONVICTIONS.value)

        driver_items = get_multiple_tree_tags(policy_holder_items, tags.CLAIM.value)
        claims_list = get_driver_claims(driv_prn_number, claim, renew_date)[0]
        claims_and_convictions(driver_items, claims_list, policy_holder_items, tags.CLAIMS.value)

    for r in range(len(dict_list), len(additional_driver_sample_list)):
        item.remove(additional_driver_sample_list[r])


def main_van(tree, fin_dict, modifications_list):
    """Populates the xml with the car risk details."""
    item = get_tree_tags(tree, tags.VAN.value)
    # Add the descriptions
    desc = [tags.IMPORTTYPEDESC.value, tags.IMMOBILISERDESC.value, tags.TRACKERDESC.value,
            tags.PARKEDOVERNIGHTDESC.value, tags.OWNERDESC.value,
            tags.REGISTEREDKEEPERDESC.value, tags.MAKE.value]
    add_desc_and_write(item, desc, fin_dict)

    # cover stuff
    cover = get_tree_tags(item, tags.COVER.value)
    desc = [tags.COVERLEVELDESC.value, tags.CLASSOFUSEDESC.value, tags.VOLUNTARYEXCESSDESC.value]
    add_desc_and_write(cover, desc, fin_dict)

    ncd_greater = get_tree_tags(item, tags.NCDGREATERZERO.value)
    desc = [tags.HOWNCDEARNDESC.value]
    add_desc_and_write(ncd_greater, desc, fin_dict)

    ncd_zero = get_tree_tags(item, tags.NCDZERO.value)
    desc = []
    ncd_zero_check(fin_dict)
    add_desc_and_write(ncd_zero, desc, fin_dict)

    driver_items = get_multiple_tree_tags(item, tags.MODIFICATIONS.value)
    if 0 < len(modifications_list) <= 7:
        add_modifications(driver_items, modifications_list, item)
    else:
        remove_extra_elements(item, tags.MODIFICATIONS.value, length_used=0)


def pre_risk_details(tree, car_dict, inception_date):
    change_aggs_timestamp(tree)
    cars_per_house = get_tree_tags(tree, "noOfCarsHousehold")
    cars_per_house.text = str(int(car_dict["noOfCarsHousehold"]))
    inception_date_tree_item = get_tree_tags(tree, "inceptionDate")
    inception_date_tree_item.text = str(pd.to_datetime(str(inception_date)).date())
