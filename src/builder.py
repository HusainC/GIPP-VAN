from sql.sql_van import *
from util import *
from concurrent.futures import wait, ALL_COMPLETED
from xml_population import *
from netezza_setup import get_netezza_df_updated, add_to_sf_sd
from src.building_util import add_to_xml


def changes_made(car_dict, policy_proposer_dict, additional_driver_list, last_transaction_ref, convictions_list,
                 claims_list, inception_date, pol_num, modifications_list, quote_occupations):

    tree = et.parse("../resource/sample.xml")
    change_aggs_timestamp(tree)
    cars_per_house = get_tree_tags(tree, tags.NOOFVEHICLESHOUSEHOLD.value)
    cars_per_house.text = str(int(car_dict[tags.NOOFVEHICLESHOUSEHOLD.value]))
    drivers_per_house = get_tree_tags(tree, tags.NOOFDRIVERSHOUSEHOLD.value)
    add_to_xml(drivers_per_house, car_dict)
    inception_date_tree_item = get_tree_tags(tree, "inceptionDate")
    inception_date_tree_item.text = str(pd.to_datetime(str(inception_date)).date())

    # Add car details
    main_van(tree, car_dict, modifications_list)

    # Add policy proposer details.
    main_driver(tree, policy_proposer_dict, convictions_list, claims_list, inception_date, quote_occupations)

    # Add additional drivers if any
    if len(additional_driver_list) > 0:
        main_additional(tree, additional_driver_list, convictions_list, claims_list, inception_date, quote_occupations)
    else:
        # remove unwanted stuff from xml
        driv = get_tree_tags(tree, "drivers")
        for additional in driv.findall("additionalDriver"):
            driv.remove(additional)

    root = tree.getroot()
    data = et.tostring(root, encoding="utf-8").decode("utf-8")
    out = clean_xml(data)
    tree = et.ElementTree(et.fromstring(out))
    tree.write(f"../requests/{get_date()}/final." + str(last_transaction_ref) + "." + str(pol_num) + ".xml")


def builder(cs, set_renewal_start_date, set_renewal_end_date, con):
    rn_quote = []
    startTime = time.time()
    """Code for using netezza Starts"""
    get_netezza_df_updated(set_renewal_start_date, set_renewal_end_date)
    df = pd.read_csv('../results/upload.csv')
    add_to_sf_sd(df, con)
    """Code for using netezza ends"""
    sql = "USE ROLE FG_RETAILPRICING;"
    cs.execute(sql)
    sql = "use warehouse WRK_RETAILPRICING_MEDIUM;"
    cs.execute(sql)
    vehicles = get_vehicle_info4(cs)
    drivers = get_driv4(cs)
    convictions = get_convictions(cs)
    claims = get_claims(cs)
    modifications = get_modifications(cs)
    occupations = get_occupations(cs)
    veh_excess = get_vehicle_info6(cs)
    counter = 0
    executionTime = (time.time() - startTime)
    print('query: ' + str(executionTime))
    path = f"../requests/{get_date()}/"
    manage_folders(path)

    stt2 = time.time()
    print(len(vehicles))
    for quotes in vehicles:
        if math.isnan(quotes[tags.GROSSWEIGHTVEHICLE.value]):
            quotes[tags.GROSSWEIGHTVEHICLE.value] = 10.0
        lastTransaction_reference = str(quotes[tags.QUOTEREFERENCE.value])
        rn_quote.append(quotes[tags.QUOTEREFERENCE.value])
        policy_number = str(quotes[tags.POLICYNUMBER.value])
        agghub_id = quotes[tags.AGGHUBID.value]
        renewal_date = pd.to_datetime(quotes[tags.RENEWALDATE.value])
        driver_list = get_items_with_agghub_id(drivers, agghub_id)
        volExcessList = get_items_with_agghub_id(veh_excess, agghub_id)
        if len(volExcessList) > 0:
            quotes[tags.VOLUNTARYEXCESS.value] = volExcessList[0][tags.COVERVOLXSALLOWED.value]
        else:
            quotes[tags.VOLUNTARYEXCESS.value] = "0"

        if quotes[tags.VOLUNTARYEXCESS.value] is None:
            quotes[tags.VOLUNTARYEXCESS.value] = "0"
            print("incorrect vol excess in pol_num = " + str(policy_number))

        quote_convictions = get_items_with_agghub_id(convictions, agghub_id)
        quote_claims = get_items_with_quote_ref(claims, lastTransaction_reference)
        quote_modifications = get_items_with_agghub_id(modifications, agghub_id)
        quote_occupations = get_items_with_agghub_id(occupations, agghub_id)
        if quotes is None or len(driver_list) <= 0:
            continue

        driver_one = get_policy_proposer(driver_list)
        if driver_one is None:
            continue
        additional_driver_list = find_additional_drivers(driver_list)
        counter += 1

        changes_made(quotes, driver_one, additional_driver_list, lastTransaction_reference,
                     quote_convictions, quote_claims, renewal_date,
                     policy_number, quote_modifications, quote_occupations)

    futures = get_store_responses()
    wait(futures, return_when=ALL_COMPLETED)
    print("wait completed")
    executionTime = (time.time() - stt2)
    print('Execution time in seconds: ' + str(executionTime))
