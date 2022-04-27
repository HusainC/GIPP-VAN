import csv
from pathlib import Path

title = {
    "003": "Mr",
    "004": "MRS",
    "005": "Ms",
    "002": "Miss",
    "3_dr": "Dr -Male",
    "5_dr_female": "Dr- Female"
}
marital_status = {
    "D": "Divorced",
    "M": "Married",
    "C": "Common Law",
    "A": "Seperated",
    "B": "Civil Partnered",
    "S": "Single",
    "W": "Widowed",
    "P": "Partner"

}

type_of_licence = {
    "F_uk_manual": "Full UK - Manual",
    "F_uk_auto": "Full UK - Auto",
    "F_uk_iam": "Full UK - IAM",
    "F_uk_pass_plus": "Full UK - Pass Plus",
    "F_full_eu": "Full EU",
    "H_full_non_eu": "Full European (non EU)",
    "N_full_international": "Full International",
    "P_provisional_uk": "Provisional UK",
    "O_provisional_eu": "Provisional EU",
    "U_provisional_noneu": "Provisional European (non EU)",
    "0_provisional_inter": "Provisional International",
    "F_FM": "Full UK - Manual",
    "F_FA": "Full UK - Auto",
    "F_FI": "Full UK - IAM",
    "F_FP": "Full UK - Pass Plus",
    "E_FE": "Full EU",
    "H_FN": "Full European (non EU)",
    "N_FW": "Full International",
    "P_PU": "Provisional UK",
    "O_PE": "Provisional EU",
    "U_PN": "Provisional European (non EU)",
    "0_PW": "Provisional International",
    "Full UK - Manual": "F_FM",
    "Full UK - Auto": "F_FA",
    "Full UK - IAM": "F_FI",
    "Full UK - Pass Plus": "F_FP",
    "Full EU": "E_FE",
    "Full European (non EU)": "H_FN",
    "Full International": "N_FW",
    "Provisional UK": "P_PU",
    "Provisional EU": "O_PE",
    "Provisional European (non EU)": "U_PN",
    "Provisional International": "0_PW",
    "A": "Accident",
    "Z": "Theft of vehicle",
    "Q": "Theft from vehicle",
    "D": "Storm / Flood Damage",
    "F": "Fire",
    "Y": "Malicious Damage",
    "W": "Windscreen"
}

owner = {
    "1": "1_PR",
    "1_leased_private": "4_LP",
    "2": "2_SP",
    "3": "3_CO",
    "4": "4_LC",
    "6": "6_FP",
    "7": "7_CL",
    "8": "8_FC",
    "9": "9_CS",
    "9_society_club": "9_OT",
    "E" : "E_CP",
    "H": "H_FO",
    "H_sibling": "H_FS",
    "1_PR": "Proposer",
    "4_LP": "Leased - private",
    "2_SP": "Spouse",
    "3_CO": "Company",
    "4_LC": "Leased - company",
    "6_FP": "Parent",
    "7_CL": "Common Law Partner",
    "8_FC": "Son/Daughter",
    "9_CS": "Society / Club",
    "9_OT": "Other",
    "E_CP": "Civil Partner",
    "H_FS": "Brother / Sister",
    "H_FO": "Other Family Member"
}

registered_keeper = {
    "1" : "1_PR",
    "1_leased_private": "1_LP",
    "2": "2_SP",
    "3": "3_CO",
    "4": "4_LC",
    "6": "6_FP",
    "7": "7_CL",
    "8": "8_FC",
    "9": "9_CS",
    "9_society_club": "9_OT",
    "E" : "E_CP",
    "H": "H_FO",
    "H_sibling": "H_FS",
    "1_PR": "Proposer",
    "1_LP": "Leased - private",
    "2_SP": "Spouse",
    "3_CO": "Company",
    "4_LC": "Leased - company",
    "6_FP": "Parent",
    "7_CL": "Common Law Partner",
    "8_FC": "Son/Daughter",
    "9_CS": "Society / Club",
    "9_OT": "Other",
    "E_CP": "Civil Partner",
    "H_FS": "Brother / Sister",
    "H_FO": "Other Family Member"
}
other_vehicle = {
    "no": "No",
    "own_another_car": "Yes - own another car",
    "named_driver_nonhd": "Yes - as named driver on another car",
    "own_motorcycle": "Yes - own/have use of a motorcycle",
    "own_van": "Yes - own/have use of a Van",
    "business_car": "Yes - on company car for business use only",
    "social_car": "Yes - on company car with social use",
    "named_driver": "Yes - as named driver on another car"


}
kept_overnight = {
    "4": "Drive",
    "7": "Locked Compound",
    "1": "Locked Garage",
    "2": "Private Property",
    "F": "Public Car Park",
    "H": "Street away from home",
    "3": "Street outside home",
    "B": "Unlocked compound",
    "I": "Unlocked Garage",
    "E": "Work Car Park"
}

immobiliser = {
    "92": "Factory fitted Alarm + Immobiliser",
    "93": "Factory Fitted immobiliser",
    "N": "No security device",
    "91": "Non-Factory fitted Alarm + Immobiliser",
    "94": "Non-Factory Fitted immobiliser",
    "99": "Thatcham Approved Cat 1",
    "100": "Thatcham Approved Cat 2"
}

# pretty sure the 04/01 social ones need to be removed
class_of_use = {
    "S": "Social only",
    "C": "Social inc. Comm",
    "1": "Business Use (PH)",
    "4": "Business use (PH + Spouse / Civil Partner)",
    "N": "Business use (spouse / Civil Parnter)",
    "2": "Business use by all drivers",
    "3": "Commercial travelling",
    "0S": "Social only",
    "0C": "Social inc. Comm",
    #"01": "Business Use (PH)",
    #"04": "Business use (PH + Spouse / Civil Partner)",
    "0N": "Business use (spouse / Civil Partner)",
    "02": "Business use by all drivers",
    "03": "Commercial travelling",
    "04": "Social, Domestic And Pleasure",
    "01": "Commuting",
    "10": "Carriage Of Own Goods",
    "09": "Carriage Of Goods For Hire And Reward",
    "19": "Business Use (PH)",
    "18": "Business use (PH + Spouse / Civil Partner)",
    "20": "Business use (spouse / Civil Partner)",
    "06": "Commercial travelling"

}

relationship_proposer = {
    "P": "Proposer",
    "S": "Spouse",
    "J": "Civil Partner",
    "W": "Common law Partner",
    "M": "Parent",
    "O": "Son/Daughter",
    "A": "Brother / Sister",
    "F": "Other family member",
    "C": "Business partner",
    "E": "Employee",
    "B": "Employer",
    "U": "Other / No Relation"
}

import_type = {
    "no": "No",
    "yes": "Yes",
    "yes_uk_import": "Yes - UK spec European import",
    "yes_euro_import": "Yes - Other European import",
    "yes_japanese_import": "Yes - Japanese import",
    "yes_us_import": "Yes - US import"
}

tracker = {
    "false": "tracker not fitted",
    "true": "tracker fitted"

}

cover_type = {
    "comprehensive": "Comprehensive",
    "tpft": "Third Party, Fire And Theft",
    "thirdParty": "Third Party Only"
}

voluntary_excess = {
    "0": "0 voluntary excess",
    "50": "50 voluntary excess",
    "100": "100 voluntary excess",
    "150": "150 voluntary excess",
    "200": "200 voluntary excess",
    "250": "250 voluntary excess",
    "300": "300 voluntary excess",
    "350": "350 voluntary excess",
    "400": "400 voluntary excess",
    "450": "450 voluntary excess",
    "500": "500 voluntary excess",
    "550": "550 voluntary excess",
    "600": "600 voluntary excess",
    "650": "650 voluntary excess",
    "700": "700 voluntary excess",
    "750": "750 voluntary excess",
    "800": "800 voluntary excess",
    "850": "850 voluntary excess",
    "900": "900 voluntary excess",
    "950": "950 voluntary excess",
    "1000": "1000 voluntary excess"
}

how_ncd = {
    "21": "Company Car - Business Only",
    "2": "Commercial Vehicle",
    "22": "Company Car - Including Social",
    "8": "Motor Cycle",
    "23": "Other",
    "11": "Private Car Bonus",
    "12": "Private Hire",
    "13": "Public Hire",
    "18": "Policyholders Civil Partner",
    "14": "Spouse Of Policyholder"
}

employment_status = {
    "V": "Voluntary Work",
    "E": "Employed",
    "F": "In Full Or Part Time Education",
    "H": "Household Duties",
    "S": "Self Employed",
    "R": "Retired",
    "U": "Unemployed",
    "I": "Independent Means",
    "N": "Not employed due to disability"
}
claim_type = {
    "A": "Accident",
    "Z": "Theft of vehicle",
    "Q": "Theft from vehicle",
    "D": "Storm / Flood Damage",
    "F": "Fire",
    "Y": "Malicious Damage",
    "W": "Windscreen"
}
medical_conditions = {
    "99_NO": "No",
    "99_D0": "DVLA - No restrictions",
    "99_D1": "DVLA - 1 year restricted Licence",
    "99_D2": "DVLA - 2 year restricted Licence",
    "99_D3": "DVLA - 3 year restricted Licence",
    "99_DU": "DVLA unaware",
    "99_DA": "Doctor Advised not to drive"
}


def read_csv(file):
    out_dict = {}
    with open(Path(f"./{file}"), "r", encoding="utf-8", newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        next(reader)  # Skipping header row
        for row in reader:
            out_dict[row[0]] = row[1]
    return out_dict


business_dict = read_csv("../resource/ABI_business_codes.csv")
occupation_dict = read_csv("../resource/ABI_occupation_codes.csv")
conviction_dict = read_csv("../resource/ABI_convictions_codes.csv")
car_make_dict = read_csv("../resource/ABI_car_make_codes.csv")
modifications_dict = read_csv("../resource/ABI_modifications_codes.csv")

type_of_address = {}
primary_phoneno = {}
fuel_type = {
    "001": "Petrol"
}

code_desc = {
        "title": title,
        "maritalStatus": marital_status,
        "type": type_of_licence,
        "registeredKeeper": registered_keeper,
        "useOtherVehicle": other_vehicle,
        "parkedOvernight": kept_overnight,
        "immobiliser": immobiliser,
        "classOfUse": class_of_use,
        "relationship": relationship_proposer,
        "parkedDaytimeData": kept_overnight,
        "importType": import_type,
        "tracker": tracker,
        "owner": owner,
        "coverType": cover_type,
        'voluntaryExcess': voluntary_excess,
        "howNcdEarn": how_ncd,
        "employmentStatusCode": employment_status,
        "employmentStatusCode": employment_status,
        "employmentOccupationCode": occupation_dict,
        "employmentBusinessCode": business_dict,
        "convictionCode": conviction_dict,
        "medicalConditions": medical_conditions,
        "claim_desc": claim_type,
        "abiCode": car_make_dict,
        "modificationAbiCode": modifications_dict
}
