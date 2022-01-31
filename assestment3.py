"""
QUESTION:

  1.	Recommend how you would normalize this data, walk through the steps you would take in a  pseudocode.
  GB||EN||HFLFQ6|09Nov14||LHR-DXB-LHR||LHR||DXB||YY||2||Return||2014-11-09|20:08:51||2014-12-12||No-PromoCode

  Try to understand the value of each short-code
  
  2. Next co-related the value to possible datatype. For example :-
  String : GB,EN, HFLFQ6, LHR-DXB-LHR, LHR, DXB, YY, Return, No-PromoCode
  Date: 09Nov14, 2014-11-09, 2014-12-12
  Time: 20:08:51
  Integer: 2

  3. From there, each value can be co-relate with it’s mean. As assumption are like below :-
  GB = Airline code for departure
  EN = Airline code for return
  HFLFQ6 = Flight ID
  LHR-DXB-LHR = Destination route, its return not 1-way
  LHR = Departure location code
  DXB = Arrive location code
  Return = Flight ticket category
  No-PromoCode = Extra flight ticket information
  09Nov14 = Departure date
  2014-11-09 = Departure date
  2014-12-12 = Return date
  20:08:51 = Departure time
  2 = No of passenger 

  4. Write a python code to apply the logic above into file attached below, convert into a dictionary output then sav it as JSON file
  https://gitlab.com/im-batman/myassestment/-/blob/master/assestment_1st/151006120126_GA_OrderData_20150901-20150930.zip

APPROXIMATE TIME TAKEN: 
  - 1.5 HOUR

ANSWER Question 1:
Pseudocode
1. Create a Dataframe for the data
2. Make an assumptions for the column of the data
3. For the datelike data, change the date format to %Y-%m-%d
4. For the time data, change the time format to %H:%M:%S
5. For categorical data such as ticket_type, encode the label with number
6. For string data like symbol and flight ID, change the alphabets to uppercase
7. For string data like country name, change the string to lowercase

"""
#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import re
import numpy as np
from unidecode import unidecode
from datetime import datetime
import json
import os


def main():
    # Read csv data
    orderData = pd.read_csv(
        "151006120126_GA OrderData_20150901-20150930.csv",
        on_bad_lines="skip",
        delimiter="\t",
        header=None,
    )

    # Drop unused column
    dropped_empty_columns = orderData.iloc[:, :29]
    dropped_empty_columns.drop(
        dropped_empty_columns.columns[
            [
                2,
                20,
                4,
                6,
                7,
                9,
                11,
                13,
                15,
                17,
                19,
                22,
                24,
            ]
        ],
        axis=1,
        inplace=True,
    )

    # Rename column
    column_mapping = {
        0: "departure_date",
        1: "country_code",
        3: "lanaguage_code",
        5: "flight_code",
        8: "destination_route",
        10: "departure_loc_code",
        12: "origin_loc_code",
        14: "ticket_class",
        16: "no_pax",
        18: "ticket_type",
        21: "departure_time",
        23: "return_date",
        25: "has_promo",
        26: "country_name",
        27: "acquisition_type",
        28: "acquisition_source",
    }
    dropped_empty_columns.rename(columns=column_mapping, inplace=True)
    cleaned_orderData = dropped_empty_columns.copy()

    # Remove null route
    cleaned_orderData.dropna(subset=["destination_route"], inplace=True)

    # Fill Null in ticket_type
    cleaned_orderData["dash_count"] = cleaned_orderData.destination_route.apply(
        dash_calculator
    )
    t1 = cleaned_orderData["dash_count"] == 1
    t2 = cleaned_orderData["dash_count"] == 2
    t3 = cleaned_orderData["dash_count"] > 2

    cleaned_orderData.loc[t1, "ticket_type"] = cleaned_orderData.loc[
        t1, "ticket_type"
    ].fillna("one way")
    cleaned_orderData.loc[t2, "ticket_type"] = cleaned_orderData.loc[
        t2, "ticket_type"
    ].fillna("return")
    cleaned_orderData.loc[t3, "ticket_type"] = cleaned_orderData.loc[
        t3, "ticket_type"
    ].fillna("multicity")
    cleaned_orderData.drop(["dash_count"], axis=1, inplace=True)

    # Normalize ticket_type
    cleaned_orderData.ticket_type = cleaned_orderData.ticket_type.apply(
        normalize_ticket_type
    )
    cleaned_orderData.drop(
        cleaned_orderData[cleaned_orderData["ticket_type"] == "6ab"].index, inplace=True
    )

    # Normalize has_promo column
    cleaned_orderData.has_promo = cleaned_orderData.has_promo.apply(normalize_has_promo)

    # normalize date
    cleaned_orderData.departure_date = pd.to_datetime(
        cleaned_orderData.departure_date
    ).dt.strftime("%Y-%m-%d")
    cond = cleaned_orderData["return_date"].str.contains("(/)|(٠)")
    cleaned_orderData["return_date"] = np.where(
        cond, cleaned_orderData.return_date, None
    )
    cleaned_orderData["return_date"] = pd.to_datetime(
        cleaned_orderData["return_date"]
    ).dt.strftime("%Y-%m-%d")

    # Normalize Time
    cleaned_orderData["departure_time"] = cleaned_orderData["departure_time"].apply(
        normalize_time
    )

    # Normalize Acquisition Type
    cleaned_orderData.acquisition_type = cleaned_orderData.acquisition_type.apply(
        normalize_acquistion_type
    )

    # Normalize ticket_class
    cleaned_orderData.ticket_class = cleaned_orderData.ticket_class.apply(
        normalized_ticket_class
    )

    # Normalize country name
    cleaned_orderData.country_name = cleaned_orderData.country_name.str.lower()

    # Normalize np.nan to None so that can be converted into Json
    cleaned_orderData = cleaned_orderData.fillna(np.nan).replace([np.nan], [None])

    # Write to json
    df_to_json(cleaned_orderData)


# count dash on route
def dash_calculator(route):
    """
    Function to count the number of dash from destination_route
    """
    return route.count("-")


def normalize_ticket_type(ticket):
    if re.search("multi", ticket, re.IGNORECASE):
        return 1
    elif re.search("one", ticket, re.IGNORECASE):
        return 2
    elif re.search("return", ticket, re.IGNORECASE):
        return 3
    else:
        return None


def normalize_has_promo(promo):
    if re.match(r"^[A-Z0-9]*$", promo):
        return 1
    elif re.search("special", promo):
        return 1
    elif re.search("skywards", promo):
        return 1
    else:
        return 0


def normalize_time(time):
    if time.replace(":", "").isnumeric():
        try:
            time = time.split()[0]
            return pd.to_datetime(time, format="%H:%M:%S").time().strftime("%H:%M:%S")
        except:
            time = time.split()[0]
            result = datetime.strptime(
                unidecode(time).replace("S", "A").replace("m", "P"), "%H:%M:%S"
            )
            return result.time().strftime("%H:%M:%S")
    else:
        return None


def normalize_has_promo(promo):
    if re.match(r"^[A-Z0-9]*$", promo):
        return 1
    elif re.search("special", promo):
        return 1
    elif re.search("skywards", promo):
        return 1
    else:
        return 0


def normalize_acquistion_type(acq_type):
    if re.search("(none)|(not)", acq_type):
        return None
    elif re.search("mail", acq_type, re.IGNORECASE):
        return "email"
    elif re.search("social", acq_type, re.IGNORECASE):
        return "social_media"
    elif acq_type.isnumeric():
        return None
    else:
        return acq_type.lower()


def normalized_ticket_class(ticket):
    """
    Function to classify ticket class

    # F	full-fare First class,[5] on airlines which have first class distinct from business class.
    # J	full-fare Business class
    # W	full-fare Premium economy[6]
    # Y	full-fare Economy class
    """
    if type(ticket).__name__ != "str":
        return None
    if ticket[0] == "F":
        return 1
    elif ticket[0] == "J":
        return 2
    elif ticket[0] == "W":
        return 3
    elif ticket[0] == "Y":
        return 4
    elif re.search("First", ticket, re.IGNORECASE):
        return 1
    elif re.search("Business", ticket, re.IGNORECASE):
        return 2
    elif re.search("Economy", ticket, re.IGNORECASE):
        return 4
    else:
        return None


def df_to_json(df):
    """
    Function to convert df to json file
    """
    if not os.path.exists("assessment_3"):
        os.makedirs("assessment_3")
    data = df.to_dict(orient="records")
    out_file = open("assessment_3/orderData.json", "w")
    json.dump(data, out_file, indent=6)
    out_file.close()


if __name__ == "__main__":
    main()
