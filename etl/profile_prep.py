import pandas as pd
import csv


tmp_df = pd.DataFrame(columns=["Country","Sector","Industry","Exchange",
                               "Description","CEO", "Address", "City", "State"])

with open("C://demo//demo1//Stock_List.csv") as stocklist:
#with open("C:\\Users\\desti\\Documents\\GitHub\\FYP_JC2007\\Source\\Stock_List.csv") as stocklist:
    stocklist_r = csv.reader(stocklist)
    next(stocklist_r)

    for row in stocklist_r:
        print(row[0])
        stockname = row[0]


        ######################################################
        #               Fetching from profile                #
        ######################################################
        try:
            filename = "C://demo//demo1//profile//profile//"+stockname+"_Profile.csv"
            with open(filename) as profile:
            #with open("C:\\Users\\desti\\Documents\\GitHub\\FYP_JC2007\\Example\\Profile.csv") as profile:
                profile_r = {}
                for row in csv.reader(profile):
                    profile_r[row[0]] = row

                print(profile_r)
                if profile_r.get("country")[1] =="":
                    country = "NaN"
                else:
                    country = profile_r.get("country")[1]

                if profile_r.get("sector")[1]=="":
                    sector="NaN"
                else:
                    sector = profile_r.get("sector")[1]

                if profile_r.get("industry")[1] == "":
                    industry = "NaN"
                else:
                    industry = profile_r.get("industry")[1]

                if profile_r.get("exchange")[1] =="":
                    exchange = "NaN"
                else:
                    exchange = profile_r.get("exchange")[1]

                if profile_r.get("description")[1] =="":
                    description = "NaN"
                else:
                    description = profile_r.get("description")[1]

                if profile_r.get("ceo")[1] =="":
                    ceo = "NaN"
                else:
                    ceo = profile_r.get("ceo")[1]

                if profile_r.get("address")[1] =="":
                    address = "NaN"
                else:
                    address = profile_r.get("address")[1]

                if profile_r.get("city")[1] =="":
                    city = "NaN"
                else:
                    city = profile_r.get("city")[1]

                if profile_r.get("state")[1] =="":
                    state = "NaN"
                else:
                    state = profile_r.get("state")[1]
        except IOError:
            country = "NaN"
            industry = "NaN"
            sector = "NaN"
            exchange = "NaN"
            description = "NaN"
            ceo = "NaN"
            address = "NaN"
            city = "NaN"
            state = "NaN"


        # How about building another database of country codes to ease this process

        if country == "Argentina":
            country = "AR"

        if country == "United States":
            country = "US"

        if country == "Australia":
            country = "AU"

        if country == "Belgium":
            country = "BE"

        if country == "Bermuda":
            country = "BM"

        if country == "Brazil":
            country = "BR"

        if country == "Canada":
            country = "CA"

        if country == "China":
            country = "CN"

        if country == "Chile":
            country = "CH"

        if country == "Colombia":
            country = "CO"

        if country == "Finland":
            country = "FIN"

        if country == "Denmark":
            country = "DK"

        if country == "France":
            country = "FR"

        if country == "Hong Kong":
            country = "HK"

        if country == "Germany":
            country = "GM"

        if country == "Israel":
            country = "IL"

        if country == "India":
            country = "IN"

        if country == "Ireland":
            country = "IRE"

        if country == "Italy":
            country = "IT"

        if country == "Japan":
            country = "JP"

        if country == "Luxembourg":
            country = "LU"

        if country == "Mexico":
            country = "MX"

        if country == "Netherlands":
            country = "NL"

        if country == "Taiwan":
            country = "TW"

        if country == "United Kingdom":
            country = "UK"

        if country == "Norway":
            country = "NO"

        if country == "Peru":
            country = "PE"

        if country == "Philippines":
            country = "PH"

        if country == "Russia":
            country = "RU"

        if country == "Singapore":
            country = "SG"

        if country == "South Korea":
            country = "KR"

        if country == "Sweden":
            country = "SW"

        if country == "Switzerland":
            country = "SZ"

        if country == "Spain":
            country = "SP"

        if country == "South Africa":
            country = "SA"

        if country == "N/A":
            country = "NaN"

        if sector == "N/A":
            sector = "NaN"

        ######################################################
        #              Put data into dataframe               #
        ######################################################
        stockname = pd.DataFrame({"Country":country,"Sector":sector,"Industry":industry,
                                  "Exchange":exchange, "Description":description, "CEO":ceo,
                                  "Address":address, "City":city, "State":state}, index=[stockname])

        tmp_df = tmp_df.append(stockname)


    print(tmp_df)
    tmp_df.to_csv("Stocks_Dataset_V2.csv")
