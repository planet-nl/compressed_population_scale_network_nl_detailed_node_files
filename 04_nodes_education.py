import pandas as pd
import polars as pl
from time import time
import sys
sys.stdout.reconfigure(encoding="utf-8")
import os
import pyreadstat

year = int(sys.argv[1])
output_folder = sys.argv[2]

# conversion
path = 'K:\\Utilities\\Code_Listings\\SSBreferentiebestanden\\'
conversion_df_1, meta_1 = pyreadstat.read_sav(os.path.join(path,'OPLEIDINGSNRREFV34.SAV'),apply_value_formats=False,usecols = ['OPLNR','CTO2016V'])
conversion_df_1 = conversion_df_1.drop(0)
conversion_df_1.columns = ['OPLNRHB_str','CTO']
#print(conversion_df_1.head())

conversion_df_2, meta_2 = pyreadstat.read_sav(os.path.join(path,'CTOREFV12.sav'),usecols = ['CTO','OPLNIVSOI2016AGG4HB'])
#print(conversion_df_2.head())

#print(len(conversion_df_1))
conversion_df = conversion_df_1.merge(conversion_df_2, on='CTO',how='left')
conversion_pl = pl.from_pandas(conversion_df)

educ_files = {2009: r"G:\Onderwijs\HOOGSTEOPLTAB\2009\120619 HOOGSTEOPLTAB 2009V1.csv",
              2010: r"G:\Onderwijs\HOOGSTEOPLTAB\2010\120918 HOOGSTEOPLTAB 2010V1.csv",
              2011: r"G:\Onderwijs\HOOGSTEOPLTAB\2011\130924 HOOGSTEOPLTAB 2011V1.csv",
              2012: r"G:\Onderwijs\HOOGSTEOPLTAB\2012\141020 HOOGSTEOPLTAB 2012V1.csv", 
              2013: r"G:\Onderwijs\HOOGSTEOPLTAB\2013\HOOGSTEOPL2013TABV3.csv", 
              2014: r"G:\Onderwijs\HOOGSTEOPLTAB\2014\HOOGSTEOPL2014TABV3.csv",
              2015: r"G:\Onderwijs\HOOGSTEOPLTAB\2015\HOOGSTEOPL2015TABV3.csv",
              2016: r"G:\Onderwijs\HOOGSTEOPLTAB\2016\HOOGSTEOPL2016TABV2.csv",
              2017: r"G:\Onderwijs\HOOGSTEOPLTAB\2017\HOOGSTEOPL2017TABV3.csv", 
              2018: r"G:\Onderwijs\HOOGSTEOPLTAB\2018\HOOGSTEOPL2018TABV3.csv", 
              2019: r"G:\Onderwijs\HOOGSTEOPLTAB\2019\HOOGSTEOPL2019TABV2.csv",
              2020: r"G:\Onderwijs\HOOGSTEOPLTAB\2020\HOOGSTEOPL2020TABV2.csv",  
              2021: r"G:\Onderwijs\HOOGSTEOPLTAB\2021\HOOGSTEOPL2021TABV2.csv",  
              2022: r"G:\Onderwijs\HOOGSTEOPLTAB\2022\HOOGSTEOPL2022TABV2.csv", 
              2023: r"G:\Onderwijs\HOOGSTEOPLTAB\2023\HOOGSTEOPL2023TABV2.csv", 
              2024: r"G:\Onderwijs\HOOGSTEOPLTAB\2024\HOOGSTEOPL2024TABV1.csv"}

educ_column =  {2009: "OPLNRHB",
                2010: "OPLNRHB",
                2011: "OPLNRHB",
                2012: "OPLNRHB",
                2013:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2014:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2015:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2016:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2017:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2018:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2019:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2020:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2021:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2022:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2023:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2024:"OPLNIVSOI2021AGG4HBmetNIRWO"}

conv_column =  {2009: "OPLNIVSOI2016AGG4HB",
                2010: "OPLNIVSOI2016AGG4HB",
                2011: "OPLNIVSOI2016AGG4HB",
                2012: "OPLNIVSOI2016AGG4HB",
                2013:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2014:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2015:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2016:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2017:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2018:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2019:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2020:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2021:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2022:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2023:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2024:"OPLNIVSOI2021AGG4HBmetNIRWO"}

if year == 2023:
    education_input = pl.read_csv(educ_files[year], columns = ['RINPERSOON',educ_column[year],'GEWICHTHOOGSTEOPL'],separator=";")
else:
    education_input = pl.read_csv(educ_files[year], columns = ['RINPERSOON',educ_column[year],'GEWICHTHOOGSTEOPL'])
print(f"Loaded data for {year}")

if year > 2012:
    education_input = (
        education_input.
        with_columns(
            pl.col(educ_column[year]).cast(pl.Utf8).str.slice(0,1).alias("educ_level")
        )
        .rename({
            "GEWICHTHOOGSTEOPL":"educ_weight",
            "RINPERSOON":"label"}
        )
        .drop(educ_column[year])
        .with_columns(pl.col("label").cast(pl.Int64))
    )
    
else:
    education_input = (education_input
        .with_columns(
            pl.col("OPLNRHB").cast(pl.Utf8).str.zfill(5).alias("OPLNRHB_str")
        )
    )

    education_input = education_input.join(conversion_pl,on="OPLNRHB_str",how="inner")

    education_input = (
        education_input.with_columns(
            pl.col(conv_column[year]).cast(pl.Utf8).str.slice(0,1).alias("educ_level")
        )
        .drop(['OPLNRHB','OPLNRHB_str','CTO','OPLNIVSOI2016AGG4HB'])
        .rename({
            "GEWICHTHOOGSTEOPL":"educ_weight",
            "RINPERSOON":"label"}
        ).with_columns(
            pl.col("label").cast(pl.Int64)
        )
    )
    

output = os.path.join(output_folder,"temp",f"education_{year}.csv")
print(f"Saving results to {output}...")
education_input.write_csv(
    output,
    include_header=True
)
print("Done.")

print("\t zipping...")
os.system(f"gzip -f {output}")
print("\tDone.")