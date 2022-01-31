"""
QUESTION:
  Given a list of bank in below URL :
  https://www.fdic.gov/resources/resolutions/bank-failures/failed-bank-list/

  Write a code to produce an output to PDF exactly like below :
  https://gitlab.com/im-batman/myassestment/-/blob/master/result.pdf

USE CASE:
  - Data should not contain bank from state NE,KS and FL
  - Data only contain bank name, city, state, closing date and current timestamp
  - Data sort base on State ascending and City descending
  - current_tmstmp column are base on MYT time
  - For odd no of rows will set as #FFF as background color (OPTIONAL)
  - For even no of rows will set as #CCC as background color (OPTIONAL)

HINT:
  - You can install additional Python packages if needed
  - DO NOT Download the data and stored in local, expected to be done realtime

APPROXIMATE TIME TAKEN: 
  - 1 HOUR

EXTERNAL LIBRARY:
  - Install wkhtmltopdf `brew install wkhtmltopdf`

"""
import pandas as pd
import numpy as np
import pdfkit
import os


def main():
    cleaned_df = data_preparation()
    html = convert_df_to_color_html(cleaned_df)
    create_pdf_from_html(html)


def data_preparation(
    url="https://www.fdic.gov/resources/resolutions/bank-failures/failed-bank-list/",
) -> pd.DataFrame:
    """
    A function data read data from html and return cleaned data
    @return DataFrame
    """
    failed_bank_df = pd.read_html(url)
    failed_bank_df = failed_bank_df[0]

    # Drop unused columns
    failed_bank_df.drop(
        ["CertCert", "Acquiring InstitutionAI", "FundFund"], axis=1, inplace=True
    )

    # Rename columns
    mapper_dict = {
        "Bank NameBank": "bank_nm",
        "CityCity": "city",
        "StateSt": "state",
        "Closing DateClosing": "close_dt",
    }
    failed_bank_df.rename(columns=mapper_dict, inplace=True)

    # Filter out some state: NE,KS and FL
    remove_state = ["NE", "KS", "FL"]
    cleaner_df = failed_bank_df[~failed_bank_df.state.isin(remove_state)]
    cleaner_df["current_tmstmp"] = pd.Timestamp.now("Asia/Kuala_Lumpur")

    # Reset index
    cleaner_df.reset_index(inplace=True)
    cleaner_df.drop(["index"], axis=1, inplace=True)
    return cleaner_df


def convert_df_to_color_html(cleaner_df) -> object:
    """
    Function to create colored html

    @return object
    """
    colored_df = cleaner_df.style.apply(rower, axis=None)
    html = colored_df.to_html()
    return html


def rower(data) -> pd.DataFrame:
    """
    Function to color odd row and even row

    @return DataFrame
    """
    s = data.index % 2 != 0
    s = pd.concat([pd.Series(s)] * data.shape[1], axis=1)  # 6 or the n of cols u have
    z = pd.DataFrame(
        np.where(s, "background-color:#fff", "background-color:#ccc"),
        index=data.index,
        columns=data.columns,
    )
    return z


def create_pdf_from_html(html):
    """
    Function to create pdf file
    """
    if not os.path.exists("assessment_1"):
        os.makedirs("assessment_1")

    pdfkit.from_string(html, "assessment_1/output.pdf")


if __name__ == "__main__":
    main()
