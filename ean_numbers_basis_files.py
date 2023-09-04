# het verkrijgen van de ean nummers en inkoop prijs voor later in andere scripts te gebruiken.

import os
import pandas as pd
import numpy as np
from datetime import datetime
import dropbox
import configparser
from pathlib import Path

date_now = datetime.now().strftime("%c").replace(":", "-")

alg_config = configparser.ConfigParser()
alg_config.read(Path.home() / "general_settings.ini")
dbx_api_key = alg_config.get("dropbox", "api_dropbox")
dbx = dropbox.Dropbox(dbx_api_key)

all_basis_files = dbx.files_list_folder(path="/macro/Basisbestanden", recursive=True)
basis_files = []


def process_entries_basis(entries):
    for entry in entries:
        if isinstance(entry, dropbox.files.FileMetadata):
            if "BasisBestand" in entry.name:
                basis_files.append(entry.name)


process_entries_basis(all_basis_files.entries)

while all_basis_files.has_more:
    all_basis_files = dbx.files_list_folder_continue(all_basis_files.cursor)
    process_entries_basis(all_basis_files.entries)

with open("basis_" + basis_files[-1], "wb") as f:
    metadata, res = dbx.files_download(path="/macro/Basisbestanden/" + basis_files[-1])
    f.write(res.content)

ean_basis = (
    pd.read_excel(
        max(Path.cwd().glob("basis_*.xlsm"), key=os.path.getctime),
        converters={"Verkoopprijs BOL (excl. comissie)": lambda x: round(pd.to_numeric(x, errors="coerce"), 2),},
        engine="openpyxl",
        usecols=["Product ID eigen","EAN", "EAN (handmatig)", "Inkoopprijs (excl. BTW)", "Verkoopprijs BOL (excl. comissie)"],
    )
    .dropna(subset=["EAN", "EAN (handmatig)"], thresh=1)  # rij verwijderen waneer bijde leeg
    .assign(ean=lambda x:np.where(((x['EAN (handmatig)'] != x['EAN (handmatig)']) ),x['EAN'],x['EAN (handmatig)'] ))
    .assign(ean=lambda x: pd.to_numeric(x["ean"].replace("\D", "0", regex=True), errors="coerce").astype(int, errors="ignore"))
    .sort_values("Inkoopprijs (excl. BTW)")
    .drop_duplicates("ean", keep="first")  # goedkopste aanbieder filteren
    .query("`Verkoopprijs BOL (excl. comissie)`.notnull()")# filteren van de artikelen die geen bol prijs hebben
)

# ean_basis.query("`EAN (handmatig)`.notna() and `EAN (handmatig)` != EAN and EAN == 0 ")
# ean_basis.query("`EAN (handmatig)`.isna() and `EAN (handmatig)` != EAN ")

ean_basis_kaal = ean_basis[["ean", "Inkoopprijs (excl. BTW)", "Verkoopprijs BOL (excl. comissie)"]]
ean_basis_eigen_ean = ean_basis[["Product ID eigen","ean"]]

ean_basis_kaal.to_csv("ean_numbers_basis_" + date_now + ".csv", index=False)
ean_basis_kaal.to_csv("ean_numbers_totaal_" + date_now + ".csv", index=False)
ean_basis_eigen_ean.to_csv("ean_eigennr_" + date_now + ".csv", index=False)
