#!/usr/bin/env python

import pandas as pd
from dbfread import DBF
# from google.colab import files # This line is specific to Google Colab for file download
import os # Added for os.path.join in case user wants to run locally

# Install dbfread library if not already installed
# For local execution, you might need to run 'pip install dbfread' in your terminal
# get_ipython().system('pip install dbfread') # Uncomment if running in Colab and dbfread is not installed
print("dbfread library installed.")

# 1. Charger les données d'occupation des sols
# Make sure the CLC-12-59.dbf file is in the /content/ directory (or adjust path)
try:
    records = []
    for record in DBF('/content/CLC-12-59.dbf', encoding='latin-1'):
        records.append(record)
    df_land_cover = pd.DataFrame(records)
    print("df_land_cover created.")
except Exception as e:
    print(f"Erreur lors du chargement du fichier DBF : {e}. Assurez-vous que le fichier est bien présent dans /content/ et qu'il est accessible.")
    exit() # Exit if the file cannot be loaded

# 2. Identifier les informations sur les municipalités et l'occupation des sols (vérification)
print("\nUnique values in 'insee' column (municipality codes): ")
print(df_land_cover['insee'].unique())
print("\nUnique values in 'nom' column (municipality names): ")
print(df_land_cover['nom'].unique())
print("\nUnique values in 'CODE_12' column (land cover classifications): ")
print(df_land_cover['CODE_12'].unique())
print("\nData type of 'CODE_12' column: ")
print(df_land_cover['CODE_12'].dtype)

# 3. Filtrer et catégoriser l'occupation des sols pour le Nord
df_nord_land_cover = df_land_cover[df_land_cover['insee'].astype(str).str.startswith('59')].copy()

category_mapping = {
    '1': 'Surfaces artificielles',
    '2': 'Zones agricoles',
    '3': 'Forêts et zones semi-naturelles',
    '4': 'Zones humides',
    '5': "Plans d'eau"
}

def map_to_broad_category(code):
    if pd.isna(code):
        return None
    code_str = str(code)
    if code_str and code_str[0] in category_mapping:
        return category_mapping[code_str[0]]
    return 'Autre'

df_nord_land_cover['Broad_Category'] = df_nord_land_cover['CODE_12'].apply(map_to_broad_category)
print("\ndf_nord_land_cover with 'Broad_Category' created.")

# 4. Calculer le pourcentage par municipalité pour le Nord (Corrigé)
df_grouped_area_nord = df_nord_land_cover.groupby(['insee', 'nom', 'Broad_Category'])['AREA_HA'].sum().reset_index()
df_total_surf_ha_nord = df_nord_land_cover.groupby(['insee', 'nom'])['surf_ha'].mean().reset_index()
df_total_surf_ha_nord = df_total_surf_ha_nord.rename(columns={'surf_ha': 'Total_Municipality_Area_HA_from_surf'})
df_sum_area_ha_nord = df_nord_land_cover.groupby(['insee', 'nom'])['AREA_HA'].sum().reset_index()
df_sum_area_ha_nord = df_sum_area_ha_nord.rename(columns={'AREA_HA': 'Total_Municipality_Area_HA_from_AREA'})

df_merged_areas_nord_corrected = pd.merge(df_grouped_area_nord, df_total_surf_ha_nord, on=['insee', 'nom'])
df_merged_areas_nord_corrected = pd.merge(df_merged_areas_nord_corrected, df_sum_area_ha_nord, on=['insee', 'nom'])

def calculate_accurate_percentage(row):
    area_ha_category = row['AREA_HA']
    total_from_surf = row['Total_Municipality_Area_HA_from_surf']
    total_from_area = row['Total_Municipality_Area_HA_from_AREA']

    if total_from_area > total_from_surf and total_from_area > 0:
        return (area_ha_category / total_from_area) * 100
    elif total_from_surf > 0:
        return (area_ha_category / total_from_surf) * 100
    else:
        return 0

df_merged_areas_nord_corrected['Percentage'] = df_merged_areas_nord_corrected.apply(calculate_accurate_percentage, axis=1)

df_percentages_nord_corrected = df_merged_areas_nord_corrected.pivot_table(
    index=['insee', 'nom'],
    columns='Broad_Category',
    values='Percentage'
)

df_percentages_nord_corrected = df_percentages_nord_corrected.reset_index()
df_percentages_nord_corrected = df_percentages_nord_corrected.fillna(0)
print("\ndf_percentages_nord_corrected created.")

# 5. Exporter les résultats vers un fichier Excel
output_filename = 'land_cover_percentages_nord.xlsx'
df_percentages_nord_corrected.to_excel(output_filename, index=False)
print(f"\nLand cover percentages for Nord (corrected) saved to {output_filename}")

# 6. Vérification (optionnel)
df_excel_verification = pd.read_excel(output_filename)
print(f"\nContenu du fichier '{output_filename}' (vérification):")
print(df_excel_verification.head())