# here goes the code to dowload fil from the SEC

import pandas as pd

file_name = 'company_2023QTR2.idx'

idx_table = pd.read_table("C:\\Users\\Fosco\\Desktop\\" + file_name)

# Remove unnecessary header and footer lines
a = idx_table.iloc[5:, :]

# Parse data into a list of dictionaries
data = []
for line in list(a.values):

    temp_string = str(line[0])
    temp_array = [element for element in temp_string.split('  ') if element != '' and (not element.isspace())]

    data.append({
        "Company Name": temp_array[0],
        "Form Type": temp_array[1],
        "CIK": temp_array[2],
        "Date Filed": temp_array[3],
        "File Name": temp_array[4]
    })

# Create DataFrame
df = pd.DataFrame(data)