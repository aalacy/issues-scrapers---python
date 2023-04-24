#import modules
import os
import glob
import pandas as pd
from pathlib import Path
import pdb

cur_path = '/media/com/ubuntu_work/pros/other'
extension = 'csv'
all_filenames = [i for i in Path(cur_path).rglob(f'*.{extension}')]

print(f"[csv] {len(all_filenames)} files")
#combine all files in the list
csv_data = []
for f in all_filenames:
    try:
        df = pd.read_csv(str(f), encoding="latin1", low_memory=False, on_bad_lines='skip')
        df.rename(columns=lambda x: x.strip(), inplace=True)
        df.rename(columns={"Sub category": "Sub Category"}, inplace=True)
        df.rename(columns={"Published at": "Published At"}, inplace=True)
        df.rename(columns={"Published year": "Published Year"}, inplace=True)
        df.rename(columns={"File path": "File Path"}, inplace=True)
        df.rename(columns={"Created at": "Created At"}, inplace=True)
        df.rename(columns={"Updated at": "Updated At"}, inplace=True)

        columns = df.columns.tolist()
        if 'Created At' in columns and 'Updated At' in columns:
            df.drop(columns=['Created At', 'Updated At'], inplace=True)
        csv_data.append(df[df['Published Year'] > 2018])
    except Exception as error:
        print(f"{f} ", error)
        pdb.set_trace()
        pass

if csv_data:
    combined_csv = pd.concat(csv_data)
    #export to csv
    combined_csv.to_csv(os.path.join(cur_path, "../other.csv"), index=False, encoding='latin1')