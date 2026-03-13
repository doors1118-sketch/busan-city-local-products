import os
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

target_dir = r"c:\Users\COMTREE\Desktop\연습"

master_file = os.path.join(target_dir, "부산광역시 조달 수요기관 마스터파일.csv")
merged_file = os.path.join(target_dir, "부산_수요기관_통합목록.xlsx")

# 1. Load data
try:
    df_master = pd.read_csv(master_file, encoding='utf-8', dtype=str)
except Exception:
    df_master = pd.read_csv(master_file, encoding='cp949', dtype=str)

df_merged = pd.read_excel(merged_file, dtype=str)

# Clean master missing values
df_master = df_master.fillna('')
df_merged = df_merged.fillna('')

# master columns: '수요기관명', '대분류', '중분류', '소분류', '세부분류', '수요기관코드', '수요기관사업자등록번호'
# merged columns: '수요기관명', '수요기관코드', '사업자등록번호'

# Create a dictionary for fast lookup from merged_file
# key: 수요기관명, value: {'code': ..., 'biz': ...}
merged_dict = {}
for idx, row in df_merged.iterrows():
    name = str(row['수요기관명']).strip()
    code = str(row['수요기관코드']).strip()
    biz = str(row['사업자등록번호']).strip()
    merged_dict[name] = {'code': code, 'biz': biz}

stats = {
    'updated': 0,
    'added': 0,
    'unchanged': 0
}

# 2. Update existing records in master
master_names = set()
updated_rows = []

for idx, row in df_master.iterrows():
    name = str(row['수요기관명']).strip()
    master_names.add(name)
    
    m_code = str(row['수요기관코드']).strip()
    m_biz = str(row['수요기관사업자등록번호']).strip()
    
    if name in merged_dict:
        t_code = merged_dict[name]['code']
        t_biz = merged_dict[name]['biz']
        
        changed = False
        if t_code and m_code != t_code:
            row['수요기관코드'] = t_code
            changed = True
        if t_biz and m_biz != t_biz:
            row['수요기관사업자등록번호'] = t_biz
            changed = True
            
        if changed:
            stats['updated'] += 1
        else:
            stats['unchanged'] += 1
    else:
        stats['unchanged'] += 1
        
    updated_rows.append(row)

# 3. Add new records from merged_file
new_rows = []
for name, info in merged_dict.items():
    if name not in master_names:
        new_row = {
            '수요기관명': name,
            '대분류': '',
            '중분류': '',
            '소분류': '',
            '세부분류': '',
            '수요기관코드': info['code'],
            '수요기관사업자등록번호': info['biz']
        }
        new_rows.append(new_row)
        stats['added'] += 1

# 4. Generate the final dataframe
df_final = pd.DataFrame(updated_rows)

if new_rows:
    df_new = pd.DataFrame(new_rows)
    df_final = pd.concat([df_final, df_new], ignore_index=True)

# 5. Save the final file
output_file = os.path.join(target_dir, "부산광역시 조달 수요기관 마스터파일_최종본.csv")
df_final.to_csv(output_file, index=False, encoding='utf-8-sig')

print(f"Update statistics:")
print(f" - Existing records updated (코드/사업자번호 수정): {stats['updated']}")
print(f" - Existing records unchanged: {stats['unchanged']}")
print(f" - New records added (마스터에 없던 기관): {stats['added']}")
print(f"Total rows in new master file: {len(df_final)}")
print(f"Saved to: {output_file}")
