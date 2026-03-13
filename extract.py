import os
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

target_dir = r"c:\Users\COMTREE\Desktop\연습"
files = os.listdir(target_dir)

all_data = []

# 부산광역시 확인 로직을 위한 키워드
busan_keywords = ['부산광역시', '부산', '부산시']

for f in files:
    if f.startswith('~$') or (not (f.endswith('.xlsx') or f.endswith('.csv'))):
        continue
        
    f_path = os.path.join(target_dir, f)
    print(f"Processing: {f}")
    
    # Read file
    try:
        if f.endswith('.csv'):
            try:
                # Try reading as tab separated utf-16 or utf-8
                try:
                    df = pd.read_csv(f_path, sep='\t', encoding='utf-16', dtype=str)
                    if len(df.columns) == 1: raise Exception()
                except:
                    df = pd.read_csv(f_path, sep='\t', encoding='utf-8-sig', dtype=str)
            except:
                df = pd.read_csv(f_path, encoding='cp949', dtype=str)
                if len(df.columns) == 1:
                     df = pd.read_csv(f_path, sep='\t', encoding='cp949', dtype=str)
        else:
            df = pd.read_excel(f_path, dtype=str)
    except Exception as e:
        print(f"Failed to read {f}: {e}")
        continue

    # We need to find the correct columns for Name, Code, BizNum
    # Column row could be the 0th row or header itself.
    
    # Strategy: convert all rows (including headers) to string, and look for patterns
    # Actually, pandas might have put it in columns or row 0.
    # Let's rebuild the dataframe to have flat list of lists
    raw_data = [list(df.columns)] + df.values.tolist()
    
    # Find the header row index
    header_idx = -1
    for i, row in enumerate(raw_data[:10]):
        row_str = ' '.join(str(x) for x in row if pd.notnull(x))
        if '수요기관' in row_str or '사업자등록번호' in row_str:
            header_idx = i
            break
            
    if header_idx == -1:
        print(f"Could not find header in {f}")
        continue
        
    headers = raw_data[header_idx]
    
    # Map column indices to data
    code_idx = -1
    name_idx = -1
    biz_idx = -1
    
    # Handle cases like ['수요기관', NaN, '수요기관사업자등록번호']
    # And the next row data determines which is code and which is name
    
    col_mapping = {}
    for i, h in enumerate(headers):
        val = str(h).strip().replace('\n', '').replace(' ', '')
        if '사업' in val or '사업자번호' in val:
            biz_idx = i
        elif val == '수요기관':
            # This could mean the next col is Name, and this col is Code, or vice versa
            # We determine by looking at next row data.
            pass
        elif '수요기관코드' in val or '기관코드' in val:
            code_idx = i
        elif '수요기관명' in val or '기관명' in val:
            name_idx = i
            
    # Resolve ambiguous '수요기관' column
    for i, h in enumerate(headers):
        val = str(h).strip().replace('\n', '').replace(' ', '')
        if val == '수요기관':
            if i + 1 < len(headers) and str(headers[i+1]) == 'nan' or 'Unnamed' in str(headers[i+1]):
                # Needs to check first data row to see which is digit (code usually numeric or starts with char+digits)
                data_row = next((r for r in raw_data[header_idx+1:] if len(r) > i+1 and pd.notnull(r[i])), None)
                if data_row:
                    v1 = str(data_row[i]).strip()
                    v2 = str(data_row[i+1]).strip()
                    if v1.isdigit() or (len(v1)>0 and v1[0].isalpha() and v1[1:].isdigit()):
                        code_idx = i
                        name_idx = i + 1
                    else:
                        name_idx = i
                        code_idx = i + 1
            else:
                # If there's no adjacent blank, maybe it's just '수요기관' as Name? We'll check the data.
                data_row = next((r for r in raw_data[header_idx+1:] if len(r) > i and pd.notnull(r[i])), None)
                if data_row:
                    v = str(data_row[i]).strip()
                    if v.isdigit() or (len(v)>0 and v[0].isalpha() and v[1:].isdigit() and len(v) < 15):
                        # likely code
                        code_idx = i
                        # Try to find name col
                    else:
                        name_idx = i
                        
    if name_idx == -1 and code_idx != -1:
        # maybe another column?
        pass

    # For files where '수요기관사업자등록번호', '수요기관', NaN
    if '조달요청' in f:
        # Based on output: Unnamed: 0 = BizNum, 1 = Code, 2 = Name
        for i, h in enumerate(headers):
            val = str(h).strip()
            if '사업자등록번호' in val:
                biz_idx = i
            elif val == '수요기관':
                code_idx = i
                name_idx = i + 1

    print(f"Matched columns -> Name: {name_idx}, Code: {code_idx}, Biz: {biz_idx}")
    
    # Extract data
    for row in raw_data[header_idx+1:]:
        name_val = str(row[name_idx]).strip() if name_idx != -1 and name_idx < len(row) else ''
        code_val = str(row[code_idx]).strip() if code_idx != -1 and code_idx < len(row) else ''
        biz_val = str(row[biz_idx]).strip() if biz_idx != -1 and biz_idx < len(row) else ''
        
        if name_val == 'nan': name_val = ''
        if code_val == 'nan': code_val = ''
        if biz_val == 'nan': biz_val = ''
        
        if not name_val and not code_val:
            continue
            
        all_data.append({
            '수요기관명': name_val,
            '수요기관코드': code_val,
            '사업자등록번호': biz_val,
            '출처파일': f
        })

print(f"Total rows extracted: {len(all_data)}")

# Convert to DataFrame
df_all = pd.DataFrame(all_data)

# Extract only rows that contain '부산' in '수요기관명' or if the user instruction meant generally "they are from Busan"
# User instruction: "해당 파일들은 부산광역시에 소재한 조달청 수요기관의 명칭...이 기재되어 있다". 
# So all valid rows are probably from Busan. We just group them all.

# Clean up values (remove newlines, extra spaces)
for col in ['수요기관명', '수요기관코드', '사업자등록번호']:
    df_all[col] = df_all[col].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
    df_all[col] = df_all[col].replace('nan', '')

# Remove empty names entirely
df_all = df_all[df_all['수요기관명'] != '']

# Grouping logic
# If texts are completely identical -> merge into one.
# If texts are similar but different -> add as different.
# Which exactly means drop duplicates by '수요기관명'.
# But if it has different codes/biz numbers, we can just aggregate them or take the first non-empty.

def merge_infos(group):
    # take first non-empty code and biznum
    codes = [x for x in group['수요기관코드'] if x]
    biznums = [x for x in group['사업자등록번호'] if x]
    return pd.Series({
        '수요기관코드': codes[0] if codes else '',
        '사업자등록번호': biznums[0] if biznums else ''
    })

merged_df = df_all.groupby('수요기관명').apply(merge_infos).reset_index()

# Sort by name
merged_df = merged_df.sort_values('수요기관명')

out_path = os.path.join(target_dir, '부산_수요기관_통합목록.xlsx')
merged_df.to_excel(out_path, index=False)
print(f"Saved merged Excel file to {out_path} with {len(merged_df)} agencies.")
