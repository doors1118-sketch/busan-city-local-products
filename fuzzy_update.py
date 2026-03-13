import os
import pandas as pd
import re
import warnings
warnings.filterwarnings('ignore')

target_dir = r"c:\Users\COMTREE\Desktop\연습"

master_file = os.path.join(target_dir, "부산광역시 조달 수요기관 마스터파일_최종본.csv")
merged_file = os.path.join(target_dir, "부산_수요기관_통합목록.xlsx")

# 1. Load data
try:
    df_master = pd.read_csv(master_file, encoding='utf-8', dtype=str)
except Exception:
    df_master = pd.read_csv(master_file, encoding='cp949', dtype=str)

df_merged = pd.read_excel(merged_file, dtype=str)

df_master = df_master.fillna('')
df_merged = df_merged.fillna('')

def normalize_name(name):
    original = str(name).strip()
    if not original:
        return ''
        
    s = original
    # 1. 공백 제거
    s = re.sub(r'\s+', '', s)
    
    # 2. 법인 형태 제거/통일
    markers = ['재단법인', '사단법인', '주식회사', '의료법인', '학교법인', '사회복지법인', 
               '(재)', '(사)', '(주)', '(의)', '(학)', '(복)']
    
    # 순서 보장을 위해 긴 것부터 대체하는 것도 방법임
    # 하지만 위 리스트 순서면 안전
    for m in markers:
        s = s.replace(m, '')
        
    # 특수문자 제거(괄호, 등)
    s = s.replace('(', '').replace(')', '')
    
    # 3. 부산광역시, 부산시 생략 처리
    # 기관명 전체가 "부산광역시"인 등 예외 방지
    if s == '부산광역시' or s == '부산시':
        return s
        
    if s.startswith('부산광역시'):
        s = s.replace('부산광역시', '', 1)
    elif s.startswith('부산시'):
        s = s.replace('부산시', '', 1)
        
    # '구청' -> '구' 변환 (가끔 사상구청 vs 사상구 로 쓰일 수 있음)
    if s.endswith('구청'):
        s = s[:-1] # '구'로 만듦
        
    return s

# 2. Build lookup dictionary with normalized names
merged_dict = {}
for idx, row in df_merged.iterrows():
    name = str(row['수요기관명']).strip()
    norm_name = normalize_name(name)
    code = str(row['수요기관코드']).strip()
    biz = str(row['사업자등록번호']).strip()
    
    if norm_name not in merged_dict:
        merged_dict[norm_name] = {'code': code, 'biz': biz, 'original': name}
    else:
        # 빈 값 보완
        if not merged_dict[norm_name]['code'] and code:
            merged_dict[norm_name]['code'] = code
        if not merged_dict[norm_name]['biz'] and biz:
            merged_dict[norm_name]['biz'] = biz

stats = {
    'updated': 0,
    'unchanged': 0
}

# 3. 매칭 및 갱신 진행
# 마스터파일의 "코드" 혹은 "사업자등록번호"가 비어있는 경우에 대해서만 유사 매칭 수행
for idx, row in df_master.iterrows():
    m_name = str(row['수요기관명']).strip()
    m_code = str(row['수요기관코드']).strip()
    m_biz = str(row['수요기관사업자등록번호']).strip()
    
    # 둘 중 하나라도 비어있는지 확인
    if not m_code or not m_biz:
        norm_name = normalize_name(m_name)
        
        if norm_name in merged_dict:
            t_code = merged_dict[norm_name]['code']
            t_biz = merged_dict[norm_name]['biz']
            
            changed = False
            if not m_code and t_code:
                df_master.at[idx, '수요기관코드'] = t_code
                m_code = t_code
                changed = True
            if not m_biz and t_biz:
                df_master.at[idx, '수요기관사업자등록번호'] = t_biz
                m_biz = t_biz
                changed = True
                
            if changed:
                stats['updated'] += 1
                # print(f"Match: '{m_name}' updated from '{merged_dict[norm_name]['original']}' (Code: {t_code}, Biz: {t_biz})")
            else:
                stats['unchanged'] += 1
        else:
            stats['unchanged'] += 1
    else:
        stats['unchanged'] += 1

# 4. Save the final updated file
output_file = os.path.join(target_dir, "부산광역시 조달 수요기관 마스터파일_최종본_수정본.csv")
df_master.to_csv(output_file, index=False, encoding='utf-8-sig')

# 기존 파일명에 덮어쓰기 위해서 os rename 사용할 수 있으나 안정성을 위해 "마스터파일_최종본.csv" 자체를 덮어씀
df_master.to_csv(master_file, index=False, encoding='utf-8-sig')

print(f"Fuzzy Match Update Statistics:")
print(f" - Missing records updated with similar names: {stats['updated']}")
print(f" - Records left as is: {stats['unchanged']}")
print(f"Total rows in master file: {len(df_master)}")
print(f"Saved to: {master_file}")
