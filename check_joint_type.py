import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

df = pd.read_excel('API_공사계약조회_전체(20260101_20260131).xlsx')

print('--- cmmnCntrctYn (공동계약여부) Values ---')
print(df['cmmnCntrctYn'].value_counts(dropna=False))

# Looking for terms like '분담', '주계약' in contract method or name
mask_bndam = df.apply(lambda row: row.astype(str).str.contains('분담').any(), axis=1)
mask_jugeyak = df.apply(lambda row: row.astype(str).str.contains('주계약').any(), axis=1)

print(f'\n분담이행 검색 건수: {mask_bndam.sum()}')
print(f'주계약자 방식 검색 건수: {mask_jugeyak.sum()}')

if mask_bndam.sum() > 0:
    print('\n[분담이행 Sample]')
    sample = df[mask_bndam].iloc[0]
    print(f"Contract: {sample['cnstwkNm']}")
    print(f"Total Amount: {sample['totCntrctAmt']} / {sample['thtmCntrctAmt']}")
    print(f"corpList: {sample['corpList']}")

if mask_jugeyak.sum() > 0:
    print('\n[주계약자 Sample]')
    sample = df[mask_jugeyak].iloc[0]
    print(f"Contract: {sample['cnstwkNm']}")
    print(f"Total Amount: {sample['totCntrctAmt']} / {sample['thtmCntrctAmt']}")
    print(f"corpList: {sample['corpList']}")
