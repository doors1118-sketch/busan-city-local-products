"""
지역업체 보호제도 기준 공통 모듈.

보호제도 계산, 경보, 대시보드 안내, 유출계약 비고가 같은 법령 기준을
참조하도록 기준액과 날짜 분기를 한 곳에서 관리한다.
"""

PROTECTION_GROUPS = {'부산광역시 및 소속기관', '정부 및 국가공공기관'}

PUB_NEW_DATE = '2026-04-21'
BUSAN_NEW_DATE = '2026-04-24'

GOV_THRESHOLDS = {'종합공사': 88e8, '전문공사': 10e8, '용역': 2.2e8}
PUB_THRESHOLDS_OLD = {'종합공사': 88e8, '전문공사': 10e8, '용역': 2.2e8}
PUB_THRESHOLDS_NEW = {'종합공사': 150e8, '전문공사': 10e8, '용역': 2.2e8}
BUSAN_THRESHOLDS_OLD = {'종합공사': 100e8, '전문공사': 10e8, '용역': 3.3e8}
BUSAN_THRESHOLDS_NEW = {'종합공사': 150e8, '전문공사': 10e8, '용역': 3.3e8}

SPECIALTY_CONSTRUCTION_KEYWORDS = [
    '전기공사', '정보통신공사', '소방시설공사', '기계설비공사',
    '전기', '통신', '소방', '기계설비', '기계공사', '정보통신',
    '조경', '실내건축', '철근·콘크리트', '철근ㆍ콘크리트',
    '상하수도', '포장', '철강구조물', '금속구조물창호',
    '도장', '습식방수', '석공사', '비계', '지반조성', '철도궤도',
    '기계설비ㆍ가스',
]


def _date_key(value):
    """YYYY-MM-DD, YYYYMMDD, datetime-like 값을 문자열 비교 가능한 날짜로 정규화."""
    raw = str(value or '').strip()
    if len(raw) >= 10 and raw[4] == '-' and raw[7] == '-':
        return raw[:10]
    digits = ''.join(ch for ch in raw if ch.isdigit())
    if len(digits) >= 8:
        return f'{digits[:4]}-{digits[4:6]}-{digits[6:8]}'
    return ''


def normalize_protection_subtype(sector, name='', main_type='', type_lrg='', type_dtl=''):
    """보호제도 기준액 조회용 세부 구분을 반환한다."""
    if sector == '용역':
        return '용역'
    if sector != '공사':
        return None

    type_lrg = str(type_lrg or '').strip()
    type_dtl = str(type_dtl or '').strip()
    main_type = str(main_type or '').strip()
    name = str(name or '').strip()

    if type_lrg.startswith('전문공사') or type_lrg == '시설물유지관리공사':
        return '전문공사'
    if type_lrg.startswith('종합공사'):
        return '종합공사'

    combined = ' '.join([main_type, type_lrg, type_dtl, name])
    if any(keyword in combined for keyword in SPECIALTY_CONSTRUCTION_KEYWORDS):
        return '전문공사'
    return '종합공사'


def get_protection_threshold(group, mid, subtype, date_value):
    """기관군/기관중분류/계약구분/기준일에 따른 보호제도 기준액을 반환한다."""
    dt = _date_key(date_value)
    if group == '정부 및 국가공공기관':
        if mid == '국가공공기관':
            table = PUB_THRESHOLDS_NEW if dt >= PUB_NEW_DATE else PUB_THRESHOLDS_OLD
        else:
            table = GOV_THRESHOLDS
    elif group == '부산광역시 및 소속기관':
        table = BUSAN_THRESHOLDS_NEW if dt >= BUSAN_NEW_DATE else BUSAN_THRESHOLDS_OLD
    else:
        return None
    return table.get(subtype)


def get_protection_law_name(group, mid):
    if group == '부산광역시 및 소속기관':
        return '지방계약법'
    if group == '정부 및 국가공공기관' and mid == '국가공공기관':
        return '공기업·준정부기관 계약사무규칙'
    if group == '정부 및 국가공공기관':
        return '국가계약법'
    return ''


def protection_law_basis_for_cache():
    """API 캐시에 싣는 대시보드용 기준 메타데이터."""
    return {
        '국가계약법': {
            '종합공사': GOV_THRESHOLDS['종합공사'] / 1e8,
            '전문공사': GOV_THRESHOLDS['전문공사'] / 1e8,
            '용역': GOV_THRESHOLDS['용역'] / 1e8,
            '대상': '정부기관(중앙행정기관, 국립대학)',
        },
        '공기업·준정부기관 계약사무규칙': {
            '종합공사': PUB_THRESHOLDS_NEW['종합공사'] / 1e8,
            '종합공사_구': PUB_THRESHOLDS_OLD['종합공사'] / 1e8,
            '전문공사': PUB_THRESHOLDS_NEW['전문공사'] / 1e8,
            '용역': PUB_THRESHOLDS_NEW['용역'] / 1e8,
            '대상': '국가공공기관(공기업, 준정부기관 등)',
            '변경일': PUB_NEW_DATE,
        },
        '지방계약법': {
            '종합공사': BUSAN_THRESHOLDS_NEW['종합공사'] / 1e8,
            '종합공사_구': BUSAN_THRESHOLDS_OLD['종합공사'] / 1e8,
            '전문공사': BUSAN_THRESHOLDS_NEW['전문공사'] / 1e8,
            '용역': BUSAN_THRESHOLDS_NEW['용역'] / 1e8,
            '대상': '부산광역시 및 소속기관',
            '변경일': BUSAN_NEW_DATE,
        },
    }
