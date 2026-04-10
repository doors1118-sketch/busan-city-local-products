#!/usr/bin/env python3
"""DB 백업 → NCP Object Storage (7일 보관, 자동 삭제)
파이프라인 완료 후 자동 실행.
"""
import os, sys, gzip, shutil, subprocess
from datetime import datetime, timedelta

# ── 설정 ──
DB_DIR = '/opt/busan'
BACKUP_DIR = '/opt/busan/backups'
DB_FILES = ['procurement_contracts.db', 'busan_agencies_master.db', 'servc_site.db']
RETENTION_DAYS = 7

# NCP Object Storage (S3 호환) — 환경변수에서 읽음
NCP_ENDPOINT = 'https://kr.object.ncloudstorage.com'
NCP_ACCESS_KEY = os.environ.get('NCP_ACCESS_KEY', '')
NCP_SECRET_KEY = os.environ.get('NCP_SECRET_KEY', '')
BUCKET_NAME = 'busan-procurement-backup'


def ensure_bucket():
    """버킷이 없으면 생성"""
    import boto3
    s3 = boto3.client('s3',
        endpoint_url=NCP_ENDPOINT,
        aws_access_key_id=NCP_ACCESS_KEY,
        aws_secret_access_key=NCP_SECRET_KEY)
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        print(f"  ✅ 버킷 '{BUCKET_NAME}' 확인")
    except:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"  ✅ 버킷 '{BUCKET_NAME}' 생성 완료")
    return s3


def backup_and_upload():
    print(f"\n{'='*50}")
    print(f" 📦 DB 백업 시작 ({datetime.now().strftime('%Y-%m-%d %H:%M')})")
    print(f"{'='*50}\n")
    
    os.makedirs(BACKUP_DIR, exist_ok=True)
    today_str = datetime.now().strftime('%Y%m%d')
    
    # 1. DB 파일 gzip 압축
    backup_files = []
    for db_name in DB_FILES:
        src = os.path.join(DB_DIR, db_name)
        if not os.path.exists(src):
            print(f"  ⚠️ {db_name} 파일 없음 — 건너뜀")
            continue
        
        dst = os.path.join(BACKUP_DIR, f"{db_name}.{today_str}.gz")
        size_mb = os.path.getsize(src) / 1024 / 1024
        print(f"  압축 중: {db_name} ({size_mb:.1f}MB) → {os.path.basename(dst)}")
        
        with open(src, 'rb') as f_in:
            with gzip.open(dst, 'wb', compresslevel=6) as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        gz_size = os.path.getsize(dst) / 1024 / 1024
        ratio = round(gz_size / size_mb * 100, 1) if size_mb > 0 else 0
        print(f"    → {gz_size:.1f}MB (압축률 {ratio}%)")
        backup_files.append(dst)
    
    if not backup_files:
        print("  ❌ 백업할 파일 없음")
        return False
    
    # 2. NCP Object Storage 업로드
    print(f"\n  [Object Storage 업로드]")
    s3 = None
    try:
        s3 = ensure_bucket()
        for fpath in backup_files:
            fname = os.path.basename(fpath)
            s3_key = f"daily/{fname}"
            fsize = os.path.getsize(fpath) / 1024 / 1024
            print(f"    업로드: {fname} ({fsize:.1f}MB) → s3://{BUCKET_NAME}/{s3_key}")
            s3.upload_file(fpath, BUCKET_NAME, s3_key)
        print(f"  ✅ 업로드 완료 ({len(backup_files)}개 파일)")
    except Exception as e:
        print(f"  ⚠️ 업로드 실패: {e}")
        print(f"  → 로컬 백업은 유지됩니다.")
    
    # 3. 오래된 로컬 백업 삭제 (7일 초과)
    print(f"\n  [로컬 정리] {RETENTION_DAYS}일 초과 백업 삭제")
    cutoff = datetime.now() - timedelta(days=RETENTION_DAYS)
    deleted = 0
    for f in os.listdir(BACKUP_DIR):
        fpath = os.path.join(BACKUP_DIR, f)
        if os.path.isfile(fpath) and f.endswith('.gz'):
            mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
            if mtime < cutoff:
                os.remove(fpath)
                print(f"    삭제: {f} (생성: {mtime.strftime('%Y-%m-%d')})")
                deleted += 1
    print(f"  정리 완료: {deleted}개 삭제, {len(os.listdir(BACKUP_DIR))}개 보관 중")
    
    # 4. 오래된 Object Storage 백업 삭제 (7일 초과)
    if s3 is not None:
        print(f"\n  [Object Storage 정리] {RETENTION_DAYS}일 초과 백업 삭제")
        try:
            objs = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix='daily/')
            s3_deleted = 0
            for obj in objs.get('Contents', []):
                key = obj['Key']
                last_mod = obj['LastModified'].replace(tzinfo=None)
                if last_mod < cutoff:
                    s3.delete_object(Bucket=BUCKET_NAME, Key=key)
                    print(f"    삭제: {key}")
                    s3_deleted += 1
            print(f"  정리 완료: {s3_deleted}개 삭제")
        except Exception as e:
            print(f"  ⚠️ Object Storage 정리 실패: {e}")
    else:
        print(f"\n  [Object Storage 정리] 통과 (S3 연결 안 됨)")
    
    print(f"\n{'='*50}")
    print(f" ✅ DB 백업 완료!")
    print(f"{'='*50}\n")
    return True


if __name__ == '__main__':
    backup_and_upload()
