import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'procurement_contracts.db'

def upgrade_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. 컬럼 추가
    try:
        cursor.execute('ALTER TABLE bid_notices_raw ADD COLUMN type TEXT')
    except sqlite3.OperationalError:
        pass # 이미 존재함
        
    try:
        cursor.execute('ALTER TABLE bid_notices_raw ADD COLUMN rgnLmtInfo TEXT')
    except sqlite3.OperationalError:
        pass # 이미 존재함
        
    # 기존 데이터는 Cnstwk로 업데이트
    cursor.execute("UPDATE bid_notices_raw SET type = 'Cnstwk' WHERE type IS NULL")
    
    conn.commit()
    conn.close()
    print("✅ bid_notices_raw 테이블 업그레이드 완료 (type, rgnLmtInfo 추가됨)")

if __name__ == '__main__':
    upgrade_db()
